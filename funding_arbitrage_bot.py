"""
跨交易所（Binance + Gate）资金费率套利机器人。

策略：纯合约多空对冲，在两所各开方向相反的仓位，价格波动完全抵消，
      结算时赚取两所资金费率差额。

运行环境:
    Python 3.10+
    pip install ccxt pandas aiohttp requests

架构:
    - 3 条 WebSocket 长连接：BN 热下单 + Gate 热下单&资费监控 + BN 资费监控（User Data Stream）
    - REST 轮询扫描：每 60s 全量扫描两所费率，过滤流动性不足的币种
    - 狙击机制：结算前 1s 通过 WS 并发下单开仓，平仓后记录盈亏到看板

重要提示:
    1. 默认连接 Binance 实盘。测试请用 --testnet 或在 config.py 中设置 TESTNET=True。
    2. 资金费率、盘口深度、滑点、借贷成本、合约保证金模式都会影响真实收益。
    3. 跨所套利需要两个交易所都有足够的 USDT 余额。
"""

from __future__ import annotations

import asyncio
import ctypes
import gc
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import resource
import time
import urllib.request
import requests
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

# aiohttp 代理 — 自动探测：本地有代理则用，服务器直连
_LOCAL_PROXY = "http://127.0.0.1:7892"
import socket
_has_proxy = False
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    s.connect(("127.0.0.1", 7892))
    s.close()
    _has_proxy = True
except (socket.error, OSError):
    pass

if _has_proxy:
    os.environ.setdefault("HTTP_PROXY", _LOCAL_PROXY)
    os.environ.setdefault("HTTPS_PROXY", _LOCAL_PROXY)
    HTTP_PROXY = _LOCAL_PROXY
else:
    HTTP_PROXY = None  # 服务器直连

import aiohttp
import ccxt.async_support as ccxt_async
import pandas as pd
from ccxt import BaseError, ExchangeError


# =========================
# 账户与策略配置
# =========================

try:
    from config import (  # type: ignore[import-not-found]
        BINANCE_API_KEY, BINANCE_API_SECRET, NOTIFY_EMAIL, NOTIFY_EMAIL_AUTH,
        GATE_API_KEY, GATE_API_SECRET,
    )
except ImportError:
    # 部署到服务器时确保 config.py 在同目录。本地没有则用空值占位。
    BINANCE_API_KEY = ""
    BINANCE_API_SECRET = ""
    NOTIFY_EMAIL = ""
    NOTIFY_EMAIL_AUTH = ""
    GATE_API_KEY = ""
    GATE_API_SECRET = ""
    raise SystemExit("缺少 config.py！复制 config.example.py → config.py 并填入真实信息。")

USE_TESTNET = False

# 仓位大小按账户可用余额动态计算。min(spot可用USDT, futures可用USDT) × 该比例。
# 90% = 留 10% 给手续费和滑点。无杠杆时总占用 ≈ 单腿名义 × 2。
POSITION_SIZE_RATIO = 0.90

# 开仓前自动划转：两账户 USDT 差异超过此比例时，从多的一侧转一半差额到少的一侧。
REBALANCE_THRESHOLD = 0.05  # 5% 差异触发划转

# 单期净收益阈值：0.02% = 0.0002。
MIN_NET_RATE = 0.0002
USE_ROUND_TRIP_FEE_FOR_ENTRY = True

# 反向套利（负费率）：借币卖出 + 做多合约，收取空头付给多头的费率。
# 借币成本按 1 小时计算（快进快出，持有 < 1h）。实际借币每小时结算一次。
REVERSE_ENABLED = True           # 反向策略总开关：--scan 时是否同时扫描负费率机会
REVERSE_MIN_NET_RATE = 0.0002    # 反向净收益阈值（扣除借币利息后）
REVERSE_RATE_BUFFER = 0.0003     # 费率波动缓冲 0.03%，净收益需超出阈值至少此值才开仓
PRE_BORROW_SIGMA = 2.0          # 预借触发: 费率偏离中位数多少个标准差
PRE_BORROW_MIN_RATE = -0.0002   # 预借触发: 费率必须已经为负 (-0.02%)
PRE_BORROW_TIMEOUT_MINUTES = 30 # 预借超时: 超过此时间未到开仓线则归还
MIN_SWITCH_SPREAD = 0.0001     # 换仓最小利差 0.01%，差额不够不换，避免摩擦损耗
BORROW_POOL_EMPTY_COOLDOWN = 600  # 借币池空后 10 分钟不再尝试该币
MARGIN_LEVEL_MIN = 1.3       # 逐仓保证金率最低阈值，币价涨62%触发，距强平18%缓冲
FUTURES_LIQ_DISTANCE_MIN = 0.30  # 合约空单距强平最小距离30%，币价涨60%+触发

# 流动性过滤不宜过严，否则会错过资金费率机会。
# 默认 hybrid: 合约侧至少 1500 万 USDT，现货侧至少 100 万 USDT。
LIQUIDITY_MODE = "hybrid"  # options: futures_only, both_legs, hybrid
MIN_FUTURES_24H_QUOTE_VOLUME = 5_000_000
MIN_SPOT_24H_QUOTE_VOLUME = 1_000_000

# 扫描/开仓: 无持仓时每隔 SCAN_INTERVAL_MINUTES 分钟全市场扫描。
# 只在实际结算前 ENTRY_WINDOW_MINUTES 分钟内才开仓，过早开的费率不可靠。
# 平仓: 开仓时记录合约的 nextFundingTime，结算后 +1 分钟平仓。
LOCAL_TIMEZONE = "Asia/Shanghai"
SCAN_INTERVAL_MINUTES = 0.5 # 无持仓时多久扫一次 (30s，配合60s窗口确保不漏)
ENTRY_WINDOW_MINUTES = 2    # 距结算前多少分钟才允许开仓 (留足扫描+下单时间)
DEFAULT_FUNDING_INTERVAL_HOURS = 8  # 获取结算时间失败时的兜底估算

# 若无法通过接口获取手续费，则使用 Binance VIP 0 标准吃单费率。
DEFAULT_SPOT_TAKER_FEE = 0.001
DEFAULT_FUTURES_TAKER_FEE = 0.0005
# Binance Alpha: 0% 平台手续费 (2025-11 起永久)，仅需链上 gas。
# BSC ~$0.03/tx，以 100 USDT 计 ≈ 0.03%。其他链: Sui ~$0.01, Solana ~$0.005。
ALPHA_GAS_USD_PER_TX = 0.03  # fallback default
ALPHA_CHAIN_GAS_USD: dict[str, float] = {
    "BSC": 0.03,
    "Solana": 0.005,
    "Base": 0.02,
    "Sui": 0.01,
    "Arbitrum": 0.02,
    "Sonic": 0.01,
    "Linea": 0.02,
}
SKIP_ALPHA_CHAINS = {"Ethereum", "TRON"}  # gas too expensive for small positions
MAX_ALPHA_GAS_MULTIPLIER = 3.0  # skip Alpha trade if actual gas > 3x expected

# Public RPCs for on-chain gas queries (used by Alpha gas monitor)
CHAIN_RPC_URL: dict[str, str] = {
    "BSC": "https://bsc-dataseed.binance.org/",
    "Base": "https://mainnet.base.org",
    "Arbitrum": "https://arb1.arbitrum.io/rpc",
    "Linea": "https://rpc.linea.build",
    "Sonic": "https://rpc.soniclabs.com",
    "Sui": "https://fullnode.mainnet.sui.io:443",
}
# EVM chains use eth_gasPrice (gas * gasPrice / 1e18 * ETH_price).
# Sui uses sui_getReferenceGasPrice. Solana gas is fixed, no RPC needed.
EVM_CHAINS = {"BSC", "Base", "Arbitrum", "Linea", "Sonic"}
USE_BNB_FEE_DISCOUNT = True
SPOT_BNB_FEE_DISCOUNT = 0.25
FUTURES_BNB_FEE_DISCOUNT = 0.10
MIN_BNB_BALANCE_FOR_FEES = 0.001

# QQ 邮箱通知 → 配置见 config.py
STATE_FILE = Path("data/funding_arbitrage_state.json")
GATE_STATE_FILE = Path("data/gate_arbitrage_state.json")
MARGIN_STATE_FILE = Path("data/margin_state.json")
CROSS_STATE_FILE = Path("data/cross_arbitrage_state.json")

# Binance 官方 REST API 基础地址（跳过 ccxt 直连）
BINANCE_SPOT_API = "https://api.binance.com"
BINANCE_FUTURES_API = "https://fapi.binance.com"

# Binance WebSocket 订阅（实时费率推送，不消耗 API 权重）。
WS_FUTURES_URL = "wss://fstream.binance.com/ws/!markPrice@arr"
WS_FUTURES_TESTNET_URL = "wss://testnet.binancefuture.com/ws/!markPrice@arr"

# ── Gate.io ──
GATE_SPOT_API = "https://api.gateio.ws/api/v4"
GATE_FUTURES_API = "https://api.gateio.ws/api/v4"
DEFAULT_GATE_SPOT_TAKER_FEE = 0.001      # Gate VIP0 现货 taker 基础费率 0.10%
DEFAULT_GATE_FUTURES_TAKER_FEE = 0.0005  # Gate 永续 taker 基础费率 0.05%
GATE_SPOT_REBATE = 0.55                  # 现货返佣 55% → 有效费率 = 基础 × 0.45
GATE_FUTURES_REBATE = 0.64               # 合约返佣 64% → 有效费率 = 基础 × 0.36
GATE_TRADING_ENABLED = True              # Gate.io 实盘交易总开关，--no-gate 可关闭

# ── 跨交易所资金费率套利 ──
CROSS_EXCHANGE_ENABLED = True            # 跨所套利开关（默认开，--cross 强制开）
CROSS_SHOW_SINGLE = True                # 跨所模式下同时展示单交易所套利机会（仅扫描，不下单）
CROSS_POSITION_SIZE_RATIO = 0.98        # 跨所资金利用率（纯期货对冲，价格风险中性，可更高）
CROSS_SNIPER_SCAN_OFFSET_SEC = 10         # 结算前N秒扫描（获取最新费率）
CROSS_SNIPER_OPEN_OFFSET_MS = 1000         # 结算前N毫秒发出开仓（预留足够网络延迟）
CROSS_LEVERAGE = 1                        # 跨所套利用的合约杠杆倍数
CROSS_SNIPE_WINDOW_SEC = 60              # 距结算≤N秒时跳过单所扫描，直接狙击
CROSS_MIN_RATE_SPREAD = 0.0003          # 最小原始费率差 0.03%
CROSS_MIN_NET_RATE = 0.0001             # 最小净收益率 0.01%（扣双边手续费后）

# 手续费折扣/返佣系数（实际手续费 = 标称 × BNB抵扣 × 返佣）
BN_FEE_DISCOUNT_FACTOR = 0.9   # BNB抵扣合约手续费10%
BN_FEE_REBATE_FACTOR = 1.0     # 返佣，1.0=无，0.8=返20%
GT_FEE_DISCOUNT_FACTOR = 1.0   # Gate无BNB抵扣
GT_FEE_REBATE_FACTOR = 0.36    # 返佣64%，实付36%

CROSS_LIQ_DISTANCE_MIN = 0.10            # 跨所强平距离阈值 10%（1x杠杆几乎不可能触发）

# Binance Alpha (pre-listing spot) API.
ALPHA_API_BASE = "https://www.binance.com"
ALPHA_TOKEN_LIST_PATH = "/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"
ALPHA_TICKER_PATH = "/bapi/defi/v1/public/alpha-trade/ticker"
ALPHA_EXCHANGE_INFO_PATH = "/bapi/defi/v1/public/alpha-trade/get-exchange-info"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.handlers.TimedRotatingFileHandler(
            LOG_DIR / "bot.log", encoding="utf-8",
            when="midnight", backupCount=7,
        ),
    ],
)
logger = logging.getLogger("funding-arbitrage-bot")


@dataclass
class ArbitrageState:
    """本地持仓状态，用于程序重启后的持仓识别。"""

    is_open: bool = False
    spot_symbol: str | None = None
    futures_symbol: str | None = None
    base: str | None = None
    amount: float = 0.0
    spot_order_id: str | None = None
    futures_order_id: str | None = None
    entry_price: float = 0.0
    predicted_funding_rate: float = 0.0
    net_rate: float = 0.0
    opened_at: str | None = None
    spot_source: str = "spot"  # "spot", "alpha", or "margin"
    next_funding_time_ms: float = 0  # 合约下次结算时间戳(毫秒)
    direction: str = "forward"  # "forward" or "reverse"
    exchange: str = "binance"  # "binance" or "gate"
    locked: bool = True  # True=刚开仓锁定期, False=自由人模式(随时可换仓)
    pre_borrow_base: str = ""  # 预借中的币种 (空=无预借)
    pre_borrow_margin_symbol: str = ""  # 预借的逐仓交易对
    pre_borrow_amount: float = 0.0  # 预借数量
    pre_borrow_at: str = ""  # 预借时间 ISO


@dataclass
class LegResult:
    """单条腿下单结果。"""

    ok: bool
    market_type: str
    symbol: str
    side: str
    amount: float
    order: dict[str, Any] | None = None
    error: str | None = None


@dataclass
class CrossArbitrageState:
    """跨交易所资金费率套利持仓。纯期货对冲：高费率所做空 + 低费率所做多。"""

    is_open: bool = False
    base: str | None = None
    amount: float = 0.0            # 名义金额（向后兼容显示用）

    short_amount: float = 0.0      # 空单实际下单张数（不同交易所 contractSize 不同）
    long_amount: float = 0.0       # 多单实际下单张数

    short_exchange: str = "binance"  # 做空交易所（费率更高）
    long_exchange: str = "gate"      # 做多交易所（费率更低）

    short_symbol: str | None = None
    long_symbol: str | None = None

    short_order_id: str | None = None
    long_order_id: str | None = None

    short_entry_price: float = 0.0
    long_entry_price: float = 0.0

    short_rate: float = 0.0        # 空单侧预测费率
    long_rate: float = 0.0         # 多单侧预测费率
    rate_spread: float = 0.0       # 原始费率差
    total_net_rate: float = 0.0    # 综合净收益率（扣双边手续费后）

    opened_at: str | None = None
    short_next_funding_time_ms: float = 0
    long_next_funding_time_ms: float = 0
    short_locked: bool = True
    long_locked: bool = True


class FundingArbitrageBot:
    """现货买入 + 永续开空的资金费率套利机器人。"""

    def __init__(self) -> None:
        self.tz = ZoneInfo(LOCAL_TIMEZONE)
        self.binance_state = self._load_state("binance")
        self.gate_state = self._load_state("gate")
        self.margin_state = self._load_margin_state()
        self.cross_state = self._load_cross_state()
        self._next_snipe_settle_ms = 0
        self.spot = self._create_spot_exchange()
        self.futures = self._create_futures_exchange()
        self.alpha_spot = self._create_alpha_spot_exchange()
        self.gate_spot = self._create_gate_spot_exchange()
        self.gate_futures = self._create_gate_futures_exchange()
        self._alpha_tokens: dict[str, dict[str, Any]] = {}
        self._alpha_last_fetch: float = 0.0
        self._rest_session = requests.Session()  # Binance 直连 REST 持久连接池
        self._scan_lock = asyncio.Lock()  # 防止预借和主扫描冲突
        self._borrow_blacklist: dict[str, float] = {}  # base → 过期时间戳, 借币池空后暂避
        self._fee_cache: dict[str, tuple[float, Any]] = {}  # key → (ts, result), 避免同轮重复查询
        self._gate_margin_bases: list[str] = []  # Gate 可借贷币种列表缓存
        self._gate_margin_bases_ts: float = 0.0
        self._leverage_set: set[tuple[str, str, int]] = set()  # 已设杠杆缓存 (exchange, symbol, leverage)
        self._fee_lock = asyncio.Lock()
        self._bn_listen_key: str | None = None
        self._bn_listen_key_ts: float = 0.0
        self._funding_event = asyncio.Event()
        self._funding_session: aiohttp.ClientSession | None = None
        self._funding_ws: Any = None
        self._gate_funding_event = asyncio.Event()
        self._gate_funding_ws: Any = None
        self._gate_funding_session: aiohttp.ClientSession | None = None
        self._gate_funding_symbol: str = ""
        self._gate_funding_baseline: float = 0.0
        self._gate_funding_amount: float = 0.0  # WS 检测到的实际资费
        # BN + Gate 交易 WS（持久连接，下单省 HTTP 握手 + to_thread 开销）
        self._bn_trade_ws: Any = None
        self._bn_trade_session: aiohttp.ClientSession | None = None
        self._bn_trade_futures: dict[str, asyncio.Future] = {}
        self._gate_trade_ws: Any = None
        self._gate_trade_session: aiohttp.ClientSession | None = None
        self._gate_trade_futures: dict[str, asyncio.Future] = {}
        # WS 持仓缓存（开平仓验证用，免 REST 网络往返）
        self._bn_ws_positions: dict[str, float] = {}  # "SYMBOL|SHORT" → abs(positionAmt)
        self._gate_ws_positions: dict[str, float] = {}  # "CONTRACT" → abs(size)

    @staticmethod
    def _ccxt_config(options: dict | None = None) -> dict[str, Any]:
        """构建 ccxt 配置，自动判断是否启用代理。"""
        cfg: dict[str, Any] = {
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "timeout": 30000,  # 30s, ccxt 默认 10s 有时不够
        }
        if HTTP_PROXY:
            cfg["aiohttp_proxy"] = HTTP_PROXY
            cfg["proxies"] = {"http": HTTP_PROXY, "https": HTTP_PROXY}
        if options:
            cfg.setdefault("options", {}).update(options)
        else:
            cfg.setdefault("options", {})
        return cfg

    @staticmethod
    def _create_spot_exchange() -> ccxt_async.binance:
        exchange = ccxt_async.binance(
            FundingArbitrageBot._ccxt_config(
                {"defaultType": "spot", "createMarketBuyOrderRequiresPrice": False}
            )
        )
        if USE_TESTNET:
            exchange.set_sandbox_mode(True)
        return exchange

    @staticmethod
    def _create_futures_exchange() -> ccxt_async.binanceusdm:
        exchange = ccxt_async.binanceusdm(
            FundingArbitrageBot._ccxt_config(
                {"defaultType": "future", "adjustForTimeDifference": True}
            )
        )
        if USE_TESTNET:
            exchange.set_sandbox_mode(True)
        return exchange

    @staticmethod
    def _create_alpha_spot_exchange() -> ccxt_async.binance:
        exchange = ccxt_async.binance(
            FundingArbitrageBot._ccxt_config(
                {"defaultType": "spot", "createMarketBuyOrderRequiresPrice": False}
            )
        )
        if USE_TESTNET:
            exchange.set_sandbox_mode(True)
        return exchange

    @staticmethod
    def _gate_ccxt_config() -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "apiKey": GATE_API_KEY,
            "secret": GATE_API_SECRET,
            "enableRateLimit": True,
            "timeout": 60000,
        }
        if HTTP_PROXY:
            cfg["aiohttp_proxy"] = HTTP_PROXY
            cfg["proxies"] = {"http": HTTP_PROXY, "https": HTTP_PROXY}
        return cfg

    @staticmethod
    def _create_gate_spot_exchange() -> ccxt_async.gate:
        return ccxt_async.gate(
            FundingArbitrageBot._gate_ccxt_config()
            | {"options": {"defaultType": "spot"}},
        )

    @staticmethod
    def _create_gate_futures_exchange() -> ccxt_async.gate:
        return ccxt_async.gate(
            FundingArbitrageBot._gate_ccxt_config()
            | {"options": {"defaultType": "swap"}},
        )

    async def _fetch_alpha_token_list(self) -> dict[str, dict[str, Any]]:
        """Fetch Binance Alpha token list. Returns dict: base -> {alpha_symbol, volume, price, chain, gas_usd}."""

        url = f"{ALPHA_API_BASE}{ALPHA_TOKEN_LIST_PATH}"
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            url,
                            headers={"User-Agent": "funding-arbitrage-bot/1.0"},
                        )
                    ).read()
                )
            )
        except Exception as exc:
            logger.warning("Failed to fetch Alpha token list: %s", exc)
            return {}

        tokens: dict[str, dict[str, Any]] = {}
        for item in response.get("data", []):
            symbol = item.get("symbol", "")
            if not symbol:
                continue
            chain = item.get("chainName", "")
            if chain in SKIP_ALPHA_CHAINS:
                continue
            alpha_id = item.get("alphaId", "")
            gas_usd = ALPHA_CHAIN_GAS_USD.get(chain, ALPHA_GAS_USD_PER_TX)
            tokens[symbol.upper()] = {
                "alpha_symbol": f"{alpha_id}USDT",
                "alpha_id": alpha_id,
                "quote_volume": float(item.get("volume24h") or 0),
                "price": float(item.get("price") or 0),
                "chain": chain,
                "gas_usd": gas_usd,
            }
        return tokens

    def _build_alpha_index(self, alpha_tokens: dict) -> dict[str, dict[str, Any]]:
        """Build base -> alpha_info index for fast lookup."""
        return {
            base: info
            for base, info in alpha_tokens.items()
        }

    def _fetch_alpha_ticker_sync(self, symbol: str) -> dict[str, Any]:
        """Sync fetch 24h ticker for an Alpha token."""

        url = f"{ALPHA_API_BASE}{ALPHA_TICKER_PATH}?symbol={symbol}"
        try:
            data = json.loads(
                urllib.request.urlopen(
                    urllib.request.Request(
                        url,
                        headers={"User-Agent": "funding-arbitrage-bot/1.0"},
                    )
                ).read()
            )
            item = data.get("data", {})
            if not item:
                return {}
            return {
                "last": float(item.get("lastPrice", 0)),
                "quoteVolume": float(item.get("quoteVolume", 0)),
            }
        except Exception:
            return {}

    async def _safe_request(
        self,
        name: str,
        request: Callable[[], Awaitable[Any]],
        default: Any = None,
        raise_error: bool = False,
    ) -> Any:
        """统一包裹所有 ccxt API 请求，便于日志和异常处理。"""
        try:
            return await request()
        except ExchangeError as exc:
            logger.error("交易所业务错误: %s | %s", name, exc)
            if raise_error:
                raise
            return default
        except BaseError as exc:
            logger.error("ccxt 请求错误: %s | %s: %s", name, type(exc).__name__, exc)
            if raise_error:
                raise
            return default

    async def initialize(self) -> None:
        """加载市场元数据，建立资费监听 WS 长连接。"""
        # 大块内存(>128KB)走 mmap，释放时直接归还 OS，不经过 glibc arena 囤积
        try:
            ctypes.CDLL("libc.so.6").mallopt(-8, 128 * 1024)  # M_MMAP_THRESHOLD
        except Exception:
            pass
        # 全部交易所 load_markets 加重试（VPS 时钟偏差/网络抖动可导致 InvalidNonce/超时）
        for name, ex, fatal in [("spot", self.spot, True), ("futures", self.futures, True),
                                ("gate_spot", self.gate_spot, False), ("gate_futures", self.gate_futures, False)]:
            for attempt in range(3):
                try:
                    await asyncio.wait_for(ex.load_markets(), timeout=30)
                    break
                except Exception as e:
                    if attempt < 2:
                        logger.warning("[初始化] %s load_markets 失败 (attempt %d/3): %s，3s 后重试...", name, attempt + 1, e)
                        await asyncio.sleep(3)
                    elif fatal:
                        raise
                    else:
                        logger.error("[初始化] %s load_markets 3 次均失败: %s", name, e)
        # 建立资费监听 + 交易 WS 长连接（Gate 资费监听复用交易 WS，不再单开连接）
        if CROSS_EXCHANGE_ENABLED:
            await asyncio.gather(
                self._ensure_bn_funding_ws(),
                self._ensure_bn_trade_ws(),
                self._ensure_gate_trade_ws(),
            )

    async def close(self) -> None:
        """关闭 ccxt 异步会话、REST 连接池、WS 长连接。"""
        self._rest_session.close()
        await asyncio.gather(
            self._safe_request("spot.close", lambda: self.spot.close()),
            self._safe_request("futures.close", lambda: self.futures.close()),
            self._safe_request("gate_spot.close", lambda: self.gate_spot.close()),
            self._safe_request("gate_futures.close", lambda: self.gate_futures.close()),
        )
        await self._close_bn_funding_ws()
        await self._close_bn_trade_ws()
        await self._close_gate_trade_ws()
        # 清理 aiohttp session
        for attr in ("_funding_session", "_gate_funding_session",
                      "_bn_trade_session", "_gate_trade_session"):
            sess = getattr(self, attr, None)
            if sess:
                try:
                    await sess.close()
                except Exception:
                    pass
        # 取消所有等待中的 WS 订单 futures
        for fut_dict_attr in ("_bn_trade_futures", "_gate_trade_futures"):
            for fut in getattr(self, fut_dict_attr, {}).values():
                if not fut.done():
                    fut.set_exception(Exception("bot shutting down"))

    @staticmethod
    def _load_state(exchange: str = "binance") -> ArbitrageState:
        file = GATE_STATE_FILE if exchange == "gate" else STATE_FILE
        if not file.exists():
            return ArbitrageState()

        try:
            with file.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return ArbitrageState(**payload)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            logger.warning("读取%s状态文件失败，将按无持仓启动: %s", exchange, exc)
            return ArbitrageState()

    def _save_state(self, exchange: str = "binance") -> None:
        state = self.gate_state if exchange == "gate" else self.binance_state
        file = GATE_STATE_FILE if exchange == "gate" else STATE_FILE
        with file.open("w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2, ensure_ascii=False)

    @staticmethod
    def _load_margin_state() -> dict:
        if not MARGIN_STATE_FILE.exists():
            return {"last_used": {}, "last_disabled": {}}
        try:
            with MARGIN_STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"last_used": {}, "last_disabled": {}}

    def _save_margin_state(self) -> None:
        with MARGIN_STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(self.margin_state, f, indent=2, ensure_ascii=False)

    def _load_cross_state(self) -> CrossArbitrageState:
        if not CROSS_STATE_FILE.exists():
            return CrossArbitrageState()
        try:
            with CROSS_STATE_FILE.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return CrossArbitrageState(**payload)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            logger.warning("读取跨交易所状态文件失败: %s", exc)
            return CrossArbitrageState()

    def _save_cross_state(self) -> None:
        with CROSS_STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(asdict(self.cross_state), f, indent=2, ensure_ascii=False)

    def _record_margin_used(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_used", {})[clean] = time.time()
        self._save_margin_state()

    def _record_margin_disabled(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_disabled", {})[clean] = time.time()
        self._save_margin_state()

    # ------------------------------------------------------------------
    # 交易历史 & 余额快照
    # ------------------------------------------------------------------

    TRADE_HISTORY_FILE: Path = Path("data/trade_history.json")
    BALANCE_SNAPSHOT_FILE: Path = Path("data/balance_snapshots.json")

    def _load_trade_history(self) -> list[dict[str, Any]]:
        if not self.TRADE_HISTORY_FILE.exists():
            return []
        try:
            return json.loads(self.TRADE_HISTORY_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

    def _record_close_trade(self, state: ArbitrageState) -> None:
        if not state.is_open:
            return
        profit = state.amount * state.entry_price * state.net_rate
        self._record_trade(
            state.base or "???", state.direction, profit, state.net_rate,
        )

    def _record_trade(self, coin: str, direction: str,
                      profit_usdt: float, net_rate: float) -> None:
        history = self._load_trade_history()
        history.append({
            "ts": datetime.now(tz=self.tz).isoformat(),
            "coin": coin,
            "direction": direction,
            "profit_usdt": round(profit_usdt, 6),
            "net_rate": round(net_rate, 6),
        })
        if len(history) > 365:
            history = history[-365:]
        self.TRADE_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.TRADE_HISTORY_FILE.write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    def _record_balance_snapshot(self, bn_total: float, gt_total: float) -> None:
        snaps = []
        if self.BALANCE_SNAPSHOT_FILE.exists():
            try:
                snaps = json.loads(self.BALANCE_SNAPSHOT_FILE.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        ts = datetime.now(tz=self.tz).isoformat()
        if snaps and snaps[-1].get("ts", "")[:16] == ts[:16]:
            snaps[-1] = {"ts": ts, "binance": round(bn_total, 2), "gate": round(gt_total, 2)}
        else:
            snaps.append({"ts": ts, "binance": round(bn_total, 2), "gate": round(gt_total, 2)})
        if len(snaps) > 60000:
            snaps = snaps[-60000:]
        self.BALANCE_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.BALANCE_SNAPSHOT_FILE.write_text(
            json.dumps(snaps, ensure_ascii=False), encoding="utf-8",
        )

    # ------------------------------------------------------------------
    # Binance 官方 REST API 直连 (不经过 ccxt)
    # ------------------------------------------------------------------

    @staticmethod
    def _binance_sign(query_string: str) -> str:
        """HMAC-SHA256 签名."""
        return hmac.new(
            BINANCE_API_SECRET.encode(), query_string.encode(), hashlib.sha256
        ).hexdigest()

    @staticmethod
    def _send_email(subject: str, body: str) -> None:
        """发送 QQ 邮箱通知（SMTP，放到 asyncio.to_thread 里调）."""
        if not NOTIFY_EMAIL or not NOTIFY_EMAIL_AUTH:
            return
        try:
            import smtplib
            from email.message import EmailMessage
            msg = EmailMessage()
            msg["From"] = NOTIFY_EMAIL
            msg["To"] = NOTIFY_EMAIL
            msg["Subject"] = subject
            msg.set_content(body)
            with smtplib.SMTP_SSL("smtp.qq.com", 465) as server:
                server.login(NOTIFY_EMAIL, NOTIFY_EMAIL_AUTH)
                server.send_message(msg)
        except Exception:
            pass  # 通知失败不影响交易

    async def _binance_request(
        self, base_url: str, path: str,
        params: dict | None = None, method: str = "GET",
    ) -> dict[str, Any]:
        """向币安官方 REST API 发签名请求，返回 JSON."""

        params = dict(params or {})
        params["timestamp"] = int(time.time() * 1000)
        params["recvWindow"] = 5000
        qs = urlencode(params)
        sig = self._binance_sign(qs)
        url = f"{base_url}{path}?{qs}&signature={sig}"

        def _do() -> dict[str, Any]:
            proxies = (
                {"http": HTTP_PROXY, "https": HTTP_PROXY} if HTTP_PROXY else None
            )
            resp = self._rest_session.request(
                method, url,
                headers={"X-MBX-APIKEY": BINANCE_API_KEY},
                proxies=proxies,
                timeout=15,
            )
            data = resp.json()
            if not resp.ok:
                raise Exception(f"HTTP {resp.status_code}: {data}")
            return data  # type: ignore[no-any-return]

        try:
            return await asyncio.to_thread(_do)
        except Exception as exc:
            msg = str(exc)
            if "HTTP 4" in msg:
                logger.warning("Binance 业务拒绝: %s %s — %s", method, path, exc)
            else:
                logger.error("Binance API 请求失败: %s %s — %s", method, path, exc)
            raise

    async def _gate_request(
        self, path: str, body: dict | None = None, method: str = "GET",
        timeout: int = 15, params: dict | None = None,
    ) -> dict[str, Any]:
        """向 Gate.io 官方 REST API 发签名请求，返回 JSON。
        Gate v4 签名：HMAC-SHA512(METHOD\nPATH\n\nBODY_SHA512\nTIMESTAMP)
        params 仅拼接到 URL（不参与签名），用于 leverage 等 query-string 参数。
        """
        from urllib.parse import urlencode
        ts = str(int(time.time()))
        body_str = json.dumps(body, separators=(",", ":")) if body else ""
        body_hash = hashlib.sha512(body_str.encode()).hexdigest()
        sign_path = f"/api/v4{path}"
        qs = urlencode(params) if params else ""
        message = f"{method}\n{sign_path}\n{qs}\n{body_hash}\n{ts}"
        sign = hmac.new(
            GATE_API_SECRET.encode(), message.encode(), hashlib.sha512,
        ).hexdigest()
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "KEY": GATE_API_KEY,
            "SIGN": sign,
            "Timestamp": ts,
        }
        url = f"{GATE_FUTURES_API}{path}"
        if params:
            url += "?" + urlencode(params)

        def _do() -> dict[str, Any]:
            resp = self._rest_session.request(
                method, url, headers=headers, data=body_str or None, timeout=timeout,
            )
            data = resp.json()
            if not resp.ok:
                raise Exception(f"HTTP {resp.status_code}: {data}")
            return data  # type: ignore[no-any-return]

        try:
            return await asyncio.to_thread(_do)
        except Exception as exc:
            msg = str(exc)
            if "HTTP 4" in msg:
                logger.warning("Gate 业务拒绝: %s %s — %s", method, path, exc)
            else:
                logger.error("Gate API 请求失败: %s %s — %s", method, path, exc)
            raise

    @staticmethod
    def _to_gate_contract(symbol: str) -> str:
        """BTC/USDT:USDT → BTC_USDT"""
        parts = symbol.split("/")
        base = parts[0]
        quote = parts[1].split(":")[0] if len(parts) > 1 else "USDT"
        return f"{base}_{quote}"

    async def _fetch_gate_funding_rates_direct(self,
            tickers: dict[str, Any] | None = None) -> dict[str, Any]:
        """从 Gate tickers 提取资金费率（轻量，不复用重端点 contracts）。
        tickers 含 funding_rate + funding_rate_indicative，比 /contracts 快 10x+。
        可传入预拉取的 tickers 避免重复请求。"""
        if tickers is None:
            tickers = await self._fetch_gate_futures_tickers_direct()
        if not tickers:
            return {}
        now_s = int(time.time())
        next_settle_ms = (((now_s // 28800) + 1) * 28800) * 1000  # Gate 8h 结算点
        result = {}
        for symbol, t in tickers.items():
            info = t.get("info", {}) if isinstance(t, dict) else {}
            rate = float((info.get("funding_rate", 0) or 0))
            indicative = float((info.get("funding_rate_indicative", 0) or 0))
            result[symbol] = {
                "info": info,
                "fundingRate": indicative if indicative != 0 else rate,
                "nextFundingRate": indicative,
                "nextFundingTime": next_settle_ms,
            }
        return result

    async def _fetch_gate_futures_tickers_direct(self) -> dict[str, Any]:
        """直连 Gate REST 获取全市场合约行情，返回 ccxt 兼容格式（带 5s 缓存）。"""
        now = time.time()
        cache = getattr(self, "_gate_ft_cache", {})
        if cache and (now - cache.get("ts", 0)) < 5:
            return cache["data"]
        try:
            tickers = await self._gate_request("/futures/usdt/tickers?timezone=utc0", timeout=30)
        except Exception:
            return {}
        result = {}
        for t in (tickers if isinstance(tickers, list) else []):
            contract = t.get("contract", "")
            if not contract.endswith("_USDT"):
                continue
            base = contract.replace("_USDT", "")
            symbol = f"{base}/USDT:USDT"
            result[symbol] = {
                "symbol": symbol,
                "last": float(t.get("last", 0) or 0),
                "info": t,
            }
        self._gate_ft_cache = {"ts": now, "data": result}
        return result

    async def _fetch_gate_spot_tickers_direct(self) -> dict[str, Any]:
        """直连 Gate REST 获取全市场现货行情，返回 ccxt 兼容格式。"""
        try:
            tickers = await self._gate_request("/spot/tickers?timezone=utc0", timeout=30)
        except Exception:
            return {}
        result = {}
        for t in (tickers if isinstance(tickers, list) else []):
            pair = t.get("currency_pair", "")
            if not pair.endswith("_USDT"):
                continue
            base = pair.replace("_USDT", "")
            symbol = f"{base}/USDT"
            result[symbol] = {
                "symbol": symbol,
                "last": float(t.get("last", 0) or 0),
                "info": t,
            }
        return result

    async def _fetch_gate_positions_direct(self, symbol: str | None = None) -> list[dict[str, Any]]:
        """直连 Gate REST 获取合约持仓，返回 ccxt 兼容格式。"""
        contract = self._to_gate_contract(symbol) if symbol else None
        path = f"/futures/usdt/positions/{contract}" if contract else "/futures/usdt/positions"
        try:
            positions = await self._gate_request(path, timeout=10)
        except Exception:
            return []
        positions = positions if isinstance(positions, list) else [positions] if isinstance(positions, dict) else []
        result = []
        for p in positions:
            name = p.get("contract", "")
            if not name.endswith("_USDT"):
                continue
            base = name.replace("_USDT", "")
            sym = f"{base}/USDT:USDT"
            size = float(p.get("size", 0) or 0)
            result.append({
                "symbol": sym,
                "contracts": size,  # 保留正负号：正=多，负=空
                "side": "long" if size > 0 else "short" if size < 0 else "",
                "notional": abs(size) * float(p.get("mark_price", 0) or 0),
                "info": p,
            })
        return result

    async def _fetch_gate_unified_balance_direct(self) -> dict[str, Any]:
        """直连 Gate REST 获取统一账户余额，返回 ccxt 兼容格式（含 free/total 嵌套）。"""
        now = time.time()
        cache = getattr(self, "_gate_balance_cache", {})
        if cache and (now - cache.get("ts", 0)) < 10:
            return cache["data"]
        try:
            acct = await self._gate_request("/unified/accounts", timeout=10)
        except Exception as exc:
            logger.warning("Gate 统一账户查询失败: %s", exc)
            empty: dict[str, Any] = {"free": {}, "total": {}, "USDT": {"free": 0.0, "total": 0.0}}
            return empty
        balances = acct.get("balances", {}) if isinstance(acct, dict) else {}
        total_equity = float(acct.get("total", 0) or 0)
        free_dict: dict[str, float] = {}
        total_dict: dict[str, float] = {}
        result: dict[str, Any] = {"info": acct}
        for currency, info in (balances.items() if isinstance(balances, dict) else []):
            if not isinstance(info, dict):
                continue
            available = float((info.get("available", 0) or 0))
            freeze = float((info.get("freeze", 0) or 0))
            free_dict[currency] = available
            total_dict[currency] = available + freeze
            result[currency] = {"free": available, "total": available + freeze, "info": info}
        if "USDT" not in result:
            free_dict["USDT"] = total_equity
            total_dict["USDT"] = total_equity
            result["USDT"] = {"free": total_equity, "total": total_equity}
        result["free"] = free_dict
        result["total"] = total_dict
        self._gate_balance_cache = {"ts": now, "data": result}
        logger.info("Gate 统一账户: USDT可用=%.2f 总计=%.2f",
                     free_dict.get("USDT", 0.0), total_dict.get("USDT", 0.0))
        return result

    async def _fetch_gate_futures_balance_direct(self) -> dict[str, Any]:
        """统一账户：直接查 /unified/accounts。"""
        return await self._fetch_gate_unified_balance_direct()

    async def _fetch_gate_spot_balance_direct(self) -> dict[str, Any]:
        """统一账户：直接查 /unified/accounts。"""
        return await self._fetch_gate_unified_balance_direct()

    @staticmethod
    def _clean_spot_symbol(symbol: str) -> str:
        """BTC/USDT → BTCUSDT"""
        return symbol.replace("/", "")

    @staticmethod
    def _clean_futures_symbol(symbol: str) -> str:
        """BTC/USDT:USDT → BTCUSDT"""
        return symbol.split(":")[0].replace("/", "")

    @staticmethod
    def _floor_usdt(amount: float) -> float:
        """USDT 划转金额向下截断到 2 位小数，避免余额不足。"""
        return math.floor(amount * 100) / 100

    @staticmethod
    def _normalize_order_response(resp: dict[str, Any], symbol: str,
                                  side: str, amount: float) -> dict[str, Any]:
        """将币安原生下单响应转成 ccxt 兼容格式。

        市价单正常情况下应全部成交; 成交不足视为部分成交。
        """
        filled_raw = resp.get("executedQty", 0)
        filled = float(filled_raw) if filled_raw else 0.0
        status = resp.get("status", "")
        if status == "FILLED" and filled <= 0:
            filled = amount  # WS 响应可能省略 executedQty，市价单默认全成交
        ok = status == "FILLED" and filled >= amount * 0.9
        return {
            "id": str(resp.get("orderId", "")),
            "clientOrderId": resp.get("clientOrderId", ""),
            "symbol": symbol,
            "side": side,
            "amount": amount,
            "filled": filled,
            "status": "closed" if ok else ("partial" if filled > 0 else "open"),
            "info": resp,
        }

    async def _binance_spot_order(self, symbol: str, side: str,
                                  quantity: float) -> dict[str, Any]:
        """币安现货市价单 POST /api/v3/order."""
        clean = self._clean_spot_symbol(symbol)
        resp = await self._binance_request(
            BINANCE_SPOT_API, "/api/v3/order",
            {"symbol": clean, "side": side.upper(), "type": "MARKET",
             "quantity": quantity},
            method="POST",
        )
        order = self._normalize_order_response(resp, symbol, side, quantity)
        if order["status"] != "closed":
            raise ExchangeError(
                f"现货{side}未完全成交: {symbol} filled={order['filled']}/{quantity}"
            )
        return order

    async def _binance_futures_order(self, symbol: str, side: str,
                                     quantity: float,
                                     reduce_only: bool = False,
                                     position_side: str | None = None) -> dict[str, Any]:
        """币安合约市价单 POST /fapi/v1/order.
        position_side: 双向持仓模式下传 "LONG" 或 "SHORT"。
        """
        clean = self._clean_futures_symbol(symbol)
        params: dict[str, Any] = {
            "symbol": clean, "side": side.upper(), "type": "MARKET",
            "quantity": quantity,
        }
        if position_side:
            params["positionSide"] = position_side.upper()
        elif reduce_only:
            params["reduceOnly"] = "true"
        resp = await self._binance_request(
            BINANCE_FUTURES_API, "/fapi/v1/order",
            params, method="POST",
        )
        order = self._normalize_order_response(resp, symbol, side, quantity)
        if order["status"] == "open":
            raise ExchangeError(
                f"合约{side}未成交: {symbol} filled=0/{quantity}"
            )
        if order["status"] == "partial":
            logger.warning("合约%s部分成交: filled=%.4f/%.4f (%.0f%%)",
                           symbol, order["filled"], quantity,
                           order["filled"] / quantity * 100 if quantity > 0 else 0)
        return order

    # ── 逐仓杠杆 API（反向套利用） ──

    async def _binance_margin_order(self, symbol: str, side: str,
                                    quantity: float) -> dict[str, Any]:
        """币安逐仓杠杆市价单 POST /sapi/v1/margin/order."""
        clean = self._clean_spot_symbol(symbol)
        resp = await self._binance_request(
            BINANCE_SPOT_API, "/sapi/v1/margin/order",
            {
                "symbol": clean, "side": side.upper(), "type": "MARKET",
                "quantity": quantity, "isIsolated": "TRUE",
            },
            method="POST",
        )
        order = self._normalize_order_response(resp, symbol, side, quantity)
        if order["status"] != "closed":
            raise ExchangeError(
                f"Margin {side}未完全成交: {symbol} filled={order['filled']}/{quantity}"
            )
        return order

    async def _binance_margin_loan(self, asset: str, amount: float,
                                    margin_symbol: str) -> dict[str, Any]:
        """逐仓杠杆借币 POST /sapi/v1/margin/loan."""
        clean = self._clean_spot_symbol(margin_symbol)
        resp = await self._binance_request(
            BINANCE_SPOT_API, "/sapi/v1/margin/loan",
            {"asset": asset, "amount": amount, "isIsolated": "TRUE", "symbol": clean},
            method="POST",
        )
        logger.info("借币成功: %s %s (pair=%s)", amount, asset, clean)
        return resp

    async def _binance_margin_repay(self, asset: str, amount: float,
                                     margin_symbol: str) -> dict[str, Any]:
        """逐仓杠杆还款 POST /sapi/v1/margin/repay."""
        clean = self._clean_spot_symbol(margin_symbol)
        resp = await self._binance_request(
            BINANCE_SPOT_API, "/sapi/v1/margin/repay",
            {"asset": asset, "amount": amount, "isIsolated": "TRUE", "symbol": clean},
            method="POST",
        )
        logger.info("还款完成: %s %s (pair=%s)", amount, asset, clean)
        return resp

    async def _binance_isolated_margin_transfer(
        self, asset: str, amount: float, margin_symbol: str, direction: str,
    ) -> dict[str, Any]:
        """spot ↔ 逐仓杠杆划转 POST /sapi/v1/margin/isolated/transfer.
        direction: "spot_to_margin" or "margin_to_spot"."""
        clean = self._clean_spot_symbol(margin_symbol)
        if direction == "spot_to_margin":
            trans_from, trans_to = "SPOT", "ISOLATED_MARGIN"
        else:
            trans_from, trans_to = "ISOLATED_MARGIN", "SPOT"
        resp = await self._binance_request(
            BINANCE_SPOT_API, "/sapi/v1/margin/isolated/transfer",
            {"asset": asset, "symbol": clean, "transFrom": trans_from,
             "transTo": trans_to, "amount": str(self._floor_usdt(amount))},
            method="POST",
        )
        logger.info("划转 %.2f %s: %s → %s", amount, asset, trans_from, trans_to)
        return resp

    async def _binance_margin_max_borrowable(
        self, asset: str, margin_symbol: str,
    ) -> tuple[bool, float]:
        """查询逐仓可借上限 GET /sapi/v1/margin/maxBorrowable."""
        clean = self._clean_spot_symbol(margin_symbol)
        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/maxBorrowable",
                {"asset": asset, "isIsolated": "TRUE", "symbol": clean},
            )
            max_amount = float(resp.get("amount", 0))
            return max_amount > 0, max_amount
        except Exception as exc:
            if "-3045" in str(exc):
                # 加入黑名单，10 分钟内不再尝试该币
                self._borrow_blacklist[asset] = time.time() + BORROW_POOL_EMPTY_COOLDOWN
                logger.warning("%s 借币池暂无库存，加入黑名单 %d 分钟。", asset, BORROW_POOL_EMPTY_COOLDOWN // 60)
            else:
                logger.warning("查询可借上限失败 %s: %s", asset, exc)
            return False, 0.0

    async def _get_isolated_margin_account(self, margin_symbol: str) -> dict[str, Any]:
        """查询逐仓杠杆账户详情 GET /sapi/v1/margin/isolated/account."""
        clean = self._clean_spot_symbol(margin_symbol)
        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
                {"symbols": clean},
            )
            assets = resp.get("assets", [])
            return assets[0] if assets else {}
        except Exception as exc:
            logger.warning("查询逐仓账户失败 %s: %s", clean, exc)
            return {}

    async def _check_margin_level(self, margin_symbol: str) -> float | None:
        """查询逐仓保证金率，失败返回 None。"""
        acct = await self._get_isolated_margin_account(margin_symbol)
        if not acct:
            return None
        try:
            return float(acct.get("marginLevel", 0))
        except (ValueError, TypeError):
            return None

    async def _check_futures_liquidation_distance(
        self, futures_symbol: str, short: bool = True
    ) -> float | None:
        """查询合约仓位距强平的距离百分比，失败返回 None。

        空单: (强平价 - 标记价) / 标记价   — 价格涨 → 距离缩小
        多单: (标记价 - 强平价) / 标记价   — 价格跌 → 距离缩小
        """
        try:
            positions = await self._safe_request(
                "futures.fetch_positions",
                lambda: self.futures.fetch_positions([futures_symbol]),
                default=[],
            )
        except Exception:
            return None
        for pos in positions:
            if pos.get("symbol") != futures_symbol:
                continue
            info = pos.get("info", {})
            liq_price = float(pos.get("liquidationPrice")
                              or info.get("liquidationPrice", 0))
            mark_price = float(pos.get("markPrice")
                               or info.get("markPrice", 0))
            if liq_price <= 0 or mark_price <= 0:
                return None
            if short:
                return (liq_price - mark_price) / mark_price
            return (mark_price - liq_price) / mark_price
        return None

    async def _disable_isolated_margin_pair(self, margin_symbol: str) -> None:
        """停用逐仓杠杆交易对，释放额度 DELETE /sapi/v1/margin/isolated/account."""
        clean = self._clean_spot_symbol(margin_symbol)
        try:
            await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
                {"symbol": clean}, method="DELETE",
            )
            logger.info("已停用逐仓交易对: %s", clean)
            self._record_margin_disabled(clean)
        except Exception as exc:
            logger.warning("停用逐仓交易对失败 %s: %s", clean, exc)

    async def _reclaim_all_usdt(self, keep_symbol: str = "") -> None:
        """回收所有账户的 USDT 到现货账户：逐仓杠杆、全仓杠杆、资金账户。

        keep_symbol: 跳过此交易对（例如反向开仓目标币种），避免划走又划回。
        """
        keep = self._clean_spot_symbol(keep_symbol) if keep_symbol else ""

        # 1) 回收逐仓杠杆
        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
            )
            assets = resp.get("assets", []) if isinstance(resp, dict) else []
            for acct in assets:
                sym = acct.get("symbol", "")
                if not sym:
                    continue
                if sym == keep:
                    logger.info("保留逐仓交易对: %s（开仓目标，不回收）", sym)
                    continue
                # 跳过零余额账户，避免反复"清空"空账户
                q = acct.get("quoteAsset", {}) or {}
                if float(q.get("netAsset", 0)) <= 0:
                    continue
                logger.info("回收逐仓资金: %s → spot", sym)
                await self._drain_margin_to_spot(sym)
        except Exception:
            pass

        # 2) 回收全仓杠杆 USDT → spot
        try:
            cross = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/account",
            )
            cross_usdt = 0.0
            cross_borrowed = 0.0
            for bal in cross.get("userAssets", []):
                if bal.get("asset") == "USDT":
                    cross_usdt = float(bal.get("netAsset", 0))
                    cross_borrowed = float(bal.get("borrowed", 0))
                    break
            if cross_borrowed <= 0 and cross_usdt > 1.0:
                logger.info("回收全仓杠杆: %.2f USDT → spot", cross_usdt)
                await self._binance_request(
                    BINANCE_SPOT_API, "/sapi/v1/margin/transfer",
                    {"asset": "USDT", "amount": self._floor_usdt(cross_usdt - 0.01), "type": 2},
                    method="POST",
                )
        except Exception:
            pass

        # 3) 回收资金账户 USDT → spot
        try:
            funding = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/asset/get-funding-asset",
                {"asset": "USDT"}, method="POST",
            )
            funding_usdt = float(funding.get("free", 0))
            if funding_usdt > 1.0:
                logger.info("回收资金账户: %.2f USDT → spot", funding_usdt)
                await self._binance_request(
                    BINANCE_SPOT_API, "/sapi/v1/asset/transfer",
                    {"asset": "USDT", "amount": round(funding_usdt, 2), "type": "FUNDING_MAIN"},
                    method="POST",
                )
        except Exception:
            pass

        # 4) 回收活期理财 USDT → spot（定期理财无法提前赎回，跳过）
        try:
            earn = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/simple-earn/flexible/position",
                {"asset": "USDT"},
            )
            rows = earn.get("rows", []) if isinstance(earn, dict) else []
            for row in rows:
                if row.get("asset") == "USDT":
                    earn_usdt = float(row.get("totalAmount", 0))
                    if earn_usdt > 1.0:
                        logger.info("回收活期理财: %.2f USDT → spot", earn_usdt)
                        await self._binance_request(
                            BINANCE_SPOT_API, "/sapi/v1/simple-earn/flexible/redeem",
                            {"asset": "USDT", "amount": round(earn_usdt, 2)},
                            method="POST",
                        )
        except Exception:
            pass

    async def _cleanup_margin_pair(self, base: str, margin_symbol: str,
                                   usdt_amount: float) -> None:
        """反向开仓失败后：划回 USDT 到现货账户。"""
        try:
            await self._binance_isolated_margin_transfer(
                "USDT", usdt_amount, margin_symbol, "margin_to_spot",
            )
        except Exception:
            pass
    async def _ensure_margin_pair_enabled(self, margin_symbol: str) -> bool:
        """确保逐仓交易对已启用。同时清理残留空壳（有USDT无借款的旧失败交易对）。

        达15上限时优先选空交易对，找不到则强制清理最久未使用的。
        """
        clean = self._clean_spot_symbol(margin_symbol)
        target_base = clean[:-4]  # "BTCUSDT" → "BTC"

        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
            )
        except Exception as exc:
            logger.warning("查询逐仓账户列表失败: %s", exc)
            return True  # 不确定状态，让后续 transfer 自己去试

        assets = resp.get("assets", []) if isinstance(resp, dict) else []
        target_enabled = False
        empty_candidates: list[tuple[float, str]] = []
        occupied_candidates: list[tuple[float, str]] = []
        stale_shells: list[str] = []  # 残留空壳：无base资产/负债，但有USDT
        now = time.time()

        for acct in assets:
            sym = acct.get("symbol", "")
            if not sym:
                continue
            base_name = self._extract_base_from_symbol(sym)

            if base_name == target_base:
                target_enabled = True
                continue

            base_asset = acct.get("baseAsset", {}) or {}
            quote_asset = acct.get("quoteAsset", {}) or {}
            base_borrowed = float(base_asset.get("borrowed", 0))
            base_net = float(base_asset.get("netAsset", 0))
            quote_net = float(quote_asset.get("netAsset", 0))

            last_disabled = self.margin_state.get("last_disabled", {}).get(sym, 0)
            if now - last_disabled < 86400:
                continue  # 24h 冷却中，不可停用

            last_used = self.margin_state.get("last_used", {}).get(sym, 0)

            # 残留空壳：没有base借款也没有base资产，但有USDT（上次失败留下的）
            if base_borrowed <= 0 and base_net <= 0:
                if quote_net > 0:
                    stale_shells.append(sym)
                else:
                    empty_candidates.append((last_used, sym))
            else:
                occupied_candidates.append((last_used, sym))

        # 先清理残留空壳——只划回 USDT，不停用（留待后续复用）
        for shell in stale_shells:
            logger.info("清理残留: %s（USDT 划回 spot，交易对保留）", shell)
            await self._drain_margin_to_spot(shell)

        if target_enabled:
            return True

        enabled_count = len(assets)
        if enabled_count < 15:
            return True  # 还有额度，transfer 会触发自动启用

        # 达15个上限，优先停用空交易对，其次清空并停用最久未使用的
        empty_candidates.sort()
        occupied_candidates.sort()

        if empty_candidates:
            _, victim = empty_candidates[0]
            logger.info("逐仓已达上限15个，停用最久未使用的空交易对: %s", victim)
            await self._disable_isolated_margin_pair(victim)
            return True

        # 无空交易对，清空最久未使用的并停用
        if not occupied_candidates:
            logger.warning(
                "逐仓已达上限15个，且全部在24h冷却中，无法释放任何交易对"
            )
            return False

        _, victim = occupied_candidates[0]
        logger.info("逐仓已达上限15个，清空并停用最久未使用的: %s", victim)
        if not await self._drain_margin_to_spot(victim):
            return False
        await self._disable_isolated_margin_pair(victim)
        return True

    @staticmethod
    def _extract_base_from_symbol(symbol: str) -> str:
        """BTCUSDT → BTC, ETHUSDT → ETH"""
        return symbol.replace("USDT", "")

    async def _drain_margin_to_spot(self, margin_symbol: str) -> bool:
        """清空逐仓交易对资产并划回 USDT：卖出 base、归还借款、划回 quote。零余额静默跳过。"""
        try:
            acct = await self._get_isolated_margin_account(margin_symbol)
            quote_net_check = float((acct.get("quoteAsset", {}) or {}).get("netAsset", 0))
            base_net_check = float((acct.get("baseAsset", {}) or {}).get("netAsset", 0))
            base_borrowed_check = float((acct.get("baseAsset", {}) or {}).get("borrowed", 0))
            if quote_net_check <= 0 and base_net_check <= 0 and base_borrowed_check <= 0:
                return True  # 空账户，无需操作
            base_asset = acct.get("baseAsset", {})
            base_net = float(base_asset.get("netAsset", 0)) if base_asset else 0.0
            base_borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0

            if base_net > 0:
                logger.info("清空资产: 卖出 %s %s [margin]", base_net, margin_symbol)
                result = await self._open_margin_spot_leg(margin_symbol, base_net)
                if not result.ok:
                    logger.warning("清空卖出失败: %s", result)

            # 重新查询最新负债（可能有利息）
            acct = await self._get_isolated_margin_account(margin_symbol)
            base_asset = acct.get("baseAsset", {})
            base_borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0

            if base_borrowed > 0:
                base_name = margin_symbol.split("/")[0] if "/" in margin_symbol else margin_symbol.replace("USDT", "")
                logger.info("清空负债: 归还 %s %s", base_borrowed, base_name)
                await self._binance_margin_repay(base_name, base_borrowed, margin_symbol)

            # 划回 USDT（_floor_usdt 已保证不超余额，无需额外 buffer）
            quote_asset = acct.get("quoteAsset", {})
            quote_net = float(quote_asset.get("netAsset", 0)) if quote_asset else 0.0
            transfer_out = self._floor_usdt(quote_net)
            drained = True
            if transfer_out > 0:
                try:
                    await self._binance_isolated_margin_transfer(
                        "USDT", transfer_out, margin_symbol, "margin_to_spot",
                    )
                except Exception:
                    drained = False
                    logger.warning("清空 %s 划转失败，USDT 仍留在逐仓。", margin_symbol)

            if drained:
                logger.info("已清空: %s", margin_symbol)
            return True
        except Exception as exc:
            logger.error("清空 %s 失败: %s", margin_symbol, exc)
            return False

    async def fetch_taker_fees(
        self,
    ) -> tuple[dict[str, float], dict[str, float]]:
        """获取 Binance 交易手续费（带缓存+锁，同轮扫描只查一次）。"""
        cache_key = "binance_fees"
        now = time.time()
        if cache_key in self._fee_cache:
            ts, cached = self._fee_cache[cache_key]
            if now - ts < 10:
                return cached
        async with self._fee_lock:
            # 双重检查：等锁期间可能已被另一个协程填充
            if cache_key in self._fee_cache:
                ts, cached = self._fee_cache[cache_key]
                if now - ts < 10:
                    return cached
            result = await self._fetch_taker_fees_impl()
            self._fee_cache[cache_key] = (time.time(), result)
            return result

    async def _fetch_taker_fees_impl(
        self,
    ) -> tuple[dict[str, float], dict[str, float]]:
        """获取交易手续费，全部走币安官方接口。

        现货: GET /sapi/v1/asset/tradeFee
        合约: GET /fapi/v1/commissionRate
        """
        default_spot = self._effective_spot_taker_fee(DEFAULT_SPOT_TAKER_FEE)
        default_fut = self._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE)

        # ── 现货费率 ──
        spot_taker: dict[str, float] = {}
        try:
            spot_list = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/asset/tradeFee",
            )
            spot_raw: float = 0.0
            for item in spot_list:
                sym = item.get("symbol", "")
                taker = float(item.get("takerCommission", 0))
                if sym and taker > 0:
                    eff = self._effective_spot_taker_fee(taker)
                    spot_taker[f"{sym[:-4]}/{sym[-4:]}"] = eff
                    if spot_raw == 0.0:
                        spot_raw = taker
                        spot_eff = eff
            if spot_raw > 0 and spot_eff != spot_raw:
                logger.info("Binance 现货费率: 基础=%.3f%% → BNB折扣后=%.3f%%",
                            spot_raw * 100, spot_eff * 100)
        except Exception:
            logger.warning("Binance 现货费率查询失败，使用默认值 %.3f%%", default_spot * 100)
        if not spot_taker:
            spot_taker = {"__default__": default_spot}

        # ── 合约费率 ──
        futures_taker: dict[str, float] = {}
        try:
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/commissionRate",
                params={"symbol": "BTCUSDT"},  # symbol 必传，费率全账户一致
            )
            raw = float(resp.get("takerCommissionRate", 0))
            if raw > 0:
                fee = self._effective_futures_taker_fee(raw)
                logger.info("Binance 合约费率: VIP基础=%.3f%% → BNB折扣后=%.3f%%",
                            raw * 100, fee * 100)
                futures_taker = {"__default__": fee}
        except Exception:
            logger.warning("Binance 合约费率查询失败，使用默认值 %.3f%%", default_fut * 100)
        if not futures_taker:
            futures_taker = {"__default__": default_fut}

        return spot_taker, futures_taker

    @staticmethod
    def _effective_spot_taker_fee(raw_fee: float) -> float:
        if not USE_BNB_FEE_DISCOUNT:
            return raw_fee
        return raw_fee * (1 - SPOT_BNB_FEE_DISCOUNT)

    @staticmethod
    def _effective_futures_taker_fee(raw_fee: float) -> float:
        if not USE_BNB_FEE_DISCOUNT:
            return raw_fee
        return raw_fee * (1 - FUTURES_BNB_FEE_DISCOUNT)

    async def scan_best_opportunity(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """
        扫描全市场 USDT 现货(主站+Alpha)与 U 本位永续，计算单期净收益率。

        现货来源:
            - main: Binance 主站现货 (symbol: BASE/USDT)
            - alpha: Binance Alpha 预上线现货 (symbol: ALPHA_<id>USDT)
        """
        alpha_tokens = await self._get_alpha_tokens()

        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self._safe_request(
                "spot.fetch_tickers",
                lambda: self.spot.fetch_tickers(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_tickers",
                lambda: self.futures.fetch_tickers(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_funding_rates",
                lambda: self.futures.fetch_funding_rates(),
                default={},
            ),
            self.fetch_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        rows: list[dict[str, Any]] = []
        futures_markets_by_base = self._build_futures_market_index()

        for base, futures_market in futures_markets_by_base.items():
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_quote_volume = self._safe_float(
                futures_ticker.get("quoteVolume")
            )

            if not self._futures_passes_volume(futures_quote_volume):
                continue

            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, is_predicted = self._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None:
                continue

            if not is_predicted and not getattr(self, '_warned_fallback_rate', False):
                logger.info("使用 lastFundingRate 作为预测费率（币安不提供 nextFundingRate 字段）。")
                self._warned_fallback_rate = True

            next_ft = self._extract_next_funding_time(funding_item)

            spot_symbol, spot_source, spot_last, spot_quote_volume, chain, alpha_fee_ratio = (
                self._resolve_spot_leg(
                    base,
                    spot_tickers,
                    alpha_tokens,
                    position_usdt,
                )
            )
            if spot_symbol is None:
                continue

            if not self._passes_liquidity_filter(
                spot_quote_volume,
                futures_quote_volume,
            ):
                continue

            if spot_source == "alpha":
                spot_fee = alpha_fee_ratio
            else:
                spot_fee = spot_taker_fees.get(
                    spot_symbol,
                    spot_taker_fees.get(
                        "__default__",
                        self._effective_spot_taker_fee(DEFAULT_SPOT_TAKER_FEE),
                    ),
                )
            futures_fee = futures_taker_fees.get(
                futures_symbol,
                futures_taker_fees.get(
                    "__default__",
                    self._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE),
                ),
            )
            open_only_net_rate = predicted_rate - spot_fee - futures_fee
            round_trip_net_rate = predicted_rate - 2 * (spot_fee + futures_fee)
            net_rate = (
                round_trip_net_rate
                if USE_ROUND_TRIP_FEE_FOR_ENTRY
                else open_only_net_rate
            )

            rows.append(
                {
                    "base": base,
                    "spot_symbol": spot_symbol,
                    "futures_symbol": futures_symbol,
                    "spot_source": spot_source,
                    "spot_last": spot_last,
                    "futures_last": futures_ticker.get("last"),
                    "spot_quote_volume": spot_quote_volume,
                    "futures_quote_volume": futures_quote_volume,
                    "quote_volume": min(spot_quote_volume, futures_quote_volume),
                    "is_predicted_rate": is_predicted,
                    "predicted_funding_rate": predicted_rate,
                    "spot_taker_fee": spot_fee,
                    "futures_taker_fee": futures_fee,
                    "open_only_net_rate": open_only_net_rate,
                    "round_trip_net_rate": round_trip_net_rate,
                    "net_rate": net_rate,
                    "chain": chain,
                    "alpha_fee_ratio": alpha_fee_ratio,
                    "next_funding_time_ms": next_ft,
                    "direction": "forward",
                    "exchange": "binance",
                }
            )

        df = pd.DataFrame(rows)
        if df.empty:
            return df

        return df.sort_values("net_rate", ascending=False).reset_index(drop=True)

    async def scan_reverse_opportunities(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """扫描负费率反向套利机会：借币卖出 + 做多合约，收取空头费率。

        Returns:
            DataFrame 按 net_rate 降序排列，含 borrow_hourly_rate 列。
        """
        if not REVERSE_ENABLED:
            return pd.DataFrame()

        alpha_tokens = await self._get_alpha_tokens()
        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self._safe_request("spot.fetch_tickers", lambda: self.spot.fetch_tickers(), default={}),
            self._safe_request("futures.fetch_tickers", lambda: self.futures.fetch_tickers(), default={}),
            self._safe_request("futures.fetch_funding_rates", lambda: self.futures.fetch_funding_rates(), default={}),
            self.fetch_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        # 预筛选：只找负费率币种，减少借币 API 查询量
        futures_index = self._build_futures_market_index()
        negative_bases: list[str] = []
        base_funding_info: dict[str, tuple[float, bool, float]] = {}  # base -> (rate, is_predicted, next_ft)
        for base, futures_market in futures_index.items():
            futures_ticker_data = futures_tickers.get(futures_market["symbol"], {})
            futures_quote_volume = self._safe_float(futures_ticker_data.get("quoteVolume"))
            if not self._futures_passes_volume(futures_quote_volume):
                continue
            funding_item = funding_rates.get(futures_market["symbol"], {})
            rate, is_predicted = self._extract_predicted_funding_rate(funding_item)
            if rate is None or rate >= 0:
                continue
            next_ft = self._extract_next_funding_time(funding_item)
            negative_bases.append(base)
            base_funding_info[base] = (rate, is_predicted, next_ft)

        if not negative_bases:
            return pd.DataFrame()

        # 批量查询借币利率（只查负费率币种）
        borrow_rates = await self._fetch_margin_borrow_rates(negative_bases)

        rows: list[dict[str, Any]] = []
        for base in negative_bases:
            borrow_rate = borrow_rates.get(base)
            if borrow_rate is None:
                continue  # 不可借贷或利率为 0

            predicted_rate, is_predicted, next_ft = base_funding_info[base]
            futures_market = futures_index[base]
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_quote_volume = self._safe_float(futures_ticker.get("quoteVolume"))

            resolved = self._resolve_spot_leg(
                base, spot_tickers, alpha_tokens, position_usdt,
            )
            if resolved is None:
                continue
            spot_symbol, spot_source, spot_last, spot_quote_volume, chain, alpha_fee_ratio = resolved

            # Alpha 代币不能做杠杆交易，反向套利只走主站现货
            if spot_source == "alpha":
                continue

            if not self._passes_liquidity_filter(spot_quote_volume, futures_quote_volume):
                continue

            spot_fee = spot_taker_fees.get(
                    spot_symbol,
                    spot_taker_fees.get("__default__", self._effective_spot_taker_fee(DEFAULT_SPOT_TAKER_FEE)),
                )
            futures_fee = futures_taker_fees.get(
                futures_symbol,
                futures_taker_fees.get("__default__", self._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE)),
            )

            income = abs(predicted_rate)  # 空头付给多头的费率
            # 借币成本 = 小时利率 × 距结算小时数（至少 1 小时）
            hours = max(1.0, (next_ft - time.time() * 1000) / 3_600_000) if next_ft > 0 else 1.0
            borrow_cost = borrow_rate * hours
            open_only_net = income - spot_fee - futures_fee - borrow_cost
            round_trip_net = income - 2 * (spot_fee + futures_fee) - borrow_cost
            net_rate = round_trip_net if USE_ROUND_TRIP_FEE_FOR_ENTRY else open_only_net

            if net_rate <= REVERSE_MIN_NET_RATE:
                continue

            rows.append({
                "base": base,
                "spot_symbol": spot_symbol,
                "futures_symbol": futures_symbol,
                "spot_source": spot_source,
                "spot_last": spot_last,
                "futures_last": futures_ticker.get("last"),
                "spot_quote_volume": spot_quote_volume,
                "futures_quote_volume": futures_quote_volume,
                "quote_volume": min(spot_quote_volume, futures_quote_volume),
                "is_predicted_rate": is_predicted,
                "predicted_funding_rate": predicted_rate,
                "spot_taker_fee": spot_fee,
                "futures_taker_fee": futures_fee,
                "borrow_hourly_rate": borrow_rate,
                "open_only_net_rate": open_only_net,
                "round_trip_net_rate": round_trip_net,
                "net_rate": net_rate,
                "chain": chain,
                "alpha_fee_ratio": alpha_fee_ratio,
                "next_funding_time_ms": next_ft,
                "direction": "reverse",
                "exchange": "binance",
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.sort_values("net_rate", ascending=False).reset_index(drop=True)

    # ── Gate.io 扫描 ──

    async def _scan_gate_forward(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """Gate.io 正向扫描：现货买入 + 合约做空。无 Alpha 回退。"""
        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self._fetch_gate_spot_tickers_direct(),
            self._fetch_gate_futures_tickers_direct(),
            self._fetch_gate_funding_rates_direct(),
            self._fetch_gate_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        rows: list[dict[str, Any]] = []
        futures_index = self._build_gate_futures_market_index()

        for base, futures_market in futures_index.items():
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_qv = self._safe_float(futures_ticker.get("quoteVolume"))
            if not self._futures_passes_volume(futures_qv):
                continue

            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, _ = self._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None:
                continue

            next_ft = self._extract_next_funding_time(funding_item)

            spot_symbol = f"{base}/USDT"
            spot_market = (getattr(self.gate_spot, "markets", None) or {}).get(spot_symbol)
            if not spot_market or not spot_market.get("active", True):
                continue
            spot_ticker = spot_tickers.get(spot_symbol, {})
            spot_last = self._safe_float(spot_ticker.get("last"))
            spot_qv = self._safe_float(spot_ticker.get("quoteVolume"))
            if spot_last <= 0:
                continue
            if not self._passes_liquidity_filter(spot_qv, futures_qv):
                continue

            spot_fee = spot_taker_fees.get(spot_symbol, spot_taker_fees.get("__default__", DEFAULT_GATE_SPOT_TAKER_FEE))
            futures_fee = futures_taker_fees.get(futures_symbol, futures_taker_fees.get("__default__", DEFAULT_GATE_FUTURES_TAKER_FEE))

            open_only_net = predicted_rate - spot_fee - futures_fee
            round_trip_net = predicted_rate - 2 * (spot_fee + futures_fee)
            net_rate = round_trip_net if USE_ROUND_TRIP_FEE_FOR_ENTRY else open_only_net

            rows.append({
                "base": base,
                "spot_symbol": spot_symbol,
                "futures_symbol": futures_symbol,
                "spot_source": "spot",
                "spot_last": spot_last,
                "futures_last": futures_ticker.get("last"),
                "spot_quote_volume": spot_qv,
                "futures_quote_volume": futures_qv,
                "quote_volume": min(spot_qv, futures_qv),
                "is_predicted_rate": False,
                "predicted_funding_rate": predicted_rate,
                "spot_taker_fee": spot_fee,
                "futures_taker_fee": futures_fee,
                "open_only_net_rate": open_only_net,
                "round_trip_net_rate": round_trip_net,
                "net_rate": net_rate,
                "chain": "",
                "alpha_fee_ratio": 0.0,
                "next_funding_time_ms": next_ft,
                "direction": "forward",
                "exchange": "gate",
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.sort_values("net_rate", ascending=False).reset_index(drop=True)

    async def _scan_gate_reverse(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """Gate.io 反向扫描：借币卖出 + 合约做多，负费率方向。"""
        if not REVERSE_ENABLED:
            return pd.DataFrame()

        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self._fetch_gate_spot_tickers_direct(),
            self._fetch_gate_futures_tickers_direct(),
            self._fetch_gate_funding_rates_direct(),
            self._fetch_gate_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        futures_index = self._build_gate_futures_market_index()

        negative_bases: list[str] = []
        base_funding_info: dict[str, tuple[float, bool, float]] = {}
        for base in futures_index:
            futures_symbol = futures_index[base]["symbol"]
            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, is_predicted = self._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None or predicted_rate >= 0:
                continue
            next_ft = self._extract_next_funding_time(funding_item)
            negative_bases.append(base)
            base_funding_info[base] = (predicted_rate, is_predicted, next_ft)

        if not negative_bases:
            return pd.DataFrame()

        borrow_rates = await self._fetch_gate_margin_borrow_rates(negative_bases)

        rows: list[dict[str, Any]] = []
        for base in negative_bases:
            borrow_rate = borrow_rates.get(base)
            if borrow_rate is None:
                continue

            predicted_rate, is_predicted, next_ft = base_funding_info[base]
            futures_market = futures_index[base]
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_qv = self._safe_float(futures_ticker.get("quoteVolume"))

            spot_symbol = f"{base}/USDT"
            spot_market = (getattr(self.gate_spot, "markets", None) or {}).get(spot_symbol)
            if not spot_market or not spot_market.get("active", True):
                continue
            spot_ticker = spot_tickers.get(spot_symbol, {})
            spot_last = self._safe_float(spot_ticker.get("last"))
            spot_qv = self._safe_float(spot_ticker.get("quoteVolume"))
            if spot_last <= 0:
                continue
            if not self._passes_liquidity_filter(spot_qv, futures_qv):
                continue

            spot_fee = spot_taker_fees.get(spot_symbol, spot_taker_fees.get("__default__", DEFAULT_GATE_SPOT_TAKER_FEE))
            futures_fee = futures_taker_fees.get(futures_symbol, futures_taker_fees.get("__default__", DEFAULT_GATE_FUTURES_TAKER_FEE))

            income = abs(predicted_rate)
            hours = max(1.0, (next_ft - time.time() * 1000) / 3_600_000) if next_ft > 0 else 1.0
            borrow_cost = borrow_rate * hours
            open_only_net = income - spot_fee - futures_fee - borrow_cost
            round_trip_net = income - 2 * (spot_fee + futures_fee) - borrow_cost
            net_rate = round_trip_net if USE_ROUND_TRIP_FEE_FOR_ENTRY else open_only_net

            if net_rate <= REVERSE_MIN_NET_RATE:
                continue

            rows.append({
                "base": base,
                "spot_symbol": spot_symbol,
                "futures_symbol": futures_symbol,
                "spot_source": "spot",
                "spot_last": spot_last,
                "futures_last": futures_ticker.get("last"),
                "spot_quote_volume": spot_qv,
                "futures_quote_volume": futures_qv,
                "quote_volume": min(spot_qv, futures_qv),
                "is_predicted_rate": is_predicted,
                "predicted_funding_rate": predicted_rate,
                "spot_taker_fee": spot_fee,
                "futures_taker_fee": futures_fee,
                "borrow_hourly_rate": borrow_rate,
                "open_only_net_rate": open_only_net,
                "round_trip_net_rate": round_trip_net,
                "net_rate": net_rate,
                "chain": "",
                "alpha_fee_ratio": 0.0,
                "next_funding_time_ms": next_ft,
                "direction": "reverse",
                "exchange": "gate",
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.sort_values("net_rate", ascending=False).reset_index(drop=True)

    # ── 跨交易所资金费率套利扫描 ──

    async def _scan_cross_exchange(self, position_usdt: float = 100.0,
                                     strict: bool = True, snipe: bool = False) -> pd.DataFrame:
        """扫描跨交易所费率差异机会：纯期货多空对冲。

        高费率所做空 + 低费率所做多 = delta 中性，赚费率差。
        strict=False: 放宽筛选，用于展示接近阈值的候选。
        snipe=True: 狙击刷新模式，只拉费率+Gate价格，复用缓存中的手续费和BN价格。
        """
        if not CROSS_EXCHANGE_ENABLED:
            return pd.DataFrame()

        cache = getattr(self, "_cross_scan_cache", {})

        if snipe and cache:
            # 狙击模式：只刷新 BN 资金费率 + Gate tickers（含费率），手续费/BN价格不变
            bn_funding, gt_tickers = await asyncio.gather(
                self._safe_request("futures.fetch_funding_rates",
                                   lambda: self.futures.fetch_funding_rates(), default={}),
                self._safe_request("gate_futures.fetch_tickers_direct",
                                   lambda: self._fetch_gate_futures_tickers_direct(), default={}),
            )
            gt_funding = await self._fetch_gate_funding_rates_direct(gt_tickers)
            bn_fee_pair = cache["bn_fee_pair"]
            gt_fee_pair = cache["gt_fee_pair"]
            bn_tickers = cache["bn_tickers"]
            # 更新缓存中的费率和 GT tickers（但不改 ts，主扫缓存仍有效）
            cache["bn_funding"] = bn_funding
            cache["gt_funding"] = gt_funding
            cache["gt_tickers"] = gt_tickers
        else:
            # 复用缓存数据，避免严格+宽松两次扫描重复拉取
            now = time.time()
            if cache and (now - cache.get("ts", 0)) < 120:
                bn_fee_pair = cache["bn_fee_pair"]
                gt_fee_pair = cache["gt_fee_pair"]
                bn_funding = cache["bn_funding"]
                gt_funding = cache["gt_funding"]
                bn_tickers = cache["bn_tickers"]
                gt_tickers = cache["gt_tickers"]
            else:
                # 所有 API 并行拉取：tickers + 费率 + 资金费率
                gt_tickers_raw, bn_fee_pair, gt_fee_pair, bn_funding, bn_tickers = await asyncio.gather(
                    self._safe_request("gate_futures.fetch_tickers_direct",
                                       lambda: self._fetch_gate_futures_tickers_direct(), default={}),
                    self.fetch_taker_fees(),
                    self._fetch_gate_taker_fees(),
                    self._safe_request("futures.fetch_funding_rates",
                                       lambda: self.futures.fetch_funding_rates(), default={}),
                    self._safe_request("futures.fetch_tickers",
                                       lambda: self.futures.fetch_tickers(), default={}),
                )
                gt_tickers = gt_tickers_raw
                gt_funding = await self._fetch_gate_funding_rates_direct(gt_tickers)
                self._cross_scan_cache = {
                    "ts": time.time(),  # 数据就绪时间，非请求发起时间
                    "bn_fee_pair": bn_fee_pair,
                    "gt_fee_pair": gt_fee_pair,
                    "bn_funding": bn_funding,
                    "gt_funding": gt_funding,
                    "bn_tickers": bn_tickers,
                    "gt_tickers": gt_tickers,
                }
        _, bn_futures_fee_dict = bn_fee_pair
        _, gt_futures_fee_dict = gt_fee_pair
        bn_futures_fee = float(bn_futures_fee_dict.get("__default__",
            self._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE)))
        gt_futures_fee = float(gt_futures_fee_dict.get("__default__", DEFAULT_GATE_FUTURES_TAKER_FEE))
        self._dash_cross_fees = (bn_futures_fee, gt_futures_fee)

        bn_market_index = self._build_futures_market_index()
        gt_market_index = self._build_gate_futures_market_index()

        # 构建 base → (symbol, rate, next_funding_time) 映射
        def _parse_rates(funding_dict: dict, market_index: dict, exchange: str) -> dict[str, tuple[str, float, float]]:
            result: dict[str, tuple[str, float, float]] = {}
            for symbol, item in funding_dict.items():
                market = market_index.get(symbol)
                if market is None:
                    # ccxt 可能用 slash 格式的 key，尝试匹配 base
                    for base, mkt in market_index.items():
                        if mkt["symbol"] == symbol:
                            market = mkt
                            break
                if market is None:
                    continue
                base = market["base"]
                rate, _ = self._extract_predicted_funding_rate(item)
                if rate is None:
                    continue
                next_ft = self._extract_next_funding_time(item)
                result[base] = (market["symbol"], rate, next_ft)
            return result

        bn_rates = _parse_rates(bn_funding, bn_market_index, "binance")
        gt_rates = _parse_rates(gt_funding, gt_market_index, "gate")

        # ── 找共同币种：同名 + 价格校验 ──
        # 两所同名且价差 <1% 才视为同一资产，否则跳过
        PRICE_DIVERGENCE_THRESHOLD = 0.01

        def _ticker_price(tickers: dict, symbol: str) -> float:
            t = tickers.get(symbol, {})
            return float(t.get("last") or 0)

        valid_pairs: dict[str, str] = {}  # bn_base → gt_base

        for base in set(bn_rates) & set(gt_rates):
            bn_sym = bn_rates[base][0]
            gt_sym = gt_rates[base][0]
            bp = _ticker_price(bn_tickers, bn_sym)
            gp = _ticker_price(gt_tickers, gt_sym)
            if bp > 0 and gp > 0 and abs(bp - gp) / min(bp, gp) >= PRICE_DIVERGENCE_THRESHOLD:
                logger.debug("[跨交易所] 价格拒绝 %s: BN=%.6f GT=%.6f 偏离=%.1f%%",
                           base, bp, gp, abs(bp - gp) / min(bp, gp) * 100)
                continue
            valid_pairs[base] = base


        if not valid_pairs:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for bn_base, gt_base in valid_pairs.items():
            if bn_base not in bn_rates or gt_base not in gt_rates:
                continue
            bn_sym, bn_rate, bn_nft = bn_rates[bn_base]
            gt_sym, gt_rate, gt_nft = gt_rates[gt_base]

            rate_spread = abs(bn_rate - gt_rate)
            if strict and rate_spread < CROSS_MIN_RATE_SPREAD:
                continue

            # 确定方向：费率高的做空，低的做多
            if bn_rate > gt_rate:
                short_ex, long_ex = "binance", "gate"
                short_sym, long_sym = bn_sym, gt_sym
                short_rate, long_rate = bn_rate, gt_rate
                short_fee, long_fee = bn_futures_fee, gt_futures_fee
                short_nft, long_nft = bn_nft, gt_nft
            else:
                short_ex, long_ex = "gate", "binance"
                short_sym, long_sym = gt_sym, bn_sym
                short_rate, long_rate = gt_rate, bn_rate
                short_fee, long_fee = gt_futures_fee, bn_futures_fee
                short_nft, long_nft = gt_nft, bn_nft

            # 净收益 = 费率差 - 双边往返手续费
            total_fees = 2 * (short_fee + long_fee)
            net_rate = rate_spread - total_fees

            # 判断是否通过严格筛选
            passes = True
            if rate_spread < CROSS_MIN_RATE_SPREAD:
                passes = False
            elif bn_nft > 0 and gt_nft > 0 and abs(bn_nft - gt_nft) > 5000:
                # 两所结算时间差>5s：如 BN 8h / GT 4h，不同步无法套利
                passes = False
            elif bn_nft > 0 and not self._within_entry_window(bn_nft):
                passes = False
            elif gt_nft > 0 and not self._within_entry_window(gt_nft):
                passes = False
            elif net_rate <= CROSS_MIN_NET_RATE:
                passes = False

            if strict and not passes:
                continue

            bn_price = float((bn_tickers.get(bn_sym) or {}).get("last") or 0)
            gt_price = float((gt_tickers.get(gt_sym) or {}).get("last") or 0)

            rows.append({
                "base": bn_base,
                "rate_spread": rate_spread,
                "net_rate": net_rate,
                "short_exchange": short_ex,
                "long_exchange": long_ex,
                "short_symbol": short_sym,
                "long_symbol": long_sym,
                "short_rate": short_rate,
                "long_rate": long_rate,
                "short_fee": short_fee,
                "long_fee": long_fee,
                "short_next_funding_time_ms": short_nft,
                "long_next_funding_time_ms": long_nft,
                "short_price": bn_price if short_ex == "binance" else gt_price,
                "long_price": bn_price if long_ex == "binance" else gt_price,
                "bn_price": bn_price,
                "gt_price": gt_price,
                "passes": passes,
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.sort_values("rate_spread", ascending=False).reset_index(drop=True)
        if strict:
            return df
        return df.head(20)

    def _resolve_spot_leg(
        self,
        base: str,
        spot_tickers: dict,
        alpha_tokens: dict[str, dict[str, Any]],
        position_usdt: float = 100.0,
    ) -> tuple[str | None, str, float | None, float, str, float]:
        """Resolve spot leg for a given base. Returns (symbol, source, last_price, quote_volume, chain, gas_fee_ratio).

        Checks main spot first, then falls back to Binance Alpha.
        """
        # Try main spot
        spot_symbol = f"{base}/USDT"
        spot_market = (getattr(self.spot, "markets", None) or {}).get(spot_symbol)
        if spot_market and self._is_valid_spot_usdt_market(spot_symbol, spot_market):
            ticker = spot_tickers.get(spot_symbol, {})
            return (
                spot_symbol,
                "spot",
                ticker.get("last"),
                self._safe_float(ticker.get("quoteVolume")),
                "",
                0.0,
            )

        # Try Alpha
        alpha_info = alpha_tokens.get(base)
        if alpha_info:
            alpha_symbol = alpha_info["alpha_symbol"]
            gas_usd = alpha_info.get("gas_usd", ALPHA_GAS_USD_PER_TX)
            gas_fee_ratio = gas_usd / position_usdt
            return (
                alpha_symbol,
                "alpha",
                alpha_info.get("price"),
                alpha_info.get("quote_volume", 0.0),
                alpha_info.get("chain", ""),
                gas_fee_ratio,
            )

        return None, "", None, 0.0, "", 0.0

    @staticmethod
    def _futures_passes_volume(futures_quote_volume: float) -> bool:
        return futures_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME

    async def _get_alpha_tokens(self) -> dict[str, dict[str, Any]]:
        """Get Alpha token list, cached for 5 minutes."""
        now = time.monotonic()
        if self._alpha_tokens and (now - self._alpha_last_fetch) < 300:
            return self._alpha_tokens
        self._alpha_tokens = await self._fetch_alpha_token_list()
        self._alpha_last_fetch = now
        return self._alpha_tokens

    async def _fetch_margin_borrow_rates(self, assets: list[str]) -> dict[str, float]:
        """批量查询逐仓杠杆借币小时利率。

        Returns:
            dict[base_upper, hourly_rate_float] — 只包含可借贷且 rate > 0 的币种。
            不可借贷的币种不会出现在返回结果中。
        """
        if not assets or not REVERSE_ENABLED:
            return {}
        rates: dict[str, float] = {}
        batch_size = 20
        for i in range(0, len(assets), batch_size):
            batch = assets[i : i + batch_size]
            try:
                resp = await self._safe_request(
                    "margin_next_hourly_interest",
                    lambda b=batch: self.spot.sapiGetMarginNextHourlyInterestRate(
                        {"assets": ",".join(b), "isIsolated": True}
                    ),
                    default=[],
                )
                for item in resp:
                    asset = item.get("asset", "").upper()
                    rate = self._safe_float(item.get("nextHourlyInterestRate"))
                    if asset and rate > 0:
                        rates[asset] = rate
            except Exception:
                logger.warning(f"借币利率批量查询失败: {batch}", exc_info=True)
        return rates

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _log_memory() -> None:
        """记录当前进程 RSS + VmData 到日志，并触发 malloc_trim 归还空闲堆内存。"""
        try:
            vm_data = vm_rss = 0
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        vm_rss = int(line.split()[1])
                    elif line.startswith("VmData:"):
                        vm_data = int(line.split()[1])
            logger.info("[内存] RSS: %.0f MB | Data: %.0f MB", vm_rss / 1024, vm_data / 1024)
            # 强制 glibc 把堆上空闲页归还 OS（pandas/numpy free 后 glibc arena 囤着不还）
            try:
                ctypes.CDLL("libc.so.6").malloc_trim(0)
            except Exception:
                pass
        except Exception:
            pass

    def _build_futures_market_index(self) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}
        bn_markets = getattr(self.futures, "markets", None)
        if not bn_markets:
            return index
        tradfi_skipped = 0
        for market in bn_markets.values():
            is_usdt_swap = (
                market.get("swap")
                and market.get("linear")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            )
            if is_usdt_swap:
                if market.get("info", {}).get("underlyingType") == "STOCK":
                    tradfi_skipped += 1
                    continue
                index[market["base"]] = market
        if tradfi_skipped:
            logger.info("Binance 跳过 %d 个 TradFi 股票代币", tradfi_skipped)
        return index

    def _build_gate_futures_market_index(self) -> dict[str, dict[str, Any]]:
        """Gate.io 版: 构建 USDT 永续合约 base → market 索引。"""
        index: dict[str, dict[str, Any]] = {}
        gt_markets = getattr(self.gate_futures, "markets", None)
        if not gt_markets:
            return index
        for market in gt_markets.values():
            is_usdt_swap = (
                market.get("swap")
                and market.get("linear")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            )
            if is_usdt_swap:
                index[market["base"]] = market
        return index

    async def _fetch_gate_taker_fees(self) -> tuple[dict[str, float], dict[str, float]]:
        """Gate 现货+合约 taker 费率（带缓存+锁，同轮扫描只查一次）。"""
        cache_key = "gate_fees"
        now = time.time()
        if cache_key in self._fee_cache:
            ts, cached = self._fee_cache[cache_key]
            if now - ts < 10:
                return cached
        async with self._fee_lock:
            if cache_key in self._fee_cache:
                ts, cached = self._fee_cache[cache_key]
                if now - ts < 10:
                    return cached
            result = await self._fetch_gate_taker_fees_impl()
            self._fee_cache[cache_key] = (time.time(), result)
            return result

    async def _fetch_gate_taker_fees_impl(self) -> tuple[dict[str, float], dict[str, float]]:
        """Gate 现货+合约 taker 费率（已扣除返佣）。失败用默认值。"""
        spot_base = DEFAULT_GATE_SPOT_TAKER_FEE
        fut_base = DEFAULT_GATE_FUTURES_TAKER_FEE
        spot_fees: dict[str, float] = {"__default__": spot_base * (1 - GATE_SPOT_REBATE)}
        fut_fees: dict[str, float] = {"__default__": fut_base * (1 - GATE_FUTURES_REBATE)}

        # 现货费率 — 直连 REST
        spot_raw_rate: float = 0.0
        try:
            spot_raw = await self._gate_request("/spot/fee", timeout=10)
            taker = float((spot_raw.get("taker_fee", 0) or 0))
            if taker > 0:
                eff = taker * (1 - GATE_SPOT_REBATE)
                spot_fees["__default__"] = eff
                spot_raw_rate = taker
        except Exception:
            pass
        if spot_raw_rate > 0:
            logger.info("Gate 现货费率: 基础=%.3f%% → 返佣%.0f%%后=%.3f%%",
                        spot_raw_rate * 100, GATE_SPOT_REBATE * 100,
                        spot_fees["__default__"] * 100)
        else:
            logger.warning("Gate 现货费率查询失败，使用默认值 %.3f%%",
                           spot_fees["__default__"] * 100)

        # 合约费率 — Gate v4 无独立 fee 端点，回退 ccxt
        fut_raw_rate: float = 0.0
        gt_markets = getattr(self.gate_futures, "markets", None)
        valid_fut_symbols = {
            m["symbol"] for m in (gt_markets or {}).values()
            if m.get("swap") and m.get("linear") and m.get("quote") == "USDT"
        } if gt_markets else set()
        try:
            fut_raw = await self._safe_request(
                "gate_futures.fetch_trading_fees",
                lambda: self.gate_futures.fetch_trading_fees(),
                default=None,
            )
            if fut_raw:
                for sym, info in fut_raw.items():
                    taker = self._safe_float(info.get("taker", 0))
                    if taker > 0 and sym in valid_fut_symbols:
                        eff = taker * (1 - GATE_FUTURES_REBATE)
                        fut_fees[sym] = eff
                        fut_fees["__default__"] = eff
                        fut_raw_rate = taker
        except Exception:
            pass
        if fut_raw_rate > 0:
            logger.info("Gate 合约费率: 基础=%.3f%% → 返佣%.0f%%后=%.3f%%",
                        fut_raw_rate * 100, GATE_FUTURES_REBATE * 100,
                        fut_fees["__default__"] * 100)
        else:
            logger.warning("Gate 合约费率查询失败，使用默认值 %.3f%%",
                           fut_fees["__default__"] * 100)

        return spot_fees, fut_fees

    async def _load_gate_margin_bases(self) -> list[str]:
        """拉取 Gate 所有可逐仓借贷的币种列表（公开接口，缓存 24h，直连 REST）。"""
        now = time.monotonic()
        if self._gate_margin_bases and (now - self._gate_margin_bases_ts) < 86_400:
            return self._gate_margin_bases
        try:
            resp = await self._gate_request("/margin/uni/currency_pairs", timeout=15)
            bases: set[str] = set()
            for item in (resp if isinstance(resp, list) else []):
                pair = str(item.get("currency_pair", "") if isinstance(item, dict) else "")
                if pair.endswith("_USDT") and pair.count("_") == 1:
                    bases.add(pair.replace("_USDT", "").upper())
            self._gate_margin_bases = sorted(bases)
            self._gate_margin_bases_ts = now
            logger.info("Gate 可借贷币种: %d 个（缓存 24h）", len(bases))
        except Exception:
            pass
        return self._gate_margin_bases

    async def _fetch_gate_margin_borrow_rates(self, assets: list[str]) -> dict[str, float]:
        """Gate 逐仓借币利率。
        先加载 Gate 支持借贷的币种列表，只查已知可借贷的币，避免大量无意义查询。
        返回 {base_upper: hourly_rate_float}。"""
        if not assets or not GATE_API_KEY:
            return {}

        # 预先筛掉 Gate 不支持借币的币种（大部分都不可借，筛掉后批量请求不会整批失败）
        margin_bases = await self._load_gate_margin_bases()
        margin_set = set(margin_bases)
        query_assets = [a for a in assets if a.upper() in margin_set]
        if not query_assets:
            return {}

        rates: dict[str, float] = {}

        def _parse_response(data) -> None:
            if isinstance(data, dict):
                for currency, rate_str in data.items():
                    base = currency.upper()
                    rate_val = self._safe_float(rate_str)
                    if rate_val > 0:
                        rates[base] = rate_val
            elif isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        pair = str(entry.get("currency_pair", ""))
                        base = pair.replace("_USDT", "").upper()
                        if base:
                            rate_val = self._safe_float(entry.get("hourly_rate", entry.get("rate", 0)))
                            if rate_val > 0:
                                rates[base] = rate_val

        batch_size = 10
        for i in range(0, len(query_assets), batch_size):
            chunk = query_assets[i:i + batch_size]
            currencies_str = ",".join(chunk)
            try:
                resp = await self.gate_spot.private_margin_get_uni_estimate_rate(
                    {"currencies": currencies_str}
                )
                _parse_response(resp)
            except Exception:
                pass  # 理论上不应再失败；若失败静默跳过

        if rates:
            logger.info("Gate 借币利率: %d/%d 币种可借贷", len(rates), len(assets))
        return rates

    # ── Gate.io 交易基础设施 ──

    async def _gate_spot_balance(self) -> float:
        """Gate 现货 USDT 可用余额。"""
        bal = await self._safe_request(
            "gate_spot.fetch_balance",
            lambda: self._fetch_gate_spot_balance_direct(),
            default={},
        )
        return self._free_balance(bal, "USDT")

    async def _gate_futures_balance(self) -> float:
        """Gate 合约 USDT 可用余额。"""
        bal = await self._safe_request(
            "gate_futures.fetch_balance",
            lambda: self._fetch_gate_futures_balance_direct(),
            default={},
        )
        return self._free_balance(bal, "USDT")

    async def _cross_get_gate_total_balance(self) -> float:
        """Gate 合约 USDT 总余额 (可用 + 锁定保证金)。"""
        try:
            bal = await self._safe_request(
                "gate_futures.fetch_balance_total",
                lambda: self._fetch_gate_futures_balance_direct(),
                default={},
            )
            return float(bal.get("USDT", {}).get("total", 0) or 0)
        except Exception:
            return 0.0

    async def _gate_transfer_usdt(
        self, amount: float, from_account: str, to_account: str,
        symbol: str | None = None,
    ) -> bool:
        """Gate 内部资金划转 (直连 REST)。margin 划转需传 symbol 参数。"""
        body: dict[str, Any] = {
            "currency": "USDT",
            "from": from_account,
            "to": to_account,
            "amount": str(self._floor_usdt(amount)),
        }
        if symbol and ("margin" in (from_account, to_account)):
            body["currency_pair"] = symbol.replace("/", "_")
        try:
            await self._gate_request("/wallet/transfers", body=body, method="POST", timeout=15)
            logger.info("Gate 划转: %.2f USDT %s → %s", amount, from_account, to_account)
            return True
        except Exception as exc:
            logger.error("Gate 划转失败: %s", exc)
            return False

    async def _gate_rebalance_accounts(self) -> None:
        """Gate 单币种保证金模式：统一余额，无需划转。"""
        pass

    async def _gate_get_position_size(self) -> float:
        """动态计算 Gate 仓位大小。每腿用一半余额。"""
        spot = await self._gate_spot_balance()
        half = spot / 2
        size = half * POSITION_SIZE_RATIO
        if size <= 0:
            logger.error("Gate 可用余额为 0: balance=%.2f", spot)
            return 0.0
        return size

    # ── Gate.io 下单方法 ──

    async def _gate_spot_order(self, symbol: str, side: str,
                                amount: float) -> dict[str, Any]:
        """Gate 现货市价单 — 直连 REST，省 ccxt 框架开销。"""
        precise = float(self.gate_spot.amount_to_precision(symbol, amount))
        pair = symbol.replace("/", "_")
        body = {
            "currency_pair": pair, "side": side, "type": "market",
            "amount": str(precise), "account": "spot", "time_in_force": "ioc",
        }
        resp = await self._gate_request("/spot/orders", body=body, method="POST", timeout=15)
        filled = float(resp.get("filled_amount", resp.get("filled_total", 0)) or 0)
        ok = resp.get("status") == "closed" and filled >= precise * 0.9
        return {"id": str(resp.get("id", "")), "symbol": symbol, "side": side,
                "amount": precise, "filled": filled,
                "status": "closed" if ok else "open", "info": resp}

    async def _gate_futures_order_direct(self, symbol: str, side: str,
                                          amount: float, reduce_only: bool = False) -> dict[str, Any]:
        """Gate 合约市价单 — 绕过 ccxt，直连 REST API，省框架开销。
        gate.io v4: size>0=买, size<0=卖; tif=ioc 市价立即成交或取消。"""
        contract = self._to_gate_contract(symbol)
        precise = float(self.gate_futures.amount_to_precision(symbol, amount))
        size = precise if side == "buy" else -precise
        body: dict[str, Any] = {
            "contract": contract, "size": size, "price": "0", "tif": "ioc",
        }
        if reduce_only:
            body["reduce_only"] = True
        resp = await self._gate_request("/futures/usdt/orders", body=body, method="POST")
        filled = abs(float(resp.get("size", 0) or 0)) - abs(float(resp.get("left", 0) or 0))
        finished = resp.get("status") == "finished"
        return {
            "id": str(resp.get("id", "")), "symbol": symbol, "side": side,
            "amount": precise, "filled": filled,
            "status": "closed" if finished else ("partial" if filled > 0 else "open"),
            "info": resp,
        }

    async def _gate_set_leverage(self, symbol: str) -> bool:
        """Gate 合约设置杠杆 1x (leverage 以 query string 传递，非 JSON body)。"""
        try:
            contract = self._to_gate_contract(symbol)
            await self._gate_request(
                f"/futures/usdt/positions/{contract}/leverage",
                method="POST", timeout=10,
                params={"leverage": str(CROSS_LEVERAGE)},
            )
            return True
        except Exception as exc:
            logger.warning("Gate 设置杠杆失败 %s: %s", symbol, exc)
            return False

    async def _binance_set_leverage(self, symbol: str) -> bool:
        """币安合约设置杠杆 1x POST /fapi/v1/leverage。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/leverage",
                {"symbol": clean, "leverage": CROSS_LEVERAGE}, method="POST",
            )
            return True
        except Exception as exc:
            logger.warning("Binance 设置杠杆失败 %s: %s", symbol, exc)
            return False

    async def _gate_calculate_amount(self, spot_symbol: str, futures_symbol: str,
                                      reference_price: float, total_usdt: float) -> float:
        """按 Gate 精度计算下单数量。返回 0 表示金额不足最小下单量。"""
        raw = total_usdt / reference_price
        # 获取最小下单量
        spot_min = futures_min = 0.0
        try:
            spot_mkt = self.gate_spot.market(spot_symbol)
            futures_mkt = self.gate_futures.market(futures_symbol)
            spot_min = float(spot_mkt.get("limits", {}).get("amount", {}).get("min", 0) or 0)
            futures_min = float(futures_mkt.get("limits", {}).get("amount", {}).get("min", 0) or 0)
        except Exception:
            pass
        min_qty = max(spot_min, futures_min) if (spot_min or futures_min) else None
        if min_qty and raw < min_qty:
            logger.warning(
                "Gate 数量不足 %s: 需要≥%.4f 个 (≈%.2f USDT), 当前只能买 %.4f 个 (%.2f USDT/腿)",
                spot_symbol, min_qty, min_qty * reference_price, raw, total_usdt,
            )
            return 0.0
        try:
            spot_amt = float(self.gate_spot.amount_to_precision(spot_symbol, raw))
            futures_amt = float(self.gate_futures.amount_to_precision(futures_symbol, raw))
        except Exception as exc:
            logger.warning("Gate 精度计算失败 %s: %s (raw=%.6f price=%.4f usdt=%.2f)",
                          spot_symbol, exc, raw, reference_price, total_usdt)
            return 0.0
        amount = min(spot_amt, futures_amt)
        if amount <= 0 or amount * reference_price < 5.0:
            return 0.0
        return amount

    async def _gate_fetch_next_funding_time(self, futures_symbol: str) -> float:
        """获取 Gate 合约下次资金费率结算时间 (ms) — 直连 REST。"""
        try:
            contract = self._to_gate_contract(futures_symbol)
            info = await self._gate_request(f"/futures/usdt/contracts/{contract}", timeout=10)
            nft = info.get("funding_next_apply")
            if nft and float(nft) > 0:
                return float(nft) * 1000  # Gate 返回秒，转为 ms
        except Exception:
            pass
        return time.time() * 1000 + DEFAULT_FUNDING_INTERVAL_HOURS * 3600_000

    # ── Gate.io Leg 方法 ──

    async def _open_gate_spot_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._gate_spot_order(symbol, "buy", amount)
            return LegResult(True, "gate_spot", symbol, "buy", amount, order=order)
        except Exception as exc:
            return LegResult(False, "gate_spot", symbol, "buy", amount, error=str(exc))

    async def _close_gate_spot_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            precise = float(self.gate_spot.amount_to_precision(symbol, amount))
            order = await self._gate_spot_order(symbol, "sell", precise)
            return LegResult(True, "gate_spot", symbol, "sell", precise, order=order)
        except Exception as exc:
            return LegResult(False, "gate_spot", symbol, "sell", amount, error=str(exc))

    async def _open_gate_futures_short_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._gate_futures_order_direct(symbol, "sell", amount)
            return LegResult(True, "gate_futures", symbol, "sell", amount, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "sell", amount, error=str(exc))

    async def _close_gate_futures_short_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            precise = float(self.gate_futures.amount_to_precision(symbol, amount))
            order = await self._gate_futures_order_direct(symbol, "buy", precise, reduce_only=True)
            return LegResult(True, "gate_futures", symbol, "buy", precise, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "buy", amount, error=str(exc))

    async def _open_gate_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._gate_futures_order_direct(symbol, "buy", amount)
            return LegResult(True, "gate_futures", symbol, "buy", amount, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "buy", amount, error=str(exc))

    async def _close_gate_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            precise = float(self.gate_futures.amount_to_precision(symbol, amount))
            order = await self._gate_futures_order_direct(symbol, "sell", precise, reduce_only=True)
            return LegResult(True, "gate_futures", symbol, "sell", precise, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "sell", amount, error=str(exc))

    # ── Gate.io 保证金方法 ──

    async def _gate_margin_borrow(self, symbol: str, base: str, amount: float) -> bool:
        """Gate 逐仓借币 — 直连 REST。"""
        try:
            pair = symbol.replace("/", "_")
            body = {"currency_pair": pair, "currency": base, "amount": str(amount)}
            await self._gate_request("/margin/uni/loans", body=body, method="POST", timeout=15)
            logger.info("Gate 借币: %s %s (pair=%s)", amount, base, symbol)
            return True
        except Exception as exc:
            logger.error("Gate 借币失败 %s %s: %s", base, amount, exc)
            msg = str(exc).lower()
            if "not enough" in msg or "insufficient" in msg or "3045" in msg:
                self._borrow_blacklist[base.upper()] = time.time() + BORROW_POOL_EMPTY_COOLDOWN
            return False

    async def _gate_margin_repay(self, symbol: str, base: str, amount: float) -> bool:
        """Gate 逐仓还款 — 直连 REST（查贷款 ID 后还款）。"""
        try:
            pair = symbol.replace("/", "_")
            # 先查该币种的未还贷款
            loans = await self._gate_request(
                f"/margin/uni/loans?currency_pair={pair}&currency={base}&status=open",
                timeout=10,
            )
            loans_list = loans if isinstance(loans, list) else []
            if not loans_list:
                logger.warning("Gate 还款: 无未还贷款 %s %s", base, symbol)
                return False
            # 取第一笔贷款 ID 还款
            loan_id = str(loans_list[0].get("id", ""))
            if not loan_id:
                logger.error("Gate 还款: 无法获取贷款 ID %s %s", base, symbol)
                return False
            await self._gate_request(
                f"/margin/uni/loans/{loan_id}",
                body={"amount": str(amount), "currency": base}, method="PATCH", timeout=15,
            )
            logger.info("Gate 还款: %s %s (pair=%s)", amount, base, symbol)
            return True
        except Exception as exc:
            logger.error("Gate 还款失败 %s %s: %s", base, amount, exc)
            return False

    async def _gate_query_margin_account(self, symbol: str) -> dict[str, Any]:
        """查询 Gate 逐仓账户状态 — 直连 REST。"""
        base = symbol.split("/")[0]
        try:
            pair = symbol.replace("/", "_")
            acct = await self._gate_request(
                f"/margin/uni/accounts?currency_pair={pair}", timeout=10,
            )
            b = acct.get("base", {}) if isinstance(acct, dict) else {}
            q = acct.get("quote", {}) if isinstance(acct, dict) else {}
            base_avail = float(b.get("available", 0) or 0)
            base_locked = float(b.get("locked", 0) or 0)
            base_debt = float(b.get("borrowed", 0) or 0)
            usdt_avail = float(q.get("available", 0) or 0)
            return {"base_net": base_avail + base_locked - base_debt,
                    "base_borrowed": base_debt,
                    "quote_net": usdt_avail,
                    "margin_level": 0.0}
        except Exception as exc:
            logger.warning("Gate 查询 margin 账户失败 %s: %s", symbol, exc)
            return {"base_net": 0.0, "base_borrowed": 0.0, "quote_net": 0.0, "margin_level": 0.0}

    # ── Gate.io 持仓检查 ──

    async def _has_gate_spot_balance(self) -> bool:
        """检查 Gate 现货是否持有仓位对应的 base 币。"""
        if not self.gate_state.base:
            return False
        bal = await self._safe_request(
            "gate_spot.fetch_balance_for_position",
            lambda: self._fetch_gate_spot_balance_direct(),
            default={},
        )
        free = float((bal.get("free", {}) or {}).get(self.gate_state.base, 0))
        return free > self.gate_state.amount * 0.5

    async def _has_gate_futures_position(self) -> bool:
        """检查 Gate 合约是否有对应方向的持仓。"""
        if not self.gate_state.futures_symbol:
            return False
        positions = await self._safe_request(
            "gate_futures.fetch_positions",
            lambda: self._fetch_gate_positions_direct(self.gate_state.futures_symbol),
            default=[],
        )
        for pos in positions:
            if pos.get("symbol") != self.gate_state.futures_symbol:
                continue
            contracts = self._position_contracts(pos)
            if self.gate_state.direction == "reverse":
                return contracts < 0  # 反向=做空合约
            return contracts > 0      # 正向=做多合约
        return False

    async def _has_gate_margin_loan(self) -> bool:
        """检查 Gate 逐仓是否有借币。"""
        if not self.gate_state.base or self.gate_state.direction != "reverse":
            return False
        acct = await self._gate_query_margin_account(self.gate_state.spot_symbol)
        return acct.get("base_borrowed", 0) > self.gate_state.amount * 0.1

    async def _has_gate_open_position(self) -> bool:
        """聚合检查 Gate 持仓是否仍然存在。不一致则重置状态。"""
        if not self.gate_state.is_open or self.gate_state.exchange != "gate":
            return False
        if self.gate_state.direction == "reverse":
            margin_ok = await self._has_gate_margin_loan()
            futures_ok = await self._has_gate_futures_position()
            if margin_ok or futures_ok:
                return True
        else:
            spot_ok = await self._has_gate_spot_balance()
            futures_ok = await self._has_gate_futures_position()
            if spot_ok or futures_ok:
                return True
        logger.warning("Gate 状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.gate_state = ArbitrageState()
        self._save_state("gate")
        return False

    # ── Gate.io 交易流程 ──

    async def _open_gate_forward_position(self, row: pd.Series) -> bool:
        """Gate 正向开仓：现货买入 + 合约做空。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])

        await self._gate_rebalance_accounts()
        position_usdt = await self._gate_get_position_size()
        if position_usdt <= 0:
            return False

        next_ft_ms = float(row.get("next_funding_time_ms", 0))
        if not self._within_entry_window(next_ft_ms):
            logger.info("Gate [%s] 不在结算窗口内，跳过。", base)
            return False

        await self._gate_set_leverage(futures_symbol)

        price = self._select_reference_price(row)
        if not price or price <= 0:
            return False
        amount = await self._gate_calculate_amount(spot_symbol, futures_symbol, price, position_usdt)
        if amount <= 0:
            logger.warning("[gate] 开仓失败(%s): 金额不足最小下单量，跳过", base)
            return False

        logger.info("Gate 正向开仓: %s %s buy spot + short futures | 费率=%+.4f%%",
                    base, amount, float(row["predicted_funding_rate"]) * 100)
        spot_task = self._open_gate_spot_leg(spot_symbol, amount)
        futures_task = self._open_gate_futures_short_leg(futures_symbol, amount)
        spot_result, futures_result = await asyncio.gather(spot_task, futures_task)

        if spot_result.ok and futures_result.ok:
            next_ft = await self._gate_fetch_next_funding_time(futures_symbol)
            self.gate_state = ArbitrageState(
                is_open=True, exchange="gate",
                spot_symbol=spot_symbol, futures_symbol=futures_symbol,
                base=base, amount=amount, entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.tz).isoformat(),
                spot_source="spot", next_funding_time_ms=next_ft,
                direction="forward",
            )
            self._save_state("gate")
            self._print_position_summary()
            self._send_email(f"[Gate 开仓] {base} 正向套利",
                             f"费率={float(row['predicted_funding_rate'])*100:.4f}% 净收益={float(row['net_rate'])*100:.4f}%")
            return True

        if spot_result.ok or futures_result.ok:
            await self._gate_emergency_close_forward(spot_result, futures_result)
        return False

    async def _open_gate_reverse_position(self, row: pd.Series) -> bool:
        """Gate 反向开仓：划转保证金 → 借币卖出 + 合约做多。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])

        if self._borrow_blacklist.get(base.upper(), 0) > time.time():
            logger.info("Gate [%s] 借币池黑名单中，跳过。", base)
            return False

        total = await self._gate_spot_balance()
        half = total / 2  # 一半做保证金抵押，一半做合约保证金
        if half <= 0:
            return False

        position_usdt = half * POSITION_SIZE_RATIO

        # 划转保证金到逐仓账户
        transfer_amt = self._floor_usdt(position_usdt)
        if transfer_amt >= 1.0:
            if not await self._gate_transfer_usdt(transfer_amt, "spot", "margin", symbol=spot_symbol):
                return False

        price = self._select_reference_price(row)
        if not price or price <= 0:
            logger.warning("[gate] 开仓失败(%s): 无参考价格", base)
            await self._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False
        amount = await self._gate_calculate_amount(spot_symbol, futures_symbol, price, position_usdt)
        if amount <= 0:
            logger.warning("[gate] 开仓失败(%s): 金额不足最小下单量，跳过", base)
            await self._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False

        if not await self._gate_margin_borrow(spot_symbol, base, amount):
            logger.warning("[gate] 开仓失败(%s): 借币失败", base)
            await self._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False

        await self._gate_set_leverage(futures_symbol)
        logger.info("Gate 反向开仓: %s %s margin sell + futures long | 费率=%+.4f%%",
                    base, amount, float(row["predicted_funding_rate"]) * 100)
        margin_task = self._open_gate_margin_spot_leg(spot_symbol, amount)
        futures_task = self._open_gate_futures_long_leg(futures_symbol, amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        if margin_result.ok and futures_result.ok:
            next_ft = await self._gate_fetch_next_funding_time(futures_symbol)
            self.gate_state = ArbitrageState(
                is_open=True, exchange="gate",
                spot_symbol=spot_symbol, futures_symbol=futures_symbol,
                base=base, amount=amount, entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.tz).isoformat(),
                spot_source="margin", next_funding_time_ms=next_ft,
                direction="reverse",
            )
            self._save_state("gate")
            self._print_position_summary()
            self._send_email(f"[Gate 开仓] {base} 反向套利",
                             f"费率={float(row['predicted_funding_rate'])*100:.4f}% 净收益={float(row['net_rate'])*100:.4f}%")
            return True

        if margin_result.ok or futures_result.ok:
            logger.warning("[gate] 开仓失败(%s): 部分成交 margin_ok=%s futures_ok=%s",
                          base, margin_result.ok, futures_result.ok)
            await self._gate_emergency_close_reverse(margin_result, futures_result, spot_symbol, base)
        else:
            logger.warning("[gate] 开仓失败(%s): 下单失败 margin_err=%s futures_err=%s",
                          base, margin_result.error, futures_result.error)
            await self._gate_margin_repay(spot_symbol, base, amount)
            await self._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
        return False

    async def _close_gate_forward_position(self) -> bool:
        """Gate 正向平仓：现货卖出 + 合约平空。"""
        symbol = self.gate_state.spot_symbol
        fsymbol = self.gate_state.futures_symbol
        amount = self.gate_state.amount
        logger.info("Gate 正向平仓: spot=%s futures=%s amount=%s", symbol, fsymbol, amount)

        spot_task = self._close_gate_spot_leg(symbol, amount)
        futures_task = self._close_gate_futures_short_leg(fsymbol, amount)
        spot_r, fut_r = await asyncio.gather(spot_task, futures_task)

        for _ in range(3):
            if spot_r.ok and fut_r.ok:
                break
            if not spot_r.ok:
                spot_r = await self._close_gate_spot_leg(symbol, amount)
            if not fut_r.ok:
                fut_r = await self._close_gate_futures_short_leg(fsymbol, amount)
            await asyncio.sleep(1.0)

        if spot_r.ok and fut_r.ok:
            logger.info("Gate 正向平仓成功。")
            self._record_close_trade(self.gate_state)
            self.gate_state = ArbitrageState()
            self._save_state("gate")
            self._send_email("[Gate 平仓] 正向套利", "平仓成功")
            return True

        logger.critical("Gate 平仓部分失败，恢复对冲。")
        if spot_r.ok and not fut_r.ok:
            await self._open_gate_spot_leg(symbol, amount)
        elif fut_r.ok and not spot_r.ok:
            await self._open_gate_futures_short_leg(fsymbol, amount)
        return False

    async def _close_gate_reverse_position(self) -> bool:
        """Gate 反向平仓：margin 买回 + 合约平多 → 还款 → USDT 划回 spot。"""
        symbol = self.gate_state.spot_symbol
        fsymbol = self.gate_state.futures_symbol
        base = self.gate_state.base
        amount = self.gate_state.amount
        logger.info("Gate 反向平仓: margin=%s futures=%s amount=%s", symbol, fsymbol, amount)

        margin_task = self._close_gate_margin_spot_leg(symbol, amount)
        futures_task = self._close_gate_futures_long_leg(fsymbol, amount)
        margin_r, fut_r = await asyncio.gather(margin_task, futures_task)

        for _ in range(3):
            if margin_r.ok and fut_r.ok:
                break
            if not margin_r.ok:
                margin_r = await self._close_gate_margin_spot_leg(symbol, amount)
            if not fut_r.ok:
                fut_r = await self._close_gate_futures_long_leg(fsymbol, amount)
            await asyncio.sleep(1.0)

        if not margin_r.ok or not fut_r.ok:
            logger.critical("Gate 反向平仓部分失败，恢复对冲。")
            if margin_r.ok and not fut_r.ok:
                await self._open_gate_margin_spot_leg(symbol, amount)
            elif fut_r.ok and not margin_r.ok:
                await self._open_gate_futures_long_leg(fsymbol, amount)
            return False

        acct = await self._gate_query_margin_account(symbol)
        borrowed = acct.get("base_borrowed", amount)
        await self._gate_margin_repay(symbol, base, max(borrowed, amount))

        acct = await self._gate_query_margin_account(symbol)
        quote_net = acct.get("quote_net", 0)
        transfer_out = self._floor_usdt(quote_net)
        if transfer_out > 0:
            await self._gate_transfer_usdt(transfer_out, "margin", "spot", symbol=symbol)

        logger.info("Gate 反向平仓完成。")
        self._record_close_trade(self.gate_state)
        self.gate_state = ArbitrageState()
        self._save_state("gate")
        self._send_email("[Gate 平仓] 反向套利", "平仓成功")
        return True

    async def _gate_emergency_close_forward(self, spot_result: LegResult,
                                             futures_result: LegResult) -> None:
        """Gate 正向部分成交应急平仓。"""
        tasks = []
        if spot_result.ok and not futures_result.ok:
            tasks.append(self._close_gate_spot_leg(spot_result.symbol, spot_result.amount))
        if futures_result.ok and not spot_result.ok:
            tasks.append(self._close_gate_futures_short_leg(futures_result.symbol, futures_result.amount))
        if tasks:
            await asyncio.gather(*tasks)

    async def _gate_emergency_close_reverse(self, margin_result: LegResult,
                                             futures_result: LegResult,
                                             symbol: str, base: str) -> None:
        """Gate 反向部分成交应急平仓。"""
        tasks = []
        if margin_result.ok and not futures_result.ok:
            tasks.append(self._close_gate_margin_spot_leg(symbol, margin_result.amount))
        if futures_result.ok and not margin_result.ok:
            tasks.append(self._close_gate_futures_long_leg(futures_result.symbol, futures_result.amount))
        if tasks:
            await asyncio.gather(*tasks)
        if not margin_result.ok:
            await self._gate_margin_repay(symbol, base, margin_result.amount)

    async def _open_gate_margin_spot_leg(self, symbol: str, amount: float) -> LegResult:
        """Gate margin 卖出（卖出借入的币）— 直连 REST。"""
        try:
            precise = float(self.gate_spot.amount_to_precision(symbol, amount))
            pair = symbol.replace("/", "_")
            body = {
                "currency_pair": pair, "side": "sell", "type": "market",
                "amount": str(precise), "account": "margin", "time_in_force": "ioc",
            }
            resp = await self._gate_request("/spot/orders", body=body, method="POST", timeout=15)
            filled = float(resp.get("filled_amount", resp.get("filled_total", 0)) or 0)
            ok = resp.get("status") == "closed" and filled >= precise * 0.9
            return LegResult(ok, "gate_margin", symbol, "sell", precise,
                             order={"id": str(resp.get("id", "")), "filled": filled,
                                    "status": "closed" if ok else "open", "info": resp})
        except Exception as exc:
            return LegResult(False, "gate_margin", symbol, "sell", amount, error=str(exc))

    async def _close_gate_margin_spot_leg(self, symbol: str, amount: float) -> LegResult:
        """Gate margin 买回（还币）— 直连 REST。"""
        try:
            precise = float(self.gate_spot.amount_to_precision(symbol, amount))
            pair = symbol.replace("/", "_")
            body = {
                "currency_pair": pair, "side": "buy", "type": "market",
                "amount": str(precise), "account": "margin", "time_in_force": "ioc",
            }
            resp = await self._gate_request("/spot/orders", body=body, method="POST", timeout=15)
            filled = float(resp.get("filled_amount", resp.get("filled_total", 0)) or 0)
            ok = resp.get("status") == "closed" and filled >= precise * 0.9
            return LegResult(ok, "gate_margin", symbol, "buy", precise,
                             order={"id": str(resp.get("id", "")), "filled": filled,
                                    "status": "closed" if ok else "open", "info": resp})
        except Exception as exc:
            return LegResult(False, "gate_margin", symbol, "buy", amount, error=str(exc))

    @staticmethod
    def _is_valid_spot_usdt_market(
        symbol: str,
        market: dict[str, Any],
    ) -> bool:
        return (
            market.get("spot")
            and market.get("quote") == "USDT"
            and market.get("active", True)
            and symbol.endswith("/USDT")
        )

    @staticmethod
    def _get_quote_volumes(
        spot_ticker: dict[str, Any],
        futures_ticker: dict[str, Any],
    ) -> tuple[float, float]:
        spot_volume = spot_ticker.get("quoteVolume") or 0
        futures_volume = futures_ticker.get("quoteVolume") or 0
        try:
            return float(spot_volume), float(futures_volume)
        except (TypeError, ValueError):
            return 0.0, 0.0

    @staticmethod
    def _passes_liquidity_filter(
        spot_quote_volume: float,
        futures_quote_volume: float,
    ) -> bool:
        if LIQUIDITY_MODE == "futures_only":
            return futures_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME
        if LIQUIDITY_MODE == "both_legs":
            return (
                futures_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME
                and spot_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME
            )
        return (
            futures_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME
            and spot_quote_volume >= MIN_SPOT_24H_QUOTE_VOLUME
        )

    @staticmethod
    def _extract_predicted_funding_rate(
        funding_item: dict[str, Any],
    ) -> tuple[float | None, bool]:
        """返回 (rate, is_from_nextFundingRate)。

        is_from_nextFundingRate=True  → 预测费率 (主站可用)
        is_from_nextFundingRate=False → 溢价推算兜底 (mark-index)/index
        """
        # 优先: nextFundingRate (只有极少数交易所返回)
        for value in (
            funding_item.get("nextFundingRate"),
            (funding_item.get("info") or {}).get("nextFundingRate"),
        ):
            if value is not None:
                try:
                    return float(value), True
                except (TypeError, ValueError):
                    continue

        # 兜底: fundingRate / lastFundingRate (币安网页"下次费率"即为此值)
        for value in (
            funding_item.get("fundingRate"),
            funding_item.get("lastFundingRate"),
            (funding_item.get("info") or {}).get("fundingRate"),
            (funding_item.get("info") or {}).get("lastFundingRate"),
        ):
            if value is not None:
                try:
                    return float(value), False
                except (TypeError, ValueError):
                    continue
        return None, False

    @staticmethod
    def _extract_next_funding_time(funding_item: dict[str, Any]) -> float:
        """从 funding rate 数据中提取 nextFundingTime (ms)。0 表示获取失败。"""
        for key in ("nextFundingTime", "nextFundingTimestamp", "nextFundingTimeMs"):
            val = funding_item.get(key)
            if val is not None:
                try:
                    f = float(val)
                    if f > 0:
                        return f
                except (TypeError, ValueError):
                    continue
        info = funding_item.get("info") or {}
        for key in ("nextFundingTime", "nextFundingTimestamp"):
            val = info.get(key)
            if val is not None:
                try:
                    f = float(val)
                    if f > 0:
                        return f
                except (TypeError, ValueError):
                    continue
        # Gate.io: funding_next_apply 是秒级时间戳，需转 ms
        gate_next = info.get("funding_next_apply")
        if gate_next is not None:
            try:
                f = float(gate_next)
                if f > 0:
                    return f * 1000 if f < 1e12 else f
            except (TypeError, ValueError):
                pass
        return 0.0

    def _within_entry_window(self, next_funding_time_ms: float) -> bool:
        """开仓时间窗口: 距 settlement 不足 ENTRY_WINDOW_MINUTES 分钟才允许入场。"""
        if next_funding_time_ms <= 0:
            return True  # 获取失败不阻止，允许入场
        now_ms = time.time() * 1000
        remaining_ms = next_funding_time_ms - now_ms
        return 0 < remaining_ms <= ENTRY_WINDOW_MINUTES * 60_000

    def _compute_funding_stats(self, df: pd.DataFrame) -> tuple[float, float]:
        """计算全市场资金费率中位数和标准差，用于异动检测。"""
        rates = pd.to_numeric(df["predicted_funding_rate"], errors="coerce").dropna()
        if rates.empty:
            return 0.0, 0.0
        return float(rates.median()), float(rates.std())

    def _detect_rate_anomaly(self, df: pd.DataFrame, median: float, std: float
                             ) -> pd.Series | None:
        """检测费率异动: 负向偏离中位数 > PRE_BORROW_SIGMA 个标准差 且 费率已为负。
        返回最优异动币（最低费率），无则返回 None。
        仅扫描反向可做的币（有 margin 交易对）。
        """
        if std <= 0 or median >= 0:
            return None
        threshold = median - PRE_BORROW_SIGMA * std
        best: pd.Series | None = None
        best_rate = 0.0
        for _, row in df.iterrows():
            rate = float(row["predicted_funding_rate"])
            direction = str(row.get("direction", "forward"))
            if direction != "reverse":
                continue
            if rate < PRE_BORROW_MIN_RATE and rate < threshold:
                if best is None or rate < best_rate:
                    best = row
                    best_rate = rate
        return best

    async def _execute_pre_borrow(self, row: pd.Series) -> bool:
        """预借: 划转 USDT 到逐仓 + 借币，但不卖出。等费率到位后秒开。"""
        base = str(row["base"])
        if self._borrow_blacklist.get(base, 0) > time.time():
            logger.info("[预借] %s 在黑名单中，跳过。", base)
            return False
        margin_symbol = str(row["spot_symbol"])
        t_start = time.perf_counter()
        price = self._select_reference_price(row)
        if not price or price <= 0:
            logger.warning("[预借] %s: 参考价格无效", base)
            return False

        # 计算 50/50 分配（ccxt + 直连 REST + 逐仓杠杆 三查）
        spot_bal, futures_bal, spot_rest, futures_rest = await asyncio.gather(
            self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
            self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
            self._safe_binance_balance(BINANCE_SPOT_API, "/api/v3/account"),
            self._safe_binance_balance(BINANCE_FUTURES_API, "/fapi/v2/balance"),
        )
        spot_usdt_ccxt = self._free_balance(spot_bal, "USDT")
        futures_usdt_ccxt = self._free_balance(futures_bal, "USDT")
        spot_usdt = max(spot_usdt_ccxt, spot_rest)
        futures_usdt = max(futures_usdt_ccxt, futures_rest)

        # 也查逐仓杠杆 USDT（上次失败的回收可能留在了逐仓）
        margin_usdt = 0.0
        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
            )
            for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                q = acct.get("quoteAsset", {})
                if q:
                    margin_usdt += float(q.get("netAsset", 0))
        except Exception:
            pass
        if margin_usdt > 0:
            logger.info("[预借] 发现逐仓 %.2f USDT 残留，回收归集。", margin_usdt)
            for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                sym = acct.get("symbol", "")
                if sym:
                    await self._drain_margin_to_spot(sym)
            spot_usdt += margin_usdt
            margin_usdt = 0.0

        total = spot_usdt + futures_usdt + margin_usdt
        half = total / 2
        position_usdt = half * POSITION_SIZE_RATIO
        if half < 5.0:
            logger.warning("[预借] %s: 余额不足 spot=%.2f futures=%.2f margin=%.2f total=%.2f half=%.2f",
                           base, spot_usdt, futures_usdt, margin_usdt, total, half)
            return False

        # 启用逐仓交易对 + 划转 USDT
        if not await self._ensure_margin_pair_enabled(margin_symbol):
            logger.warning("[预借] %s: 无法启用逐仓交易对", base)
            return False

        need = self._floor_usdt(min(half, spot_usdt))
        if need >= 1.0:
            logger.info("[预借 1/3] 划转 %.2f USDT: 现货 → 逐仓 [%s]", need, base)
            try:
                await self._binance_isolated_margin_transfer(
                    "USDT", need, margin_symbol, "spot_to_margin",
                )
            except Exception as exc:
                logger.error("[预借] %s: 划转失败 %s", base, exc)
                return False
        else:
            logger.info("[预借 1/3] 逐仓已有足够 USDT，跳过划转。")

        # 借币
        amount = self._calculate_precise_amount(
            spot_symbol=margin_symbol, futures_symbol=str(row["futures_symbol"]),
            reference_price=price, spot_source="margin", total_usdt=position_usdt,
        )
        if amount <= 0:
            logger.warning("[预借] %s: 数量计算失败", base)
            return False

        nominal = amount * price
        can_borrow, max_borrowable = await self._binance_margin_max_borrowable(base, margin_symbol)
        if not can_borrow or max_borrowable < amount:
            logger.warning("[预借] %s: 借币池不足 need=%s max=%s 池剩余=%.0f%%",
                         base, amount, max_borrowable,
                         max_borrowable / amount * 100 if amount > 0 else 0)
            return False

        logger.info("[预借 2/3] 借币 %s x%s | 价格=%.6f 名义=%.2f USDT | 池剩余=%.0f%%",
                    base, amount, price, nominal,
                    (1 - amount / max_borrowable) * 100 if max_borrowable > 0 else 0)
        try:
            await self._binance_margin_loan(base, amount, margin_symbol)
        except Exception as exc:
            logger.error("[预借] %s: 借币失败 %s", base, exc)
            return False

        # 记录状态
        self.binance_state.pre_borrow_base = base
        self.binance_state.pre_borrow_margin_symbol = margin_symbol
        self.binance_state.pre_borrow_amount = amount
        self.binance_state.pre_borrow_at = datetime.now(tz=self.tz).isoformat()
        self._save_state()
        elapsed = (time.perf_counter() - t_start) * 1000
        target = (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100
        logger.info(
            "[预借 3/3] 完成! %s x%s | 费率=%+.4f%% 目标=%.3f%% | "
            "耗时 %.0fms | 等待费率到位秒开",
            base, amount, float(row["predicted_funding_rate"]) * 100,
            target, elapsed,
        )
        return True

    async def _cancel_pre_borrow(self) -> None:
        """归还预借的币 + 划回 USDT。"""
        base = self.binance_state.pre_borrow_base
        margin_symbol = self.binance_state.pre_borrow_margin_symbol
        amount = self.binance_state.pre_borrow_amount
        elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
            self.binance_state.pre_borrow_at)).total_seconds() / 60 if self.binance_state.pre_borrow_at else 0
        logger.info("[预借] 取消 | %s x%s | 已等待 %.0f min | 归还 + 划回 USDT", base, amount, elapsed)
        try:
            await self._binance_margin_repay(base, amount, margin_symbol)
        except Exception as exc:
            logger.error("[预借] 归还失败 %s: %s", base, exc)
        await self._drain_margin_to_spot(margin_symbol)
        self.binance_state.pre_borrow_base = ""
        self.binance_state.pre_borrow_margin_symbol = ""
        self.binance_state.pre_borrow_amount = 0.0
        self.binance_state.pre_borrow_at = ""
        self._save_state()

    async def _fetch_premium_index_all(self) -> list[dict[str, Any]]:
        """查询全部 USDT 永续合约溢价指数 (含资金费率). 公开接口无需签名.
        返回 [{"symbol": "BTCUSDT", "markPrice": "...", "lastFundingRate": "...", ...}, ...]"""
        url = f"{BINANCE_FUTURES_API}/fapi/v1/premiumIndex"
        def _do():
            proxies = {"http": HTTP_PROXY, "https": HTTP_PROXY} if HTTP_PROXY else None
            resp = self._rest_session.get(url, proxies=proxies, timeout=15)
            resp.raise_for_status()
            return resp.json()
        try:
            data = await asyncio.to_thread(_do)
            return data if isinstance(data, list) else []
        except Exception as e:
            logger.error("premiumIndex 查询失败: %s", e)
            return []

    async def _run_ws_monitor(self) -> None:
        """REST 快速轮询费率: 每 10s 查 fapi/v1/premiumIndex。
        fstream WS 被墙，用 REST 替代，延迟从 1s 变为 10s。"""
        if not REVERSE_ENABLED:
            return

        while True:
            try:
                data = await self._fetch_premium_index_all()
                if data:
                    await self._handle_ws_mark_price(data)
            except Exception as exc:
                logger.error("费率轮询异常: %s，10s 后重试", exc)
            await asyncio.sleep(10)

    async def _handle_ws_mark_price(self, data: list[dict[str, Any]]) -> None:
        """处理 REST premiumIndex 数据（替代 WS !markPrice@arr 解析）。
        每条: {"symbol": "BTCUSDT", "markPrice": "...", "lastFundingRate": "...", ...}"""

        if self._scan_lock.locked():
            return

        has_position = await self.has_open_arbitrage_position()
        if has_position:
            if self.binance_state.pre_borrow_base:
                await self._cancel_pre_borrow()
            return

        # 构建费率列表
        futures_index = self._build_futures_market_index()
        t0 = time.perf_counter()
        rates_data: list[dict[str, Any]] = []
        negative_count = 0
        for entry in data:
            symbol = str(entry.get("symbol", ""))
            if not symbol.endswith("USDT"):
                continue
            base = symbol[:-4]
            if base not in futures_index:
                continue
            try:
                rate = float(entry.get("lastFundingRate", 0))
                mark_price = float(entry.get("markPrice", 0))
            except (TypeError, ValueError):
                continue
            rates_data.append({
                "base": base,
                "rate": rate,
                "futures_symbol": symbol,
                "mark_price": mark_price,
            })
            if rate < 0:
                negative_count += 1

        if not rates_data:
            self._poll_count = getattr(self, '_poll_count', 0) + 1
            if self._poll_count <= 3 or self._poll_count % 30 == 0:
                logger.info("[轮询] 第 %d 次, 暂无可匹配数据", self._poll_count)
            return
        temp_df = pd.DataFrame(rates_data)
        rates_series = pd.to_numeric(temp_df["rate"])
        median = float(rates_series.median())
        std = float(rates_series.std())
        min_rate = float(rates_series.min())
        t1 = time.perf_counter()

        # ── 心跳: 每 6 轮 (~1 min) 输出一次 ──
        self._poll_count = getattr(self, '_poll_count', 0) + 1
        if self._poll_count <= 3 or self._poll_count % 6 == 0:
            open_threshold = (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100
            anomaly_threshold = median - PRE_BORROW_SIGMA * std
            anomaly_list = temp_df[temp_df["rate"] < anomaly_threshold]
            logger.info(
                "[轮询] #%d | %d 币 | 中位数=%+.4f%% σ=%.4f%% 最低=%+.4f%% | "
                "负费率 %d 个 | 2σ阈值=%+.4f%% 超出 %d 个 | 耗时 %.1fms",
                self._poll_count, len(rates_data), median * 100, std * 100, min_rate * 100,
                negative_count, anomaly_threshold * 100, len(anomaly_list),
                (t1 - t0) * 1000,
            )
            if len(anomaly_list) > 0:
                top_anomaly = anomaly_list.nsmallest(5, "rate")
                anomaly_str = " | ".join(
                    f"{r['base']}={float(r['rate'])*100:+.3f}%" for _, r in top_anomaly.iterrows()
                )
                logger.info("[轮询] 超2σ: %s", anomaly_str)
            else:
                top_neg = temp_df.nsmallest(3, "rate")
                neg_str = " | ".join(
                    f"{r['base']}={float(r['rate'])*100:+.3f}%" for _, r in top_neg.iterrows()
                )
                logger.info("[轮询] Top3 最低: %s", neg_str)

        # ── 预借状态检查 ──
        if self.binance_state.pre_borrow_base:
            pb_rows = temp_df[temp_df["base"] == self.binance_state.pre_borrow_base]
            if pb_rows.empty:
                logger.info("[预借] %s 已不在市场 → 取消", self.binance_state.pre_borrow_base)
                await self._cancel_pre_borrow()
                return
            current_rate = float(pb_rows.iloc[0]["rate"]) * 100
            elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                self.binance_state.pre_borrow_at)).total_seconds() / 60
            target = (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100
            if elapsed > PRE_BORROW_TIMEOUT_MINUTES:
                await self._cancel_pre_borrow()
                return
            if int(elapsed) % 5 == 0 and getattr(self, '_pb_last_log_min', -1) != int(elapsed):
                self._pb_last_log_min = int(elapsed)
                logger.info("[预借] %s 等待中 | 当前费率=%+.4f%% 目标=%.3f%% | 已等 %.0f/%.0f min",
                           self.binance_state.pre_borrow_base, current_rate, target,
                           elapsed, PRE_BORROW_TIMEOUT_MINUTES)
            return

        # ── 异动检测 ──
        # 冷却: 上次预借失败后 120s 内不重试(避免和主扫描打架)
        if getattr(self, '_pre_borrow_cooldown_until', 0) > time.time():
            return
        if std <= 0:
            return
        anomaly_threshold = median - PRE_BORROW_SIGMA * std
        best: dict[str, Any] | None = None
        best_rate = 0.0
        for _, row in temp_df.iterrows():
            rate = float(row["rate"])
            if rate < PRE_BORROW_MIN_RATE and rate < anomaly_threshold:
                if best is None or rate < best_rate:
                    best = row.to_dict()
                    best_rate = rate

        if best is None:
            return

        base = str(best["base"])

        # 确认有逐仓杠杆交易对，避免 KORU 这类无 margin pair 的币误触发
        borrow_check = await self._fetch_margin_borrow_rates([base])
        if base not in borrow_check:
            return

        # 先设冷却再执行，防止并发竞态
        self._pre_borrow_cooldown_until = time.time() + 120
        logger.info(
            "=" * 60 + "\n"
            "  [异动!] %s 费率暴跌 | 当前=%+.4f%% | 中位数=%+.4f%% | σ=%.4f%%\n"
            "  异动阈值=%+.4f%% (中位数-%.0fσ) | 负费率共 %d 个 | 触发预借\n"
            + "=" * 60,
            base, best_rate * 100, median * 100, std * 100,
            anomaly_threshold * 100, PRE_BORROW_SIGMA, negative_count,
        )

        if self._scan_lock.locked():
            logger.info("[异动] 全量扫描进行中，预借稍后处理。")
            return

        async with self._scan_lock:
            if await self.has_open_arbitrage_position():
                return
            if self.binance_state.pre_borrow_base:
                return

            futures_symbol = str(best["futures_symbol"])
            mark_price = float(best.get("mark_price", 0))
            minimal_row = pd.Series({
                "base": base,
                "spot_symbol": f"{base}/USDT",
                "futures_symbol": futures_symbol,
                "predicted_funding_rate": best_rate,
                "spot_last": mark_price,
                "futures_last": mark_price,
                "direction": "reverse",
            })
            logger.info("[异动] 开始预借 %s | 费率=%+.4f%% | 标记价=%.6f",
                       base, best_rate * 100, mark_price)
            ok = await self._execute_pre_borrow(minimal_row)
            if not ok:
                logger.info("[异动] 预借失败，120s 冷却。")

    async def _check_binance_position(self) -> bool:
        """验证 Binance 持仓是否仍然存在，不一致则重置状态。"""
        if not self.binance_state.is_open:
            return False

        if self.binance_state.direction == "reverse":
            futures_ok = await self._has_futures_long_position()
            if futures_ok:
                return True
            logger.warning("状态文件显示有反向持仓，但交易所未发现合约多仓，重置状态。")
            self.binance_state = ArbitrageState()
            self._save_state()
            return False

        spot_ok, futures_ok = await asyncio.gather(
            self._has_spot_balance(),
            self._has_futures_short_position(),
        )
        if spot_ok or futures_ok:
            return True

        logger.warning("状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.binance_state = ArbitrageState()
        self._save_state()
        return False

    async def has_open_arbitrage_position(self) -> bool:
        """检查是否任一交易所有套利持仓。"""
        if self.cross_state.is_open:
            return True
        binance_ok, gate_ok = await asyncio.gather(
            self._check_binance_position(),
            self._has_gate_open_position(),
        )
        return binance_ok or gate_ok

    async def _rebalance_accounts(self) -> None:
        """开仓前划转 USDT：让 spot 和 futures 余额尽量均等，最大化可用仓位。

        如果差异 < REBALANCE_THRESHOLD 则不操作。
        资金费率收入打到 futures 账户，长期运行后 spot 侧会成为瓶颈，
        所以需要定期从 futures 划回 spot。
        """
        spot_free, futures_free = await asyncio.gather(
            self._safe_request(
                "spot.fetch_balance_for_rebalance",
                lambda: self.spot.fetch_balance(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_balance_for_rebalance",
                lambda: self.futures.fetch_balance(),
                default={},
            ),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")
        if spot_usdt <= 0 and futures_usdt > 0:
            logger.warning("现货余额获取失败（测试网现货 API 可能不可用），跳过划转。")
            return
        total = spot_usdt + futures_usdt
        if total <= 0:
            return

        diff = abs(spot_usdt - futures_usdt)
        if diff / total < REBALANCE_THRESHOLD:
            return  # 差异不大，不划转

        half_diff = self._floor_usdt(diff / 2)
        if half_diff < 1.0:
            return  # 金额太小

        if spot_usdt > futures_usdt:
            logger.info("划转 %.2f USDT: spot → futures", half_diff)
            await self._safe_request(
                "transfer_spot_to_futures",
                lambda: self.spot.transfer("USDT", half_diff, "spot", "future"),
                raise_error=True,
            )
        else:
            logger.info("划转 %.2f USDT: futures → spot", half_diff)
            await self._safe_request(
                "transfer_futures_to_spot",
                lambda: self.futures.transfer("USDT", half_diff, "future", "spot"),
                raise_error=True,
            )

    async def _get_position_size(self) -> float:
        """获取动态仓位大小：取 spot 和 futures 可用 USDT 的最小值 × 比例。

        无杠杆：spot 买币 + futures 保证金各占一份，所以取两者最小值。
        """
        spot_free, futures_free = await asyncio.gather(
            self._safe_request(
                "spot.fetch_balance_for_size",
                lambda: self.spot.fetch_balance(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_balance_for_size",
                lambda: self.futures.fetch_balance(),
                default={},
            ),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")

        # 现货 API 不可用时（如测试网），用合约余额估算（假设两账户各半）
        if spot_usdt <= 0 and futures_usdt > 0 and USE_TESTNET:
            estimated_total = futures_usdt * 0.5
            logger.warning(
                "现货余额获取失败（测试网现货 API 可能不可用），假设两账户均分: 每腿 ~%.2f USDT",
                estimated_total * POSITION_SIZE_RATIO,
            )
            size = estimated_total * POSITION_SIZE_RATIO
        else:
            size = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO

        if size <= 0:
            logger.error("可用 USDT 余额为 0: spot=%s futures=%s", spot_usdt, futures_usdt)
            return 0.0

        logger.info(
            "动态仓位: spot可用=%.2f futures可用=%.2f → 每腿名义=%.2f USDT",
            spot_usdt, futures_usdt, size,
        )
        return size

    async def has_enough_bnb_for_fees(self) -> bool:
        """Check spot wallet has enough BNB for fee discount (covers both spot + futures)."""
        if not USE_BNB_FEE_DISCOUNT:
            return True
        if USE_TESTNET:
            return True  # 测试网现货 API 不可用，跳过 BNB 检查

        spot_balance = await self._safe_request(
            "spot.fetch_balance_for_bnb_fee_check",
            lambda: self.spot.fetch_balance(),
            default={},
        )
        spot_bnb = self._free_balance(spot_balance, "BNB")
        if spot_bnb < MIN_BNB_BALANCE_FOR_FEES:
            logger.error(
                "现货 BNB 余额不足，无法享受手续费折扣: spot_bnb=%s min=%s",
                spot_bnb, MIN_BNB_BALANCE_FOR_FEES,
            )
            return False
        return True

    async def _safe_binance_balance(self, base_url: str, path: str) -> float:
        """直连 Binance REST 查 USDT 余额，ccxt 查不到时的 fallback。"""
        try:
            data = await self._binance_request(base_url, path)
            if path == "/api/v3/account":
                for b in data.get("balances", []):
                    if b.get("asset") == "USDT":
                        return float(b.get("free", 0))
            else:
                for b in (data if isinstance(data, list) else []):
                    if b.get("asset") == "USDT":
                        return float(b.get("balance", 0))
        except Exception:
            pass
        return 0.0

    @staticmethod
    def _free_balance(balance: dict[str, Any], asset: str) -> float:
        value = balance.get("free", {}).get(asset, 0)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    async def _has_spot_balance(self) -> bool:
        if not self.binance_state.base:
            return False

        balance = await self._safe_request(
            "spot.fetch_balance",
            lambda: self.spot.fetch_balance(),
            default={},
        )
        free = balance.get("free", {}).get(self.binance_state.base, 0)
        used = balance.get("used", {}).get(self.binance_state.base, 0)
        try:
            return float(free) + float(used) > self.binance_state.amount * 0.5
        except (TypeError, ValueError):
            return False

    async def _has_futures_short_position(self) -> bool:
        if not self.binance_state.futures_symbol:
            return False

        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.binance_state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.binance_state.futures_symbol:
                continue
            contracts = self._position_contracts(position)
            return contracts < 0 or (
                position.get("side") == "short" and abs(contracts) > 0
            )
        return False

    async def _has_futures_long_position(self) -> bool:
        if not self.binance_state.futures_symbol:
            return False
        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.binance_state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.binance_state.futures_symbol:
                continue
            contracts = self._position_contracts(position)
            return contracts > 0 or (
                position.get("side") == "long" and abs(contracts) > 0
            )
        return False

    @staticmethod
    def _position_contracts(position: dict[str, Any]) -> float:
        for key in ("contracts", "contractSize"):
            value = position.get(key)
            if value is None:
                continue
            try:
                contracts = float(value)
                if key == "contracts":
                    return contracts
            except (TypeError, ValueError):
                pass

        info = position.get("info") or {}
        raw_amount = info.get("positionAmt", 0)
        try:
            return float(raw_amount)
        except (TypeError, ValueError):
            return 0.0

    async def open_arbitrage_position(self, row: pd.Series) -> bool:
        """并发执行现货买入和合约开空（正向）或借币卖出+合约开多（反向）。

        现货来源: main 主站、alpha 预上线市场或 margin 逐仓杠杆。
        Alpha 开仓前会查询链上实时 Gas，超标则跳过。
        """
        exchange = str(row.get("exchange", "binance"))
        if exchange == "gate":
            direction = str(row.get("direction", "forward"))
            if direction == "reverse":
                return await self._open_gate_reverse_position(row)
            return await self._open_gate_forward_position(row)

        direction = str(row.get("direction", "forward"))
        if direction == "reverse":
            return await self._open_reverse_position(row)

        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        spot_source = str(row.get("spot_source", "spot"))

        # 先把逐仓杠杆里闲置的 USDT 收回 spot（反向失败留下的）
        await self._reclaim_all_usdt()
        # 划转资金使两账户均衡，再按可用余额计算每腿名义
        await self._rebalance_accounts()
        position_usdt = await self._get_position_size()
        if position_usdt <= 0:
            logger.error("可用余额不足，放弃开仓。")
            return False

        # 检查是否在结算窗口内
        next_ft_ms = float(row.get("next_funding_time_ms", 0))
        if not self._within_entry_window(next_ft_ms):
            remaining_min = (next_ft_ms - time.time() * 1000) / 60_000 if next_ft_ms > 0 else 0
            logger.info(
                "距结算还有 %.0f 分钟，超出窗口 (%d min)，不开仓。费率可能变化。",
                remaining_min, ENTRY_WINDOW_MINUTES,
            )
            return False

        if spot_source == "alpha":
            chain = str(row.get("chain", ""))
            alpha_fee_ratio = float(row.get("alpha_fee_ratio", 0))
            expected_gas = alpha_fee_ratio * position_usdt
            if expected_gas > 0 and not await self._check_alpha_gas(chain, expected_gas):
                logger.info("Alpha Gas 超标，放弃本次开仓。")
                return False

        price = self._select_reference_price(row)

        if not price or price <= 0:
            logger.error("参考价格无效，放弃开仓: %s", row.to_dict())
            return False

        amount = self._calculate_precise_amount(
            spot_symbol=spot_symbol,
            futures_symbol=futures_symbol,
            reference_price=price,
            spot_source=spot_source,
            total_usdt=position_usdt,
        )
        if amount <= 0:
            logger.error("精度处理后的下单数量无效，放弃开仓。")
            return False

        logger.info(
            "触发开仓: %s [%s] | 数量=%s | 参考价=%s | 预测资金费率=%.6f | 净收益=%.6f",
            spot_symbol,
            spot_source,
            amount,
            price,
            row["predicted_funding_rate"],
            row["net_rate"],
        )

        spot_task = self._open_spot_leg(spot_symbol, amount, spot_source)
        futures_task = self._open_futures_short_leg(futures_symbol, amount)
        spot_result, futures_result = await asyncio.gather(
            spot_task,
            futures_task,
        )

        if spot_result.ok and futures_result.ok:
            # 获取合约下次结算时间，跳过即将发生的结算，瞄准下一个
            next_ft = await self._fetch_next_funding_time(futures_symbol)
            next_ft_dt = datetime.fromtimestamp(next_ft / 1000, tz=self.tz) if next_ft > 0 else None
            if next_ft_dt:
                logger.info(
                    "合约 %s 下次结算: %s (距今 %.0f 分钟)",
                    futures_symbol,
                    next_ft_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    (next_ft - time.time() * 1000) / 60000,
                )

            self.binance_state = ArbitrageState(
                is_open=True,
                spot_symbol=spot_symbol,
                futures_symbol=futures_symbol,
                base=str(row["base"]),
                amount=amount,
                spot_order_id=str(spot_result.order.get("id")),
                futures_order_id=str(futures_result.order.get("id")),
                entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.tz).isoformat(),
                spot_source=spot_source,
                next_funding_time_ms=next_ft,
            )
            self._save_state()
            self._print_position_summary()
            await asyncio.to_thread(
                self._send_email,
                f"bazfbot 开仓: {row['base']}",
                f"币种: {row['base']} [{spot_source}]\n"
                f"数量: {amount}\n"
                f"费率: {float(row['predicted_funding_rate'])*100:.3f}%\n"
                f"净收益: {float(row['net_rate'])*100:.3f}%\n"
                f"结算: {datetime.fromtimestamp(next_ft/1000, tz=self.tz).strftime('%H:%M')}",
            )
            return True

        if spot_result.ok or futures_result.ok:
            # 只有一腿成交才需要应急平仓
            logger.critical(
                "双腿下单未同时成功，触发应急平仓。spot=%s futures=%s",
                spot_result, futures_result,
            )
            await self._emergency_close_exposed_leg(spot_result, futures_result)
        else:
            logger.warning("双腿均未成交: spot=%s futures=%s — 无需应急。", spot_result, futures_result)
        return False

    async def _open_reverse_position(self, row: pd.Series) -> bool:
        """反向套利开仓: 借币 → margin卖出 → 合约开多。任一环节失败则全部撤销。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])
        margin_symbol = spot_symbol  # same pair, e.g. "BTC/USDT"

        if self._borrow_blacklist.get(base, 0) > time.time():
            remaining = int(self._borrow_blacklist[base] - time.time())
            logger.info("%s 借币池黑名单中 (剩余 %ds)，跳过。", base, remaining)
            return False

        # [1/5] 回收闲置资金（跳过目标交易对，避免划走又划回）+ 50/50 分配
        await self._reclaim_all_usdt(keep_symbol=margin_symbol)
        spot_bal, futures_bal, target_margin_acct = await asyncio.gather(
            self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
            self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
            self._get_isolated_margin_account(margin_symbol),
        )
        spot_usdt = self._free_balance(spot_bal, "USDT")
        futures_usdt = self._free_balance(futures_bal, "USDT")
        target_quote = target_margin_acct.get("quoteAsset", {})
        target_margin_usdt = float(target_quote.get("netAsset", 0)) if target_quote else 0.0
        total_usdt = spot_usdt + futures_usdt + target_margin_usdt
        half = total_usdt / 2
        if half <= 0:
            logger.error("[1/5] 总余额不足，放弃反向开仓。")
            return False

        # 如果目标逐仓交易对 USDT 超过 half（上次失败遗留），先划回现货用于合约侧
        if target_margin_usdt > half + 1.0:
            excess = self._floor_usdt(target_margin_usdt - half)
            logger.info("[1/5] 逐仓杠杆 USDT 过多 (%.2f > half %.2f)，划回 %.2f 到现货",
                        target_margin_usdt, half, excess)
            try:
                await self._binance_isolated_margin_transfer(
                    "USDT", excess, margin_symbol, "margin_to_spot",
                )
                target_margin_usdt = half
                spot_usdt += excess
            except Exception as exc:
                logger.warning("[1/5] 划回失败: %s，跳过。", exc)

        # 必要时补足合约侧（做多保证金），只划差额
        if futures_usdt < half:
            need = self._floor_usdt(min(half - futures_usdt, spot_usdt))
            if need >= 1.0:
                logger.info("[1/5] 划转 %.2f USDT: spot → futures（补做多保证金）", need)
                try:
                    await self._safe_request(
                        "transfer_spot_to_futures",
                        lambda: self.spot.transfer("USDT", need, "spot", "future"),
                        raise_error=True,
                    )
                    spot_usdt -= need
                    futures_usdt += need
                except Exception as exc:
                    logger.error("[1/5] spot→futures 划转失败: %s，放弃开仓。", exc)
                    return False
        # 合约有多余且现货不够抵押时，划回 spot
        elif spot_usdt < half and futures_usdt > half:
            need = self._floor_usdt(min(half - spot_usdt, futures_usdt - half))
            if need >= 1.0:
                logger.info("[1/5] 划转 %.2f USDT: futures → spot（补抵押金）", need)
                try:
                    await self._safe_request(
                        "transfer_futures_to_spot",
                        lambda: self.futures.transfer("USDT", need, "future", "spot"),
                        raise_error=True,
                    )
                    spot_usdt += need
                    futures_usdt -= need
                except Exception as exc:
                    logger.error("[1/5] futures→spot 划转失败: %s，放弃开仓。", exc)
                    return False

        position_usdt = half * POSITION_SIZE_RATIO

        # [2/5] 确保逐仓交易对已启用 + 划转 50% 资金 → 逐仓杠杆
        if not await self._ensure_margin_pair_enabled(margin_symbol):
            logger.error("[2/5] 无法启用逐仓交易对 %s（已达上限且无法释放）", base)
            return False

        margin_acct = await self._get_isolated_margin_account(margin_symbol)
        quote_asset = margin_acct.get("quoteAsset", {})
        margin_usdt = float(quote_asset.get("netAsset", 0)) if quote_asset else 0.0

        # 直接查现货账户 USDT 余额（绕过 ccxt 缓存，避免 _reclaim_all_usdt 划转后数据过期）
        spot_actual = spot_usdt
        try:
            spot_acct = await self._binance_request(BINANCE_SPOT_API, "/api/v3/account")
            for b in (spot_acct.get("balances", []) if isinstance(spot_acct, dict) else []):
                if b.get("asset") == "USDT":
                    spot_actual = float(b.get("free", 0))
                    break
        except Exception:
            pass

        shortfall = self._floor_usdt(min(max(0.0, half - margin_usdt), spot_actual))
        # 留 0.01 缓冲，避免余额刚好等于划转金额时被币安拒绝
        transfer_amt = max(0.0, shortfall - 0.01)
        if transfer_amt < 1.0:
            logger.info("[2/5] 逐仓杠杆已有 %.2f USDT ≥ %.2f，跳过划转。", margin_usdt, half)
        else:
            logger.info("[2/5] 划转 %.2f USDT 到逐仓杠杆 [%s]（50%%抵押 + 另50%%在合约做多）",
                        transfer_amt, base)
            try:
                await self._binance_isolated_margin_transfer(
                    "USDT", transfer_amt, margin_symbol, "spot_to_margin",
                )
            except Exception as exc:
                logger.error("[2/5] 划转抵押失败: %s", exc)
                return False

        # [3/5] 计算数量 + 借币（反向不限进场窗口）
        price = self._select_reference_price(row)
        if not price or price <= 0:
            logger.error("[3/5] 参考价格无效，划回 USDT 并放弃开仓。")
            await self._drain_margin_to_spot(margin_symbol)
            return False

        amount = self._calculate_precise_amount(
            spot_symbol=spot_symbol, futures_symbol=futures_symbol,
            reference_price=price, spot_source="margin", total_usdt=position_usdt,
        )
        if amount <= 0:
            logger.error("[3/5] 精度处理后的下单数量无效，划回 USDT 并放弃开仓。")
            await self._drain_margin_to_spot(margin_symbol)
            return False

        logger.info("[3/5] 价格=%.6f 数量=%s 名义=%.2f USDT", price, amount, amount * price)

        # 检查是否已预借 (pre-borrow)
        base_asset = margin_acct.get("baseAsset", {}) if margin_acct else {}
        already_borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0
        if already_borrowed >= amount:
            logger.info("[3/5] 已预借 %s x%s (borrowed=%.4f)，跳过借币。", base, amount, already_borrowed)
        else:
            need_borrow = amount - already_borrowed if already_borrowed > 0 else amount
            can_borrow, max_borrowable = await self._binance_margin_max_borrowable(base, margin_symbol)
            if not can_borrow or max_borrowable < need_borrow:
                logger.error("[3/5] 无法借够 %s: need=%s max=%s，划回 USDT 并放弃开仓。",
                             base, need_borrow, max_borrowable)
                await self._drain_margin_to_spot(margin_symbol)
                return False

            try:
                await self._binance_margin_loan(base, need_borrow, margin_symbol)
            except Exception as exc:
                logger.error("[3/5] 借币 %s API 失败: %s，划回 USDT 并放弃开仓。", base, exc)
                await self._drain_margin_to_spot(margin_symbol)
                return False

        # [4/5] 并发下单 — margin 卖出 + 合约开多
        logger.info(
            "[4/5] 下单: sell %s %s [margin] + buy %s %s [long] | 费率=%.4f%%",
            amount, base, amount, futures_symbol,
            float(row["predicted_funding_rate"]) * 100,
        )
        margin_task = self._open_margin_spot_leg(spot_symbol, amount)
        futures_task = self._open_futures_long_leg(futures_symbol, amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        # [5/5] 处理结果
        if margin_result.ok and futures_result.ok:
            next_ft = await self._fetch_next_funding_time(futures_symbol)
            next_ft_dt = datetime.fromtimestamp(next_ft / 1000, tz=self.tz) if next_ft > 0 else None
            if next_ft_dt:
                logger.info("合约 %s 下次结算: %s (距今 %.0f 分钟)",
                            futures_symbol, next_ft_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            (next_ft - time.time() * 1000) / 60000)

            self.binance_state = ArbitrageState(
                is_open=True,
                spot_symbol=spot_symbol,
                futures_symbol=futures_symbol,
                base=base,
                amount=amount,
                spot_order_id=str(margin_result.order.get("id")),
                futures_order_id=str(futures_result.order.get("id")),
                entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.tz).isoformat(),
                spot_source="margin",
                next_funding_time_ms=next_ft,
                direction="reverse",
            )
            self._save_state()
            self._record_margin_used(margin_symbol)
            self._print_position_summary()
            await asyncio.to_thread(
                self._send_email,
                f"bazfbot 反向开仓: {base}",
                f"币种: {base} [margin]\n数量: {amount}\n负费率: {float(row['predicted_funding_rate'])*100:.3f}%\n"
                f"净收益: {float(row['net_rate'])*100:.3f}%\n"
                f"结算: {datetime.fromtimestamp(next_ft/1000, tz=self.tz).strftime('%H:%M')}",
            )
            return True

        # 应急撤销：仅当至少一腿成交时才需要
        if margin_result.ok or futures_result.ok:
            logger.critical("[!!] 反向开仓双腿未同时成交！margin=%s futures=%s", margin_result, futures_result)
            await self._emergency_close_exposed_leg(
                margin_result, futures_result,
                direction="reverse", margin_symbol=margin_symbol, base=base,
            )
            await self._cleanup_margin_pair(base, margin_symbol, position_usdt)
        else:
            logger.warning("反向双腿均未成交: margin=%s futures=%s — 无需应急。", margin_result, futures_result)
            # USDT 留在逐仓杠杆，不划回，下轮可直接重试
        return False

    @staticmethod
    def _select_reference_price(row: pd.Series) -> float:
        for key in ("spot_last", "futures_last"):
            value = row.get(key)
            try:
                if value and float(value) > 0:
                    return float(value)
            except (TypeError, ValueError):
                continue
        return 0.0

    def _calculate_precise_amount(
        self,
        spot_symbol: str,
        futures_symbol: str,
        reference_price: float,
        spot_source: str = "spot",
        total_usdt: float = 100.0,
    ) -> float:
        raw_amount = total_usdt / reference_price

        futures_amount = float(
            self.futures.amount_to_precision(futures_symbol, raw_amount)
        )
        futures_market = self.futures.market(futures_symbol)
        min_futures = self._market_min_amount(futures_market)

        if spot_source == "alpha":
            # Alpha has stepSize 0.01, so round down to nearest 0.01.
            spot_amount = math.floor(raw_amount * 100) / 100
            min_spot = 0.01
        else:
            spot_amount = float(self.spot.amount_to_precision(spot_symbol, raw_amount))
            spot_market = self.spot.market(spot_symbol)
            min_spot = self._market_min_amount(spot_market)

        amount = min(spot_amount, futures_amount)
        min_amount = max(min_spot, min_futures)

        if amount < min_amount:
            logger.error(
                "下单数量低于交易所最小数量: amount=%s min=%s",
                amount,
                min_amount,
            )
            return 0.0

        notional = amount * reference_price
        min_notional: float = 5.0  # Binance 最低名义价值
        spot_mkt = self.spot.market(spot_symbol)
        spot_min_cost = (spot_mkt.get("limits") or {}).get("cost", {}).get("min")
        if spot_min_cost:
            min_notional = max(min_notional, float(spot_min_cost))
        fut_min_cost = (futures_market.get("limits") or {}).get("cost", {}).get("min")
        if fut_min_cost:
            min_notional = max(min_notional, float(fut_min_cost))

        if notional < min_notional:
            logger.error(
                "名义价值不足: %.2f USDT < %.0f USDT（最低限制），放弃开仓。",
                notional, min_notional,
            )
            return 0.0

        if notional > total_usdt * 1.01:
            amount = math.floor((total_usdt / reference_price) * 1e8) / 1e8
            amount = float(self.spot.amount_to_precision(spot_symbol, amount))
            amount = float(self.futures.amount_to_precision(futures_symbol, amount))

        return amount

    @staticmethod
    def _market_min_amount(market: dict[str, Any]) -> float:
        limits = market.get("limits") or {}
        amount_limits = limits.get("amount") or {}
        value = amount_limits.get("min")
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    async def _open_spot_leg(
        self, symbol: str, amount: float, spot_source: str = "spot"
    ) -> LegResult:
        if spot_source == "alpha":
            return await self._open_alpha_spot_leg(symbol, amount)
        if spot_source == "margin":
            return await self._open_margin_spot_leg(symbol, amount)
        try:
            order = await self._binance_spot_order(symbol, "buy", amount)
            return LegResult(True, "spot", symbol, "buy", amount, order=order)
        except Exception as exc:
            logger.error("现货买入失败 %s: %s", symbol, exc)
            return LegResult(False, "spot", symbol, "buy", amount, error=str(exc))

    async def _open_alpha_spot_leg(self, symbol: str, amount: float) -> LegResult:
        logger.info("Alpha 现货买入: symbol=%s amount=%s", symbol, amount)
        try:
            order = await self._binance_spot_order(symbol, "buy", amount)
            return LegResult(True, "alpha_spot", symbol, "buy", amount, order=order)
        except Exception as exc:
            logger.error("Alpha 现货买入失败 %s: %s", symbol, exc)
            return LegResult(False, "alpha_spot", symbol, "buy", amount, error=str(exc))

    async def _open_futures_short_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._binance_futures_order(symbol, "sell", amount)
            return LegResult(True, "futures", symbol, "sell", amount, order=order)
        except Exception as exc:
            logger.error("合约开空失败 %s: %s", symbol, exc)
            return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))

    async def _open_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._binance_futures_order(symbol, "buy", amount)
            return LegResult(True, "futures", symbol, "buy", amount, order=order)
        except Exception as exc:
            logger.error("合约开多失败 %s: %s", symbol, exc)
            return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))

    async def _emergency_close_exposed_leg(
        self,
        spot_result: LegResult,
        futures_result: LegResult,
        direction: str = "forward",
        margin_symbol: str | None = None,
        base: str | None = None,
    ) -> None:
        """若只成交一条腿，立刻用市价单撤销风险敞口。"""
        close_tasks: list = []
        repay_task: Any | None = None

        if direction == "reverse" and margin_symbol and base:
            if spot_result.ok and not futures_result.ok:
                logger.critical("Margin 已卖出但合约开多失败，买回并归还借款。")
                close_tasks.append(
                    self._close_margin_spot_leg(spot_result.symbol, spot_result.amount)
                )
            if futures_result.ok and not spot_result.ok:
                logger.critical("合约已开多但 Margin 卖出失败，平多。")
                close_tasks.append(
                    self._close_futures_long_leg(futures_result.symbol, futures_result.amount)
                )
            # Always try to repay the borrowed coin
            if not spot_result.ok:
                repay_task = self._binance_margin_repay(base, spot_result.amount, margin_symbol)
        else:
            if spot_result.ok and not futures_result.ok:
                logger.critical("现货已买入但合约开空失败，市价卖出现货。")
                spot_source = "alpha" if spot_result.market_type == "alpha_spot" else "spot"
                close_tasks.append(
                    self._close_spot_leg(spot_result.symbol, spot_result.amount, spot_source)
                )
            if futures_result.ok and not spot_result.ok:
                logger.critical("合约已开空但现货买入失败，市价平空合约。")
                close_tasks.append(
                    self._close_futures_short_leg(futures_result.symbol, futures_result.amount)
                )

        if close_tasks:
            await asyncio.gather(*close_tasks)
        if repay_task:
            try:
                await repay_task
            except Exception as exc:
                logger.error("应急还款失败: %s", exc)
        if close_tasks or repay_task:
            await asyncio.to_thread(
                self._send_email,
                "bazfbot 应急平仓！",
                "下单出现单腿成交，已反向平仓止损。请立即检查持仓。",
            )

    async def _close_spot_leg(
        self, symbol: str, amount: float, spot_source: str = "spot"
    ) -> LegResult:
        if spot_source == "alpha":
            return await self._close_alpha_spot_leg(symbol, amount)
        if spot_source == "margin":
            return await self._close_margin_spot_leg(symbol, amount)
        precise = float(self.spot.amount_to_precision(symbol, amount))
        try:
            order = await self._binance_spot_order(symbol, "sell", precise)
            return LegResult(True, "spot", symbol, "sell", precise, order=order)
        except Exception as exc:
            logger.error("现货卖出失败 %s: %s", symbol, exc)
            return LegResult(False, "spot", symbol, "sell", precise, error=str(exc))

    async def _close_alpha_spot_leg(self, symbol: str, amount: float) -> LegResult:
        logger.info("Alpha 现货卖出: symbol=%s amount=%s", symbol, amount)
        precise = math.floor(amount * 100) / 100
        try:
            order = await self._binance_spot_order(symbol, "sell", precise)
            return LegResult(True, "alpha_spot", symbol, "sell", precise, order=order)
        except Exception as exc:
            logger.error("Alpha 现货卖出失败 %s: %s", symbol, exc)
            return LegResult(False, "alpha_spot", symbol, "sell", precise, error=str(exc))

    async def _open_margin_spot_leg(self, symbol: str, amount: float) -> LegResult:
        """反向套利：卖出借来的币（margin 现货卖单）."""
        logger.info("Margin 卖出: symbol=%s amount=%s", symbol, amount)
        try:
            order = await self._binance_margin_order(symbol, "sell", amount)
            return LegResult(True, "margin", symbol, "sell", amount, order=order)
        except Exception as exc:
            logger.error("Margin 卖出失败 %s: %s", symbol, exc)
            return LegResult(False, "margin", symbol, "sell", amount, error=str(exc))

    async def _close_margin_spot_leg(self, symbol: str, amount: float) -> LegResult:
        """反向套利平仓：买回币还借款（margin 现货买单）."""
        precise = float(self.spot.amount_to_precision(symbol, amount))
        logger.info("Margin 买回: symbol=%s amount=%s", symbol, precise)
        try:
            order = await self._binance_margin_order(symbol, "buy", precise)
            return LegResult(True, "margin", symbol, "buy", precise, order=order)
        except Exception as exc:
            logger.error("Margin 买回失败 %s: %s", symbol, exc)
            return LegResult(False, "margin", symbol, "buy", precise, error=str(exc))

    async def _close_futures_short_leg(self, symbol: str, amount: float) -> LegResult:
        precise = float(self.futures.amount_to_precision(symbol, amount))
        try:
            order = await self._binance_futures_order(symbol, "buy", precise, reduce_only=True)
            return LegResult(True, "futures", symbol, "buy", precise, order=order)
        except Exception as exc:
            logger.error("合约平空失败 %s: %s", symbol, exc)
            return LegResult(False, "futures", symbol, "buy", precise, error=str(exc))

    async def _close_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        precise = float(self.futures.amount_to_precision(symbol, amount))
        try:
            order = await self._binance_futures_order(symbol, "sell", precise, reduce_only=True)
            return LegResult(True, "futures", symbol, "sell", precise, order=order)
        except Exception as exc:
            logger.error("合约平多失败 %s: %s", symbol, exc)
            return LegResult(False, "futures", symbol, "sell", precise, error=str(exc))

    async def _fetch_next_funding_time(self, futures_symbol: str) -> float:
        """获取合约的 nextFundingTime (毫秒时间戳)。失败则按 8h 估算。"""
        try:
            info = await self._safe_request(
                f"futures.fetch_funding_rate({futures_symbol})",
                lambda: self.futures.fetch_funding_rate(futures_symbol),
                default={},
            )
            nft = info.get("nextFundingTime") or info.get("nextFundingTimestamp")
            if nft and float(nft) > 0:
                return float(nft)
        except Exception as e:
            logger.warning("获取 %s 结算时间失败: %s，按 8h 估算", futures_symbol, e)

        # fallback: 现在 + 8 小时
        return time.time() * 1000 + DEFAULT_FUNDING_INTERVAL_HOURS * 3600_000

    def _should_exit(self, exchange: str = "binance") -> bool:
        """持仓决策时机: 自由人模式随时触发，锁定期等结算过后才触发。"""
        state = self.gate_state if exchange == "gate" else self.binance_state
        if not state.is_open:
            return False
        if not state.locked:
            return True  # 自由人模式，每分钟扫描换仓机会
        if state.next_funding_time_ms <= 0:
            return False
        return time.time() * 1000 >= state.next_funding_time_ms


    async def close_arbitrage_position(self, exchange: str = "binance") -> bool:
        """结算后双腿并发平仓。正向: 卖出现货+买入平空。反向: margin买回+卖出平多+还款。"""
        if exchange == "gate":
            if not self.gate_state.is_open:
                return True
            if self.gate_state.direction == "reverse":
                return await self._close_gate_reverse_position()
            return await self._close_gate_forward_position()

        if not self.binance_state.is_open:
            return True
        if self.binance_state.direction == "reverse":
            return await self._close_reverse_position()

        logger.info(
            "开始平仓套利头寸: spot=%s futures=%s amount=%s",
            self.binance_state.spot_symbol,
            self.binance_state.futures_symbol,
            self.binance_state.amount,
        )

        spot_task = self._close_spot_leg(
            self.binance_state.spot_symbol,
            self.binance_state.amount,
            self.binance_state.spot_source,
        )
        futures_task = self._close_futures_short_leg(
            self.binance_state.futures_symbol,
            self.binance_state.amount,
        )
        spot_result, futures_result = await asyncio.gather(spot_task, futures_task)

        # 重试失败的腿（最多3次）
        for attempt in range(3):
            if spot_result.ok and futures_result.ok:
                break
            if not spot_result.ok:
                logger.warning("现货卖出失败，重试 %d/3", attempt + 1)
                spot_result = await self._close_spot_leg(
                    self.binance_state.spot_symbol, self.binance_state.amount, self.binance_state.spot_source,
                )
            if not futures_result.ok:
                logger.warning("合约平空失败，重试 %d/3", attempt + 1)
                futures_result = await self._close_futures_short_leg(
                    self.binance_state.futures_symbol, self.binance_state.amount,
                )
            await asyncio.sleep(1.0)

        if spot_result.ok and futures_result.ok:
            logger.info("套利平仓成功。现货卖出+合约买入平空均已完成。")
            self._record_close_trade(self.binance_state)
            self.binance_state = ArbitrageState()
            self._save_state()
            await asyncio.to_thread(
                self._send_email, "bazfbot 平仓", "双腿已平，仓位已清空。"
            )
            return True

        # 重试耗尽：恢复对冲
        logger.critical("平仓失败（重试3次），尝试恢复对冲。spot=%s futures=%s", spot_result, futures_result)
        if spot_result.ok and not futures_result.ok:
            logger.critical("合约平空失败，买回现货恢复对冲")
            await self._open_spot_leg(
                self.binance_state.spot_symbol, self.binance_state.amount, self.binance_state.spot_source,
            )
        elif futures_result.ok and not spot_result.ok:
            logger.critical("现货卖出失败，重开空单恢复对冲")
            await self._open_futures_short_leg(
                self.binance_state.futures_symbol, self.binance_state.amount,
            )
        await asyncio.to_thread(
            self._send_email,
            "bazfbot 平仓失败！",
            "平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    async def _close_reverse_position(self) -> bool:
        """平仓反向套利: margin买回 + 合约平多 + 还款 + 划回 USDT。"""
        if not self.binance_state.is_open or self.binance_state.direction != "reverse":
            return True

        logger.info(
            "[平仓 1/4] 反向平仓: margin=%s futures=%s amount=%s",
            self.binance_state.spot_symbol, self.binance_state.futures_symbol, self.binance_state.amount,
        )

        # [1] 并发 — margin买回 + 合约平多
        margin_task = self._close_margin_spot_leg(self.binance_state.spot_symbol, self.binance_state.amount)
        futures_task = self._close_futures_long_leg(self.binance_state.futures_symbol, self.binance_state.amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        # 重试失败的腿（最多3次）
        for attempt in range(3):
            if margin_result.ok and futures_result.ok:
                break
            if not margin_result.ok:
                logger.warning("Margin 买回失败，重试 %d/3", attempt + 1)
                margin_result = await self._close_margin_spot_leg(
                    self.binance_state.spot_symbol, self.binance_state.amount,
                )
            if not futures_result.ok:
                logger.warning("合约平多失败，重试 %d/3", attempt + 1)
                futures_result = await self._close_futures_long_leg(
                    self.binance_state.futures_symbol, self.binance_state.amount,
                )
            await asyncio.sleep(1.0)

        if not margin_result.ok or not futures_result.ok:
            logger.critical(
                "[平仓 1/4] 下单异常（重试3次），尝试恢复对冲。margin=%s futures=%s",
                margin_result, futures_result,
            )
            if margin_result.ok and not futures_result.ok:
                logger.critical("合约平多失败，卖出 margin 现货恢复对冲")
                await self._open_margin_spot_leg(self.binance_state.spot_symbol, self.binance_state.amount)
            elif futures_result.ok and not margin_result.ok:
                logger.critical("Margin 买回失败，重开多单恢复对冲")
                await self._open_futures_long_leg(self.binance_state.futures_symbol, self.binance_state.amount)
            await asyncio.to_thread(
                self._send_email,
                "bazfbot 平仓失败！",
                "反向平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
            )
            return False

        # [2] 查询负债 + 还款
        base = self.binance_state.base
        amount = self.binance_state.amount
        margin_symbol = self.binance_state.spot_symbol
        logger.info("[平仓 2/4] 查询逐仓负债...")
        margin_acct = await self._get_isolated_margin_account(margin_symbol)
        base_asset = margin_acct.get("baseAsset", {})
        borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0
        repay_amount = max(borrowed, amount)
        logger.info("[平仓 2/4] 还款 %s %s (借入=%s 负债=%s)", repay_amount, base, amount, borrowed)
        try:
            await self._binance_margin_repay(base, repay_amount, margin_symbol)
        except Exception as exc:
            logger.error("还款失败需手动处理: %s", exc)
            # 尝试用买入的全部余额还款
            try:
                net_asset = float(base_asset.get("netAsset", amount)) if base_asset else amount
                await self._binance_margin_repay(base, net_asset, margin_symbol)
            except Exception as exc2:
                logger.error("二次还款也失败: %s", exc2)

        # [3] 划回所有 USDT（逐仓杠杆 → spot）
        margin_acct = await self._get_isolated_margin_account(margin_symbol)
        quote_asset = margin_acct.get("quoteAsset", {})
        quote_net = float(quote_asset.get("netAsset", 0)) if quote_asset else 0.0
        transfer_out = self._floor_usdt(quote_net)
        if transfer_out > 0:
            logger.info("[平仓 3/4] 划回 %.2f USDT: margin → spot", transfer_out)
            try:
                await self._binance_isolated_margin_transfer(
                    "USDT", transfer_out, margin_symbol, "margin_to_spot",
                )
            except Exception:
                pass

        logger.info("[平仓 4/4] 反向套利平仓完成: 平多+买回+还款。")
        self._record_close_trade(self.binance_state)
        self.binance_state = ArbitrageState()
        self._save_state()
        await asyncio.to_thread(
            self._send_email, "bazfbot 反向平仓", "双腿已平，借款已归还，仓位已清空。"
        )
        return True

    # ── 跨交易所资金费率套利：交易方法 ──

    async def _cross_get_bn_futures_balance(self) -> float:
        """获取 Binance 合约 USDT 可用余额。"""
        bal = await self._safe_request(
            "futures.fetch_balance_x",
            lambda: self.futures.fetch_balance(),
            default={},
        )
        return self._free_balance(bal, "USDT")

    async def _cross_get_bn_total_balance(self) -> float:
        """获取 Binance 合约 USDT 总余额 (可用 + 锁定保证金)。"""
        try:
            raw = await self._binance_request(BINANCE_FUTURES_API, "/fapi/v2/balance", {})
            for b in (raw or []):
                if b.get("asset") == "USDT":
                    return float(b.get("balance", 0) or 0)
        except Exception:
            pass
        return 0.0

    async def _cross_verify_binance_position(self, symbol: str, position_side: str) -> float:
        """验证币安实际持仓量（处理 API 返回 filled=0 但实际成交的情况）。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v2/positionRisk",
                {"symbol": clean},
            )
            if isinstance(resp, list):
                for pos in resp:
                    if isinstance(pos, dict) and pos.get("positionSide") == position_side:
                        return abs(float(pos.get("positionAmt", 0) or 0))
            elif isinstance(resp, dict) and resp.get("positionSide") == position_side:
                return abs(float(resp.get("positionAmt", 0) or 0))
        except Exception as exc:
            logger.warning("验证币安持仓失败 %s: %s", symbol, exc)
        return 0.0

    async def _cross_verify_gate_position(self, symbol: str) -> float:
        """验证 Gate 合约实际持仓量（处理 API 返回异常但实际已成交的情况）。"""
        try:
            positions = await self._safe_request(
                "gate_futures.fetch_positions_verify",
                lambda: self._fetch_gate_positions_direct(symbol),
                default=[],
            )
            for pos in positions:
                if pos.get("symbol") == symbol:
                    return abs(float(pos.get("contracts", 0) or 0))
        except Exception as exc:
            logger.warning("验证Gate持仓失败 %s: %s", symbol, exc)
        return 0.0

    def _ws_position_check(self, exchange: str, symbol: str, position_side: str,
                           expect_zero: bool, amount: float) -> tuple[bool, float]:
        """WS 缓存快速查持仓（免 REST）。返回 (confirmed, position_size)。
        cache miss 返回 (False, -1) 表示需等待或 REST 兜底。"""
        if exchange == "binance":
            clean = self._clean_futures_symbol(symbol)
            pos = self._bn_ws_positions.get(f"{clean}|{position_side}")
        else:
            pos = self._gate_ws_positions.get(self._to_gate_contract(symbol))
        if pos is None:
            return False, -1.0
        if expect_zero:
            return pos < amount * 0.1, pos
        return pos > 0, pos

    async def _query_bn_funding_amount(self, symbol: str, settle_ms: int) -> float:
        """查币安结算时刻的实际 FUNDING_FEE 金额。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/income",
                {"symbol": clean, "incomeType": "FUNDING_FEE",
                 "startTime": settle_ms - 5000, "endTime": settle_ms + 60000, "limit": 1},
            )
            if isinstance(resp, list) and resp:
                return float(resp[0].get("income", 0) or 0)
        except Exception:
            pass
        return 0.0

    async def _check_bn_funding_received(self, symbol: str, after_ms: float) -> bool:
        """检查币安是否已收到指定币种的资金费（结算时间之后）。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/income",
                {"symbol": clean, "incomeType": "FUNDING_FEE",
                 "startTime": int(after_ms) - 5000, "limit": 3},
            )
            if isinstance(resp, list):
                for item in resp:
                    if isinstance(item, dict) and item.get("time", 0) >= after_ms - 1000:
                        ft = datetime.fromtimestamp(item["time"] / 1000, tz=self.tz)
                        logger.info("[资费] BN 资费到账时间: %s", ft.strftime("%H:%M:%S.%f")[:-3])
                        return True
        except Exception:
            pass
        return False

    async def _check_gate_funding_received(self, symbol: str) -> bool:
        """检查Gate是否已收到指定币种的资金费（通过持仓 pnl_fund 变化判断）。"""
        try:
            positions = await self._safe_request(
                "gate_funding_check",
                lambda: self._fetch_gate_positions_direct(symbol),
                default=[],
            )
            for pos in positions:
                if pos.get("symbol") == symbol:
                    pnl_fund = float(pos.get("info", {}).get("pnl_fund", 0) or 0)
                    if abs(pnl_fund) > 0.0001:
                        return True
        except Exception:
            pass
        return False

    async def _ensure_bn_funding_ws(self) -> None:
        """确保 BN 用户数据流 WS 长连接存活（断线自动重连）。
        启动时调用一次，之后持续监听 FUNDING_FEE 事件。"""
        ws = getattr(self, "_funding_ws", None)
        if ws and not ws.closed:
            # 每 45min 刷新 listenKey（60min 有效期）
            if time.time() - getattr(self, "_bn_listen_key_ts", 0) < 2700:
                return
            logger.info("[资费监听] listenKey 即将过期，刷新")
            await self._close_bn_funding_ws()
        try:
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="POST",
            )
            self._bn_listen_key = resp.get("listenKey", "")
            self._bn_listen_key_ts = time.time()
            if not self._bn_listen_key:
                logger.warning("[资费监听] listenKey 创建失败")
                return
            if not self._funding_session:
                class _Resolver:
                    async def resolve(self, host, port=0, family=0):
                        if host == "fstream.binance.com":
                            return [{"hostname": host, "host": "52.69.16.71", "port": port,
                                     "family": socket.AF_INET, "proto": 6, "flags": socket.AI_NUMERICHOST}]
                        return [{"hostname": host, "host": host, "port": port,
                                 "family": socket.AF_INET, "proto": 6, "flags": 0}]
                    async def close(self): pass
                connector = aiohttp.TCPConnector(resolver=_Resolver(), ssl=False)
                self._funding_session = aiohttp.ClientSession(connector=connector)
            ws_url = f"wss://fstream.binance.com/ws/{self._bn_listen_key}"
            self._funding_ws = await self._funding_session.ws_connect(ws_url)
            logger.info("[资费监听] BN WS 长连接已建立")
            asyncio.create_task(self._read_bn_funding_stream())
        except Exception as exc:
            logger.warning("[资费监听] BN WS 建立失败: %s", exc)

    async def _close_bn_funding_ws(self) -> None:
        """关闭 BN WS 长连接。"""
        try:
            ws = getattr(self, "_funding_ws", None)
            if ws:
                await ws.close()
                self._funding_ws = None
        except Exception:
            pass
        try:
            if self._bn_listen_key:
                await self._binance_request(
                    BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="DELETE",
                )
                self._bn_listen_key = None
        except Exception:
            pass

    async def _read_bn_funding_stream(self) -> None:
        """持久读 BN 用户数据流，检测 FUNDING_FEE 设 event，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_funding_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        if data.get("e") == "ACCOUNT_UPDATE":
                            # 缓存所有持仓到 WS cache（开平仓验证免 REST）
                            for p in (data.get("a", {}).get("P", []) or []):
                                sym = p.get("s", "")
                                ps = p.get("ps", "")
                                if sym and ps:
                                    self._bn_ws_positions[f"{sym}|{ps}"] = abs(float(p.get("pa", 0) or 0))
                            if data.get("a", {}).get("m") == "FUNDING_FEE":
                                logger.info("[资费监听] BN 资费已到账")
                                self._funding_event.set()
                        elif data.get("e") == "listenKeyExpired":
                            logger.warning("[资费监听] listenKey 过期，重连")
                            break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[资费监听] BN WS 读取异常: %s", exc)
            # 断线重连
            self._funding_ws = None
            await asyncio.sleep(3)
            try:
                await self._ensure_bn_funding_ws()
                break  # _ensure 会创建新的 reader task
            except Exception:
                logger.warning("[资费监听] BN WS 重连失败，3s后重试")
                await asyncio.sleep(3)

    async def _ensure_gate_funding_ws(self) -> None:
        """确保 Gate 资费监听 WS 长连接存活，断线自动重连。
        Gate futures.positions 订阅要求 payload 指定 1-2 个合约名，不支持空列表。
        因此初始化时只建连接 + 启动 reader，开仓后通过 _gate_subscribe_position 动态订阅。"""
        ws = getattr(self, "_gate_funding_ws", None)
        if ws and not ws.closed:
            return
        await self._close_gate_funding_ws()
        try:
            if not self._gate_funding_session:
                self._gate_funding_session = aiohttp.ClientSession()
            self._gate_funding_ws = await asyncio.wait_for(
                self._gate_funding_session.ws_connect("wss://fx-ws.gateio.ws/v4/ws/usdt"),
                timeout=15,
            )
            await self._gate_ws_login(self._gate_funding_ws, label="资费监听")
            logger.info("[资费监听] Gate WS 长连接已建立")
            asyncio.create_task(self._read_gate_funding_stream())
        except Exception as exc:
            logger.warning("[资费监听] Gate WS 建立失败: %s", exc)

    async def _gate_subscribe_position(self, contract: str) -> bool:
        ws = getattr(self, "_gate_trade_ws", None)
        if not ws or ws.closed:
            logger.warning("Gate subscribe fail %s: trade WS not connected", contract)
            return False
        try:
            t = int(time.time())
            ch = "futures.positions"
            ev = "subscribe"
            sign_msg = f"channel={ch}&event={ev}&time={t}"
            sign = hmac.new(GATE_API_SECRET.encode(), sign_msg.encode(), hashlib.sha512).hexdigest()
            await ws.send_json({
                "time": t, "channel": ch, "event": ev,
                "payload": [contract],
                "auth": {"method": "api_key", "KEY": GATE_API_KEY, "SIGN": sign},
            })
            logger.info("Gate subscribed %s on trade WS", contract)
            return True
        except Exception as exc:
            logger.warning("Gate subscribe %s error: %s", contract, exc)
            return False


    async def _close_gate_funding_ws(self) -> None:
        """关闭 Gate WS 长连接。"""
        try:
            ws = getattr(self, "_gate_funding_ws", None)
            if ws:
                await ws.close()
                self._gate_funding_ws = None
        except Exception:
            pass

    async def _read_gate_funding_stream(self) -> None:
        """持久读 Gate WS，检测目标持仓 pnl_fund 变化设 event，断线自动重连。
        每条消息实时读取 _gate_funding_symbol / _gate_funding_baseline，
        配合 sniper 开仓后动态设定目标，持续监听不退出。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_gate_funding_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        if data.get("channel") == "futures.ping":
                            await ws.send_json({"time": int(time.time()), "channel": "futures.pong"})
                        elif data.get("channel") == "futures.positions" and data.get("event") == "update":
                            # 每条消息实时读目标（sniper 开仓后动态设定）
                            symbol = getattr(self, "_gate_funding_symbol", "")
                            baseline = getattr(self, "_gate_funding_baseline", 0.0)
                            contract = self._to_gate_contract(symbol) if symbol else ""
                            if not contract:
                                continue
                            for pos in data.get("result", []):
                                if pos.get("contract") == contract:
                                    pnl = float(pos.get("pnl_fund", 0) or 0)
                                    if abs(pnl - baseline) > 0.0001:
                                        self._gate_funding_amount = round(pnl - baseline, 6)
                                        logger.info("[资费监听] Gate 资费到账: %s pnl_fund %.4f→%.4f (资费=%.4f)",
                                                    symbol, baseline, pnl, self._gate_funding_amount)
                                        self._gate_funding_event.set()
                                        # 不退出 — 持续监听，sniper 清 event 设新 baseline 后继续检测
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[资费监听] Gate WS 读取异常: %s", exc)
            self._gate_funding_ws = None
            await asyncio.sleep(3)
            try:
                await self._ensure_gate_funding_ws()
                break  # _ensure 会创建新 reader task
            except Exception:
                logger.warning("[资费监听] Gate WS 重连失败，3s后重试")
                await asyncio.sleep(3)


    # ════════════════════════════════════════════════════════════════
    # BN + Gate 交易 WS（持久长连接，下单省 HTTP 握手 + to_thread）
    # ════════════════════════════════════════════════════════════════

    async def _ensure_bn_trade_ws(self) -> None:
        """确保 BN 交易 WS 长连接存活（wss://ws-fapi.binance.com/ws-fapi/v1），断线自动重连。"""
        ws = getattr(self, "_bn_trade_ws", None)
        if ws and not ws.closed:
            return
        await self._close_bn_trade_ws()
        try:
            if not self._bn_trade_session:
                self._bn_trade_session = aiohttp.ClientSession()
            self._bn_trade_ws = await self._bn_trade_session.ws_connect(
                "wss://ws-fapi.binance.com/ws-fapi/v1"
            )
            logger.info("[交易WS] BN 交易 WS 长连接已建立")
            asyncio.create_task(self._read_bn_trade_ws())
        except Exception as exc:
            logger.warning("[交易WS] BN 交易 WS 建立失败: %s", exc)

    async def _close_bn_trade_ws(self) -> None:
        """关闭 BN 交易 WS，取消所有等待中的订单。"""
        try:
            ws = getattr(self, "_bn_trade_ws", None)
            if ws:
                await ws.close()
                self._bn_trade_ws = None
        except Exception:
            pass
        for fut in getattr(self, "_bn_trade_futures", {}).values():
            if not fut.done():
                fut.set_exception(Exception("BN trade WS closed"))

    async def _read_bn_trade_ws(self) -> None:
        """持久读 BN 交易 WS 响应，按 id 分发到对应 Future，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_bn_trade_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        rid = data.get("id", "")
                        if rid in getattr(self, "_bn_trade_futures", {}):
                            fut = self._bn_trade_futures.pop(rid)
                            if not fut.done():
                                if data.get("status") == 200:
                                    fut.set_result(data.get("result", {}))
                                else:
                                    err = data.get("error", {})
                                    fut.set_exception(
                                        Exception(f"BN WS order failed: {err}"))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[交易WS] BN 交易 WS 读取异常: %s", exc)
            # 断线重连
            self._bn_trade_ws = None
            for fut in getattr(self, "_bn_trade_futures", {}).values():
                if not fut.done():
                    fut.set_exception(Exception("BN trade WS disconnected"))
            self._bn_trade_futures.clear()
            await asyncio.sleep(3)
            try:
                await self._ensure_bn_trade_ws()
                break
            except Exception:
                await asyncio.sleep(3)

    async def _bn_trade_ws_order(self, symbol: str, side: str, quantity: float,
                                  reduce_only: bool = False,
                                  position_side: str | None = None) -> dict[str, Any]:
        """BN 合约下单 — 走持久 WS 长连接，省 HTTP 握手 + headers 开销。"""
        import uuid as _uuid
        await self._ensure_bn_trade_ws()
        clean = self._clean_futures_symbol(symbol)
        ts = int(time.time() * 1000)
        params: dict[str, Any] = {
            "apiKey": BINANCE_API_KEY,
            "symbol": clean,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": ts,
            "recvWindow": 5000,
        }
        if position_side:
            params["positionSide"] = position_side.upper()
        elif reduce_only:
            params["reduceOnly"] = "true"
        # 签名（标准 Binance HMAC-SHA256，按 key 排序后 URL-encode）
        sorted_items = sorted((str(k), str(v)) for k, v in params.items())
        qs = "&".join(f"{k}={v}" for k, v in sorted_items)
        params["signature"] = self._binance_sign(qs)

        rid = str(_uuid.uuid4())[:8]
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._bn_trade_futures[rid] = fut
        try:
            t0 = time.perf_counter()
            t0_ms = int(time.time() * 1000)
            await self._bn_trade_ws.send_json(
                {"id": rid, "method": "order.place", "params": params})
            result = await asyncio.wait_for(fut, timeout=15.0)
            t1 = time.perf_counter()
            raw = result.get("result", {})
            transact_time = raw.get("transactTime", 0) or raw.get("updateTime", 0)
            fill_ms = transact_time - t0_ms if transact_time > 0 else 0
            logger.info("[延迟] BN WS 下单 %s %s: 往返 %.0fms | 成交 %dms",
                        symbol, side, (t1 - t0) * 1000, fill_ms)
            # 传内层 result（含 executedQty / status:FILLED），非外层信封（status:200）
            order = self._normalize_order_response(raw, symbol, side, quantity)
            if order["status"] == "open":
                raise ExchangeError(
                    f"合约{side}未成交: {symbol} filled=0/{quantity}")
            if order["status"] == "partial":
                logger.warning("合约%s部分成交: filled=%.4f/%.4f (%.0f%%)",
                               symbol, order["filled"], quantity,
                               order["filled"] / quantity * 100 if quantity > 0 else 0)
            return order
        except asyncio.TimeoutError:
            self._bn_trade_futures.pop(rid, None)
            raise Exception(f"BN WS order timeout: {symbol} {side}")
        except Exception:
            self._bn_trade_futures.pop(rid, None)
            raise

    # ── Gate 交易 WS（futures.order_place 频道直连下单） ──

    async def _ensure_gate_trade_ws(self) -> None:
        """确保 Gate 交易 WS 长连接存活（futures.order_place），断线自动重连，先 login 再就绪。"""
        ws = getattr(self, "_gate_trade_ws", None)
        if ws and not ws.closed:
            return
        await self._close_gate_trade_ws()
        try:
            if not self._gate_trade_session:
                self._gate_trade_session = aiohttp.ClientSession()
            self._gate_trade_ws = await self._gate_trade_session.ws_connect(
                "wss://fx-ws.gateio.ws/v4/ws/usdt"
            )
            logger.info("[交易WS] Gate 交易 WS 长连接已建立")
            await self._gate_ws_login(self._gate_trade_ws, label="交易WS")
            asyncio.create_task(self._read_gate_trade_ws())
            asyncio.create_task(self._gate_trade_ws_ping())
        except Exception as exc:
            logger.warning("[交易WS] Gate 交易 WS 建立失败: %s", exc)

    async def _gate_ws_login(self, ws=None, label: str = "交易WS") -> None:
        """Gate WS request_private 登录 futures.login，登录后可发 request/subscribe。
        ws 默认使用 _gate_trade_ws，也可传入其他 WS 连接。"""
        import uuid as _uuid
        if ws is None:
            ws = getattr(self, "_gate_trade_ws", None)
        if not ws or ws.closed:
            raise Exception(f"Gate WS login: ws 未连接")
        channel = "futures.login"
        event = "api"
        req_id = _uuid.uuid4().hex[:16]
        t = int(time.time())
        req_params: dict[str, Any] = {}
        sign_msg = f"{event}\n{channel}\n{json.dumps(req_params, separators=(',', ':'))}\n{t}"
        sign = hmac.new(
            GATE_API_SECRET.encode(), sign_msg.encode(), hashlib.sha512,
        ).hexdigest()
        msg = {
            "id": req_id, "time": t, "channel": channel, "event": event,
            "payload": {
                "req_id": req_id, "timestamp": str(t),
                "api_key": GATE_API_KEY, "signature": sign,
                "req_param": req_params,
                "req_header": {"X-Gate-Channel-Id": "ccxt"},
            },
        }
        await ws.send_json(msg)
        t_deadline = time.perf_counter() + 10.0
        while time.perf_counter() < t_deadline:
            try:
                raw = await asyncio.wait_for(ws.receive(), timeout=t_deadline - time.perf_counter())
            except asyncio.TimeoutError:
                raise Exception("Gate WS login timeout")
            if raw.type == aiohttp.WSMsgType.TEXT:
                d = json.loads(raw.data)
                hdr = d.get("header", {})
                if hdr.get("channel") == channel and hdr.get("event") == event:
                    if hdr.get("status") == "200":
                        logger.info("[%s] Gate WS 登录成功", label)
                        return
                    raise Exception(f"Gate WS login failed: status={hdr.get('status')}")
            elif raw.type == aiohttp.WSMsgType.CLOSED:
                raise Exception(f"Gate WS closed during login: code={raw.data}")
        raise Exception("Gate WS login timeout")

    async def _read_gate_trade_ws(self) -> None:
        """持久读 Gate 交易 WS 响应：下单回执 + 持仓变动（资费结算监听），断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_gate_trade_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        hdr = data.get("header", {})
                        ch = hdr.get("channel", "") or data.get("channel", "")
                        ev = hdr.get("event", "") or data.get("event", "")
                        if ch == "futures.ping":
                            await ws.send_json({"time": int(time.time()), "channel": "futures.pong"})
                        # ── 持仓变动（资费结算监听 + 持仓缓存）──
                        elif ch == "futures.positions" and ev == "update":
                            result = data.get("data", {}).get("result", data.get("result", []))
                            for pos in (result if isinstance(result, list) else [result]):
                                ctr = pos.get("contract", "")
                                if ctr:
                                    # 缓存所有合约持仓（开平仓验证免 REST）
                                    self._gate_ws_positions[ctr] = abs(float(pos.get("size", 0) or 0))
                            # 资费结算检测
                            symbol = getattr(self, "_gate_funding_symbol", "")
                            baseline = getattr(self, "_gate_funding_baseline", 0.0)
                            contract = self._to_gate_contract(symbol) if symbol else ""
                            if contract:
                                for pos in (result if isinstance(result, list) else [result]):
                                    if pos.get("contract") == contract:
                                        pnl = float(pos.get("pnl_fund", 0) or 0)
                                        if abs(pnl - baseline) > 0.0001:
                                            self._gate_funding_amount = round(pnl - baseline, 6)
                                            logger.info("[资费监听] Gate 资费到账: %s pnl_fund %.4f→%.4f (资费=%.4f)",
                                                        symbol, baseline, pnl, self._gate_funding_amount)
                                            self._gate_funding_event.set()
                        # ── 下单回执 ──
                        elif ch == "futures.order_place" and ev == "api":
                            rid = data.get("request_id", "")
                            result = data.get("data", {}).get("result", {})
                            order = result[0] if isinstance(result, list) else result
                            for key in (rid, order.get("text", ""), str(order.get("id", ""))):
                                if key and key in getattr(self, "_gate_trade_futures", {}):
                                    fut = self._gate_trade_futures[key]  # 不 pop，ack 消息不解析
                                    if not fut.done():
                                        errs = data.get("data", {}).get("errs", {})
                                        if errs:
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_exception(
                                                Exception(f"Gate WS order error: {errs.get('message', '') or str(errs)}"))
                                        elif hdr.get("status") and hdr.get("status") != "200":
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_exception(
                                                Exception(f"Gate WS order failed: status={hdr.get('status')}"))
                                        elif "id" in order:
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_result(order)
                                        # else: ack-only 回执 (无 id)，不解析，等第二条消息
                                    break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[交易WS] Gate 交易 WS 读取异常: %s", exc)
            # 断线重连
            self._gate_trade_ws = None
            for fut in getattr(self, "_gate_trade_futures", {}).values():
                if not fut.done():
                    fut.set_exception(Exception("Gate trade WS disconnected"))
            self._gate_trade_futures.clear()
            await asyncio.sleep(3)
            try:
                await self._ensure_gate_trade_ws()
                break
            except Exception:
                await asyncio.sleep(3)

    async def _gate_trade_ws_order(self, symbol: str, side: str,
                                    amount: float, reduce_only: bool = False,
                                    ) -> dict[str, Any]:
        """Gate 合约市价单 — 走 WS futures.order_place 持久连接。
        使用 request_private 认证模式（api_key + signature 嵌入 payload），
        而非 subscribe_private 的 auth.method 模式。"""
        import uuid as _uuid
        await self._ensure_gate_trade_ws()
        contract = self._to_gate_contract(symbol)
        precise = int(float(self.gate_futures.amount_to_precision(symbol, amount)))
        size = precise if side == "buy" else -precise
        t = int(time.time())
        text = f"t-{_uuid.uuid4().hex[:8]}"
        req_id = _uuid.uuid4().hex[:16]
        req_params: dict[str, Any] = {
            "contract": contract, "size": size, "price": "0", "tif": "ioc",
            "text": text, "settle": "usdt",
        }
        if reduce_only:
            req_params["reduce_only"] = True
        channel = "futures.order_place"
        event = "api"
        # request_private 签名: HMAC-SHA512("{event}\n{channel}\n{json(reqParams)}\n{time}")
        sign_msg = f"{event}\n{channel}\n{json.dumps(req_params, separators=(',', ':'))}\n{t}"
        sign = hmac.new(
            GATE_API_SECRET.encode(), sign_msg.encode(), hashlib.sha512,
        ).hexdigest()
        msg = {
            "id": req_id,
            "time": t, "channel": channel, "event": event,
            "payload": {
                "req_id": req_id,
                "timestamp": str(t),
                "api_key": GATE_API_KEY,
                "signature": sign,
                "req_param": req_params,
                "req_header": {"X-Gate-Channel-Id": "ccxt"},
            },
        }
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._gate_trade_futures[req_id] = fut
        self._gate_trade_futures[text] = fut  # fallback 用 text 也能匹配
        try:
            t0 = time.perf_counter()
            t0_sec = int(time.time())
            await self._gate_trade_ws.send_json(msg)
            result = await asyncio.wait_for(fut, timeout=15.0)
            t1 = time.perf_counter()
            rtt_ms = (t1 - t0) * 1000
            if isinstance(result, list):
                result = result[0] if result else {}
            # Gate order_place 响应含 create_time (秒)，可算成交延迟
            fill_info = ""
            if "status" in result:
                ct = result.get("create_time", 0) or result.get("finish_time", 0)
                if ct > 0:
                    fill_ms = int((ct - t0_sec) * 1000)
                    fill_info = f" | 成交 {fill_ms}ms"
            logger.info("[延迟] Gate WS 下单 %s %s: 往返 %.0fms%s", symbol, side, rtt_ms, fill_info)
            # Gate order_place 返回 ack (接单回执)，不含 fill 数据
            # result 可能是 req_param echo (无 status 字段) 或真实订单对象 (有 status 字段)
            if "status" in result:
                filled = abs(float(result.get("size", 0) or 0)) - abs(float(result.get("left", 0) or 0))
                finished = result.get("status") == "finished"
            else:
                # ack 回执模式: IOC 市价单已送达，假设全部成交，由上层调用者 REST 验证
                filled = precise
                finished = True
            return {
                "id": str(result.get("id", "")), "symbol": symbol, "side": side,
                "amount": precise, "filled": filled,
                "status": "closed" if finished else ("partial" if filled > 0 else "open"),
                "info": result,
            }
        except asyncio.TimeoutError:
            self._gate_trade_futures.pop(req_id, None)
            self._gate_trade_futures.pop(text, None)
            raise Exception(f"Gate WS order timeout: {symbol} {side}")
        except Exception:
            self._gate_trade_futures.pop(req_id, None)
            self._gate_trade_futures.pop(text, None)
            raise

    async def _gate_trade_ws_ping(self) -> None:
        """Gate 交易 WS 心跳，每 25 秒 ping 一次防止断线。"""
        while True:
            await asyncio.sleep(25)
            ws = getattr(self, "_gate_trade_ws", None)
            if not ws or ws.closed:
                break
            try:
                await ws.send_json({"time": int(time.time()), "channel": "futures.ping"})
            except Exception:
                break

    async def _close_gate_trade_ws(self) -> None:
        """关闭 Gate 交易 WS，取消所有等待中的订单。"""
        try:
            ws = getattr(self, "_gate_trade_ws", None)
            if ws:
                await ws.close()
                self._gate_trade_ws = None
        except Exception:
            pass
        for fut in getattr(self, "_gate_trade_futures", {}).values():
            if not fut.done():
                fut.set_exception(Exception("Gate trade WS closed"))

    # ════════════════════════════════════════════════════════════════
    # 跨交易所开平仓
    # ════════════════════════════════════════════════════════════════

    async def _cross_ensure_leverage(self, exchange: str, symbol: str) -> None:
        """确保合约杠杆已设为 CROSS_LEVERAGE（同币种同倍数只设一次，失败不缓存下次重试）。"""
        key = (exchange, symbol, CROSS_LEVERAGE)
        if key in self._leverage_set:
            return
        ok = False
        if exchange == "binance":
            ok = await self._binance_set_leverage(symbol)
        else:
            ok = await self._gate_set_leverage(symbol)
        if ok:
            self._leverage_set.add(key)

    async def _cross_open_short_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """在指定交易所做空合约。WS 缓存验证，无 REST 网络往返。"""
        position_side = "SHORT"
        if exchange == "binance":
            order, ws_error = None, None
            try:
                order = await self._bn_trade_ws_order(symbol, "sell", amount, position_side="SHORT")
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("合约开空WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            # WS 缓存验证
            ok, pos = self._ws_position_check("binance", symbol, "SHORT", expect_zero=False, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, pos = self._ws_position_check("binance", symbol, "SHORT", expect_zero=False, amount=amount)
            if not ok:
                pos = await self._cross_verify_binance_position(symbol, "SHORT")
                ok = pos > 0
            if ok:
                if ws_error:
                    logger.warning("开空持仓验证通过 %s: filled=%.4f (API异常但持仓存在)", symbol, pos)
                return LegResult(True, "futures", symbol, "sell", pos,
                                 order=order or {"id": "verified", "symbol": symbol, "side": "sell",
                                                "amount": pos, "filled": pos, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("开空确认失败 %s: err=%s", symbol, ws_error or "持仓验证未通过")
            return LegResult(False, "futures", symbol, "sell", amount, error=ws_error or "position not found")
        else:
            order, ws_error = None, None
            try:
                order = await self._gate_trade_ws_order(symbol, "sell", amount)
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("Gate开空WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
            if not ok:
                pos = await self._cross_verify_gate_position(symbol)
                ok = pos > 0
            if ok:
                if ws_error:
                    logger.warning("Gate开空WS缓存验证通过 %s: filled=%.4f", symbol, pos)
                else:
                    logger.info("[跨交易所] Gate开空成功 %s: filled=%.4f", symbol, pos)
                return LegResult(True, "futures", symbol, "sell", pos,
                                 order=order or {"id": "verified", "symbol": symbol, "side": "sell",
                                                "amount": pos, "filled": pos, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("Gate开空确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "sell", amount, error=ws_error or "Gate short position not found")

    async def _cross_open_long_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """在指定交易所做多合约。WS 缓存验证，无 REST 网络往返。"""
        if exchange == "binance":
            order, ws_error = None, None
            try:
                order = await self._bn_trade_ws_order(symbol, "buy", amount, position_side="LONG")
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("合约开多WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, pos = self._ws_position_check("binance", symbol, "LONG", expect_zero=False, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, pos = self._ws_position_check("binance", symbol, "LONG", expect_zero=False, amount=amount)
            if not ok:
                pos = await self._cross_verify_binance_position(symbol, "LONG")
                ok = pos > 0
            if ok:
                if ws_error:
                    logger.warning("开多持仓验证通过 %s: filled=%.4f (API异常但持仓存在)", symbol, pos)
                return LegResult(True, "futures", symbol, "buy", pos,
                                 order=order or {"id": "verified", "symbol": symbol, "side": "buy",
                                                "amount": pos, "filled": pos, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("开多确认失败 %s: err=%s", symbol, ws_error or "持仓验证未通过")
            return LegResult(False, "futures", symbol, "buy", amount, error=ws_error or "position not found")
        else:
            order, ws_error = None, None
            try:
                order = await self._gate_trade_ws_order(symbol, "buy", amount)
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("Gate开多WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
            if not ok:
                pos = await self._cross_verify_gate_position(symbol)
                ok = pos > 0
            if ok:
                if ws_error:
                    logger.warning("Gate开多WS缓存验证通过 %s: filled=%.4f", symbol, pos)
                else:
                    logger.info("[跨交易所] Gate开多成功 %s: filled=%.4f", symbol, pos)
                return LegResult(True, "futures", symbol, "buy", pos,
                                 order=order or {"id": "verified", "symbol": symbol, "side": "buy",
                                                "amount": pos, "filled": pos, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("Gate开多确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "buy", amount, error=ws_error or "Gate long position not found")

    async def _cross_close_short_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """平空单（买入平空）。WS 缓存验证，无 REST 网络往返。"""
        if exchange == "binance":
            order, ws_error = None, None
            try:
                order = await self._bn_trade_ws_order(symbol, "buy", amount, reduce_only=True, position_side="SHORT")
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("平空WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, remaining = self._ws_position_check("binance", symbol, "SHORT", expect_zero=True, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, remaining = self._ws_position_check("binance", symbol, "SHORT", expect_zero=True, amount=amount)
            if not ok:
                remaining = await self._cross_verify_binance_position(symbol, "SHORT")
                ok = remaining < amount * 0.1
            if ok:
                if ws_error:
                    logger.warning("平空WS缓存验证通过 %s: 仓位已消失 (API异常但持仓已平)", symbol)
                return LegResult(True, "futures", symbol, "buy", amount,
                                 order=order or {"id": "verified_close", "symbol": symbol, "side": "buy",
                                                "amount": amount, "filled": amount, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("平空确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "buy", amount, error=ws_error or "close short position still exists")
        else:
            order, ws_error = None, None
            try:
                order = await self._gate_trade_ws_order(symbol, "buy", amount, reduce_only=True)
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("Gate平空WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
            if not ok:
                remaining = await self._cross_verify_gate_position(symbol)
                ok = remaining < amount * 0.1
            if ok:
                if ws_error:
                    logger.warning("Gate平空WS缓存验证通过 %s: 仓位已消失", symbol)
                return LegResult(True, "futures", symbol, "buy", amount,
                                 order=order or {"id": "verified_close", "symbol": symbol, "side": "buy",
                                                "amount": amount, "filled": amount, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("Gate平空确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "buy", amount, error=ws_error or "Gate close short position still exists")

    async def _cross_close_long_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """平多单（卖出平多）。WS 缓存验证，无 REST 网络往返。"""
        if exchange == "binance":
            order, ws_error = None, None
            try:
                order = await self._bn_trade_ws_order(symbol, "sell", amount, reduce_only=True, position_side="LONG")
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("平多WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, remaining = self._ws_position_check("binance", symbol, "LONG", expect_zero=True, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, remaining = self._ws_position_check("binance", symbol, "LONG", expect_zero=True, amount=amount)
            if not ok:
                remaining = await self._cross_verify_binance_position(symbol, "LONG")
                ok = remaining < amount * 0.1
            if ok:
                if ws_error:
                    logger.warning("平多WS缓存验证通过 %s: 仓位已消失 (API异常但持仓已平)", symbol)
                return LegResult(True, "futures", symbol, "sell", amount,
                                 order=order or {"id": "verified_close", "symbol": symbol, "side": "sell",
                                                "amount": amount, "filled": amount, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("平多确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "sell", amount, error=ws_error or "close long position still exists")
        else:
            order, ws_error = None, None
            try:
                order = await self._gate_trade_ws_order(symbol, "sell", amount, reduce_only=True)
            except Exception as exc:
                ws_error = str(exc)
                logger.warning("Gate平多WS异常 %s: %s，以WS缓存验证为准", symbol, exc)
            ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
            if not ok:
                await asyncio.sleep(0.05)
                ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
            if not ok:
                remaining = await self._cross_verify_gate_position(symbol)
                ok = remaining < amount * 0.1
            if ok:
                if ws_error:
                    logger.warning("Gate平多WS缓存验证通过 %s: 仓位已消失", symbol)
                return LegResult(True, "futures", symbol, "sell", amount,
                                 order=order or {"id": "verified_close", "symbol": symbol, "side": "sell",
                                                "amount": amount, "filled": amount, "status": "closed",
                                                "info": {"verified_after_error": ws_error or ""}})
            logger.error("Gate平多确认失败 %s: err=%s", symbol, ws_error or "WS缓存+REST均未通过")
            return LegResult(False, "futures", symbol, "sell", amount, error=ws_error or "Gate close long position still exists")

    async def _cross_calculate_amount(self, exchange: str, symbol: str,
                                       reference_price: float, total_usdt: float) -> float:
        """按交易所精度计算合约下单数量（考虑 contractSize 转换为合约张数）。"""
        raw_coins = total_usdt / reference_price
        ex = self.futures if exchange == "binance" else self.gate_futures
        try:
            market = ex.market(symbol)
            contract_size = float(market.get("contractSize", 1) or 1)
            raw_contracts = raw_coins / contract_size
            min_qty = float(market.get("limits", {}).get("amount", {}).get("min", 0) or 0)
            if min_qty > 0 and raw_contracts < min_qty:
                est_usdt = min_qty * contract_size * reference_price
                logger.warning("[跨交易所] %s 数量不足 %s: 需要≥%.4f张 (≈%.2f USDT), 当前 %.4f张",
                               exchange, symbol, min_qty, est_usdt, raw_contracts)
                return 0.0
            precise = float(ex.amount_to_precision(symbol, raw_contracts))
            logger.info("[跨交易所] %s %s: price=%.6f usdt=%.2f contractSize=%.4f → %s张",
                        exchange, symbol, reference_price, total_usdt, contract_size, precise)
            return precise
        except Exception as exc:
            logger.error("[跨交易所] %s 精度计算失败 %s: %s", exchange, symbol, exc)
            return 0.0

    async def _open_cross_exchange_position(self, *,
                                              base: str,
                                              short_ex: str, short_sym: str,
                                              long_ex: str, long_sym: str,
                                              short_amount: float, long_amount: float,
                                              short_price: float, long_price: float,
                                              short_rate: float, long_rate: float,
                                              rate_spread: float, net_rate: float,
                                              short_next_funding_time_ms: float,
                                              long_next_funding_time_ms: float) -> bool:
        """开仓跨交易所套利：并发下空单+多单，设置 cross_state。"""
        logger.info(
            "[跨交易所] 开仓 %s: %s空@%s(%.4f%%) × %.4f张 + %s多@%s(%.4f%%) × %.4f张 | 费率差=%.4f%% | 净收益=%.4f%%",
            base, base, short_ex, short_rate * 100, short_amount,
            base, long_ex, long_rate * 100, long_amount,
            rate_spread * 100, net_rate * 100,
        )

        short_task = self._cross_open_short_leg(short_ex, short_sym, short_amount)
        long_task = self._cross_open_long_leg(long_ex, long_sym, long_amount)
        short_result, long_result = await asyncio.gather(short_task, long_task)

        if short_result.ok and long_result.ok:
            # 用实际成交均价，不用扫描参考价
            short_fill = self._extract_fill_price(short_result.order)
            long_fill = self._extract_fill_price(long_result.order)
            self.cross_state = CrossArbitrageState(
                is_open=True,
                base=base,
                amount=short_amount * short_price,  # 近似 USDT 名义值（显示用）
                short_amount=short_amount,
                long_amount=long_amount,
                short_exchange=short_ex,
                long_exchange=long_ex,
                short_symbol=short_sym,
                long_symbol=long_sym,
                short_order_id=str(short_result.order.get("id", "")),
                long_order_id=str(long_result.order.get("id", "")),
                short_entry_price=short_fill or short_price,
                long_entry_price=long_fill or long_price,
                short_rate=short_rate,
                long_rate=long_rate,
                rate_spread=rate_spread,
                total_net_rate=net_rate,
                opened_at=datetime.now(tz=self.tz).isoformat(),
                short_next_funding_time_ms=short_next_funding_time_ms,
                long_next_funding_time_ms=long_next_funding_time_ms,
            )
            self._save_cross_state()
            await asyncio.to_thread(
                self._send_email,
                "bazfbot 跨所开仓",
                f"币种: {base}\n"
                f"空 {short_ex}: {short_sym} × {short_amount}张 费率 {short_rate*100:.4f}%\n"
                f"多 {long_ex}: {long_sym} × {long_amount}张 费率 {long_rate*100:.4f}%\n"
                f"数量: {short_amount}/{long_amount} | 费率差: {rate_spread*100:.4f}%\n"
                f"净收益: {net_rate*100:.4f}%",
            )
            return True

        # 一腿或两腿失败 → 应急平掉成功腿
        logger.critical("[跨交易所] 开仓未同时成功: short=%s long=%s", short_result, long_result)
        if short_result.ok:
            logger.critical("[跨交易所] 空单已成交但多单失败，立即平空恢复中性")
            await self._cross_close_short_leg(short_ex, short_sym, short_amount)
        if long_result.ok:
            logger.critical("[跨交易所] 多单已成交但空单失败，立即平多恢复中性")
            await self._cross_close_long_leg(long_ex, long_sym, long_amount)
        return False

    async def _close_cross_exchange_position(self) -> bool:
        """平仓跨交易所套利：平空 + 平多。"""
        if not self.cross_state.is_open:
            return True

        cs = self.cross_state
        logger.info("[跨交易所] 平仓 %s: 平空@%s(%s) + 平多@%s(%s)",
                     cs.base, cs.short_exchange, cs.short_symbol,
                     cs.long_exchange, cs.long_symbol)

        short_task = self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
        long_task = self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
        short_r, long_r = await asyncio.gather(short_task, long_task)

        # 重试失败腿
        for attempt in range(3):
            if short_r.ok and long_r.ok:
                break
            if not short_r.ok:
                logger.warning("[跨交易所] 平空失败，重试 %d/3", attempt + 1)
                short_r = await self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
            if not long_r.ok:
                logger.warning("[跨交易所] 平多失败，重试 %d/3", attempt + 1)
                long_r = await self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
            await asyncio.sleep(0.5)

        if short_r.ok and long_r.ok:
            logger.info("[跨交易所] 平仓成功。")
            await self._record_cross_trade(short_close_order=short_r.order,
                                            long_close_order=long_r.order)
            self.cross_state = CrossArbitrageState()
            self._save_cross_state()
            await asyncio.to_thread(
                self._send_email, "bazfbot 跨所平仓", f"{cs.base} 跨所套利已平仓。"
            )
            return True

        # 部分失败：重开已平腿恢复对冲
        logger.critical("[跨交易所] 平仓部分失败！short=%s long=%s", short_r, long_r)
        if short_r.ok and not long_r.ok:
            logger.critical("[跨交易所] 重开多单恢复对冲")
            await self._cross_open_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
        elif long_r.ok and not short_r.ok:
            logger.critical("[跨交易所] 重开空单恢复对冲")
            await self._cross_open_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
        await asyncio.to_thread(
            self._send_email,
            "bazfbot 跨所平仓失败！",
            f"{cs.base} 平仓单腿失败，已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    @staticmethod
    def _extract_fill_price(order: dict[str, Any] | None) -> float:
        """从订单响应中提取成交均价（兼容 Binance + Gate 格式）。"""
        if not order:
            return 0.0
        info = order.get("info", {})
        # BN: avgPrice / cummulativeQuoteQty
        for src in (info, order):
            if isinstance(src, dict):
                avg = src.get("avgPrice")
                if avg:
                    return float(avg)
                qq = src.get("cummulativeQuoteQty")
                eq = src.get("executedQty")
                if qq and eq:
                    qty = float(eq)
                    if qty > 0:
                        return float(qq) / qty
        # Gate: info.fill_price 直接就是成交均价
        if isinstance(info, dict) and info.get("fill_price"):
            return float(info["fill_price"])
        # 回退：顶级字段
        for key in ("price", "fill_price", "average"):
            if order.get(key):
                return float(order[key])
        return 0.0

    async def _query_order_actual_fee(self, exchange: str, symbol: str,
                                       order_id: str,
                                       close_info: dict | None = None) -> float:
        """查询实际成交手续费。
        - Binance: GET /fapi/v1/userTrades?orderId=... 累加 commission
        - Gate: 优先用 close_info["fee"]，否则 GET /futures/usdt/orders/{id}
        """
        if not order_id or order_id in ("", "0", "verified", "verified_close"):
            return 0.0
        if exchange == "gate":
            if close_info:
                fee = float(close_info.get("fee", 0) or 0)
                if fee > 0:
                    return fee
            try:
                resp = await self._gate_request(f"/futures/usdt/orders/{order_id}")
                return float(resp.get("fee", 0) or 0)
            except Exception:
                return 0.0
        # Binance
        try:
            clean = self._clean_futures_symbol(symbol)
            trades = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/userTrades",
                {"symbol": clean, "orderId": int(order_id)},
            )
            total = 0.0
            for t in (trades if isinstance(trades, list) else []):
                total += abs(float(t.get("commission", 0) or 0))
            return total
        except Exception:
            return 0.0

    async def _record_cross_trade(self, short_close_order: dict | None = None,
                                   long_close_order: dict | None = None) -> None:
        """平仓后从交易所查询实际资费+手续费+成交价，不做估算。"""
        cs = self.cross_state
        if not cs.is_open or cs.short_amount <= 0 or cs.long_amount <= 0:
            return
        short_amount = cs.short_amount
        long_amount = cs.long_amount
        short_entry = cs.short_entry_price
        long_entry = cs.long_entry_price
        short_notional = short_amount * short_entry if short_entry else 0
        long_notional = long_amount * long_entry if long_entry else 0

        # 提取平仓成交价
        short_exit = self._extract_fill_price(short_close_order) if short_close_order else 0.0
        long_exit = self._extract_fill_price(long_close_order) if long_close_order else 0.0

        # ── 查实际手续费（4 单并发：开空+平空+开多+平多）──
        short_close_id = str(short_close_order.get("id", "")) if short_close_order else ""
        long_close_id = str(long_close_order.get("id", "")) if long_close_order else ""
        short_close_info = short_close_order.get("info") if short_close_order else None
        long_close_info = long_close_order.get("info") if long_close_order else None

        s_open_fee, s_close_fee, l_open_fee, l_close_fee = await asyncio.gather(
            self._query_order_actual_fee(cs.short_exchange, cs.short_symbol, cs.short_order_id or ""),
            self._query_order_actual_fee(cs.short_exchange, cs.short_symbol, short_close_id, short_close_info),
            self._query_order_actual_fee(cs.long_exchange, cs.long_symbol, cs.long_order_id or ""),
            self._query_order_actual_fee(cs.long_exchange, cs.long_symbol, long_close_id, long_close_info),
        )
        short_fee_actual = round(s_open_fee + s_close_fee, 6)
        long_fee_actual = round(l_open_fee + l_close_fee, 6)

        # 费率估算（仅当交易所查询失败时回退）
        bn_fee, gt_fee = getattr(self, "_dash_cross_fees", (0.00045, 0.00018))
        _fee_of = {"binance": bn_fee, "gate": gt_fee}
        _dsc_of = {"binance": BN_FEE_DISCOUNT_FACTOR, "gate": GT_FEE_DISCOUNT_FACTOR}
        _rbt_of = {"binance": BN_FEE_REBATE_FACTOR, "gate": GT_FEE_REBATE_FACTOR}
        if short_fee_actual <= 0:
            short_fee_est = round(short_notional * _fee_of.get(cs.short_exchange, bn_fee) * 2, 6)
            short_fee_actual = round(short_fee_est * _dsc_of.get(cs.short_exchange, 1.0) * _rbt_of.get(cs.short_exchange, 1.0), 6)
        if long_fee_actual <= 0:
            long_fee_est = round(long_notional * _fee_of.get(cs.long_exchange, gt_fee) * 2, 6)
            long_fee_actual = round(long_fee_est * _dsc_of.get(cs.long_exchange, 1.0) * _rbt_of.get(cs.long_exchange, 1.0), 6)

        # ── 查实际资费（BN income API / Gate WS 监听结果）──
        short_actual_funding = 0.0
        long_actual_funding = 0.0
        if cs.short_exchange == "binance":
            short_actual_funding = await self._query_bn_funding_amount(
                cs.short_symbol, int(cs.short_next_funding_time_ms))
        elif cs.short_exchange == "gate":
            short_actual_funding = self._gate_funding_amount
        if cs.long_exchange == "binance":
            long_actual_funding = await self._query_bn_funding_amount(
                cs.long_symbol, int(cs.long_next_funding_time_ms))
        elif cs.long_exchange == "gate":
            long_actual_funding = self._gate_funding_amount

        # ── 做空侧 PnL ──
        short_price_pnl = round(short_amount * (short_entry - short_exit), 6)
        short_funding_pnl = round(short_actual_funding, 6) if short_actual_funding else round(short_notional * cs.short_rate, 6)
        short_net = round(short_price_pnl + short_funding_pnl - short_fee_actual, 6)

        # ── 做多侧 PnL ──
        long_price_pnl = round(long_amount * (long_exit - long_entry), 6)
        long_funding_pnl = round(-long_actual_funding, 6) if long_actual_funding else round(-long_notional * cs.long_rate, 6)
        long_net = round(long_price_pnl + long_funding_pnl - long_fee_actual, 6)

        net_pnl = round(short_net + long_net, 6)

        self._write_cross_trade_record(
            cs.base, f"cross({cs.short_exchange}空+{cs.long_exchange}多)",
            cs.short_amount, cs.short_entry_price, cs.total_net_rate, cs.rate_spread,
            short_entry=short_entry, short_exit=short_exit,
            long_entry=long_entry, long_exit=long_exit,
            net_pnl=net_pnl,
            short_exchange=cs.short_exchange, short_price_pnl=short_price_pnl,
            short_funding_pnl=short_funding_pnl,
            short_fee=short_fee_actual, short_net=short_net,
            long_exchange=cs.long_exchange, long_price_pnl=long_price_pnl,
            long_funding_pnl=long_funding_pnl,
            long_fee=long_fee_actual, long_net=long_net)

    def _write_cross_trade_record(self, coin: str, direction: str, amount: float,
                                   entry_price: float, net_rate: float, rate_spread: float,
                                   **extra) -> None:
        """写入一条跨所交易记录。profit_usdt 优先用真实 net_pnl。"""
        try:
            history = self._load_trade_history()
            real_pnl = extra.get("net_pnl")
            record = {
                "time": datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S"),
                "coin": coin,
                "direction": direction,
                "profit_usdt": round(real_pnl, 6) if real_pnl is not None else round(amount * entry_price * net_rate, 6),
                "net_rate": net_rate,
                "rate_spread": rate_spread,
                "amount": amount,
            }
            record.update({k: v for k, v in extra.items() if v is not None})  # 保留零值
            history.append(record)
            with self.TRADE_HISTORY_FILE.open("w", encoding="utf-8") as f:
                json.dump(history[-500:], f, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.warning("记录跨所交易历史失败: %s", exc)

    def _should_exit_cross(self) -> bool:
        """跨所套利退出判断：两腿都过了结算时间且 unlocked。"""
        cs = self.cross_state
        if not cs.is_open:
            return False
        if not cs.short_locked and not cs.long_locked:
            return True
        now_ms = time.time() * 1000
        short_ok = cs.short_next_funding_time_ms > 0 and now_ms >= cs.short_next_funding_time_ms
        long_ok = cs.long_next_funding_time_ms > 0 and now_ms >= cs.long_next_funding_time_ms
        return short_ok and long_ok

    async def _verify_cross_position(self) -> bool:
        """验证两交易所实际仓位与 cross_state 一致。不一致则清理所有腿。"""
        cs = self.cross_state
        if not cs.is_open:
            return False

        async def _check_futures_pos(exchange: str, symbol: str, expect_short: bool, expect_amount: float) -> tuple[bool, str]:
            """检查指定交易所是否有对应方向的合约持仓。返回 (ok, detail)。"""
            if exchange == "binance":
                try:
                    clean = self._clean_futures_symbol(symbol)
                    resp = await self._binance_request(
                        BINANCE_FUTURES_API, "/fapi/v2/positionRisk",
                        {"symbol": clean},
                    )
                    if isinstance(resp, list):
                        for pos in resp:
                            if not isinstance(pos, dict):
                                continue
                            ps = pos.get("positionSide", "")
                            if expect_short and ps != "SHORT":
                                continue
                            if not expect_short and ps != "LONG":
                                continue
                            amt = abs(float(pos.get("positionAmt", 0) or 0))
                            if amt >= expect_amount * 0.1:
                                detail = f"positionAmt={amt} side={ps}"
                                return True, detail
                        return False, f"no matching positionSide in {len(resp)} positions"
                    return False, f"unexpected resp type: {type(resp)}"
                except Exception as exc:
                    logger.warning("[跨交易所] 验证BN持仓异常 %s: %s", symbol, exc)
                    return True, f"API error, assume exists: {exc}"
            else:  # gate
                try:
                    positions = await self._safe_request(
                        f"verify_pos_gate",
                        lambda: self._fetch_gate_positions_direct(symbol),
                        default=[],
                    )
                    for pos in positions:
                        contracts = float(pos.get("contracts", 0) or 0)
                        if abs(contracts) < expect_amount * 0.1:
                            continue
                        if expect_short:
                            return contracts < 0, f"contracts={contracts}"
                        else:
                            return contracts > 0, f"contracts={contracts}"
                    return False, f"no position in {len(positions)} results"
                except Exception as exc:
                    logger.warning("[跨交易所] 验证GT持仓异常 %s: %s", symbol, exc)
                    return True, f"API error, assume exists: {exc}"

        short_ok, long_ok = await asyncio.gather(
            _check_futures_pos(cs.short_exchange, cs.short_symbol, expect_short=True, expect_amount=cs.short_amount),
            _check_futures_pos(cs.long_exchange, cs.long_symbol, expect_short=False, expect_amount=cs.long_amount),
        )
        logger.info("[跨交易所] 持仓验证: short(%s)=%s(%s) long(%s)=%s(%s)",
                    cs.short_exchange, short_ok[0], short_ok[1],
                    cs.long_exchange, long_ok[0], long_ok[1])

        if short_ok[0] and long_ok[0]:
            return True
        if not short_ok[0] and not long_ok[0]:
            logger.warning("[跨交易所] 两所均无实际持仓，重置状态。")
            self.cross_state = CrossArbitrageState()
            self._save_cross_state()
            return False

        # 应急：不管验证结果，两腿都尝试平仓，杜绝裸仓
        logger.critical("[跨交易所] 单腿持仓异常！short_ok=%s long_ok=%s → 尝试平两腿",
                        short_ok, long_ok)
        s_close_r, l_close_r = await asyncio.gather(
            self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount),
            self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount),
        )
        await self._record_cross_trade(short_close_order=s_close_r.order,
                                        long_close_order=l_close_r.order)
        self.cross_state = CrossArbitrageState()
        self._save_cross_state()
        return False

    def _print_cross_opportunity_table(self, df: pd.DataFrame, position_usdt: float) -> None:
        """打印跨交易所套利机会表。"""
        if df.empty:
            return
        est_profit = position_usdt * POSITION_SIZE_RATIO
        has_col = "passes" in df.columns
        lines = []
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════════════════")
        lines.append(f"  跨交易所资金费率套利 (纯期货多空对冲)  —  每腿 ≈{est_profit:.0f} USDT")
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════════════════")
        header = (f"  {'':1s}{'币种':<8s} {'BN价':>10s} {'GT价':>10s} {'费率差':>7s} {'净收益':>7s} "
                  f"{'做空所':>7s} {'空费率':>8s} {'空结算':>7s} "
                  f"{'做多所':>7s} {'多费率':>8s} {'多结算':>7s}")
        lines.append(header)
        lines.append("  " + "-" * 110)
        now_ms = time.time() * 1000
        for _, r in df.head(20).iterrows():
            if has_col:
                mark = "✓" if r.get("passes", False) else "✗"
            else:
                mark = " "
            bn_p = float(r.get("bn_price", 0))
            gt_p = float(r.get("gt_price", 0))
            short_nft = float(r.get("short_next_funding_time_ms", 0))
            long_nft = float(r.get("long_next_funding_time_ms", 0))
            short_m = max(0, (short_nft - now_ms) / 60000) if short_nft > 0 else -1
            long_m = max(0, (long_nft - now_ms) / 60000) if long_nft > 0 else -1
            short_wait = f"{int(short_m)}m" if short_m >= 0 else "N/A"
            long_wait = f"{int(long_m)}m" if long_m >= 0 else "N/A"
            lines.append(
                f"  {mark}{str(r['base']):<8s} {bn_p:>10.6f} {gt_p:>10.6f} {float(r['rate_spread'])*100:>6.4f}% {float(r['net_rate'])*100:>6.4f}% "
                f"{str(r['short_exchange']):>7s} {float(r['short_rate'])*100:>7.4f}% {short_wait:>7s} "
                f"{str(r['long_exchange']):>7s} {float(r['long_rate'])*100:>7.4f}% {long_wait:>7s}"
            )
        passes_any = any(r.get("passes", False) for _, r in df.head(20).iterrows())
        if has_col and not passes_any:
            lines.append("  (无候选通过严格筛选 — 费率差/净收益/结算窗口未达标)")
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════")
        for line in lines:
            logger.info(line)

    async def _cross_check_liquidation(self, exchange: str, symbol: str, short: bool) -> float | None:
        """查询跨所单腿距强平距离。复用 Binance 已有方法，Gate 单独实现。"""
        if exchange == "binance":
            return await self._check_futures_liquidation_distance(symbol, short)
        # Gate: 通过 ccxt fetch_positions
        try:
            positions = await self._safe_request(
                "gate_futures.fetch_positions_x",
                lambda: self._fetch_gate_positions_direct(symbol),
                default=[],
            )
            for pos in positions:
                if pos.get("symbol") != symbol:
                    continue
                liq_price = float(pos.get("liquidationPrice") or 0)
                mark_price = float(pos.get("markPrice") or 0)
                if liq_price <= 0 or mark_price <= 0:
                    return None
                if short:
                    return (liq_price - mark_price) / mark_price
                return (mark_price - liq_price) / mark_price
        except Exception:
            pass
        return None

    def _print_cross_position_summary(self) -> None:
        """打印跨交易所持仓摘要（费率、结算时间、预估收益）。"""
        cs = self.cross_state
        if not cs.is_open:
            return
        now_ms = time.time() * 1000
        short_wait = max(0, (cs.short_next_funding_time_ms - now_ms) / 60000) if cs.short_next_funding_time_ms > 0 else -1
        long_wait = max(0, (cs.long_next_funding_time_ms - now_ms) / 60000) if cs.long_next_funding_time_ms > 0 else -1

        short_settle = f"{short_wait:.0f}min后" if short_wait >= 0 else "未知"
        long_settle = f"{long_wait:.0f}min后" if long_wait >= 0 else "未知"

        notional = cs.amount * cs.short_entry_price if cs.short_entry_price > 0 else 0
        est_profit = notional * cs.total_net_rate if notional > 0 else 0

        logger.info("  ═══════════════════════════════════════════════════════════════")
        logger.info("  跨交易所持仓  —  %s 空×%.4f张 多×%.4f张  (≈%.0f USDT/腿)",
                     cs.base, cs.short_amount, cs.long_amount, notional)
        logger.info("  ═══════════════════════════════════════════════════════════════")
        logger.info("    做空: %6s  %-12s  费率=%+7.4f%%  结算: %s",
                     cs.short_exchange.upper(), cs.short_symbol, cs.short_rate * 100, short_settle)
        logger.info("    做多: %6s  %-12s  费率=%+7.4f%%  结算: %s",
                     cs.long_exchange.upper(), cs.long_symbol, cs.long_rate * 100, long_settle)
        logger.info("    ───────────────────────────────────────────────────────────")
        logger.info("    原始费率差: %+7.4f%%    净收益率: %+7.4f%%",
                     cs.rate_spread * 100, cs.total_net_rate * 100)
        if est_profit > 0:
            logger.info("    预估单期收益: %7.2f USDT", est_profit)
        logger.info("    开仓时间: %s", cs.opened_at or '未知')
        logger.info("  ═══════════════════════════════════════════════════════════════")

    async def _detect_orphaned_cross_positions(self) -> bool:
        """扫描两所所有持仓，检测 cross_state 为空时是否有游离仓位。
        有游离仓则平仓并返回 True，无则返回 False。"""
        found = False
        try:
            # 并行获取两所仓位
            bn_positions_raw = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v2/positionRisk", {}
            )
            gt_positions = await self._safe_request(
                "orphan_gt_positions",
                lambda: self._fetch_gate_positions_direct(),
                default=[],
            )

            # 解析 Binance 仓位：聚合 positionAmt（找有仓的）
            bn_positions: dict[str, dict[str, Any]] = {}  # base → {symbol, side, amount}
            if isinstance(bn_positions_raw, list):
                for pos in bn_positions_raw:
                    if not isinstance(pos, dict):
                        continue
                    amt = abs(float(pos.get("positionAmt", 0) or 0))
                    if amt < 1:  # 忽略极小/零仓位
                        continue
                    sym = str(pos.get("symbol", ""))
                    side = str(pos.get("positionSide", ""))
                    base = sym.replace("USDT", "")  # AGLDUSDT → AGLD
                    bn_positions[base] = {"symbol": f"{base}/USDT:USDT", "side": side,
                                          "amount": amt, "exchange": "binance"}

            # 解析 Gate 仓位
            gt_pos_map: dict[str, dict[str, Any]] = {}
            for pos in (gt_positions or []):
                contracts = abs(float(pos.get("contracts", 0) or 0))
                if contracts < 1:
                    continue
                sym = str(pos.get("symbol", ""))
                # sym format: "AGLD/USDT:USDT"
                base = sym.split("/")[0] if "/" in sym else sym.replace("USDT", "")
                side = "LONG" if float(pos.get("contracts", 0) or 0) > 0 else "SHORT"
                gt_pos_map[base] = {"symbol": sym, "side": side,
                                    "amount": contracts, "exchange": "gate"}

            # 找到两所都有仓位的币种 → 配对游离仓
            common = set(bn_positions.keys()) & set(gt_pos_map.keys())
            for base in common:
                bn = bn_positions[base]
                gt = gt_pos_map[base]
                if bn["side"] == gt["side"]:
                    continue
                close_amount = min(bn["amount"], gt["amount"])
                short_ex = "binance" if bn["side"] == "SHORT" else "gate"
                short_sym = bn["symbol"] if bn["side"] == "SHORT" else gt["symbol"]
                long_ex = "gate" if bn["side"] == "SHORT" else "binance"
                long_sym = gt["symbol"] if bn["side"] == "SHORT" else bn["symbol"]
                logger.critical("[跨交易所] ⚠ 检测到配对游离仓位 %s！立即平两腿", base)
                await asyncio.gather(
                    self._cross_close_short_leg(short_ex, short_sym, close_amount),
                    self._cross_close_long_leg(long_ex, long_sym, close_amount),
                )
                # 记录交易（无持仓期间费率数据，按 0 记录，至少历史可查）
                self._write_cross_trade_record(
                    base, f"orphan_close({short_ex}空+{long_ex}多)",
                    close_amount, 0.0, 0.0, 0.0)
                logger.critical("[跨交易所] 配对游离仓位 %s 清理完成", base)
                found = True
                bn_positions.pop(base, None)
                gt_pos_map.pop(base, None)

            # 剩余单边仓位 — 并发平掉
            all_orphans = {**bn_positions, **gt_pos_map}
            if all_orphans:
                tasks = []
                for base, info in all_orphans.items():
                    logger.critical("[跨交易所] ⚠ 检测到孤立单腿仓位！%s %s %s %.0f张 → 立即平仓",
                                  info["exchange"], info["symbol"], info["side"], info["amount"])
                    self._write_cross_trade_record(
                        base, f"orphan_single({info['exchange']}{'空' if info['side']=='SHORT' else '多'})",
                        info["amount"], 0.0, 0.0, 0.0)
                    if info["side"] == "SHORT":
                        tasks.append(self._cross_close_short_leg(info["exchange"], info["symbol"], info["amount"]))
                    else:
                        tasks.append(self._cross_close_long_leg(info["exchange"], info["symbol"], info["amount"]))
                await asyncio.gather(*tasks)
                found = True
            return found
        except Exception as exc:
            logger.warning("[跨交易所] 游离仓位检测异常: %s", exc)
            return False

    async def _run_cross_exchange_cycle(self, force_entry: bool) -> None:
        """跨交易所套利主循环。"""
        self._log_memory()
        has_position = self.cross_state.is_open

        # 启动/状态重置后先检查游离仓位，防止裸仓
        if not has_position:
            if await self._detect_orphaned_cross_positions():
                return  # 发现孤仓并已清理，暂停本轮，下轮正常决策

        if has_position:
            valid = await self._verify_cross_position()
            if not valid:
                has_position = False

        # force_entry 时强平
        if force_entry and has_position:
            logger.info("[跨交易所] 即时模式：强制平仓。")
            await self._close_cross_exchange_position()
            has_position = False

        if has_position:
            cs = self.cross_state

            # ── 风控：强平距离监控 ──
            short_dist = await self._cross_check_liquidation(
                cs.short_exchange, cs.short_symbol, short=True)
            long_dist = await self._cross_check_liquidation(
                cs.long_exchange, cs.long_symbol, short=False)

            if short_dist is not None:
                logger.info("[跨交易所] 空单距强平 %.1f%% (阈值 %.0f%%)",
                          short_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
            if long_dist is not None:
                logger.info("[跨交易所] 多单距强平 %.1f%% (阈值 %.0f%%)",
                          long_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)

            if short_dist is not None and short_dist < CROSS_LIQ_DISTANCE_MIN:
                logger.critical("[跨交易所] 空单距强平 %.1f%% < %.0f%%，强制平仓！",
                              short_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
                await self._close_cross_exchange_position()
                await asyncio.to_thread(self._send_email, "bazfbot 跨所强平！",
                    f"{cs.base} 空单@{cs.short_exchange} 距强平仅 {short_dist*100:.1f}%，已强制平仓。")
                has_position = False

            if has_position and long_dist is not None and long_dist < CROSS_LIQ_DISTANCE_MIN:
                logger.critical("[跨交易所] 多单距强平 %.1f%% < %.0f%%，强制平仓！",
                              long_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
                await self._close_cross_exchange_position()
                await asyncio.to_thread(self._send_email, "bazfbot 跨所强平！",
                    f"{cs.base} 多单@{cs.long_exchange} 距强平仅 {long_dist*100:.1f}%，已强制平仓。")
                has_position = False

        if has_position:
            should_exit = self._should_exit_cross()
            if should_exit:
                logger.info("[跨交易所] 结算已过，直接平仓。")
                await self._close_cross_exchange_position()
                has_position = False
            else:
                self._print_cross_position_summary()
                cs = self.cross_state
                now_ms = time.time() * 1000
                short_wait = max(0, (cs.short_next_funding_time_ms - now_ms) / 60000) if cs.short_next_funding_time_ms > 0 else 0
                long_wait = max(0, (cs.long_next_funding_time_ms - now_ms) / 60000) if cs.long_next_funding_time_ms > 0 else 0
                logger.info("[跨交易所] 持仓 %s | 空@%s %.0fmin后结算 | 多@%s %.0fmin后结算 | 费率差=%.4f%%",
                             cs.base, cs.short_exchange, short_wait, cs.long_exchange, long_wait,
                             cs.rate_spread * 100)

        if has_position:
            bn_bal, gt_bal = await asyncio.gather(
                self._cross_get_bn_futures_balance(),
                self._gate_futures_balance(),
            )
            await self._update_cross_dashboard(bn_bal, gt_bal)
            return

        # ── 无持仓：狙击模式 ──
        # T-10s 扫描获取最新费率+设杠杆 → T-1s 直接开仓
        snipe_ms = getattr(self, "_next_snipe_settle_ms", 0)
        if snipe_ms:
            remain_ms = snipe_ms - time.time() * 1000
            if remain_ms <= 0:
                self._next_snipe_settle_ms = 0
            elif remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000:
                # Step 1: T-10s 扫描+设杠杆
                scan_at_ms = snipe_ms - CROSS_SNIPER_SCAN_OFFSET_SEC * 1000
                await asyncio.sleep(max(0, (scan_at_ms - time.time() * 1000) / 1000))
                logger.info("[跨交易所] 狙击扫描：距结算 %dms",
                             max(0, int(snipe_ms - time.time() * 1000)))
                bn_bal, gt_bal = await asyncio.gather(
                    self._cross_get_bn_futures_balance(),
                    self._gate_futures_balance(),
                )
                if bn_bal <= 0 or gt_bal <= 0:
                    logger.error("[跨交易所] 狙击模式余额不足")
                    self._next_snipe_settle_ms = 0
                    return
                position_usdt = min(bn_bal, gt_bal) * CROSS_POSITION_SIZE_RATIO
                # 清 Gate tickers 5s 缓存，确保狙击用最新价格
                self._gate_ft_cache = {}
                # 狙击扫描 12s 超时保护，超时回退主扫缓存
                try:
                    cross_df = await asyncio.wait_for(
                        self._scan_cross_exchange(position_usdt, strict=True, snipe=True),
                        timeout=12,
                    )
                except asyncio.TimeoutError:
                    logger.warning("[跨交易所] 狙击扫描超时，回退主扫缓存数据")
                    cross_df = getattr(self, "_dash_cross_df", pd.DataFrame())
                self._dash_cross_df = cross_df
                passing = cross_df[cross_df["passes"] == True] if not cross_df.empty and "passes" in cross_df.columns else cross_df
                if passing.empty:
                    logger.info("[跨交易所] 狙击扫描无合格候选，放弃")
                    self._next_snipe_settle_ms = 0
                    await self._update_cross_dashboard(bn_bal, gt_bal)
                    return
                # 遍历通过筛选的候选，直到一个通过数量计算
                chosen = None
                for idx in range(min(10, len(passing))):
                    candidate = passing.iloc[idx]
                    logger.info("[跨交易所] 狙击目标 #%d %s: 费率差=%.4f%% 净收益=%.4f%%",
                                 idx + 1, str(candidate["base"]),
                                 float(candidate["rate_spread"]) * 100,
                                 float(candidate["net_rate"]) * 100)

                    # ── 计算开仓参数 ──
                    c_base = str(candidate["base"])
                    c_short_ex = str(candidate["short_exchange"])
                    c_long_ex = str(candidate["long_exchange"])
                    c_short_sym = str(candidate["short_symbol"])
                    c_long_sym = str(candidate["long_symbol"])
                    c_short_price = float(candidate["short_price"])
                    c_long_price = float(candidate["long_price"])
                    if c_short_price <= 0 or c_long_price <= 0:
                        logger.warning("[跨交易所] 狙击 #%d 价格为零，跳过", idx + 1)
                        continue
                    c_short_rate = float(candidate["short_rate"])
                    c_long_rate = float(candidate["long_rate"])
                    c_rate_spread = float(candidate["rate_spread"])
                    c_net_rate = float(candidate["net_rate"])
                    c_short_nft = float(candidate["short_next_funding_time_ms"])
                    c_long_nft = float(candidate["long_next_funding_time_ms"])

                    c_short_bal = bn_bal if c_short_ex == "binance" else gt_bal
                    c_long_bal = bn_bal if c_long_ex == "binance" else gt_bal
                    c_short_notional = c_short_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE
                    c_long_notional = c_long_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE
                    c_min_notional = min(c_short_notional, c_long_notional)
                    c_short_qty = await self._cross_calculate_amount(c_short_ex, c_short_sym, c_short_price, c_min_notional)
                    c_long_qty = await self._cross_calculate_amount(c_long_ex, c_long_sym, c_long_price, c_min_notional)
                    if c_short_qty <= 0 or c_long_qty <= 0:
                        logger.warning("[跨交易所] 狙击 #%d 数量不足，尝试下一位", idx + 1)
                        continue
                    est_short = c_min_notional
                    est_long = c_min_notional
                    if est_short > c_short_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE * 1.1 or \
                       est_long > c_long_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE * 1.1:
                        logger.warning("[跨交易所] 狙击 #%d 数量超限，跳过", idx + 1)
                        continue
                    if est_short < 5.5 or est_long < 5.5:
                        logger.warning("[跨交易所] 狙击 #%d 仓位太小(≈%.2f USDT)，跳过", idx + 1, est_short)
                        continue
                    chosen = candidate
                    break

                if chosen is None:
                    logger.warning("[跨交易所] 所有狙击候选均未通过数量计算，放弃本轮")
                    self._next_snipe_settle_ms = 0
                    return
                candidate = chosen
                # 目标确认后立即订阅 Gate 资费 WS，不等开仓成功后再订
                gt_sym = c_short_sym if c_short_ex == "gate" else c_long_sym
                self._gate_funding_symbol = gt_sym
                self._gate_funding_baseline = 0.0
                try:
                    await self._gate_subscribe_position(gt_sym)
                except Exception:
                    pass
                try:
                    existing = await self._safe_request(
                        "gate_funding_baseline",
                        lambda: self._fetch_gate_positions_direct(gt_sym),
                        default=[],
                    )
                    for pos in existing:
                        if pos.get("symbol") == gt_sym:
                            self._gate_funding_baseline = float(pos.get("info", {}).get("pnl_fund", 0) or 0)
                            break
                except Exception:
                    pass
                logger.info("[资费监听] Gate 目标 %s baseline=%.4f (已提前订阅)", gt_sym, self._gate_funding_baseline)
                # 提前设杠杆，省去开仓时 API 调用延迟 (~66ms)
                await asyncio.gather(
                    self._cross_ensure_leverage(c_short_ex, c_short_sym),
                    self._cross_ensure_leverage(c_long_ex, c_long_sym),
                )

                # Step 2: T-1s 只管发单
                open_at_ms = snipe_ms - CROSS_SNIPER_OPEN_OFFSET_MS
                await asyncio.sleep(max(0, (open_at_ms - time.time() * 1000) / 1000))
                logger.info("[跨交易所] 狙击开仓 %s: 距结算 %dms",
                             c_base, max(0, int(snipe_ms - time.time() * 1000)))
                ok = await self._open_cross_exchange_position(
                    base=c_base,
                    short_ex=c_short_ex, short_sym=c_short_sym,
                    long_ex=c_long_ex, long_sym=c_long_sym,
                    short_amount=c_short_qty, long_amount=c_long_qty,
                    short_price=c_short_price, long_price=c_long_price,
                    short_rate=c_short_rate, long_rate=c_long_rate,
                    rate_spread=c_rate_spread, net_rate=c_net_rate,
                    short_next_funding_time_ms=c_short_nft,
                    long_next_funding_time_ms=c_long_nft,
                )
                if ok:
                    settle_ms = max(c_short_nft, c_long_nft)
                    try:
                        # 清除双 WS 事件标记
                        self._funding_event.clear()
                        self._gate_funding_event.clear()
                        # 等到结算时刻
                        await asyncio.sleep(max(0, (settle_ms - time.time() * 1000) / 1000))
                        t_settle = time.perf_counter()
                        logger.info("[延迟] 到达结算时刻，开始等 WS 推送")
                        # 真正异步等双 WS 推送（set 瞬间唤醒，0 轮询延迟，最多 10s）
                        bn_t = asyncio.create_task(self._funding_event.wait())
                        gt_t = asyncio.create_task(self._gate_funding_event.wait())
                        await asyncio.wait([bn_t, gt_t], timeout=10.0)
                        t_event = time.perf_counter()
                        bn_ok = self._funding_event.is_set()
                        gt_ok = self._gate_funding_event.is_set()
                        ws_latency_ms = (t_event - t_settle) * 1000
                        logger.info("[延迟] WS 推送检测耗时: %.1fms (BN=%s GT=%s)", ws_latency_ms, bn_ok, gt_ok)
                        # WS 未收到的用 REST 兜底
                        if not bn_ok:
                            bn_sym = c_short_sym if c_short_ex == "binance" else c_long_sym
                            bn_ok = await self._check_bn_funding_received(bn_sym, settle_ms)
                        if not gt_ok:
                            gt_ok = await self._check_gate_funding_received(gt_sym)
                        logger.info("[资费监听] 确认完成: BN=%s GT=%s", bn_ok, gt_ok)
                        if not bn_ok or not gt_ok:
                            logger.warning("[资费监听] 未全部确认，强制平仓")
                    except Exception as exc:
                        logger.critical("[跨交易所] 等待资费异常: %s，直接平仓", exc)
                    finally:
                        t_close_start = time.perf_counter()
                        await self._close_cross_exchange_position()
                        t_close_end = time.perf_counter()
                        logger.info("[延迟] 平仓执行耗时: %.1fms", (t_close_end - t_close_start) * 1000)
                self._next_snipe_settle_ms = 0
                await self._update_cross_dashboard(bn_bal, gt_bal)
                return

        bn_bal, gt_bal = await asyncio.gather(
            self._cross_get_bn_futures_balance(),
            self._gate_futures_balance(),
        )
        logger.info("[跨交易所] 账户余额: BN合约=%.2f | GT合约=%.2f USDT", bn_bal, gt_bal)
        await self._update_cross_dashboard(bn_bal, gt_bal)

        if bn_bal <= 0 or gt_bal <= 0:
            logger.error("[跨交易所] 余额不足: BN=%.2f GT=%.2f", bn_bal, gt_bal)
            return

        position_usdt = min(bn_bal, gt_bal) * CROSS_POSITION_SIZE_RATIO
        logger.info("[跨交易所] 估算仓位: 每腿 ≈ %.2f USDT (使用率=%.0f%%)",
                     position_usdt, CROSS_POSITION_SIZE_RATIO * 100)

        logger.info("┏━━ [跨交易所] 主扫开始 ━━")
        cross_df = await self._scan_cross_exchange(position_usdt, strict=True)
        bn_fee, gt_fee = getattr(self, "_dash_cross_fees", (0.0, 0.0))
        logger.info("[跨交易所] 手续费: BN合约=%.4f%%(BNB折扣后) | GT合约=%.4f%%(返佣后) | 双边开平=%.4f%%",
                     bn_fee * 100, gt_fee * 100, 2 * (bn_fee + gt_fee) * 100)
        self._dash_cross_df = cross_df

        if cross_df.empty:
            # 严格筛选没结果 → 宽松扫展示接近的候选
            cross_df = await self._scan_cross_exchange(position_usdt, strict=False)
            self._dash_cross_df = cross_df
            if cross_df.empty:
                logger.info("[跨交易所] 两所无共同币种，无法扫描。")
                self._next_snipe_settle_ms = 0
                return

        # 更新下次狙击结算时间（只取 passes=True 中费率差最高的）
        if not cross_df.empty:
            passing = cross_df[cross_df["passes"] == True] if "passes" in cross_df.columns else cross_df
            if not passing.empty:
                best = passing.iloc[0]
                self._next_snipe_settle_ms = min(
                    float(best.get("short_next_funding_time_ms", 0)),
                    float(best.get("long_next_funding_time_ms", 0)),
                )
            else:
                self._next_snipe_settle_ms = 0

        self._print_cross_opportunity_table(cross_df, position_usdt)

        # 若刚发现机会且结算在 20s 内 → 直接跳狙击，不再等下一轮
        if not getattr(self, "_snipe_loop_guard", False) and self._next_snipe_settle_ms:
            remain_ms = self._next_snipe_settle_ms - time.time() * 1000
            if 0 < remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000 * 2:
                logger.info("[跨交易所] 距结算仅 %dms，直接进入狙击流程", int(remain_ms))
                self._snipe_loop_guard = True
                try:
                    await self._run_cross_exchange_cycle(False)
                finally:
                    self._snipe_loop_guard = False
                gc.collect()
                return

    async def _update_cross_dashboard(self, bn_fut_bal: float, gt_fut_bal: float) -> None:
        """在跨所模式下填充看板余额变量。"""
        self._dash_futures_usdt = bn_fut_bal
        # 用总余额(含锁定保证金)，避免开仓后曲线骤降
        bn_total, gt_total = await asyncio.gather(
            self._cross_get_bn_total_balance(),
            self._cross_get_gate_total_balance(),
        )
        self._dash_total_usdt = bn_total
        self._dash_gate_spot = gt_total
        self._dash_gate_df = pd.DataFrame()

    async def run_once(self, force_entry: bool = False) -> None:
        """执行一次持仓检查、全市场扫描和开仓/平仓判断。

        Binance 和 Gate.io 各自独立运行，互不阻挡。
        force_entry=True: 跳过结算时间检查，允许随时开仓。
        """
        # ── 跨交易所套利模式 ──
        if CROSS_EXCHANGE_ENABLED:
            await self._run_cross_exchange_cycle(force_entry)

            # ── 单交易所扫描（仅展示，不下单）──
            if CROSS_SHOW_SINGLE:
                await self._scan_single_for_dashboard()

            bn_total = getattr(self, "_dash_total_usdt", 0.0)
            gt_total = getattr(self, "_dash_gate_spot", 0.0)
            self._record_balance_snapshot(bn_total, gt_total)
            self._write_dashboard()
            # 强制回收内存，防止 RSS 持续上涨
            gc.collect()
            self._log_memory()
            return

        # ── 并行检查两交易所持仓 ──
        tasks = [self._check_binance_position()]
        gate_task = None
        if GATE_TRADING_ENABLED:
            gate_task = asyncio.create_task(self._has_gate_open_position())
        results = await asyncio.gather(*tasks)
        has_binance = bool(results[0])
        if gate_task:
            results.append(await gate_task)
        has_gate = bool(results[1]) if len(results) > 1 else False

        # ── 风险监控: Binance ──
        if has_binance and self.binance_state.direction == "reverse":
            margin_level = await self._check_margin_level(self.binance_state.spot_symbol)
            if margin_level is not None and margin_level < MARGIN_LEVEL_MIN:
                logger.critical("保证金率过低 %.2f < %.1f，强制平仓！",
                              margin_level, MARGIN_LEVEL_MIN)
                await self.close_arbitrage_position("binance")
                await asyncio.to_thread(self._send_email, "bazfbot 强制平仓！",
                    f"逐仓保证金率 {margin_level:.2f} 低于阈值 {MARGIN_LEVEL_MIN}，已强制平仓。")
                has_binance = False

        if has_binance and self.binance_state.direction == "forward":
            dist = await self._check_futures_liquidation_distance(
                self.binance_state.futures_symbol, short=True)
            if dist is not None and dist < FUTURES_LIQ_DISTANCE_MIN:
                logger.critical("合约空单距强平 %.1f%% < %.0f%%，强制平仓！",
                              dist * 100, FUTURES_LIQ_DISTANCE_MIN * 100)
                await self.close_arbitrage_position("binance")
                await asyncio.to_thread(self._send_email, "bazfbot 强制平仓！",
                    f"合约空单距强平仅 {dist*100:.1f}%，已强制平仓。")
                has_binance = False

        if has_gate:
            logger.info("Gate 持仓中，风险监控 (Gate 暂不细粒度风控)。")

        # ── force_entry 时强平所有持仓 ──
        if force_entry and (has_binance or has_gate):
            logger.info("即时模式：强制平仓所有持仓。")
            if has_binance:
                await self.close_arbitrage_position("binance")
                has_binance = False
            if has_gate:
                await self.close_arbitrage_position("gate")
                has_gate = False

        # ── Binance 交易循环 ──
        spot_usdt = futures_usdt = margin_total = 0.0
        await self._run_once_binance(force_entry, has_binance, has_gate)

        # ── Gate.io 交易循环 ──
        if GATE_TRADING_ENABLED:
            await self._run_once_gate(has_binance, has_gate)

        # 记录余额快照
        bn_total = getattr(self, "_dash_total_usdt", 0.0)
        gt_total = getattr(self, "_dash_gate_spot", 0.0)
        self._record_balance_snapshot(bn_total, gt_total)

        # 写看板
        self._write_dashboard()

    async def _run_once_binance(self, force_entry: bool, has_binance: bool, has_gate: bool) -> None:
        """Binance 独立交易循环。"""
        has_position = has_binance

        scan_for_switch = has_position and self._should_exit("binance")
        if scan_for_switch:
            if not self.binance_state.locked:
                logger.info("[Binance] 自由人模式，扫描全市场寻找换仓机会...")
            else:
                logger.info("[Binance] 锁定期结算已过，扫描全市场对比择优...")
        elif has_position:
            if self.binance_state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.binance_state.next_funding_time_ms / 1000, tz=self.tz)
                wait_min = max(0, (self.binance_state.next_funding_time_ms - time.time() * 1000) / 60000)
                logger.info("[Binance] 锁定期，结算时间 %s (%.0f 分钟后)，持仓待涨。",
                            settle_dt.strftime("%H:%M:%S"), wait_min)
            else:
                logger.info("[Binance] 锁定期，持仓待涨。")
            self._print_position_summary("binance")

        if not await self.has_enough_bnb_for_fees():
            return

        logger.info("┏━━ [Binance] 主扫开始 ━━")
        spot_free, futures_free = await asyncio.gather(
            self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
            self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")

        margin_total = 0.0
        try:
            resp = await self._binance_request(BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account")
            for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                q = acct.get("quoteAsset", {})
                if q:
                    margin_total += float(q.get("netAsset", 0))
        except Exception:
            pass

        total_usdt = spot_usdt + futures_usdt + margin_total
        logger.info("[Binance] 账户余额: 现货=%.2f | 合约=%.2f | 逐仓杠杆=%.2f | 合计=%.2f USDT",
                    spot_usdt, futures_usdt, margin_total, total_usdt)
        if total_usdt <= 0:
            logger.error("[Binance] 可用余额为 0，请充值。")
            return
        estimate_forward = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
        estimate_reverse = total_usdt / 2 * POSITION_SIZE_RATIO
        position_usdt = max(estimate_forward, estimate_reverse)
        if position_usdt <= 0:
            logger.error("[Binance] 估算仓位为 0，跳过本轮。")
            return

        df_forward, df_reverse = await asyncio.gather(
            self.scan_best_opportunity(position_usdt),
            self.scan_reverse_opportunities(position_usdt),
        )
        frames = []
        if not df_forward.empty:
            frames.append(df_forward)
        if not df_reverse.empty:
            frames.append(df_reverse)

        if not frames:
            logger.info("[Binance] 本轮未扫描到满足条件的机会。")
            return

        df = pd.concat(frames, ignore_index=True).sort_values(
            "net_rate", ascending=False).reset_index(drop=True)

        # 预借逻辑
        if REVERSE_ENABLED and not has_position:
            if self.binance_state.pre_borrow_base:
                pre_row = df[(df["base"] == self.binance_state.pre_borrow_base)
                             & (df.get("direction", "forward") == "reverse")]
                if not pre_row.empty:
                    r = pre_row.iloc[0]
                    net = float(r["net_rate"])
                    target = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER
                    if net > target:
                        elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                            self.binance_state.pre_borrow_at)).total_seconds() / 60 if self.binance_state.pre_borrow_at else 0
                        logger.info("[预借→秒开] %s | 净利=%+.4f%% 已到开仓线 | 等待 %.0f min",
                                   self.binance_state.pre_borrow_base, net * 100, elapsed)
                        self.binance_state.pre_borrow_base = ""
                        self.binance_state.pre_borrow_margin_symbol = ""
                        self.binance_state.pre_borrow_amount = 0.0
                        self.binance_state.pre_borrow_at = ""
                        self._save_state()
                        if await self._open_reverse_position(r):
                            return
                        logger.warning("[预借→秒开] 开仓失败，下轮重试。")
                    else:
                        elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                            self.binance_state.pre_borrow_at)).total_seconds() / 60
                        if elapsed > PRE_BORROW_TIMEOUT_MINUTES:
                            await self._cancel_pre_borrow()
                else:
                    logger.info("[预借] %s 已不在候选列表 → 取消", self.binance_state.pre_borrow_base)
                    await self._cancel_pre_borrow()
            elif total_usdt > 10:
                median, std = self._compute_funding_stats(df)
                anomaly = self._detect_rate_anomaly(df, median, std)
                if anomaly is not None:
                    base = str(anomaly["base"])
                    rate = float(anomaly["predicted_funding_rate"]) * 100
                    logger.info("费率异动: %s → %.4f%% (中位数=%.4f%%, σ=%.4f%%)，触发预借！",
                               base, rate, median * 100, std * 100)
                    await self._execute_pre_borrow(anomaly)

        # 打印 Binance Top 10
        top10 = df.head(10)
        sep = "  " + "-" * 113
        header = (f"  {'#':<3} {'方向':<4} {'币种':<8} {'来源':<5} {'资金费率':>8} "
                  f"{'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'结算倒计时':>10}")
        logger.info("\n" + "=" * 113 + "\n"
                    "  Binance 套利机会 Top 10 — 每腿名义: %.2f USDT\n" % position_usdt
                    + "=" * 113 + "\n" + header + "\n" + sep)
        for rank, (_, row) in enumerate(top10.iterrows(), 1):
            nft = float(row.get("next_funding_time_ms", 0))
            if nft > 0:
                mins = max(0, (nft - time.time() * 1000) / 60000)
                countdown = f"{int(mins)}min后" if mins < 120 else f"{mins/60:.0f}h后"
            else:
                countdown = "N/A"
            direction_label = "反向" if str(row.get("direction")) == "reverse" else "正向"
            borrow_fee = float(row.get("borrow_hourly_rate") or 0)
            held = has_position and str(row["base"]) == self.binance_state.base and str(row.get("direction", "forward")) == self.binance_state.direction
            held_tag = " ← 持仓" if held else ""
            logger.info("  %-3d %-4s %-8s %-5s %7.3f%% %7.3f%% %7.3f%% %7.3f%% %7.3f%% %10s%s",
                       rank, direction_label, str(row["base"])[:8], str(row["spot_source"])[:5],
                       float(row["predicted_funding_rate"]) * 100,
                       float(row["spot_taker_fee"]) * 100,
                       float(row["futures_taker_fee"]) * 100,
                       borrow_fee * 100, float(row["net_rate"]) * 100, countdown, held_tag)
        logger.info(sep + "\n" + "=" * 113)

        # Binance 交易决策
        await self._trade_decision(df, "binance", self.binance_state, has_position,
                                   scan_for_switch, force_entry, spot_usdt,
                                   futures_usdt, margin_total, total_usdt, position_usdt)

        # 缓存仪表盘数据
        self._dash_futures_usdt = futures_usdt
        self._dash_total_usdt = total_usdt
        self._dash_df = df

    async def _run_once_gate(self, has_binance: bool, has_gate: bool) -> None:
        """Gate.io 独立交易循环。"""
        has_position = has_gate

        scan_for_switch = has_position and self._should_exit("gate")
        if scan_for_switch:
            if not self.gate_state.locked:
                logger.info("[Gate] 自由人模式，扫描全市场寻找换仓机会...")
            else:
                logger.info("[Gate] 锁定期结算已过，扫描全市场对比择优...")
        elif has_position:
            if self.gate_state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.gate_state.next_funding_time_ms / 1000, tz=self.tz)
                wait_min = max(0, (self.gate_state.next_funding_time_ms - time.time() * 1000) / 60000)
                logger.info("[Gate] 锁定期，结算时间 %s (%.0f 分钟后)，持仓待涨。",
                            settle_dt.strftime("%H:%M:%S"), wait_min)
            else:
                logger.info("[Gate] 锁定期，持仓待涨。")
            self._print_position_summary("gate")

        # 估算 Gate 仓位（单币种保证金模式：现货=合约=统一余额）
        try:
            gate_spot_usdt = await self._gate_spot_balance()
        except Exception:
            gate_spot_usdt = 0.0
        gate_position_usdt = (gate_spot_usdt / 2) * POSITION_SIZE_RATIO if gate_spot_usdt > 0 else 100.0

        logger.info("[Gate] 账户余额: %.2f USDT (统一账户)", gate_spot_usdt)

        try:
            df_gf, df_gr = await asyncio.gather(
                self._scan_gate_forward(gate_position_usdt),
                self._scan_gate_reverse(gate_position_usdt),
            )
        except Exception as exc:
            logger.warning("Gate.io 扫描失败: %s", exc)
            return

        gframes = []
        if not df_gf.empty:
            gframes.append(df_gf)
        if not df_gr.empty:
            gframes.append(df_gr)

        if not gframes:
            logger.info("[Gate] 本轮未扫描到满足条件的机会。")
            return

        gdf = pd.concat(gframes, ignore_index=True).sort_values(
            "net_rate", ascending=False).reset_index(drop=True)

        # 打印 Gate Top 10
        gtop = gdf.head(10)
        gsep = "  " + "-" * 113
        gheader = (f"  {'#':<3} {'方向':<4} {'币种':<8} {'来源':<5} {'资金费率':>8} "
                   f"{'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'结算倒计时':>10}")
        logger.info("\n" + "=" * 113 + "\n"
                    "  Gate.io 套利机会 Top 10 — 每腿名义: %.2f USDT\n" % gate_position_usdt
                    + "=" * 113 + "\n" + gheader + "\n" + gsep)
        for rank, (_, row) in enumerate(gtop.iterrows(), 1):
            nft = float(row.get("next_funding_time_ms", 0))
            if nft > 0:
                mins = max(0, (nft - time.time() * 1000) / 60000)
                countdown = f"{int(mins)}min后" if mins < 120 else f"{mins/60:.0f}h后"
            else:
                countdown = "N/A"
            direction_label = "反向" if str(row.get("direction")) == "reverse" else "正向"
            borrow_fee = float(row.get("borrow_hourly_rate") or 0)
            logger.info("  %3d  %s  %-8s %-5s %+8.3f%% %7.3f%% %7.3f%% %7.3f%% %+8.3f%% %10s",
                       rank, direction_label, str(row["base"])[:8],
                       str(row.get("spot_source", "spot"))[:5],
                       float(row["predicted_funding_rate"]) * 100,
                       float(row["spot_taker_fee"]) * 100,
                       float(row["futures_taker_fee"]) * 100,
                       borrow_fee * 100, float(row["net_rate"]) * 100, countdown)
        logger.info(gsep + "\n" + "=" * 113)

        # Gate 交易决策
        await self._trade_decision(gdf, "gate", self.gate_state, has_position,
                                   scan_for_switch, False,
                                   gate_spot_usdt, 0.0, 0.0,
                                   gate_spot_usdt, gate_position_usdt)

        # 缓存 Gate 仪表盘数据（单币种保证金：现货即总余额）
        self._dash_gate_spot = gate_spot_usdt
        self._dash_gate_df = gdf

    async def _trade_decision(self, df: pd.DataFrame, exchange: str,
                              state: ArbitrageState, has_position: bool,
                              scan_for_switch: bool, force_entry: bool,
                              spot_usdt: float, futures_usdt: float,
                              margin_total: float, total_usdt: float,
                              position_usdt: float) -> None:
        """统一交易决策：换仓/平仓/开仓。exchange="binance"|"gate"。"""
        if scan_for_switch:
            best = None
            if not df.empty:
                for _, row in df.iterrows():
                    net = float(row["net_rate"])
                    direction = str(row.get("direction", "forward"))
                    threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER if direction == "reverse" else MIN_NET_RATE
                    if net <= threshold:
                        continue
                    nft = float(row.get("next_funding_time_ms", 0))
                    same = (str(row["base"]) == state.base and direction == state.direction)
                    if direction != "reverse":
                        if not self._within_entry_window(nft) and nft > 0 and not same:
                            continue
                    best = row
                    break

            if best is None:
                if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                    logger.info("[%s] 无合格候选，手持币未到结算窗口 (结算 %s)，继续持有。",
                               exchange, settle_dt.strftime("%m-%d %H:%M"))
                    return
                logger.info("[%s] 结算窗口内无合格候选，平仓。", exchange)
                await self.close_arbitrage_position(exchange)
                return

            best_base = str(best["base"])
            best_direction = str(best.get("direction", "forward"))
            same_coin = (best_base == state.base and best_direction == state.direction)

            if same_coin:
                keep_value = float(best["net_rate"])
                nft = float(best.get("next_funding_time_ms", 0))
                if keep_value > 0:
                    state.next_funding_time_ms = nft if nft > 0 else 0
                    state.locked = False
                    state.opened_at = datetime.now(tz=self.tz).isoformat()
                    self._save_state(exchange)
                    if nft > 0:
                        settle_dt = datetime.fromtimestamp(nft / 1000, tz=self.tz)
                        logger.info("[%s] 当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式，下轮结算 %s。",
                                   exchange, state.base, keep_value * 100,
                                   settle_dt.strftime("%m-%d %H:%M"))
                    else:
                        logger.info("[%s] 当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式。",
                                   exchange, state.base, keep_value * 100)
                    return
                if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                    logger.info("[%s] 当前 %s 净利=%.4f%% 未到结算窗口 (结算 %s)，继续持有。",
                               exchange, state.base, keep_value * 100,
                               settle_dt.strftime("%m-%d %H:%M") if settle_dt else "?")
                    return
                logger.info("[%s] 当前 %s 结算窗口内净利=%.4f%%，平仓。", exchange, state.base, keep_value * 100)
                await self.close_arbitrage_position(exchange)
                return

            # 不同币
            current_mask = (df["base"] == state.base) & (df["direction"] == state.direction)
            if current_mask.any():
                cr = df[current_mask].iloc[0]
                keep_value = float(cr["predicted_funding_rate"]) - float(cr.get("borrow_hourly_rate", 0)) \
                    if state.direction == "reverse" else float(cr["predicted_funding_rate"])
            else:
                keep_value = -1.0

            close_fee_rate = float(best["spot_taker_fee"]) + float(best["futures_taker_fee"])
            switch_value = float(best["open_only_net_rate"]) - close_fee_rate

            if switch_value > keep_value + MIN_SWITCH_SPREAD and switch_value > 0:
                logger.info("[%s] 切换: %s→%s | 保持净利=%.4f%% < 切换净利=%.4f%% (利差>%.3f%%)",
                           exchange, state.base, best_base, keep_value * 100,
                           switch_value * 100, MIN_SWITCH_SPREAD * 100)
                if await self.close_arbitrage_position(exchange):
                    await self.open_arbitrage_position(best)
                return

            if keep_value > 0:
                nft = float(cr.get("next_funding_time_ms", 0))
                state.next_funding_time_ms = nft if nft > 0 else 0
                state.locked = False
                state.opened_at = datetime.now(tz=self.tz).isoformat()
                self._save_state(exchange)
                logger.info("[%s] 当前 %s 仍有正收益 (净利=%.4f%%)，解锁自由人模式。",
                           exchange, state.base, keep_value * 100)
                return

            if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                logger.info("[%s] 当前 %s 净利=%.4f%% 未到结算窗口 (结算 %s)，继续持有。",
                           exchange, state.base, keep_value * 100, settle_dt.strftime("%m-%d %H:%M"))
                return
            logger.info("[%s] 当前 %s 结算窗口内净利=%.4f%%，平仓。", exchange, state.base, keep_value * 100)
            await self.close_arbitrage_position(exchange)
            return

        # 无持仓：找最优开仓候选
        if not has_position:
            for _, candidate in df.iterrows():
                net = float(candidate["net_rate"])
                direction = str(candidate.get("direction", "forward"))
                threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER if direction == "reverse" else MIN_NET_RATE
                if net <= threshold:
                    break
                nft = float(candidate.get("next_funding_time_ms", 0))
                if direction != "reverse":
                    if not self._within_entry_window(nft) and nft > 0 and not force_entry:
                        continue
                logger.info("[%s] 开仓 %s: net=%.4f%% > threshold=%.4f%%",
                           exchange, candidate["base"], net * 100, threshold * 100)
                ok = await self.open_arbitrage_position(candidate)
                if ok:
                    return
                logger.warning("[%s] 开仓失败，尝试下一个候选。", exchange)
            logger.info("[%s] 所有候选均不符合开仓条件。", exchange)


    async def _query_evm_gas_price(self, chain: str) -> float | None:
        """Query current gas price for an EVM chain via JSON-RPC. Returns gas price in USD per tx."""

        rpc_url = CHAIN_RPC_URL.get(chain)
        if not rpc_url:
            return None
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_gasPrice",
            "params": [],
        }).encode("utf-8")
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            rpc_url,
                            data=payload,
                            headers={
                                "Content-Type": "application/json",
                                "User-Agent": "funding-arbitrage-bot/1.0",
                            },
                        )
                    ).read()
                )
            )
            gas_wei = int(response.get("result", "0x0"), 16)
            if gas_wei <= 0:
                return None
            # Standard ERC-20 transfer: ~65000 gas. Simple swap: ~150000 gas.
            # Alpha DEX trade: estimate ~200000 gas.
            estimated_gas_units = 200_000
            gas_eth = (gas_wei * estimated_gas_units) / 1e18
            # Approximate ETH price for gas chains (BSC uses BNB ~$600, others use ETH ~$2500)
            eth_price = 600 if chain == "BSC" else 2500
            return gas_eth * eth_price
        except Exception as exc:
            logger.warning("Failed to query gas for %s: %s", chain, exc)
            return None

    async def _query_sui_gas_price(self) -> float | None:
        """Query Sui reference gas price. Returns gas price in USD per tx."""

        rpc_url = CHAIN_RPC_URL.get("Sui")
        if not rpc_url:
            return None
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_getReferenceGasPrice",
            "params": [],
        }).encode("utf-8")
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            rpc_url,
                            data=payload,
                            headers={
                                "Content-Type": "application/json",
                                "User-Agent": "funding-arbitrage-bot/1.0",
                            },
                        )
                    ).read()
                )
            )
            ref_price_mist = int(response.get("result", "0"), 10)
            if ref_price_mist <= 0:
                return None
            # Sui: ~5M gas units for a simple move call. SUI price ~$2.
            estimated_gas_units = 5_000_000
            gas_sui = (ref_price_mist * estimated_gas_units) / 1e9  # MIST -> SUI
            return gas_sui * 2.0  # SUI ~$2
        except Exception as exc:
            logger.warning("Failed to query Sui gas: %s", exc)
            return None

    async def _check_alpha_gas(self, chain: str, expected_gas_usd: float) -> bool:
        """Check if current on-chain gas is within acceptable range. Returns True if safe to proceed."""
        if not chain or chain not in CHAIN_RPC_URL:
            return True  # Solana or unknown chain — gas is stable enough, skip check

        if chain in EVM_CHAINS:
            actual_gas = await self._query_evm_gas_price(chain)
        elif chain == "Sui":
            actual_gas = await self._query_sui_gas_price()
        else:
            return True

        if actual_gas is None:
            logger.warning("无法查询 %s 链上实时 Gas，跳过 Gas 监测，允许开仓。", chain)
            return True

        limit = expected_gas_usd * MAX_ALPHA_GAS_MULTIPLIER
        if actual_gas > limit:
            logger.warning(
                "Alpha Gas 超标: chain=%s 实际=$%.4f 预期=$%.4f 上限=$%.4f 跳过开仓",
                chain, actual_gas, expected_gas_usd, limit,
            )
            return False

        logger.info(
            "Alpha Gas 正常: chain=%s 实际=$%.4f 预期=$%.4f",
            chain, actual_gas, expected_gas_usd,
        )
        return True

    def _print_position_summary(self, exchange: str = "binance") -> None:
        state = self.gate_state if exchange == "gate" else self.binance_state
        if not state.is_open:
            return
        position_notional = state.amount * state.entry_price
        estimated_profit = position_notional * state.net_rate
        if state.direction == "reverse":
            side_label = "long"
            source_label = "margin"
        else:
            side_label = "short"
            source_label = state.spot_source
        tag = "Gate" if exchange == "gate" else "Binance"
        logger.info(
            "[%s] 套利持仓 [%s]: %s [%s] + %s %s | amount=%s | entry=%s "
            "| 预测单期净利润≈%.4f USDT | opened_at=%s",
            tag, state.direction,
            state.spot_symbol, source_label,
            state.futures_symbol, side_label,
            state.amount, state.entry_price,
            estimated_profit, state.opened_at,
        )

    async def run_forever(self) -> None:
        """打卡模式主循环: 有持仓按实际结算时间平仓, 无持仓定时扫描。
        双循环: REST 快速轮询费率 (预借) + 60s 全量扫 (开平仓决策)。
        有持仓时 1s 密集监控，发现单腿消失立即平另一腿。"""
        await self.initialize()

        if CROSS_EXCHANGE_ENABLED:
            logger.info("启动完成，进入跨交易所套利循环：60s 全量决策（有持仓时 1s 守护）。")
        else:
            logger.info("启动完成，进入双循环: REST 费率轮询 + 60s 全量决策。")

        async def poller_loop():
            if not CROSS_EXCHANGE_ENABLED:
                await self._run_ws_monitor()

        async def position_watchdog():
            """有跨所持仓时每秒验证，单腿消失 → 立即平另一腿。"""
            while True:
                if CROSS_EXCHANGE_ENABLED and self.cross_state.is_open:
                    await self._verify_cross_position()
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)  # 无持仓时低频率

        async def main_loop():
            first_run = True
            while True:
                if not first_run:
                    sleep_seconds = self._seconds_until_next_wakeup()
                    wakeup_at = datetime.now(tz=self.tz) + timedelta(seconds=sleep_seconds)
                    has_pos = await self.has_open_arbitrage_position()
                    logger.info(
                        "┗━━ 主扫结束 | 休眠 %.0f 分钟, 下次: %s (%s持仓)",
                        sleep_seconds / 60,
                        wakeup_at.strftime("%m-%d %H:%M:%S"),
                        "有" if has_pos else "无",
                    )
                    await asyncio.sleep(sleep_seconds)
                else:
                    first_run = False
                async with self._scan_lock:
                    await self.run_once()

        poller_task = asyncio.create_task(poller_loop())
        watchdog_task = asyncio.create_task(position_watchdog())
        main_task = asyncio.create_task(main_loop())

        try:
            await asyncio.gather(poller_task, watchdog_task, main_task)
        except Exception:
            poller_task.cancel()
            watchdog_task.cancel()
            main_task.cancel()
            raise

    async def _scan_single_for_dashboard(self) -> None:
        """跨所模式下扫描单交易所套利机会（仅展示，不交易）。
        BN 和 Gate 并行扫描，互不依赖。"""
        async def _scan_bn() -> None:
            try:
                spot_free, futures_free = await asyncio.gather(
                    self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
                    self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
                )
                spot_usdt = self._free_balance(spot_free, "USDT")
                futures_usdt = self._free_balance(futures_free, "USDT")

                margin_total = 0.0
                try:
                    resp = await self._binance_request(BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account")
                    for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                        q = acct.get("quoteAsset", {})
                        if q:
                            margin_total += float(q.get("netAsset", 0))
                except Exception:
                    pass

                total_usdt = spot_usdt + futures_usdt + margin_total
                estimate_fwd = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
                estimate_rev = total_usdt / 2 * POSITION_SIZE_RATIO
                position_usdt = max(estimate_fwd, estimate_rev, 100)

                df_forward, df_reverse = await asyncio.gather(
                    self.scan_best_opportunity(position_usdt),
                    self.scan_reverse_opportunities(position_usdt),
                )
                frames = [d for d in [df_forward, df_reverse] if not d.empty]
                if frames:
                    df = pd.concat(frames, ignore_index=True).sort_values(
                        "net_rate", ascending=False).reset_index(drop=True)
                else:
                    df = pd.DataFrame()

                self._dash_futures_usdt = futures_usdt
                self._dash_total_usdt = total_usdt
                self._dash_df = df
            except Exception:
                pass

        async def _scan_gate() -> None:
            if not GATE_TRADING_ENABLED:
                return
            try:
                gate_spot_usdt = await self._gate_spot_balance()
                gate_position_usdt = (gate_spot_usdt / 2) * POSITION_SIZE_RATIO if gate_spot_usdt > 0 else 100.0
                df_gf, df_gr = await asyncio.gather(
                    self._scan_gate_forward(gate_position_usdt),
                    self._scan_gate_reverse(gate_position_usdt),
                )
                gframes = [d for d in [df_gf, df_gr] if not d.empty]
                if gframes:
                    gdf = pd.concat(gframes, ignore_index=True).sort_values(
                        "net_rate", ascending=False).reset_index(drop=True)
                else:
                    gdf = pd.DataFrame()
                self._dash_gate_spot = gate_spot_usdt
                self._dash_gate_df = gdf
            except Exception:
                pass

        await asyncio.gather(_scan_bn(), _scan_gate())

    def _write_dashboard(self) -> None:
        """生成手机端自适应 Dashboard。含盈亏日历、资金曲线。"""
        futures_usdt = getattr(self, "_dash_futures_usdt", 0.0)
        total_usdt = getattr(self, "_dash_total_usdt", 0.0)
        df = getattr(self, "_dash_df", pd.DataFrame())
        gate_spot = getattr(self, "_dash_gate_spot", 0.0)
        gate_df = getattr(self, "_dash_gate_df", pd.DataFrame())
        cross_df = getattr(self, "_dash_cross_df", pd.DataFrame())

        now_str = datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S")
        now = datetime.now(tz=self.tz)

        # ── 合并扫描结果 ──
        combined_rows = []
        if not df.empty:
            for _, row in df.iterrows():
                d = row.to_dict()
                d["exchange"] = "BN"
                combined_rows.append(d)
        if not gate_df.empty:
            for _, row in gate_df.iterrows():
                d = row.to_dict()
                d["exchange"] = "GT"
                combined_rows.append(d)
        combined_rows.sort(key=lambda r: float(r.get("net_rate", -999)), reverse=True)
        top10 = combined_rows[:10]

        # ── 跨交易所 Top 10 ──
        cross_top10 = []
        if not cross_df.empty:
            for _, row in cross_df.head(10).iterrows():
                cross_top10.append(row.to_dict())

        # ── 交易历史 & 累计盈亏 ──
        history = self._load_trade_history()

        def _get_real_pnl(h: dict) -> float:
            """真实净盈亏：优先用 net_pnl，旧记录回退 profit_usdt。"""
            n = h.get("net_pnl")
            if n is not None:
                return float(n)
            return float(h.get("profit_usdt", 0) or 0)

        total_profit = sum(_get_real_pnl(h) for h in history)
        profit_cls = "positive" if total_profit >= 0 else "negative"

        # 累计分项：平仓盈亏 / 资费 / 手续费
        cum_price = 0.0
        cum_funding = 0.0
        cum_fee = 0.0
        for h in history:
            if "short_exchange" in h:
                cum_price += (h.get("short_price_pnl", 0) or 0) + (h.get("long_price_pnl", 0) or 0)
                cum_funding += (h.get("short_funding_pnl", 0) or 0) + (h.get("long_funding_pnl", 0) or 0)
                sf = (h.get("short_actual_fee") or h.get("short_fee")) or 0
                lf = (h.get("long_actual_fee") or h.get("long_fee")) or 0
                cum_fee += sf + lf
            elif "price_pnl" in h:
                cum_price += (h.get("price_pnl", 0) or 0)
                cum_funding += (h.get("funding_pnl", 0) or 0)
                cum_fee += (h.get("fee_total", 0) or 0)

        # 月度盈亏：按日期汇总（使用真实净盈亏）
        month_pnl: dict[str, float] = {}
        for h in history:
            ts = h.get("ts") or h.get("time", "")
            try:
                d = ts[:10]  # YYYY-MM-DD
            except (ValueError, KeyError):
                continue
            month_pnl[d] = month_pnl.get(d, 0) + _get_real_pnl(h)

        # 当日盈亏
        today_str = now.strftime("%Y-%m-%d")
        today_pnl = month_pnl.get(today_str, 0.0)
        today_cls = "positive" if today_pnl >= 0 else "negative"

        # ── 统计 ──
        total_trades = len(history)
        if total_trades > 0:
            wins = sum(1 for h in history if _get_real_pnl(h) > 0)
            win_rate = wins / total_trades * 100
            best = max(_get_real_pnl(h) for h in history)
            worst = min(_get_real_pnl(h) for h in history)
            stats_html = (
                f'<div class="stats">'
                f'<span>总交易 <b>{total_trades}</b> 笔</span>'
                f'<span>胜率 <b>{win_rate:.0f}%</b></span>'
                f'<span>累计 <b class="{profit_cls}">{total_profit:+.4f}</b></span>'
                f'<span>最佳 <b class="positive">+{best:.4f}</b></span>'
                f'<span>最差 <b class="negative">{worst:.4f}</b></span>'
                f'</div>')
        else:
            stats_html = '<div class="stats"><span>暂无交易记录</span></div>'

        # ── 余额快照（资金曲线）──
        snaps = []
        if self.BALANCE_SNAPSHOT_FILE.exists():
            try:
                with self.BALANCE_SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
                    snaps = json.load(f)
            except (OSError, json.JSONDecodeError):
                pass
        curve_labels, curve_total, curve_bn, curve_gt = [], [], [], []
        if snaps:
            step = max(1, len(snaps) // 200)
            first_date = snaps[0].get("ts", "")[:10]
            last_date = snaps[-1].get("ts", "")[:10]
            same_day = first_date == last_date
            for s in snaps[::step]:
                ts = s.get("ts", "")
                if same_day:
                    label = ts[11:16]
                else:
                    label = ts[5:16].replace("T", " ")
                bn_v = s.get("binance", 0)
                gt_v = s.get("gate", 0)
                curve_labels.append(label)
                curve_total.append(round(bn_v + gt_v, 2))
                curve_bn.append(round(bn_v, 2))
                curve_gt.append(round(gt_v, 2))

        # ── 月历数据（JS 动态渲染）──
        pnl_json = json.dumps(month_pnl, ensure_ascii=False)
        today_iso = now.strftime("%Y-%m-%d")

        # ── 余额卡片 ──
        grand_total = futures_usdt + (gate_spot if GATE_TRADING_ENABLED else 0)
        gate_card = ""
        if GATE_TRADING_ENABLED:
            gate_card = f"""<div class="card"><div class="label">Gate 交易</div><div class="value" style="color:#2955E7">{gate_spot:,.2f}</div></div>
            """
        balance_cards = f"""
        <div class="balances">
            <div class="card"><div class="label">BN 合约</div><div class="value" style="color:#f0b90b">{futures_usdt:,.2f}</div></div>
            {gate_card}<div class="card"><div class="label">总计</div><div class="value" style="color:#e0e0e0">{grand_total:,.2f}</div></div>
            <div class="card"><div class="label">累计盈亏</div><div class="value {profit_cls}">{total_profit:+.4f}</div></div>
            <div class="card"><div class="label">今日盈亏</div><div class="value {today_cls}">{today_pnl:+.4f}</div></div>
            <div class="card"><div class="label">平仓盈亏</div><div class="value {"positive" if cum_price>=0 else "negative"}">{cum_price:+.4f}</div></div>
            <div class="card"><div class="label">累计资费</div><div class="value {"positive" if cum_funding>=0 else "negative"}">{cum_funding:+.4f}</div></div>
            <div class="card"><div class="label">手续费</div><div class="value negative">{cum_fee:.4f}</div></div>
        </div>"""

        # ── 当前持仓 ──
        def _pos_html(state, tag):
            if not state.is_open:
                return ""
            pos_notional = state.amount * state.entry_price
            est_profit = pos_notional * state.net_rate
            dir_label = "反向" if state.direction == "reverse" else "正向"
            dir_cls = "reverse" if state.direction == "reverse" else "forward"
            settle_dt = ""
            if state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    state.next_funding_time_ms / 1000, tz=self.tz
                ).strftime("%m-%d %H:%M:%S")
            hold_str = ""
            if state.opened_at:
                try:
                    opened = datetime.fromisoformat(state.opened_at)
                    elapsed = now - opened
                    h = int(elapsed.total_seconds() // 3600)
                    m = int((elapsed.total_seconds() % 3600) // 60)
                    hold_str = f"已持有: {h}h{m:02d}m"
                except (ValueError, TypeError):
                    pass
            return f"""
            <h3>{tag} <span class="muted" style="font-size:0.7rem">{hold_str}</span></h3>
            <div class="position {dir_cls}">
                <span class="badge {dir_cls}">{dir_label}</span>
                <strong>{state.base}</strong>
                <span>数量: {state.amount:.4f}</span>
                <span>入场价: {state.entry_price:.4f}</span>
                <span>预估净利润: <span class="{'positive' if est_profit > 0 else 'negative'}">{est_profit:+.4f} USDT</span></span>
                <span>净费率: <span class="{'positive' if state.net_rate > 0 else 'negative'}">{state.net_rate*100:+.4f}%</span></span>
                <span>下次结算: {settle_dt}</span>
            </div>"""

        if self.binance_state.is_open or self.gate_state.is_open or self.cross_state.is_open:
            cross_html = ""
            if self.cross_state.is_open:
                cs = self.cross_state
                cs_notional = cs.short_amount * cs.short_entry_price if cs.short_entry_price > 0 else cs.amount
                cs_est = cs_notional * cs.total_net_rate
                hold_str = ""
                if cs.opened_at:
                    try:
                        opened = datetime.fromisoformat(cs.opened_at)
                        elapsed = now - opened
                        h = int(elapsed.total_seconds() // 3600)
                        m = int((elapsed.total_seconds() % 3600) // 60)
                        hold_str = f"已持有: {h}h{m:02d}m"
                    except (ValueError, TypeError):
                        pass
                short_settle = ""
                if cs.short_next_funding_time_ms > 0:
                    short_settle = datetime.fromtimestamp(cs.short_next_funding_time_ms / 1000, tz=self.tz).strftime("%m-%d %H:%M:%S")
                long_settle = ""
                if cs.long_next_funding_time_ms > 0:
                    long_settle = datetime.fromtimestamp(cs.long_next_funding_time_ms / 1000, tz=self.tz).strftime("%m-%d %H:%M:%S")
                cross_html = f"""
            <h3>🌐 跨交易所套利 <span class="muted" style="font-size:0.7rem">{hold_str}</span></h3>
            <div class="position cross" style="border:2px solid #f0b90b; background:linear-gradient(135deg,#1a2a1a,#1a1a2a)">
                <strong>{cs.base}</strong>
                <span>数量: 空{cs.short_amount:.4f}张 / 多{cs.long_amount:.4f}张</span>
                <span>费率差: <span class="positive">{cs.rate_spread*100:+.4f}%</span></span>
                <span>预估利润: <span class="{'positive' if cs_est > 0 else 'negative'}">{cs_est:+.4f} USDT</span></span>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px">
                <div style="padding:8px;background:rgba(240,75,75,0.15);border-radius:6px">
                    <span style="color:#e74c3c">🔻 做空 @ {cs.short_exchange.upper()}</span><br>
                    <span style="font-size:0.8rem">费率: {cs.short_rate*100:+.4f}% | 结算: {short_settle}</span>
                </div>
                <div style="padding:8px;background:rgba(14,203,129,0.15);border-radius:6px">
                    <span style="color:#0ecb81">🔺 做多 @ {cs.long_exchange.upper()}</span><br>
                    <span style="font-size:0.8rem">费率: {cs.long_rate*100:+.4f}% | 结算: {long_settle}</span>
                </div>
                </div>
            </div>"""
            position_html = f"""
        <div class="section">
            <h2>当前持仓</h2>
            {cross_html}
            {_pos_html(self.binance_state, "Binance")}
            {_pos_html(self.gate_state, "Gate")}
        </div>"""
        else:
            pre_borrow_html = ""
            if self.binance_state.pre_borrow_base:
                elapsed = (now - datetime.fromisoformat(
                    self.binance_state.pre_borrow_at)).total_seconds() / 60
                pre_borrow_html = f"""
        <div class="section">
            <h2>预借等待中</h2>
            <div class="position reverse">
                <span class="badge reverse">反向</span>
                <strong>{self.binance_state.pre_borrow_base}</strong>
                <span>数量: {self.binance_state.pre_borrow_amount}</span>
                <span>已等待: {elapsed:.0f} / {PRE_BORROW_TIMEOUT_MINUTES} min</span>
            </div>
        </div>"""
            position_html = f"""
        <div class="section">
            <h2>当前持仓</h2>
            <p class="muted">无持仓</p>
        </div>
        {pre_borrow_html}"""

        # ── 资金曲线 ──
        has_curve = bool(curve_labels)
        labels_json = json.dumps(curve_labels, ensure_ascii=False) if has_curve else "[]"
        total_json = json.dumps(curve_total, ensure_ascii=False) if has_curve else "[]"
        bn_json = json.dumps(curve_bn, ensure_ascii=False) if has_curve else "[]"
        gt_json = json.dumps(curve_gt, ensure_ascii=False) if has_curve else "[]"

        # ── 月历（JS 动态）──
        calendar_html = f"""
        <div class="section">
            <h2><button class="cal-nav" onclick="navCal(-1)">&#9664;</button> <span id="calTitle">--</span> <button class="cal-nav" onclick="navCal(1)">&#9654;</button></h2>
            <div class="table-wrap">
            <table class="calendar" id="calTable"></table>
            </div>
        </div>"""

        # ── 最近交易 ──
        recent_html = ""
        if history:
            recent = history[-20:][::-1]
            rows = ""
            has_detailed = any("price_pnl" in h or "short_exchange" in h for h in history)
            for h in recent:
                ts_raw = h.get("ts") or h.get("time", "")
                ts_short = ts_raw[:16].replace("T", " ")
                direction = str(h.get("direction", ""))
                if "cross" in direction or "orphan" in direction:
                    tag = "跨所"
                    tag_cls = "cross"
                else:
                    tag = "反" if direction == "reverse" else "正"
                    tag_cls = "reverse" if direction == "reverse" else "forward"
                amount = float(h.get("amount", 0))
                amount_str = f"{amount:.4f}" if amount else "-"
                net_pnl = h.get("net_pnl", h.get("profit_usdt", 0)) or 0
                pnl_cls = "positive" if net_pnl >= 0 else "negative"
                # 新版跨所记录：按交易所拆分
                if "short_exchange" in h:
                    short_ex = h.get("short_exchange", "")
                    long_ex = h.get("long_exchange", "")
                    sp = h.get("short_price_pnl", 0) or 0
                    sf = h.get("short_funding_pnl", 0) or 0
                    sfee = (h.get("short_actual_fee") or h.get("short_fee")) or 0
                    sn = h.get("short_net", 0) or 0
                    lp = h.get("long_price_pnl", 0) or 0
                    lf = h.get("long_funding_pnl", 0) or 0
                    lfee = (h.get("long_actual_fee") or h.get("long_fee")) or 0
                    ln = h.get("long_net", 0) or 0
                    s_exit = h.get("short_exit") or 0
                    l_exit = h.get("long_exit") or 0
                    cd1 = f"{s_exit:.4f}" if s_exit else "-"
                    cd2 = f"{l_exit:.4f}" if l_exit else "-"
                    # 分组头行：时间/类型/币种/数量 + 汇总净盈亏
                    rows += (f'<tr class="cross-group"><td>{ts_short}</td>'
                             f'<td><span class="badge cross">跨所</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{amount_str}</td>'
                             f'<td class="right muted" colspan="3">平仓价 {short_ex}空{cd1} / {long_ex}多{cd2}</td>'
                             f'<td class="right {pnl_cls}"><b>{net_pnl:+.4f}</b></td></tr>')
                    s_cls = "bn" if short_ex == "binance" else "gt"
                    l_cls = "bn" if long_ex == "binance" else "gt"
                    # 做空侧: 价格盈亏 / 资费 / 手续费 / 净盈亏
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {s_cls}">{short_ex}空</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if sp>=0 else "negative"}">{sp:+.4f}</td>'
                             f'<td class="right {"positive" if sf>=0 else "negative"}">{sf:+.4f}</td>'
                             f'<td class="right negative">{sfee:.4f}</td>'
                             f'<td class="right {"positive" if sn>=0 else "negative"}">{sn:+.4f}</td></tr>')
                    # 做多侧
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {l_cls}">{long_ex}多</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if lp>=0 else "negative"}">{lp:+.4f}</td>'
                             f'<td class="right {"positive" if lf>=0 else "negative"}">{lf:+.4f}</td>'
                             f'<td class="right negative">{lfee:.4f}</td>'
                             f'<td class="right {"positive" if ln>=0 else "negative"}">{ln:+.4f}</td></tr>')
                elif "price_pnl" in h:
                    # 旧版跨所记录（无拆分）：直接展示汇总
                    price_pnl = h.get("price_pnl", 0) or 0
                    funding_pnl = h.get("funding_pnl", 0) or 0
                    fee_total = h.get("fee_total", 0) or 0
                    rows += (f'<tr><td>{ts_short}</td>'
                             f'<td><span class="badge {tag_cls}">{tag}</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{amount_str}</td>'
                             f'<td class="right {"positive" if price_pnl>=0 else "negative"}">{price_pnl:+.4f}</td>'
                             f'<td class="right positive">{funding_pnl:+.4f}</td>'
                             f'<td class="right negative">{fee_total:.4f}</td>'
                             f'<td class="right {pnl_cls}">{net_pnl:+.4f}</td></tr>')
                else:
                    pnl = float(h.get("profit_usdt", 0))
                    rows += (f'<tr><td>{ts_short}</td>'
                             f'<td><span class="badge {tag_cls}">{tag}</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{amount_str}</td>'
                             f'<td class="right">-</td><td class="right">-</td><td class="right">-</td>'
                             f'<td class="right {pnl_cls}">{pnl:+.4f}</td></tr>')
            if has_detailed:
                header = '<tr><th>时间</th><th>类型</th><th>币种</th><th class="right">数量</th><th class="right">价格盈亏</th><th class="right">资费</th><th class="right">手续费</th><th class="right">净盈亏</th></tr>'
            else:
                header = '<tr><th>时间</th><th>类型</th><th>币种</th><th class="right">数量</th><th class="right">价格盈亏</th><th class="right">资费</th><th class="right">手续费</th><th class="right">净盈亏</th></tr>'
            recent_html = f"""
        <div class="section">
            <h2>历史交易 <span class="muted">(最近20笔)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>{header}</thead>
                <tbody>{rows}</tbody>
            </table>
            </div>
        </div>"""

        # ── Top 10 表格 ──
        rows_html = ""
        if top10:
            for row in top10:
                nft = float(row.get("next_funding_time_ms", 0))
                if nft > 0:
                    mins = max(0, (nft - time.time() * 1000) / 60000)
                    countdown = f"{int(mins)}min" if mins < 120 else f"{mins/60:.0f}h"
                else:
                    countdown = "N/A"
                direction = str(row.get("direction", "forward"))
                dir_label = "反" if direction == "reverse" else "正"
                dir_cls = "reverse" if direction == "reverse" else "forward"
                net_rate = float(row.get("net_rate", 0))
                net_cls = "positive" if net_rate > 0 else "negative"
                borrow = float(row.get("borrow_hourly_rate", 0))
                exchange = str(row.get("exchange", ""))
                exc_cls = "gt" if exchange == "GT" else "bn"
                held = False
                if self.binance_state.is_open:
                    held = held or (str(row["base"]) == self.binance_state.base
                                   and direction == self.binance_state.direction)
                if self.gate_state.is_open:
                    held = held or (str(row["base"]) == self.gate_state.base
                                   and direction == self.gate_state.direction)
                held_mark = " ★" if held else ""
                spot_p = float(row.get("spot_last", 0) or 0)
                fut_p = float(row.get("futures_last", 0) or 0)
                rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="badge {exc_cls}">{exchange}</span></td>
                    <td><span class="badge {dir_cls}">{dir_label}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td class="right">{spot_p:.6f}</td>
                    <td class="right">{fut_p:.6f}</td>
                    <td class="right">{float(row['predicted_funding_rate'])*100:+.3f}%</td>
                    <td class="right">{float(row['spot_taker_fee'])*100:.3f}%</td>
                    <td class="right">{float(row['futures_taker_fee'])*100:.3f}%</td>
                    <td class="right">{borrow*100:.3f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td class="right">{countdown}</td>
                </tr>"""
        else:
            rows_html = '<tr><td colspan="11" class="muted center">暂无套利机会</td></tr>'

        table_html = f"""
        <div class="section">
            <h2>套利机会 Top 10 <span class="muted">(Binance+Gate)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>所</th><th>方向</th><th>币种</th><th class="right">现货价</th><th class="right">合约价</th>
                        <th class="right">费率</th><th class="right">现货费</th><th class="right">合约费</th>
                        <th class="right">借币费</th><th class="right">净收益</th><th class="right">结算</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            </div>
        </div>"""

        # ── 跨交易所机会表格 ──
        cross_rows_html = ""
        if cross_top10:
            for row in cross_top10:
                rate_spread = float(row.get("rate_spread", 0))
                net_rate = float(row.get("net_rate", 0))
                short_ex = str(row.get("short_exchange", ""))
                long_ex = str(row.get("long_exchange", ""))
                short_rate = float(row.get("short_rate", 0))
                long_rate = float(row.get("long_rate", 0))
                net_cls = "positive" if net_rate > 0 else "negative"
                passes = row.get("passes", True)
                short_nft = float(row.get("short_next_funding_time_ms", 0))
                long_nft = float(row.get("long_next_funding_time_ms", 0))
                now_ms = time.time() * 1000
                s_mins = max(0, (short_nft - now_ms) / 60000) if short_nft > 0 else -1
                l_mins = max(0, (long_nft - now_ms) / 60000) if long_nft > 0 else -1
                s_countdown = f"{int(s_mins)}min" if s_mins >= 0 else "N/A"
                l_countdown = f"{int(l_mins)}min" if l_mins >= 0 else "N/A"
                held = self.cross_state.is_open and str(row.get("base", "")) == self.cross_state.base
                held_mark = " ★" if held else ""
                pass_mark = "✓" if passes else "✗"
                pass_cls = "pass" if passes else "fail"
                bn_p = float(row.get("bn_price", 0))
                gt_p = float(row.get("gt_price", 0))
                short_cls = "bn" if short_ex == "binance" else "gt"
                long_cls = "bn" if long_ex == "binance" else "gt"
                cross_rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="{pass_cls}">{pass_mark}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td class="right">{bn_p:.6f}</td>
                    <td class="right">{gt_p:.6f}</td>
                    <td class="right">{rate_spread*100:+.4f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td><span class="badge {short_cls}">{short_ex.upper()}</span></td>
                    <td class="right">{short_rate*100:+.4f}%</td>
                    <td class="right">{s_countdown}</td>
                    <td><span class="badge {long_cls}">{long_ex.upper()}</span></td>
                    <td class="right">{long_rate*100:+.4f}%</td>
                    <td class="right">{l_countdown}</td>
                </tr>"""
        else:
            cross_rows_html = '<tr><td colspan="12" class="muted center">暂无跨所套利机会</td></tr>'

        cross_table_html = f"""
        <div class="section">
            <h2>跨交易所套利机会 <span class="muted">(BN↔GT 纯期货对冲)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>通过</th><th>币种</th><th class="right">BN价</th><th class="right">GT价</th><th class="right">费率差</th><th class="right">净收益</th>
                        <th>做空所</th><th class="right">空费率</th><th class="right">空结算</th>
                        <th>做多所</th><th class="right">多费率</th><th class="right">多结算</th>
                    </tr>
                </thead>
                <tbody>{cross_rows_html}</tbody>
            </table>
            </div>
        </div>"""

        # ── Chart.js 曲线 ──
        chart_section = ""
        if has_curve:
            chart_section = f"""
        <div class="section">
            <h2>资金曲线</h2>
            <div style="position:relative;height:320px"><canvas id="equityChart"></canvas></div>
        </div>"""

        html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<meta http-equiv="refresh" content="30">
<title>ArbiBot · 套利看板</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root {{
  --bg: #080c14;
  --surface: #111827;
  --surface2: #1a2332;
  --border: #1e2d3d;
  --text: #c8d6e5;
  --text2: #8395a7;
  --muted: #576574;
  --green: #0ecb81;
  --red: #e74c3c;
  --orange: #f0a030;
  --blue: #4090e0;
  --purple: #a855f7;
  --bn: #f0b90b;
  --gt: #2955E7;
  --radius: 12px;
  --radius-sm: 8px;
}}
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'PingFang SC', system-ui, sans-serif;
  background: var(--bg);
  color: var(--text);
  padding: 12px;
  max-width: 860px;
  margin: 0 auto;
  line-height: 1.4;
  -webkit-font-smoothing: antialiased;
}}
h1 {{ font-size: 1.35rem; font-weight: 800; letter-spacing: -0.5px; }}
h2 {{ font-size: 0.9rem; margin-bottom: 10px; color: var(--text2); font-weight: 600; display:flex; align-items:center; gap:8px; }}
h2::after {{ content:''; flex:1; height:1px; background:var(--border); border-radius:1px; }}
h3 {{ font-size: 0.82rem; margin-bottom: 6px; color: var(--text2); font-weight: 600; }}

/* Header */
.header {{
  text-align: center;
  margin-bottom: 16px;
  padding: 20px 0 12px;
}}
.header .logo {{
  display: inline-flex; align-items: center; gap: 10px;
  background: linear-gradient(135deg, #f0b90b20, #2955E720);
  border: 1px solid var(--border);
  border-radius: 40px;
  padding: 8px 24px;
  margin-bottom: 8px;
}}
.header .logo .dot {{
  width: 10px; height: 10px;
  border-radius: 50%;
  background: var(--green);
  box-shadow: 0 0 10px var(--green);
  animation: pulse 2s infinite;
}}
@keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:.4}} }}
.header h1 {{ background: linear-gradient(135deg, #f0b90b, #0ecb81); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }}
.header .ts {{ color: var(--muted); font-size: 0.7rem; margin-top: 4px; }}

/* Cards */
.balances {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 8px;
  margin-bottom: 12px;
}}
.card {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius-sm);
  padding: 10px 8px;
  text-align: center;
  transition: border-color .2s;
}}
.card:hover {{ border-color: #334155; }}
.card .label {{ font-size: 0.6rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.5px; }}
.card .value {{ font-size: 0.85rem; font-weight: 700; margin-top: 3px; }}
.stats {{
  display: flex; flex-wrap: wrap; gap: 8px 20px;
  justify-content: center; font-size: 0.7rem; color: var(--text2);
  margin-bottom: 12px; padding: 10px;
  background: var(--surface); border-radius: var(--radius-sm);
  border: 1px solid var(--border);
}}
.stats b {{ color: var(--text); font-weight: 600; }}

/* Sections */
.section {{
  background: var(--surface);
  border: 1px solid var(--border);
  border-radius: var(--radius);
  padding: 14px;
  margin-bottom: 12px;
}}

/* Position */
.position {{
  display: flex; flex-wrap: wrap; gap: 8px 16px;
  align-items: center; font-size: 0.8rem;
  padding: 10px 12px;
  border-radius: var(--radius-sm);
  background: var(--surface2);
}}
.position.reverse {{ border-left: 3px solid var(--orange); }}
.position.forward {{ border-left: 3px solid var(--blue); }}

/* Badges */
.badge {{
  display: inline-block; padding: 2px 8px; border-radius: 4px;
  font-size: 0.6rem; font-weight: 700; letter-spacing: 0.3px;
}}
.badge.reverse {{ background: #f0a03022; color: var(--orange); border:1px solid #f0a03044; }}
.badge.forward {{ background: #4090e022; color: var(--blue); border:1px solid #4090e044; }}
.badge.cross {{ background: #a855f722; color: var(--purple); border:1px solid #a855f744; }}
.badge.bn {{ background: #f0b90b22; color: var(--bn); border:1px solid #f0b90b44; }}
.badge.gt {{ background: #2955E722; color: #6b9fff; border:1px solid #2955E744; }}
.pass {{ color: var(--green); font-weight: bold; font-size:1.1em; }}
.fail {{ color: var(--red); }}

/* Tables */
.table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; border-radius: var(--radius-sm); }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.72rem; white-space: nowrap; }}
thead th {{
  color: var(--muted); font-weight: 600; font-size: 0.65rem;
  text-transform: uppercase; letter-spacing: 0.5px;
  padding: 8px 6px; background: var(--surface2);
  position: sticky; top: 0; z-index: 1;
}}
tbody td {{ padding: 6px; border-bottom: 1px solid #1a1a2e; }}
tbody tr:hover {{ background: #ffffff04; }}
tr.held {{ background: #0ecb8108; }}
tr.held:hover {{ background: #0ecb8112; }}
tr.cross-group {{ border-top: 2px solid #2a2a40; }}
tr.cross-group td {{ padding-top: 10px; }}
tr.cross-sub td {{ color: var(--text2); font-size: 0.68rem; padding: 2px 6px; }}
tr.cross-sub td:first-child {{ padding-left: 18px; }}

/* Calendar */
.calendar {{ width: 100%; }}
.calendar td {{
  text-align: center; padding: 5px 3px; font-size: 0.75rem;
  border-radius: 6px; cursor: default;
}}
.calendar td small {{ display: block; margin-top: 1px; }}
.cal-nav {{
  background: var(--surface2); border: 1px solid var(--border);
  color: var(--text2); border-radius: 6px; padding: 4px 10px;
  font-size: 0.75rem; cursor: pointer;
}}
.cal-nav:hover {{ background: var(--border); color: var(--text); }}
#calTitle {{ display: inline-block; min-width: 130px; text-align: center; font-weight:600; }}

/* Helpers */
.right {{ text-align: right; }}
.center {{ text-align: center; }}
.positive {{ color: var(--green); font-weight: 600; }}
.negative {{ color: var(--red); font-weight: 600; }}
.muted {{ color: var(--muted); }}

.footer {{
  text-align: center; color: #2a2a40; font-size: 0.65rem;
  margin-top: 16px; padding-bottom: 20px;
}}

/* Mobile */
@media (max-width: 600px) {{
  body {{ padding: 8px; }}
  .balances {{ grid-template-columns: repeat(4, 1fr); gap: 5px; }}
  .card {{ padding: 7px 4px; }}
  .card .value {{ font-size: 0.75rem; }}
  .card .label {{ font-size: 0.55rem; }}
  .section {{ padding: 10px; }}
  table {{ font-size: 0.64rem; }}
  thead th {{ font-size: 0.6rem; padding: 6px 4px; }}
  tbody td {{ padding: 4px; }}
  .header .logo {{ padding: 6px 16px; }}
  h1 {{ font-size: 1.1rem; }}
  .stats {{ gap: 4px 10px; font-size: 0.64rem; }}
}}
</style>
</head>
<body>
<div class="header">
  <div class="logo">
    <span class="dot"></span>
    <h1>ArbiBot 套利看板</h1>
  </div>
  <div class="ts">更新 {now_str} · 30s 自动刷新</div>
</div>
{balance_cards}
{stats_html}
{position_html}
{calendar_html}
{chart_section}
{recent_html}
{table_html}
{cross_table_html}
<div class="footer">ArbiBot &copy; 2026 · Binance + Gate.io</div>

<script>
// ── 月历渲染 ──
var ALL_PNL = {pnl_json};
var TODAY = "{today_iso}";
var calYear = new Date().getFullYear();
var calMonth = new Date().getMonth() + 1; // 1-12

function renderCal(y, m) {{
    document.getElementById('calTitle').textContent = y + '年' + m + '月';
    var fd = new Date(y, m - 1, 1).getDay(); // 0=Sun
    fd = fd === 0 ? 6 : fd - 1; // Mon=0
    var days = new Date(y, m, 0).getDate();
    var html = '<tr><th>一</th><th>二</th><th>三</th><th>四</th><th>五</th><th>六</th><th>日</th></tr><tr>';
    for (var i = 0; i < fd; i++) html += '<td></td>';
    for (var d = 1; d <= days; d++) {{
        var ds = y + '-' + String(m).padStart(2,'0') + '-' + String(d).padStart(2,'0');
        var pnl = ALL_PNL[ds] || 0;
        var alpha = 0;
        if (pnl > 0) alpha = Math.min(0.9, 0.2 + Math.abs(pnl) * 2);
        else if (pnl < 0) alpha = Math.min(0.9, 0.2 + Math.abs(pnl) * 2);
        var bg = pnl > 0 ? 'rgba(76,175,132,' + alpha + ')' : (pnl < 0 ? 'rgba(224,85,96,' + alpha + ')' : 'transparent');
        var style = ds === TODAY ? 'border:2px solid #f0b90b;font-weight:700' : '';
        html += '<td style="' + style + '"><div style="background:' + bg + ';border-radius:4px;padding:2px 4px">' + d + '<br><small style="font-size:0.55rem;color:#888">' + (pnl >= 0 ? '+' : '') + pnl.toFixed(2) + '</small></div></td>';
        var col = (fd + d) % 7;
        if (col === 0 && d < days) html += '</tr><tr>';
    }}
    html += '</tr>';
    document.getElementById('calTable').innerHTML = html;
}}
function navCal(dir) {{
    calMonth += dir;
    if (calMonth < 1) {{ calMonth = 12; calYear--; }}
    if (calMonth > 12) {{ calMonth = 1; calYear++; }}
    renderCal(calYear, calMonth);
}}
renderCal(calYear, calMonth);
</script>"""

        if has_curve:
            html += f"""
<script>
(function() {{
    try {{
        var ctx = document.getElementById('equityChart');
        if (!ctx) return;
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {labels_json},
                datasets: [
                    {{
                        label: '总计',
                        data: {total_json},
                        borderColor: '#4caf84',
                        backgroundColor: 'rgba(76,175,132,0.05)',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 2.0,
                        tension: 0.3,
                    }},
                    {{
                        label: 'BN',
                        data: {bn_json},
                        borderColor: '#f0b90b',
                        backgroundColor: 'transparent',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 1.0,
                        borderDash: [5, 5],
                        tension: 0.3,
                    }},
                    {{
                        label: 'Gate',
                        data: {gt_json},
                        borderColor: '#17c9a4',
                        backgroundColor: 'transparent',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 1.0,
                        borderDash: [3, 3],
                        tension: 0.3,
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: true, position: 'top', labels: {{ color: '#aaa', font: {{ size: 10 }}, boxWidth: 16 }} }} }},
                scales: {{
                    x: {{ ticks: {{ maxTicksLimit: 12, maxRotation: 0, color: '#666', font: {{ size: 10 }} }}, grid: {{ color: '#252540' }} }},
                    y: {{ ticks: {{ color: '#666', font: {{ size: 10 }} }}, grid: {{ color: '#252540' }} }}
                }},
                interaction: {{ intersect: false, mode: 'index' }}
            }}
        }});
    }} catch(e) {{}}
}})();
</script>"""

        html += """
</body>
</html>"""

        dashboard_path = Path("data/dashboard.html")
        dashboard_path.parent.mkdir(parents=True, exist_ok=True)
        dashboard_path.write_text(html, encoding="utf-8")

    def _seconds_until_next_wakeup(self) -> float:
        """每分钟唤醒扫描，持有期间检查是否有更优机会可换仓。"""
        return SCAN_INTERVAL_MINUTES * 60


def _print_opportunity_table(df: pd.DataFrame, ref_usdt: float, direction: str = "positive") -> None:
    """格式化打印套利机会表格。"""
    top20 = df.head(20)
    if direction == "reverse":
        header = (
            f"  反向套利机会 (Negative Funding) — 借币卖出 + 做多合约，收取空头费率\n"
            f"  借币成本已计入净收益（按 1 小时利息计算）\n"
            f"  共 {len(df)} 个机会, 显示 Top 20 | 利润按每 {ref_usdt:.0f} USDT/腿 参考值"
        )
        col_header = (
            f"  {'排名':<4} {'币种':<10} {'现货价':>10} {'合约价':>10} {'来源':<6} {'链':<8} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {spot_p:>10.6f} {fut_p:>10.6f} {source:<6} {chain:<8} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% {borrow:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )
    else:
        header = (
            f"  正向套利机会 (Positive Funding) — 做多现货 + 做空合约，收取多头费率\n"
            f"  共 {len(df)} 个机会, 显示 Top 20 | 利润按每 {ref_usdt:.0f} USDT/腿 参考值"
        )
        col_header = (
            f"  {'排名':<4} {'币种':<10} {'现货价':>10} {'合约价':>10} {'来源':<6} {'链':<12} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {spot_p:>10.6f} {fut_p:>10.6f} {source:<6} {chain:<12} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )

    print(f"\n{'='*110}")
    print(header)
    print(f"{'='*110}")
    print(col_header)
    print(f"  {'-'*108}")

    for rank, (_, row) in enumerate(top20.iterrows(), 1):
        kwargs = dict(
            rank=rank,
            base=str(row["base"])[:10],
            source=str(row["spot_source"])[:6],
            chain=str(row.get("chain", ""))[:12] if direction == "positive" else str(row.get("chain", ""))[:8],
            spot_p=float(row.get("spot_last", 0) or 0),
            fut_p=float(row.get("futures_last", 0) or 0),
            rate=float(row["predicted_funding_rate"]) * 100,
            spot_fee=float(row["spot_taker_fee"]) * 100,
            fut_fee=float(row["futures_taker_fee"]) * 100,
            net=float(row["net_rate"]) * 100,
            profit=float(row["net_rate"]) * ref_usdt,
        )
        if direction == "reverse":
            kwargs["borrow"] = float(row.get("borrow_hourly_rate", 0)) * 100
        print(row_fmt.format(**kwargs))

    print(f"{'='*90}\n")


async def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="跨交易所资金费率套利机器人 (Binance + Gate)")
    parser.add_argument("--scan", action="store_true", help="扫描模式：打印机会为不干（正向+反向），不下单")
    parser.add_argument("--scan-reverse", action="store_true", help="扫描模式：仅反向套利（负费率）")
    parser.add_argument("--live", action="store_true", help="实盘模式（不加此参数不实际下单）")
    parser.add_argument("--no-gate", action="store_true", help="仅用 Binance，禁用 Gate")
    parser.add_argument("--cross", action="store_true", help="跨交易所套利模式（必加）")
    args = parser.parse_args()

    global USE_TESTNET, GATE_TRADING_ENABLED, CROSS_EXCHANGE_ENABLED
    if args.no_gate:
        GATE_TRADING_ENABLED = False
        logger.info("--no-gate: Gate.io 交易已禁用")
    if args.cross:
        CROSS_EXCHANGE_ENABLED = True
        logger.info("--cross: 跨交易所资金费率套利模式已启用")
    if args.live:
        USE_TESTNET = False
        # 重建 exchange (testnet sandbox 在 __init__ 里设置的，这里重新标记)
        logger.warning("实盘模式已启用！请确认 API Key 不是测试网 Key。")

    bot = FundingArbitrageBot()
    if args.live:
        bot.spot.set_sandbox_mode(False)
        bot.futures.set_sandbox_mode(False)
        bot.alpha_spot.set_sandbox_mode(False)

    try:
        await bot.initialize()

        if args.scan or args.scan_reverse:
            spot_free, futures_free = await asyncio.gather(
                bot._safe_request("spot.fetch_balance", lambda: bot.spot.fetch_balance(), default={}),
                bot._safe_request("futures.fetch_balance", lambda: bot.futures.fetch_balance(), default={}),
            )
            spot_usdt = bot._free_balance(spot_free, "USDT")
            futures_usdt = bot._free_balance(futures_free, "USDT")
            ref_usdt = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
            if ref_usdt <= 0:
                ref_usdt = 100.0
                logger.info("余额不足，按 %.0f USDT/腿 参考值显示。", ref_usdt)
            show_positive = args.scan or not args.scan_reverse
            show_reverse = REVERSE_ENABLED and (args.scan or args.scan_reverse)

            logger.info("扫描模式：拉取全市场套利机会...")

            # 正向套利
            if show_positive:
                df_pos = await bot.scan_best_opportunity(ref_usdt)
                if df_pos.empty:
                    print("\n  未发现正向套利机会。")
                else:
                    _print_opportunity_table(df_pos, ref_usdt, direction="positive")

            # 反向套利
            if show_reverse:
                df_rev = await bot.scan_reverse_opportunities(ref_usdt)
                if df_rev.empty:
                    print("\n  未发现反向套利机会（无可借贷币种或净收益不足）。")
                else:
                    _print_opportunity_table(df_rev, ref_usdt, direction="reverse")

            # ── Gate.io 扫描 ──
            try:
                df_gf = await bot._scan_gate_forward(ref_usdt)
                df_gr = await bot._scan_gate_reverse(ref_usdt)
                print("\n  ═══════════════════════════════════════════════════════")
                print("  Gate.io 套利机会")
                print("  ═══════════════════════════════════════════════════════")
                if not df_gf.empty:
                    _print_opportunity_table(df_gf, ref_usdt, direction="positive")
                else:
                    print("\n  [Gate 正向] 未发现机会。")
                if not df_gr.empty:
                    _print_opportunity_table(df_gr, ref_usdt, direction="reverse")
                else:
                    print("\n  [Gate 反向] 未发现机会（无可借贷币种或 API Key 未配置）。")
            except Exception as exc:
                print(f"\n  [Gate.io] 扫描失败: {exc}")

            # ── 跨交易所费率差扫描 ──
            if CROSS_EXCHANGE_ENABLED:
                try:
                    cross_df = await bot._scan_cross_exchange(ref_usdt, strict=False)
                    if cross_df.empty:
                        print("\n  [跨交易所] 两所无共同币种，无法扫描。")
                    else:
                        bot._print_cross_opportunity_table(cross_df, ref_usdt)
                except Exception as exc:
                    print(f"\n  [跨交易所] 扫描失败: {exc}")

            return

        # 打卡循环模式
        await bot.run_forever()

    finally:
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，程序退出。")
