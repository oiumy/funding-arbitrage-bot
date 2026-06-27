"""
Binance 现货-永续合约资金费率套利机器人。

运行环境:
    Python 3.10+
    pip install ccxt pandas

重要提示:
    1. 默认连接 Binance Testnet。实盘前务必在测试网完整跑通。
    2. 资金费率、盘口深度、滑点、借贷成本、合约保证金模式都会影响真实收益。
    3. 本脚本使用 REST 轮询，不使用 WebSocket，适合低频“打卡式”运行。
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import logging.handlers
import math
import os
import time
import urllib.request
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
SCAN_INTERVAL_MINUTES = 1   # 无持仓时多久扫一次 (1分钟确保不错过窗口)
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
CROSS_SNIPER_OFFSET_SEC = 5              # 结算前N秒扫描+开仓
CROSS_SNIPE_WINDOW_SEC = 85              # 距结算≤N秒时跳过单所扫描，直接狙击（须 > 完整周期14s + 休眠60s）
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
    amount: float = 0.0

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
        self._scan_lock = asyncio.Lock()  # 防止预借和主扫描冲突
        self._borrow_blacklist: dict[str, float] = {}  # base → 过期时间戳, 借币池空后暂避
        self._fee_cache: dict[str, tuple[float, Any]] = {}  # key → (ts, result), 避免同轮重复查询
        self._gate_margin_bases: list[str] = []  # Gate 可借贷币种列表缓存
        self._gate_margin_bases_ts: float = 0.0
        self._fee_lock = asyncio.Lock()

    @staticmethod
    def _ccxt_config(options: dict | None = None) -> dict[str, Any]:
        """构建 ccxt 配置，自动判断是否启用代理。"""
        cfg: dict[str, Any] = {
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
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
            logger.error("ccxt 请求错误: %s | %s", name, exc)
            if raise_error:
                raise
            return default

    async def initialize(self) -> None:
        """加载市场元数据。amount_to_precision 依赖该数据。"""
        await asyncio.gather(
            self._safe_request("spot.load_markets", lambda: self.spot.load_markets(), raise_error=True),
            self._safe_request("futures.load_markets", lambda: self.futures.load_markets(), raise_error=True),
            self._safe_request("gate_spot.load_markets", lambda: self.gate_spot.load_markets(), raise_error=False),
            self._safe_request("gate_futures.load_markets", lambda: self.gate_futures.load_markets(), raise_error=False),
        )

    async def close(self) -> None:
        """关闭 ccxt 异步会话。"""
        await asyncio.gather(
            self._safe_request("spot.close", lambda: self.spot.close()),
            self._safe_request("futures.close", lambda: self.futures.close()),
            self._safe_request("gate_spot.close", lambda: self.gate_spot.close()),
            self._safe_request("gate_futures.close", lambda: self.gate_futures.close()),
        )

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
        import requests as _requests

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
            resp = _requests.request(
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
        filled = float(resp.get("executedQty", 0))
        status = resp.get("status", "")
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
            self._safe_request("gate_spot.fetch_tickers", lambda: self.gate_spot.fetch_tickers(), default={}),
            self._safe_request("gate_futures.fetch_tickers", lambda: self.gate_futures.fetch_tickers(), default={}),
            self._safe_request("gate_futures.fetch_funding_rates", lambda: self.gate_futures.fetch_funding_rates(), default={}),
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
            spot_market = self.gate_spot.markets.get(spot_symbol)
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
            self._safe_request("gate_spot.fetch_tickers", lambda: self.gate_spot.fetch_tickers(), default={}),
            self._safe_request("gate_futures.fetch_tickers", lambda: self.gate_futures.fetch_tickers(), default={}),
            self._safe_request("gate_futures.fetch_funding_rates", lambda: self.gate_futures.fetch_funding_rates(), default={}),
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
            spot_market = self.gate_spot.markets.get(spot_symbol)
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
                                     strict: bool = True) -> pd.DataFrame:
        """扫描跨交易所费率差异机会：纯期货多空对冲。

        高费率所做空 + 低费率所做多 = delta 中性，赚费率差。
        strict=False: 放宽筛选，用于展示接近阈值的候选。
        """
        if not CROSS_EXCHANGE_ENABLED:
            return pd.DataFrame()

        # 并行获取两所费率 + 手续费 + 全量ticker（用于价格校验）
        bn_fee_pair, gt_fee_pair, bn_funding, gt_funding, bn_tickers, gt_tickers = await asyncio.gather(
            self.fetch_taker_fees(),
            self._fetch_gate_taker_fees(),
            self._safe_request("futures.fetch_funding_rates",
                               lambda: self.futures.fetch_funding_rates(), default={}),
            self._safe_request("gate_futures.fetch_funding_rates",
                               lambda: self.gate_futures.fetch_funding_rates(), default={}),
            self._safe_request("futures.fetch_tickers",
                               lambda: self.futures.fetch_tickers(), default={}),
            self._safe_request("gate_futures.fetch_tickers",
                               lambda: self.gate_futures.fetch_tickers(), default={}),
        )
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
        spot_market = self.spot.markets.get(spot_symbol)
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
                self.logger.warning(f"借币利率批量查询失败: {batch}", exc_info=True)
        return rates

    @staticmethod
    def _safe_float(value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    def _build_futures_market_index(self) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}
        for market in self.futures.markets.values():
            is_usdt_swap = (
                market.get("swap")
                and market.get("linear")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            )
            if is_usdt_swap:
                index[market["base"]] = market
        return index

    def _build_gate_futures_market_index(self) -> dict[str, dict[str, Any]]:
        """Gate.io 版: 构建 USDT 永续合约 base → market 索引。"""
        index: dict[str, dict[str, Any]] = {}
        for market in self.gate_futures.markets.values():
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

        spot_raw_rate: float = 0.0
        valid_spot_symbols = {
            m["symbol"] for m in self.gate_spot.markets.values()
            if m.get("spot") and m.get("quote") == "USDT" and m.get("active", True)
        }
        try:
            spot_raw = await self._safe_request(
                "gate.fetch_trading_fees",
                lambda: self.gate_spot.fetch_trading_fees(),
                default=None,
            )
            if spot_raw:
                for sym, info in spot_raw.items():
                    taker = self._safe_float(info.get("taker", 0))
                    if taker > 0 and sym in valid_spot_symbols:
                        eff = taker * (1 - GATE_SPOT_REBATE)
                        spot_fees[sym] = eff
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

        fut_raw_rate: float = 0.0
        # 只信任 USDT 永续合约的交易对，排除 delivery/spot 混入
        valid_fut_symbols = {
            m["symbol"] for m in self.gate_futures.markets.values()
            if m.get("swap") and m.get("linear") and m.get("quote") == "USDT"
        }
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
        """拉取 Gate 所有可逐仓借贷的币种列表（公开接口，缓存 24h）。"""
        now = time.monotonic()
        if self._gate_margin_bases and (now - self._gate_margin_bases_ts) < 86_400:
            return self._gate_margin_bases
        try:
            resp = await self._safe_request(
                "gate_margin_currency_pairs",
                lambda: self.gate_spot.public_margin_get_uni_currency_pairs(),
                default=[],
            )
            bases: set[str] = set()
            for item in resp:
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
        """Gate 逐仓借币利率（ccxt 隐式 API，复用连接池）。
        先加载 Gate 支持借贷的币种列表，只查已知可借贷的币，避免大量无意义查询。
        返回 {base_upper: hourly_rate_float}。"""
        if not assets or not GATE_API_KEY or not self.gate_spot:
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
            lambda: self.gate_spot.fetch_balance(),
            default={},
        )
        return self._free_balance(bal, "USDT")

    async def _gate_futures_balance(self) -> float:
        """Gate 合约 USDT 可用余额。"""
        bal = await self._safe_request(
            "gate_futures.fetch_balance",
            lambda: self.gate_futures.fetch_balance(),
            default={},
        )
        return self._free_balance(bal, "USDT")

    async def _cross_get_gate_total_balance(self) -> float:
        """Gate 合约 USDT 总余额 (可用 + 锁定保证金)。"""
        try:
            bal = await self._safe_request(
                "gate_futures.fetch_balance_total",
                lambda: self.gate_futures.fetch_balance(),
                default={},
            )
            return float(bal.get("USDT", {}).get("total", 0) or 0)
        except Exception:
            return 0.0

    async def _gate_transfer_usdt(
        self, amount: float, from_account: str, to_account: str,
        symbol: str | None = None,
    ) -> bool:
        """Gate 内部资金划转。margin 划转需传 symbol 参数。"""
        params: dict[str, Any] = {}
        if symbol and ("margin" in (from_account, to_account)):
            params["symbol"] = symbol
        try:
            await self._safe_request(
                f"gate_transfer_{from_account}_to_{to_account}",
                lambda: self.gate_spot.transfer(
                    "USDT", self._floor_usdt(amount),
                    from_account, to_account, params=params,
                ),
                raise_error=True,
            )
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
        """Gate 现货市价单 via ccxt。"""
        precise = float(self.gate_spot.amount_to_precision(symbol, amount))
        order = await self._safe_request(
            f"gate_spot.{side}_order",
            lambda: self.gate_spot.create_order(symbol, "market", side, precise),
            raise_error=True,
        )
        filled = float(order.get("filled", 0))
        ok = order.get("status") == "closed" and filled >= precise * 0.9
        return {"id": str(order.get("id", "")), "symbol": symbol, "side": side,
                "amount": precise, "filled": filled,
                "status": "closed" if ok else "open", "info": order}

    async def _gate_futures_order(self, symbol: str, side: str,
                                   amount: float, reduce_only: bool = False) -> dict[str, Any]:
        """Gate 合约市价单 via ccxt。"""
        precise = float(self.gate_futures.amount_to_precision(symbol, amount))
        params: dict[str, Any] = {"reduceOnly": reduce_only} if reduce_only else {}
        order = await self._safe_request(
            f"gate_futures.{side}_order",
            lambda: self.gate_futures.create_order(symbol, "market", side, precise, params=params),
            raise_error=True,
        )
        filled = float(order.get("filled", 0))
        ok = order.get("status") == "closed" and filled >= precise * 0.9
        return {"id": str(order.get("id", "")), "symbol": symbol, "side": side,
                "amount": precise, "filled": filled,
                "status": "closed" if ok else "open", "info": order}

    async def _gate_set_leverage(self, symbol: str) -> bool:
        """Gate 合约设置杠杆 1x。"""
        try:
            await self.gate_futures.set_leverage(1, symbol)
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
                {"symbol": clean, "leverage": 1}, method="POST",
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
        """获取 Gate 合约下次资金费率结算时间 (ms)。"""
        try:
            info = await self._safe_request(
                f"gate_futures.fetch_funding_rate({futures_symbol})",
                lambda: self.gate_futures.fetch_funding_rate(futures_symbol),
                default={},
            )
            nft = info.get("nextFundingTime") or info.get("nextFundingTimestamp")
            if nft and float(nft) > 0:
                return float(nft)
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
            order = await self._gate_futures_order(symbol, "sell", amount)
            return LegResult(True, "gate_futures", symbol, "sell", amount, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "sell", amount, error=str(exc))

    async def _close_gate_futures_short_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            precise = float(self.gate_futures.amount_to_precision(symbol, amount))
            order = await self._gate_futures_order(symbol, "buy", precise, reduce_only=True)
            return LegResult(True, "gate_futures", symbol, "buy", precise, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "buy", amount, error=str(exc))

    async def _open_gate_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            order = await self._gate_futures_order(symbol, "buy", amount)
            return LegResult(True, "gate_futures", symbol, "buy", amount, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "buy", amount, error=str(exc))

    async def _close_gate_futures_long_leg(self, symbol: str, amount: float) -> LegResult:
        try:
            precise = float(self.gate_futures.amount_to_precision(symbol, amount))
            order = await self._gate_futures_order(symbol, "sell", precise, reduce_only=True)
            return LegResult(True, "gate_futures", symbol, "sell", precise, order=order)
        except Exception as exc:
            return LegResult(False, "gate_futures", symbol, "sell", amount, error=str(exc))

    # ── Gate.io 保证金方法 ──

    async def _gate_margin_borrow(self, symbol: str, base: str, amount: float) -> bool:
        """Gate 逐仓借币。"""
        try:
            await self.gate_spot.borrow_isolated_margin(symbol, base, amount)
            logger.info("Gate 借币: %s %s (pair=%s)", amount, base, symbol)
            return True
        except Exception as exc:
            logger.error("Gate 借币失败 %s %s: %s", base, amount, exc)
            msg = str(exc).lower()
            if "not enough" in msg or "insufficient" in msg or "3045" in msg:
                self._borrow_blacklist[base.upper()] = time.time() + BORROW_POOL_EMPTY_COOLDOWN
            return False

    async def _gate_margin_repay(self, symbol: str, base: str, amount: float) -> bool:
        """Gate 逐仓还款。"""
        try:
            await self.gate_spot.repay_isolated_margin(symbol, base, amount)
            logger.info("Gate 还款: %s %s (pair=%s)", amount, base, symbol)
            return True
        except Exception as exc:
            logger.error("Gate 还款失败 %s %s: %s", base, amount, exc)
            return False

    async def _gate_query_margin_account(self, symbol: str) -> dict[str, Any]:
        """查询 Gate 逐仓账户状态。"""
        base = symbol.split("/")[0]
        try:
            bal = await self._safe_request(
                "gate_margin.fetch_balance",
                lambda: self.gate_spot.fetch_balance(params={"type": "margin"}),
                default={},
            )
            base_free = self._free_balance(bal, base)
            base_used = float((bal.get("used", {}) or {}).get(base, 0))
            base_debt = float((bal.get("debt", {}) or {}).get(base, 0))
            usdt_free = self._free_balance(bal, "USDT")
            return {"base_net": base_free + base_used - base_debt,
                    "base_borrowed": base_debt,
                    "quote_net": usdt_free,
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
            lambda: self.gate_spot.fetch_balance(),
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
            lambda: self.gate_futures.fetch_positions([self.gate_state.futures_symbol]),
            default=[],
        )
        for pos in positions:
            if pos.get("symbol") != self.gate_state.futures_symbol:
                continue
            contracts = self._position_contracts(pos)
            if self.gate_state.direction == "reverse":
                return contracts > 0
            return contracts < 0
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
        """Gate margin 卖出（卖出借入的币）。"""
        try:
            precise = float(self.gate_spot.amount_to_precision(symbol, amount))
            order = await self._safe_request(
                "gate_margin_sell",
                lambda: self.gate_spot.create_order(
                    symbol, "market", "sell", precise,
                    params={"account": "margin"},
                ),
                raise_error=True,
            )
            filled = float(order.get("filled", 0))
            ok = order.get("status") == "closed" and filled >= precise * 0.9
            return LegResult(ok, "gate_margin", symbol, "sell", precise,
                             order={"id": str(order.get("id", "")), "filled": filled,
                                    "status": "closed" if ok else "open", "info": order})
        except Exception as exc:
            return LegResult(False, "gate_margin", symbol, "sell", amount, error=str(exc))

    async def _close_gate_margin_spot_leg(self, symbol: str, amount: float) -> LegResult:
        """Gate margin 买回（还币）。"""
        try:
            precise = float(self.gate_spot.amount_to_precision(symbol, amount))
            order = await self._safe_request(
                "gate_margin_buy",
                lambda: self.gate_spot.create_order(
                    symbol, "market", "buy", precise,
                    params={"account": "margin"},
                ),
                raise_error=True,
            )
            filled = float(order.get("filled", 0))
            ok = order.get("status") == "closed" and filled >= precise * 0.9
            return LegResult(ok, "gate_margin", symbol, "buy", precise,
                             order={"id": str(order.get("id", "")), "filled": filled,
                                    "status": "closed" if ok else "open", "info": order})
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
        import requests as _requests
        url = f"{BINANCE_FUTURES_API}/fapi/v1/premiumIndex"
        def _do():
            proxies = {"http": HTTP_PROXY, "https": HTTP_PROXY} if HTTP_PROXY else None
            resp = _requests.get(url, proxies=proxies, timeout=15)
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
            logger.warning("现货余额获取失败（测试网正常现象），跳过划转。")
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

        # 测试网：现货 API 可能不可用，用合约余额估算（假设两账户各半）
        if spot_usdt <= 0 and futures_usdt > 0 and USE_TESTNET:
            estimated_total = futures_usdt * 0.5
            logger.warning(
                "现货余额获取失败（测试网正常现象），假设两账户均分: 每腿 ~%.2f USDT",
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

    async def _cross_open_short_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """在指定交易所做空合约（开仓前设 1x 杠杆）。"""
        if exchange == "binance":
            await self._binance_set_leverage(symbol)
            try:
                order = await self._binance_futures_order(symbol, "sell", amount, position_side="SHORT")
                return LegResult(True, "futures", symbol, "sell", amount, order=order)
            except Exception as exc:
                logger.warning("合约开空异常 %s: %s，验证实际持仓...", symbol, exc)
                actual = await self._cross_verify_binance_position(symbol, "SHORT")
                if actual > 0:
                    logger.warning("合约开空实际已成交 %s: filled=%.4f (API返回0但持仓存在)",
                                   symbol, actual)
                    return LegResult(True, "futures", symbol, "sell", actual,
                                     order={"id": "verified", "symbol": symbol, "side": "sell",
                                            "amount": actual, "filled": actual, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("合约开空确认失败 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))
        else:
            await self._gate_set_leverage(symbol)
            try:
                order = await self._gate_futures_order(symbol, "sell", amount)
                ok = order.get("status") == "closed"
                if ok:
                    logger.info("[跨交易所] Gate开空成功 %s: filled=%.4f", symbol, order["filled"])
                else:
                    logger.error("[跨交易所] Gate开空未成交 %s: status=%s", symbol, order.get("status"))
                return LegResult(ok, "futures", symbol, "sell", amount, order=order,
                                 error=None if ok else "Gate short not filled")
            except Exception as exc:
                logger.error("[跨交易所] Gate开空异常 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))

    async def _cross_open_long_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """在指定交易所做多合约（开仓前设 1x 杠杆）。"""
        if exchange == "binance":
            await self._binance_set_leverage(symbol)
            try:
                order = await self._binance_futures_order(symbol, "buy", amount, position_side="LONG")
                return LegResult(True, "futures", symbol, "buy", amount, order=order)
            except Exception as exc:
                logger.warning("合约开多异常 %s: %s，验证实际持仓...", symbol, exc)
                actual = await self._cross_verify_binance_position(symbol, "LONG")
                if actual > 0:
                    logger.warning("合约开多实际已成交 %s: filled=%.4f (API返回0但持仓存在)",
                                   symbol, actual)
                    return LegResult(True, "futures", symbol, "buy", actual,
                                     order={"id": "verified", "symbol": symbol, "side": "buy",
                                            "amount": actual, "filled": actual, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("合约开多确认失败 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))
        else:
            await self._gate_set_leverage(symbol)
            try:
                order = await self._gate_futures_order(symbol, "buy", amount)
                ok = order.get("status") == "closed"
                if ok:
                    logger.info("[跨交易所] Gate开多成功 %s: filled=%.4f", symbol, order["filled"])
                else:
                    logger.error("[跨交易所] Gate开多未成交 %s: status=%s", symbol, order.get("status"))
                return LegResult(ok, "futures", symbol, "buy", amount, order=order,
                                 error=None if ok else "Gate long not filled")
            except Exception as exc:
                logger.error("[跨交易所] Gate开多异常 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))

    async def _cross_close_short_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """平空单（买入平空）。"""
        if exchange == "binance":
            try:
                order = await self._binance_futures_order(symbol, "buy", amount, reduce_only=True, position_side="SHORT")
                return LegResult(True, "futures", symbol, "buy", amount, order=order)
            except Exception as exc:
                # API 返回 filled=0 但可能已成交，验证仓位是否消失
                remaining = await self._cross_verify_binance_position(symbol, "SHORT")
                if remaining < amount * 0.1:
                    logger.warning("平空已成交 %s: 仓位已消失 (API返回filled=0)", symbol)
                    return LegResult(True, "futures", symbol, "buy", amount,
                                     order={"id": "verified_close", "symbol": symbol, "side": "buy",
                                            "amount": amount, "filled": amount, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("平空确认失败 %s: 剩余=%.4f err=%s", symbol, remaining, exc)
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))
        else:
            try:
                order = await self._gate_futures_order(symbol, "buy", amount, reduce_only=True)
                ok = order.get("status") == "closed"
                return LegResult(ok, "futures", symbol, "buy", amount, order=order,
                                 error=None if ok else "Gate close short not filled")
            except Exception as exc:
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))

    async def _cross_close_long_leg(self, exchange: str, symbol: str, amount: float) -> LegResult:
        """平多单（卖出平多）。"""
        if exchange == "binance":
            try:
                order = await self._binance_futures_order(symbol, "sell", amount, reduce_only=True, position_side="LONG")
                return LegResult(True, "futures", symbol, "sell", amount, order=order)
            except Exception as exc:
                # API 返回 filled=0 但可能已成交，验证仓位是否消失
                remaining = await self._cross_verify_binance_position(symbol, "LONG")
                if remaining < amount * 0.1:
                    logger.warning("平多已成交 %s: 仓位已消失 (API返回filled=0)", symbol)
                    return LegResult(True, "futures", symbol, "sell", amount,
                                     order={"id": "verified_close", "symbol": symbol, "side": "sell",
                                            "amount": amount, "filled": amount, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("平多确认失败 %s: 剩余=%.4f err=%s", symbol, remaining, exc)
                return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))
        else:
            try:
                order = await self._gate_futures_order(symbol, "sell", amount, reduce_only=True)
                ok = order.get("status") == "closed"
                return LegResult(ok, "futures", symbol, "sell", amount, order=order,
                                 error=None if ok else "Gate close long not filled")
            except Exception as exc:
                return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))

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

    async def _open_cross_exchange_position(self, row: pd.Series) -> bool:
        """开仓跨交易所套利：高费率所做空 + 低费率所做多。"""
        base = str(row["base"])
        short_ex = str(row["short_exchange"])
        long_ex = str(row["long_exchange"])
        short_sym = str(row["short_symbol"])
        long_sym = str(row["long_symbol"])

        # 获取两所合约余额
        bn_bal, gt_bal = await asyncio.gather(
            self._cross_get_bn_futures_balance(),
            self._gate_futures_balance(),
        )
        short_bal = bn_bal if short_ex == "binance" else gt_bal
        long_bal = bn_bal if long_ex == "binance" else gt_bal

        if short_bal <= 0 or long_bal <= 0:
            logger.error("[跨交易所] 余额不足: BN=%.2f GT=%.2f", bn_bal, gt_bal)
            return False

        # 取较小余额计算统一仓位
        common_usdt = min(short_bal, long_bal) * POSITION_SIZE_RATIO
        if common_usdt <= 0:
            logger.error("[跨交易所] 可用仓位为 0")
            return False

        # 获取两所价格（两所价格可能不同，各用各的算数量）
        short_ex_obj = self.futures if short_ex == "binance" else self.gate_futures
        long_ex_obj = self.futures if long_ex == "binance" else self.gate_futures
        short_ticker, long_ticker = await asyncio.gather(
            self._safe_request(f"cross_ticker_{short_ex}",
                               lambda: short_ex_obj.fetch_ticker(short_sym), default=None),
            self._safe_request(f"cross_ticker_{long_ex}",
                               lambda: long_ex_obj.fetch_ticker(long_sym), default=None),
        )
        short_price = float(short_ticker["last"]) if short_ticker and short_ticker.get("last") else 0
        long_price = float(long_ticker["last"]) if long_ticker and long_ticker.get("last") else 0
        if short_price <= 0 or long_price <= 0:
            logger.error("[跨交易所] 获取价格失败: %s=%.6f %s=%.6f", short_ex, short_price, long_ex, long_price)
            return False

        # 分别按各所价格 + 余额算数量，取较小值保证两边都够
        short_notional = short_bal * POSITION_SIZE_RATIO
        long_notional = long_bal * POSITION_SIZE_RATIO
        short_qty = await self._cross_calculate_amount(short_ex, short_sym, short_price, short_notional)
        long_qty = await self._cross_calculate_amount(long_ex, long_sym, long_price, long_notional)
        if short_qty <= 0 or long_qty <= 0:
            return False
        amount = min(short_qty, long_qty)
        # 用实际价格反算，确认两边都不超
        est_short_usdt = amount * short_price
        est_long_usdt = amount * long_price
        if est_short_usdt > short_bal * POSITION_SIZE_RATIO * 1.1 or est_long_usdt > long_bal * POSITION_SIZE_RATIO * 1.1:
            logger.error("[跨交易所] 数量超限: short≈%.2f long≈%.2f USDT", est_short_usdt, est_long_usdt)
            return False

        # 币安合约最低名义价值 5 USDT，低于此值会报 -4164
        MIN_CROSS_NOTIONAL = 5.5
        if est_short_usdt < MIN_CROSS_NOTIONAL or est_long_usdt < MIN_CROSS_NOTIONAL:
            logger.warning("[跨交易所] 仓位太小: short≈%.2f long≈%.2f USDT < %.1f，跳过",
                           est_short_usdt, est_long_usdt, MIN_CROSS_NOTIONAL)
            return False

        logger.info(
            "[跨交易所] 开仓 %s: %s空@%s(%.4f%%) + %s多@%s(%.4f%%) | 数量=%s | 费率差=%.4f%% | 净收益=%.4f%%",
            base, base, short_ex, float(row["short_rate"]) * 100,
            base, long_ex, float(row["long_rate"]) * 100,
            amount, float(row["rate_spread"]) * 100, float(row["net_rate"]) * 100,
        )

        # 并发开两腿
        short_task = self._cross_open_short_leg(short_ex, short_sym, amount)
        long_task = self._cross_open_long_leg(long_ex, long_sym, amount)
        short_result, long_result = await asyncio.gather(short_task, long_task)

        if short_result.ok and long_result.ok:
            self.cross_state = CrossArbitrageState(
                is_open=True,
                base=base,
                amount=amount,
                short_exchange=short_ex,
                long_exchange=long_ex,
                short_symbol=short_sym,
                long_symbol=long_sym,
                short_order_id=str(short_result.order.get("id", "")),
                long_order_id=str(long_result.order.get("id", "")),
                short_entry_price=short_price,
                long_entry_price=long_price,
                short_rate=float(row["short_rate"]),
                long_rate=float(row["long_rate"]),
                rate_spread=float(row["rate_spread"]),
                total_net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.tz).isoformat(),
                short_next_funding_time_ms=float(row["short_next_funding_time_ms"]),
                long_next_funding_time_ms=float(row["long_next_funding_time_ms"]),
            )
            self._save_cross_state()
            await asyncio.to_thread(
                self._send_email,
                "bazfbot 跨所开仓",
                f"币种: {base}\n"
                f"空 {short_ex}: {short_sym} 费率 {float(row['short_rate'])*100:.4f}%\n"
                f"多 {long_ex}: {long_sym} 费率 {float(row['long_rate'])*100:.4f}%\n"
                f"数量: {amount} | 费率差: {float(row['rate_spread'])*100:.4f}%\n"
                f"净收益: {float(row['net_rate'])*100:.4f}%",
            )
            return True

        # 一腿或两腿失败 → 应急平掉成功腿
        logger.critical("[跨交易所] 开仓未同时成功: short=%s long=%s", short_result, long_result)
        if short_result.ok:
            logger.critical("[跨交易所] 空单已成交但多单失败，立即平空恢复中性")
            await self._cross_close_short_leg(short_ex, short_sym, amount)
        if long_result.ok:
            logger.critical("[跨交易所] 多单已成交但空单失败，立即平多恢复中性")
            await self._cross_close_long_leg(long_ex, long_sym, amount)
        return False

    async def _close_cross_exchange_position(self) -> bool:
        """平仓跨交易所套利：平空 + 平多。"""
        if not self.cross_state.is_open:
            return True

        cs = self.cross_state
        logger.info("[跨交易所] 平仓 %s: 平空@%s(%s) + 平多@%s(%s)",
                     cs.base, cs.short_exchange, cs.short_symbol,
                     cs.long_exchange, cs.long_symbol)

        short_task = self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.amount)
        long_task = self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.amount)
        short_r, long_r = await asyncio.gather(short_task, long_task)

        # 重试失败腿
        for attempt in range(3):
            if short_r.ok and long_r.ok:
                break
            if not short_r.ok:
                logger.warning("[跨交易所] 平空失败，重试 %d/3", attempt + 1)
                short_r = await self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.amount)
            if not long_r.ok:
                logger.warning("[跨交易所] 平多失败，重试 %d/3", attempt + 1)
                long_r = await self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.amount)
            await asyncio.sleep(1.0)

        if short_r.ok and long_r.ok:
            logger.info("[跨交易所] 平仓成功。")
            short_exit = self._extract_fill_price(short_r.order) if short_r.order else 0.0
            long_exit = self._extract_fill_price(long_r.order) if long_r.order else 0.0
            self._record_cross_trade(short_exit=short_exit, long_exit=long_exit)
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
            await self._cross_open_long_leg(cs.long_exchange, cs.long_symbol, cs.amount)
        elif long_r.ok and not short_r.ok:
            logger.critical("[跨交易所] 重开空单恢复对冲")
            await self._cross_open_short_leg(cs.short_exchange, cs.short_symbol, cs.amount)
        await asyncio.to_thread(
            self._send_email,
            "bazfbot 跨所平仓失败！",
            f"{cs.base} 平仓单腿失败，已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    @staticmethod
    def _extract_fill_price(order: dict[str, Any] | None) -> float:
        """从订单响应中提取成交均价。"""
        if not order:
            return 0.0
        for src in (order.get("info", {}), order):
            avg = src.get("avgPrice")
            if avg:
                return float(avg)
            qq = src.get("cummulativeQuoteQty")
            eq = src.get("executedQty")
            if qq and eq:
                qty = float(eq)
                if qty > 0:
                    return float(qq) / qty
        for key in ("price", "fill_price", "average"):
            if order.get(key):
                return float(order[key])
        return 0.0

    def _record_cross_trade(self, short_exit: float = 0.0, long_exit: float = 0.0) -> None:
        """记录跨交易所套利交易到 trade_history。按交易所拆分真实盈亏。"""
        cs = self.cross_state
        if not cs.is_open or cs.amount <= 0:
            return
        amount = cs.amount
        short_entry = cs.short_entry_price
        long_entry = cs.long_entry_price
        short_notional = amount * short_entry if short_entry else 0
        long_notional = amount * long_entry if long_entry else 0

        # 费率、抵扣、返佣查找表
        bn_fee, gt_fee = getattr(self, "_dash_cross_fees", (0.00045, 0.00018))
        _fee_of = {"binance": bn_fee, "gate": gt_fee}
        _dsc_of = {"binance": BN_FEE_DISCOUNT_FACTOR, "gate": GT_FEE_DISCOUNT_FACTOR}
        _rbt_of = {"binance": BN_FEE_REBATE_FACTOR, "gate": GT_FEE_REBATE_FACTOR}

        short_fee_rate = _fee_of.get(cs.short_exchange, bn_fee)
        long_fee_rate = _fee_of.get(cs.long_exchange, gt_fee)

        # ── 做空侧（高费率所）──
        short_price_pnl = round(amount * (short_entry - short_exit), 6)
        short_funding_pnl = round(short_notional * cs.short_rate, 6)
        short_fee_gross = round(short_notional * short_fee_rate * 2, 6)
        short_fee_actual = round(short_fee_gross
                                 * _dsc_of.get(cs.short_exchange, 1.0)
                                 * _rbt_of.get(cs.short_exchange, 1.0), 6)
        short_net = round(short_price_pnl + short_funding_pnl - short_fee_actual, 6)

        # ── 做多侧（低费率所）──
        long_price_pnl = round(amount * (long_exit - long_entry), 6)
        long_funding_pnl = round(-long_notional * cs.long_rate, 6)
        long_fee_gross = round(long_notional * long_fee_rate * 2, 6)
        long_fee_actual = round(long_fee_gross
                                * _dsc_of.get(cs.long_exchange, 1.0)
                                * _rbt_of.get(cs.long_exchange, 1.0), 6)
        long_net = round(long_price_pnl + long_funding_pnl - long_fee_actual, 6)

        net_pnl = round(short_net + long_net, 6)

        self._write_cross_trade_record(
            cs.base, f"cross({cs.short_exchange}空+{cs.long_exchange}多)",
            cs.amount, cs.short_entry_price, cs.total_net_rate, cs.rate_spread,
            short_entry=short_entry, short_exit=short_exit,
            long_entry=long_entry, long_exit=long_exit,
            net_pnl=net_pnl,
            short_exchange=cs.short_exchange, short_price_pnl=short_price_pnl,
            short_funding_pnl=short_funding_pnl,
            short_fee=short_fee_gross, short_actual_fee=short_fee_actual, short_net=short_net,
            long_exchange=cs.long_exchange, long_price_pnl=long_price_pnl,
            long_funding_pnl=long_funding_pnl,
            long_fee=long_fee_gross, long_actual_fee=long_fee_actual, long_net=long_net)

    def _write_cross_trade_record(self, coin: str, direction: str, amount: float,
                                   entry_price: float, net_rate: float, rate_spread: float,
                                   **extra) -> None:
        """写入一条跨所交易记录。extra 支持 sniper 模式的详细盈亏字段。"""
        try:
            history = self._load_trade_history()
            record = {
                "time": datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S"),
                "coin": coin,
                "direction": direction,
                "profit_usdt": round(amount * entry_price * net_rate, 6),
                "net_rate": net_rate,
                "rate_spread": rate_spread,
                "amount": amount,
            }
            record.update({k: v for k, v in extra.items() if v})  # 合并额外字段
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

        async def _check_futures_pos(exchange: str, symbol: str, expect_short: bool) -> tuple[bool, str]:
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
                            if amt >= cs.amount * 0.1:
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
                        lambda: self.gate_futures.fetch_positions([symbol]),
                        default=[],
                    )
                    for pos in positions:
                        contracts = float(pos.get("contracts", 0) or 0)
                        if abs(contracts) < cs.amount * 0.1:
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
            _check_futures_pos(cs.short_exchange, cs.short_symbol, expect_short=True),
            _check_futures_pos(cs.long_exchange, cs.long_symbol, expect_short=False),
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
        await asyncio.gather(
            self._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.amount),
            self._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.amount),
        )
        self._record_cross_trade()
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
        header = (f"  {'币种':<8s} {'BN价':>10s} {'GT价':>10s} {'费率差':>7s} {'净收益':>7s} "
                  f"{'做空所':>6s} {'空费率':>7s} {'空结算':>7s} "
                  f"{'做多所':>6s} {'多费率':>7s} {'多结算':>7s}")
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
                f"{str(r['short_exchange']):>6s} {float(r['short_rate'])*100:>7.4f}% {short_wait:>7s} "
                f"{str(r['long_exchange']):>6s} {float(r['long_rate'])*100:>7.4f}% {long_wait:>7s}"
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
                lambda: self.gate_futures.fetch_positions([symbol]),
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
        logger.info("  跨交易所持仓  —  %s × %.4f 张  (每腿≈%.0f USDT)", cs.base, cs.amount, notional)
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
                lambda: self.gate_futures.fetch_positions(),
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

        # ── 无持仓：狙击模式检查 → 查询余额 → 扫描 → 开仓 ──
        # 如果上次扫描发现有候选快结算了，精准等到 T-5s 再扫+开
        snipe_ms = getattr(self, "_next_snipe_settle_ms", 0)
        if snipe_ms:
            remain_ms = snipe_ms - time.time() * 1000
            if remain_ms <= 0:
                self._next_snipe_settle_ms = 0
            elif remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000:
                await asyncio.sleep(max(0, (remain_ms - CROSS_SNIPER_OFFSET_SEC * 1000) / 1000))
                logger.info("[跨交易所] 狙击模式：距结算 %dms，立刻扫描+开仓",
                             max(0, int(snipe_ms - time.time() * 1000)))
                # 查余额算仓位
                bn_bal, gt_bal = await asyncio.gather(
                    self._cross_get_bn_futures_balance(),
                    self._gate_futures_balance(),
                )
                if bn_bal <= 0 or gt_bal <= 0:
                    logger.error("[跨交易所] 狙击模式余额不足")
                    self._next_snipe_settle_ms = 0
                    return
                position_usdt = min(bn_bal, gt_bal) * CROSS_POSITION_SIZE_RATIO
                cross_df = await self._scan_cross_exchange(position_usdt, strict=True)
                self._dash_cross_df = cross_df
                passing = cross_df[cross_df["passes"] == True] if not cross_df.empty and "passes" in cross_df.columns else cross_df
                if not cross_df.empty and not passing.empty:
                    candidate = passing.iloc[0]
                    logger.info("[跨交易所] 狙击开仓 %s: 费率差=%.4f%% 净收益=%.4f%%",
                                 str(candidate["base"]), float(candidate["rate_spread"]) * 100,
                                 float(candidate["net_rate"]) * 100)
                    ok = await self._open_cross_exchange_position(candidate)
                    if ok:
                        settle_ms = min(float(candidate["short_next_funding_time_ms"]),
                                        float(candidate["long_next_funding_time_ms"]))
                        wait_ms = settle_ms + 2000 - time.time() * 1000
                        if wait_ms > 0:
                            await asyncio.sleep(wait_ms / 1000)
                        await self._close_cross_exchange_position()
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
                return

        # 更新下次狙击结算时间
        if not cross_df.empty:
            best = cross_df.iloc[0]
            self._next_snipe_settle_ms = min(
                float(best.get("short_next_funding_time_ms", 0)),
                float(best.get("long_next_funding_time_ms", 0)),
            )

        self._print_cross_opportunity_table(cross_df, position_usdt)

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

    @staticmethod
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
            while True:
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
        cum_fee_gross = 0.0
        cum_fee_actual = 0.0
        for h in history:
            if "short_exchange" in h:
                cum_price += (h.get("short_price_pnl", 0) or 0) + (h.get("long_price_pnl", 0) or 0)
                cum_funding += (h.get("short_funding_pnl", 0) or 0) + (h.get("long_funding_pnl", 0) or 0)
                sf = (h.get("short_actual_fee") or h.get("short_fee")) or 0
                lf = (h.get("long_actual_fee") or h.get("long_fee")) or 0
                cum_fee_actual += sf + lf
                cum_fee_gross += (h.get("short_fee", 0) or 0) + (h.get("long_fee", 0) or 0)
            elif "price_pnl" in h:
                cum_price += (h.get("price_pnl", 0) or 0)
                cum_funding += (h.get("funding_pnl", 0) or 0)
                cum_fee_actual += (h.get("fee_total", 0) or 0)
                cum_fee_gross += (h.get("fee_total", 0) or 0)

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
        curve_labels, curve_data = [], []
        if snaps:
            step = max(1, len(snaps) // 200)
            # 智能时间格式：同一天只显示 HH:MM，跨天显示 MM-DD HH:MM
            first_date = snaps[0].get("ts", "")[:10]
            last_date = snaps[-1].get("ts", "")[:10]
            same_day = first_date == last_date
            for s in snaps[::step]:
                ts = s.get("ts", "")
                if same_day:
                    label = ts[11:16]  # HH:MM
                else:
                    label = ts[5:16].replace("T", " ")  # MM-DD HH:MM
                curve_labels.append(label)
                curve_data.append(round(s.get("binance", 0) + s.get("gate", 0), 2))

        # ── 月历数据（JS 动态渲染）──
        pnl_json = json.dumps(month_pnl, ensure_ascii=False)
        today_iso = now.strftime("%Y-%m-%d")

        # ── 余额卡片 ──
        grand_total = total_usdt + gate_spot
        gate_cards = ""
        if GATE_TRADING_ENABLED:
            gate_cards = f"""
            <div class="card"><div class="label">Gate 交易</div><div class="value" style="color:#17c9a4">{gate_spot:,.2f}</div></div>
            <div class="card total"><div class="label">总计</div><div class="value" style="color:#f0b90b">{grand_total:,.2f}</div></div>"""
        else:
            gate_cards = f"""
            <div class="card total"><div class="label">总计</div><div class="value" style="color:#f0b90b">{grand_total:,.2f}</div></div>"""
        balance_cards = f"""
        <div class="balances">
            <div class="card"><div class="label">BN 合约</div><div class="value">{futures_usdt:,.2f}</div></div>
            <div class="card"><div class="label">BN 合计</div><div class="value">{total_usdt:,.2f}</div></div>
            {gate_cards}
            <div class="card"><div class="label">累计盈亏</div><div class="value {profit_cls}">{total_profit:+.4f}</div></div>
            <div class="card"><div class="label">今日盈亏</div><div class="value {today_cls}">{today_pnl:+.4f}</div></div>
            <div class="card"><div class="label">平仓盈亏</div><div class="value {"positive" if cum_price>=0 else "negative"}">{cum_price:+.4f}</div></div>
            <div class="card"><div class="label">累计资费</div><div class="value {"positive" if cum_funding>=0 else "negative"}">{cum_funding:+.4f}</div></div>
            <div class="card"><div class="label">手续费(标称)</div><div class="value negative">{cum_fee_gross:.4f}</div></div>
            <div class="card"><div class="label">手续费(实付)</div><div class="value negative">{cum_fee_actual:.4f}</div></div>
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
                cs_notional = cs.amount * cs.short_entry_price
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
                <span>数量: {cs.amount:.4f} (每腿)</span>
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
        curve_json = json.dumps(curve_data, ensure_ascii=False) if curve_data else "[]"
        labels_json = json.dumps(curve_labels, ensure_ascii=False) if curve_labels else "[]"

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
                    sfe = h.get("short_fee", 0) or 0
                    sfa = h.get("short_actual_fee") or sfe  # 兼容旧记录
                    sn = h.get("short_net", 0) or 0
                    lp = h.get("long_price_pnl", 0) or 0
                    lf = h.get("long_funding_pnl", 0) or 0
                    lfe = h.get("long_fee", 0) or 0
                    lfa = h.get("long_actual_fee") or lfe
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
                    # 做空侧: 价格盈亏 / 资费 / 手续费(标称/实付) / 净盈亏
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {s_cls}">{short_ex}空</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if sp>=0 else "negative"}">{sp:+.4f}</td>'
                             f'<td class="right {"positive" if sf>=0 else "negative"}">{sf:+.4f}</td>'
                             f'<td class="right negative">{sfe:.4f}<span class="muted">/{sfa:.4f}</span></td>'
                             f'<td class="right {"positive" if sn>=0 else "negative"}">{sn:+.4f}</td></tr>')
                    # 做多侧
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {l_cls}">{long_ex}多</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if lp>=0 else "negative"}">{lp:+.4f}</td>'
                             f'<td class="right {"positive" if lf>=0 else "negative"}">{lf:+.4f}</td>'
                             f'<td class="right negative">{lfe:.4f}<span class="muted">/{lfa:.4f}</span></td>'
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
                header = '<tr><th>时间</th><th>类型</th><th>币种</th><th class="right">数量</th><th class="right">价格盈亏</th><th class="right">资费</th><th class="right">手续费(标称/实付)</th><th class="right">净盈亏</th></tr>'
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
        if curve_data:
            chart_section = f"""
        <div class="section">
            <h2>资金曲线 <span class="muted">(BN+Gate 总余额)</span></h2>
            <div style="position:relative;height:280px"><canvas id="equityChart"></canvas></div>
        </div>"""

        html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no">
<meta http-equiv="refresh" content="30">
<title>bazfbot 套利看板</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: #0f0f1a; color: #e0e0e0; padding: 10px; max-width: 800px; margin: 0 auto; }}
h1 {{ font-size: 1.2rem; margin-bottom: 2px; }}
h2 {{ font-size: 0.95rem; margin-bottom: 8px; color: #aaa; }}
.header {{ text-align: center; margin-bottom: 12px; }}
.header .ts {{ color: #555; font-size: 0.75rem; }}
.section {{ background: #1a1a2e; border-radius: 10px; padding: 12px; margin-bottom: 10px; }}
.balances {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(65px, 1fr)); gap: 6px; margin-bottom: 10px; }}
.card {{ background: #1a1a2e; border-radius: 8px; padding: 8px 6px; text-align: center; }}
.card.total {{ background: #16213e; border: 1px solid #f0b90b44; }}
.card .label {{ font-size: 0.6rem; color: #888; }}
.card .value {{ font-size: 0.9rem; font-weight: 700; margin-top: 2px; }}
.stats {{ display: flex; flex-wrap: wrap; gap: 8px 16px; justify-content: center; font-size: 0.7rem; color: #999; margin-bottom: 8px; }}
.stats b {{ color: #ddd; }}
.position {{ display: flex; flex-wrap: wrap; gap: 6px 12px; align-items: center; font-size: 0.82rem; }}
.position.reverse {{ border-left: 3px solid #f0a030; padding-left: 8px; }}
.position.forward {{ border-left: 3px solid #4090e0; padding-left: 8px; }}
.badge {{ display: inline-block; padding: 2px 6px; border-radius: 3px; font-size: 0.6rem; font-weight: 700; }}
.badge.reverse {{ background: #f0a030; color: #1a1a2e; }}
.badge.forward {{ background: #4090e0; color: #fff; }}
.badge.cross {{ background: #a855f7; color: #fff; }}
.badge.bn {{ background: #f0b90b; color: #111; }}
.badge.gt {{ background: #2955E7; color: #fff; }}
.pass {{ color: #0ecb81; font-weight: bold; }}
.fail {{ color: #e74c3c; }}
.table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.73rem; white-space: nowrap; }}
th, td {{ padding: 4px 5px; text-align: left; border-bottom: 1px solid #252540; }}
th {{ color: #888; font-weight: 600; position: sticky; top: 0; background: #1a1a2e; }}
.calendar td {{ text-align: center; padding: 4px 2px; font-size: 0.75rem; cursor: default; }}
.calendar td small {{ display: block; }}
.right {{ text-align: right; }}
.center {{ text-align: center; }}
.positive {{ color: #4caf84; font-weight: 600; }}
.negative {{ color: #e05560; font-weight: 600; }}
.muted {{ color: #666; }}
tr.held {{ background: #1e2a1e; }}
tr.cross-group {{ border-top: 2px solid #444; }}
tr.cross-group td {{ padding-top: 12px; }}
tr.cross-sub td {{ color: #999; font-size: 0.85em; padding: 1px 6px; }}
tr.cross-sub td:first-child {{ padding-left: 16px; }}
.footer {{ text-align: center; color: #444; font-size: 0.65rem; margin-top: 12px; }}
.cal-nav {{ background: none; border: 1px solid #444; color: #aaa; border-radius: 4px; padding: 2px 8px; font-size: 0.8rem; cursor: pointer; }}
.cal-nav:hover {{ background: #252540; }}
#calTitle {{ display: inline-block; min-width: 120px; text-align: center; }}
@media (max-width: 500px) {{
    .balances {{ grid-template-columns: repeat(3, 1fr); }}
    table {{ font-size: 0.65rem; }}
    th, td {{ padding: 3px 3px; }}
    .calendar td {{ font-size: 0.68rem; }}
    .stats {{ gap: 4px 10px; font-size: 0.65rem; }}
}}
</style>
</head>
<body>
<div class="header">
    <h1>bazfbot 套利看板</h1>
    <div class="ts">更新: {now_str} | 30s 刷新</div>
</div>
{balance_cards}
{stats_html}
{position_html}
{calendar_html}
{chart_section}
{recent_html}
{table_html}
{cross_table_html}
<div class="footer">bazfbot · Binance + Gate.io</div>

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

        if curve_data:
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
                datasets: [{{
                    label: '总资金 (USDT)',
                    data: {curve_json},
                    borderColor: '#4caf84',
                    backgroundColor: 'rgba(76,175,132,0.08)',
                    fill: true,
                    pointRadius: 0,
                    borderWidth: 1.5,
                    tension: 0.3,
                }}]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: false }} }},
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
    parser = argparse.ArgumentParser(description="币安资金费率套利机器人")
    parser.add_argument("--scan", action="store_true", help="仅扫描打印机会（正向+反向），不下单")
    parser.add_argument("--scan-reverse", action="store_true", help="仅扫描反向套利机会（负费率）")
    parser.add_argument("--live", action="store_true", help="实盘模式 (默认测试网)")
    parser.add_argument("--no-gate", action="store_true", help="禁用 Gate.io 交易 (仅 Binance)")
    parser.add_argument("--cross", action="store_true", help="启用跨交易所资金费率差异套利")
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
