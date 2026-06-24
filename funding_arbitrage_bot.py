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
    from config import API_KEY, API_SECRET, NOTIFY_EMAIL, NOTIFY_EMAIL_AUTH  # type: ignore[import-not-found]
except ImportError:
    # 部署到服务器时确保 config.py 在同目录。本地没有则用空值占位。
    API_KEY = ""
    API_SECRET = ""
    NOTIFY_EMAIL = ""
    NOTIFY_EMAIL_AUTH = ""
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
MIN_SWITCH_SPREAD = 0.0001     # 换仓最小利差 0.01%，差额不够不换，避免摩擦损耗
MARGIN_LEVEL_MIN = 1.3       # 逐仓保证金率最低阈值，币价涨62%触发，距强平18%缓冲
FUTURES_LIQ_DISTANCE_MIN = 0.30  # 合约空单距强平最小距离30%，币价涨60%+触发

# 流动性过滤不宜过严，否则会错过资金费率机会。
# 默认 hybrid: 合约侧至少 1500 万 USDT，现货侧至少 100 万 USDT。
LIQUIDITY_MODE = "hybrid"  # options: futures_only, both_legs, hybrid
MIN_FUTURES_24H_QUOTE_VOLUME = 15_000_000
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
MIN_BNB_BALANCE_FOR_FEES = 0.02

# QQ 邮箱通知 → 配置见 config.py
STATE_FILE = Path("data/funding_arbitrage_state.json")
MARGIN_STATE_FILE = Path("data/margin_state.json")

# Binance 官方 REST API 基础地址（跳过 ccxt 直连）
BINANCE_SPOT_API = "https://api.binance.com"
BINANCE_FUTURES_API = "https://fapi.binance.com"

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
        logging.StreamHandler(),
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
    locked: bool = True  # True=刚开仓锁定期, False=自由人模式(随时可换仓)


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


class FundingArbitrageBot:
    """现货买入 + 永续开空的资金费率套利机器人。"""

    def __init__(self) -> None:
        self.tz = ZoneInfo(LOCAL_TIMEZONE)
        self.state = self._load_state()
        self.margin_state = self._load_margin_state()
        self.spot = self._create_spot_exchange()
        self.futures = self._create_futures_exchange()
        self.alpha_spot = self._create_alpha_spot_exchange()
        self._alpha_tokens: dict[str, dict[str, Any]] = {}
        self._alpha_last_fetch: float = 0.0

    @staticmethod
    def _ccxt_config(options: dict | None = None) -> dict[str, Any]:
        """构建 ccxt 配置，自动判断是否启用代理。"""
        cfg: dict[str, Any] = {
            "apiKey": API_KEY,
            "secret": API_SECRET,
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
        await self._safe_request(
            "spot.load_markets",
            lambda: self.spot.load_markets(),
            raise_error=True,
        )
        await self._safe_request(
            "futures.load_markets",
            lambda: self.futures.load_markets(),
            raise_error=True,
        )

    async def close(self) -> None:
        """关闭 ccxt 异步会话。"""
        await self._safe_request("spot.close", lambda: self.spot.close())
        await self._safe_request("futures.close", lambda: self.futures.close())

    @staticmethod
    def _load_state() -> ArbitrageState:
        if not STATE_FILE.exists():
            return ArbitrageState()

        try:
            with STATE_FILE.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            return ArbitrageState(**payload)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            logger.warning("读取状态文件失败，将按无持仓启动: %s", exc)
            return ArbitrageState()

    def _save_state(self) -> None:
        with STATE_FILE.open("w", encoding="utf-8") as file:
            json.dump(asdict(self.state), file, indent=2, ensure_ascii=False)

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

    def _record_margin_used(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_used", {})[clean] = time.time()
        self._save_margin_state()

    def _record_margin_disabled(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_disabled", {})[clean] = time.time()
        self._save_margin_state()

    # ------------------------------------------------------------------
    # Binance 官方 REST API 直连 (不经过 ccxt)
    # ------------------------------------------------------------------

    @staticmethod
    def _binance_sign(query_string: str) -> str:
        """HMAC-SHA256 签名."""
        return hmac.new(
            API_SECRET.encode(), query_string.encode(), hashlib.sha256
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
                headers={"X-MBX-APIKEY": API_KEY},
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

        市价单正常情况下应全部成交; 若成交不足 90% 视为异常。
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
            "status": "closed" if ok else "open",
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
                                     reduce_only: bool = False) -> dict[str, Any]:
        """币安合约市价单 POST /fapi/v1/order."""
        clean = self._clean_futures_symbol(symbol)
        params: dict[str, Any] = {
            "symbol": clean, "side": side.upper(), "type": "MARKET",
            "quantity": quantity,
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        resp = await self._binance_request(
            BINANCE_FUTURES_API, "/fapi/v1/order",
            params, method="POST",
        )
        order = self._normalize_order_response(resp, symbol, side, quantity)
        if order["status"] != "closed":
            raise ExchangeError(
                f"合约{side}未完全成交: {symbol} filled={order['filled']}/{quantity}"
            )
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
                logger.warning("%s 借币池暂无库存，稍后重试。", asset)
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
        """清空逐仓交易对资产并划回 USDT：卖出 base、归还借款、划回 quote。不停用交易对。"""
        try:
            acct = await self._get_isolated_margin_account(margin_symbol)
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
            for item in spot_list:
                sym = item.get("symbol", "")
                taker = float(item.get("takerCommission", 0))
                if sym and taker > 0:
                    spot_taker[f"{sym[:-4]}/{sym[-4:]}"] = (
                        self._effective_spot_taker_fee(taker)
                    )
        except Exception:
            logger.warning("现货费率查询失败，使用默认值 %.3f%%", default_spot * 100)
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
                logger.info("合约费率: VIP基础=%.3f%% → BNB折扣后=%.3f%%",
                            raw * 100, fee * 100)
                futures_taker = {"__default__": fee}
        except Exception:
            logger.warning("合约费率查询失败，使用默认值 %.3f%%", default_fut * 100)
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
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        return df.sort_values("net_rate", ascending=False).reset_index(drop=True)

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
        return 0.0

    def _within_entry_window(self, next_funding_time_ms: float) -> bool:
        """开仓时间窗口: 距 settlement 不足 ENTRY_WINDOW_MINUTES 分钟才允许入场。"""
        if next_funding_time_ms <= 0:
            return True  # 获取失败不阻止，允许入场
        now_ms = time.time() * 1000
        remaining_ms = next_funding_time_ms - now_ms
        return 0 < remaining_ms <= ENTRY_WINDOW_MINUTES * 60_000

    async def has_open_arbitrage_position(self) -> bool:
        """根据状态文件和交易所当前仓位确认是否仍有套利持仓。"""
        if not self.state.is_open:
            return False

        if self.state.direction == "reverse":
            futures_ok = await self._has_futures_long_position()
            if futures_ok:
                return True
            logger.warning("状态文件显示有反向持仓，但交易所未发现合约多仓，重置状态。")
            self.state = ArbitrageState()
            self._save_state()
            return False

        spot_ok, futures_ok = await asyncio.gather(
            self._has_spot_balance(),
            self._has_futures_short_position(),
        )
        if spot_ok or futures_ok:
            return True

        logger.warning("状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.state = ArbitrageState()
        self._save_state()
        return False

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

    @staticmethod
    def _free_balance(balance: dict[str, Any], asset: str) -> float:
        value = balance.get("free", {}).get(asset, 0)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    async def _has_spot_balance(self) -> bool:
        if not self.state.base:
            return False

        balance = await self._safe_request(
            "spot.fetch_balance",
            lambda: self.spot.fetch_balance(),
            default={},
        )
        free = balance.get("free", {}).get(self.state.base, 0)
        used = balance.get("used", {}).get(self.state.base, 0)
        try:
            return float(free) + float(used) > self.state.amount * 0.5
        except (TypeError, ValueError):
            return False

    async def _has_futures_short_position(self) -> bool:
        if not self.state.futures_symbol:
            return False

        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.state.futures_symbol:
                continue
            contracts = self._position_contracts(position)
            return contracts < 0 or (
                position.get("side") == "short" and abs(contracts) > 0
            )
        return False

    async def _has_futures_long_position(self) -> bool:
        if not self.state.futures_symbol:
            return False
        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.state.futures_symbol:
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

            self.state = ArbitrageState(
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
        can_borrow, max_borrowable = await self._binance_margin_max_borrowable(base, margin_symbol)
        if not can_borrow or max_borrowable < amount:
            logger.error("[3/5] 无法借够 %s: need=%s max=%s，划回 USDT 并放弃开仓。",
                         base, amount, max_borrowable)
            await self._drain_margin_to_spot(margin_symbol)
            return False

        try:
            await self._binance_margin_loan(base, amount, margin_symbol)
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

            self.state = ArbitrageState(
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

    def _should_exit(self) -> bool:
        """持仓决策时机: 自由人模式随时触发，锁定期等结算过后才触发。"""
        if not self.state.is_open:
            return False
        if not self.state.locked:
            return True  # 自由人模式，每分钟扫描换仓机会
        if self.state.next_funding_time_ms <= 0:
            return False
        return time.time() * 1000 >= self.state.next_funding_time_ms


    async def close_arbitrage_position(self) -> bool:
        """结算后双腿并发平仓。正向: 卖出现货+买入平空。反向: margin买回+卖出平多+还款。"""
        if not self.state.is_open:
            return True

        if self.state.direction == "reverse":
            return await self._close_reverse_position()

        logger.info(
            "开始平仓套利头寸: spot=%s futures=%s amount=%s",
            self.state.spot_symbol,
            self.state.futures_symbol,
            self.state.amount,
        )

        spot_task = self._close_spot_leg(
            self.state.spot_symbol,
            self.state.amount,
            self.state.spot_source,
        )
        futures_task = self._close_futures_short_leg(
            self.state.futures_symbol,
            self.state.amount,
        )
        spot_result, futures_result = await asyncio.gather(spot_task, futures_task)

        # 重试失败的腿（最多3次）
        for attempt in range(3):
            if spot_result.ok and futures_result.ok:
                break
            if not spot_result.ok:
                logger.warning("现货卖出失败，重试 %d/3", attempt + 1)
                spot_result = await self._close_spot_leg(
                    self.state.spot_symbol, self.state.amount, self.state.spot_source,
                )
            if not futures_result.ok:
                logger.warning("合约平空失败，重试 %d/3", attempt + 1)
                futures_result = await self._close_futures_short_leg(
                    self.state.futures_symbol, self.state.amount,
                )
            await asyncio.sleep(1.0)

        if spot_result.ok and futures_result.ok:
            logger.info("套利平仓成功。现货卖出+合约买入平空均已完成。")
            self.state = ArbitrageState()
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
                self.state.spot_symbol, self.state.amount, self.state.spot_source,
            )
        elif futures_result.ok and not spot_result.ok:
            logger.critical("现货卖出失败，重开空单恢复对冲")
            await self._open_futures_short_leg(
                self.state.futures_symbol, self.state.amount,
            )
        await asyncio.to_thread(
            self._send_email,
            "bazfbot 平仓失败！",
            "平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    async def _close_reverse_position(self) -> bool:
        """平仓反向套利: margin买回 + 合约平多 + 还款 + 划回 USDT。"""
        if not self.state.is_open or self.state.direction != "reverse":
            return True

        logger.info(
            "[平仓 1/4] 反向平仓: margin=%s futures=%s amount=%s",
            self.state.spot_symbol, self.state.futures_symbol, self.state.amount,
        )

        # [1] 并发 — margin买回 + 合约平多
        margin_task = self._close_margin_spot_leg(self.state.spot_symbol, self.state.amount)
        futures_task = self._close_futures_long_leg(self.state.futures_symbol, self.state.amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        # 重试失败的腿（最多3次）
        for attempt in range(3):
            if margin_result.ok and futures_result.ok:
                break
            if not margin_result.ok:
                logger.warning("Margin 买回失败，重试 %d/3", attempt + 1)
                margin_result = await self._close_margin_spot_leg(
                    self.state.spot_symbol, self.state.amount,
                )
            if not futures_result.ok:
                logger.warning("合约平多失败，重试 %d/3", attempt + 1)
                futures_result = await self._close_futures_long_leg(
                    self.state.futures_symbol, self.state.amount,
                )
            await asyncio.sleep(1.0)

        if not margin_result.ok or not futures_result.ok:
            logger.critical(
                "[平仓 1/4] 下单异常（重试3次），尝试恢复对冲。margin=%s futures=%s",
                margin_result, futures_result,
            )
            if margin_result.ok and not futures_result.ok:
                logger.critical("合约平多失败，卖出 margin 现货恢复对冲")
                await self._open_margin_spot_leg(self.state.spot_symbol, self.state.amount)
            elif futures_result.ok and not margin_result.ok:
                logger.critical("Margin 买回失败，重开多单恢复对冲")
                await self._open_futures_long_leg(self.state.futures_symbol, self.state.amount)
            await asyncio.to_thread(
                self._send_email,
                "bazfbot 平仓失败！",
                "反向平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
            )
            return False

        # [2] 查询负债 + 还款
        base = self.state.base
        amount = self.state.amount
        margin_symbol = self.state.spot_symbol
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
        self.state = ArbitrageState()
        self._save_state()
        await asyncio.to_thread(
            self._send_email, "bazfbot 反向平仓", "双腿已平，借款已归还，仓位已清空。"
        )
        return True

    async def run_once(self, force_entry: bool = False) -> None:
        """执行一次持仓检查、全市场扫描和开仓/平仓判断。

        force_entry=True: 跳过结算时间检查，允许随时开仓。
        """
        has_position = await self.has_open_arbitrage_position()

        if has_position and self.state.direction == "reverse":
            margin_level = await self._check_margin_level(self.state.spot_symbol)
            if margin_level is not None and margin_level < MARGIN_LEVEL_MIN:
                logger.critical(
                    "保证金率过低 %.2f < %.1f，强制平仓！",
                    margin_level, MARGIN_LEVEL_MIN,
                )
                await self.close_arbitrage_position()
                await asyncio.to_thread(
                    self._send_email,
                    "bazfbot 强制平仓！",
                    f"逐仓保证金率 {margin_level:.2f} 低于阈值 {MARGIN_LEVEL_MIN}，已强制平仓。请检查持仓。",
                )
                return

        if has_position and self.state.direction == "forward":
            dist = await self._check_futures_liquidation_distance(
                self.state.futures_symbol, short=True,
            )
            if dist is not None and dist < FUTURES_LIQ_DISTANCE_MIN:
                logger.critical(
                    "合约空单距强平 %.1f%% < %.0f%%，强制平仓！",
                    dist * 100, FUTURES_LIQ_DISTANCE_MIN * 100,
                )
                await self.close_arbitrage_position()
                await asyncio.to_thread(
                    self._send_email,
                    "bazfbot 强制平仓！",
                    f"合约空单距强平仅 {dist*100:.1f}% (阈值 {FUTURES_LIQ_DISTANCE_MIN*100:.0f}%)，已强制平仓。",
                )
                return

        if has_position and force_entry:
            logger.info("即时模式：强制平仓现有持仓。")
            await self.close_arbitrage_position()
            return

        scan_for_switch = has_position and self._should_exit()
        if scan_for_switch:
            if not self.state.locked:
                logger.info("自由人模式，扫描全市场寻找换仓机会...")
            else:
                logger.info("锁定期结算已过，扫描全市场对比择优...")
        elif has_position:
            if self.state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.state.next_funding_time_ms / 1000, tz=self.tz)
                wait_min = max(0, (self.state.next_funding_time_ms - time.time() * 1000) / 60000)
                logger.info("锁定期，结算时间 %s (%.0f 分钟后)，持仓待涨。",
                            settle_dt.strftime("%H:%M:%S"), wait_min)
            else:
                logger.info("锁定期，持仓待涨。")
            self._print_position_summary()

        if not await self.has_enough_bnb_for_fees():
            return

        # 扫描展示用估算仓位，不划转资金（开仓时才按需划转）
        spot_free, futures_free = await asyncio.gather(
            self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
            self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")

        # 查询逐仓杠杆总余额
        margin_total = 0.0
        try:
            resp = await self._binance_request(
                BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account",
            )
            for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                q = acct.get("quoteAsset", {})
                if q:
                    margin_total += float(q.get("netAsset", 0))
        except Exception:
            pass

        total_usdt = spot_usdt + futures_usdt + margin_total
        logger.info(
            "账户余额: 现货=%.2f | 合约=%.2f | 逐仓杠杆=%.2f | 合计=%.2f USDT",
            spot_usdt, futures_usdt, margin_total, total_usdt,
        )
        if total_usdt <= 0:
            logger.error("可用余额为 0，请充值。")
            return
        estimate_forward = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
        estimate_reverse = total_usdt / 2 * POSITION_SIZE_RATIO
        position_usdt = max(estimate_forward, estimate_reverse)
        if position_usdt <= 0:
            logger.error("估算仓位为 0 (spot=%.1f futures=%.1f margin=%.1f)，跳过本轮。", spot_usdt, futures_usdt, margin_total)
            return

        # 并行扫描正向 + 反向
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
            logger.info("本轮未扫描到满足条件的机会（正向+反向）。")
            return

        df = pd.concat(frames, ignore_index=True).sort_values(
            "net_rate", ascending=False,
        ).reset_index(drop=True)

        # 打印前 10 个机会（正向+反向混排）
        top10 = df.head(10)
        sep = "  " + "-" * 113
        header = (
            f"  {'#':<3} {'方向':<4} {'币种':<8} {'来源':<5} {'资金费率':>8} "
            f"{'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'结算倒计时':>10}"
        )
        logger.info(
            "\n" + "=" * 113 + "\n"
            "  当前可套利机会 (正向+反向 Top 10) — 每腿名义: %.2f USDT\n" % position_usdt
            + "=" * 113 + "\n"
            + header + "\n" + sep
        )
        for rank, (_, row) in enumerate(top10.iterrows(), 1):
            nft = float(row.get("next_funding_time_ms", 0))
            if nft > 0:
                mins = max(0, (nft - time.time() * 1000) / 60000)
                countdown = f"{int(mins)}min后" if mins < 120 else f"{mins/60:.0f}h后"
            else:
                countdown = "N/A"
            direction_label = "反向" if str(row.get("direction")) == "reverse" else "正向"
            borrow_fee = float(row.get("borrow_hourly_rate") or 0)
            held = has_position and str(row["base"]) == self.state.base and str(row.get("direction", "forward")) == self.state.direction
            held_tag = " ← 持仓" if held else ""
            logger.info(
                "  %-3d %-4s %-8s %-5s %7.3f%% %7.3f%% %7.3f%% %7.3f%% %7.3f%% %10s%s",
                rank, direction_label, str(row["base"])[:8], str(row["spot_source"])[:5],
                float(row["predicted_funding_rate"]) * 100,
                float(row["spot_taker_fee"]) * 100,
                float(row["futures_taker_fee"]) * 100,
                borrow_fee * 100, float(row["net_rate"]) * 100, countdown,
                held_tag,
            )
        logger.info(sep + "\n" + "=" * 113)

        self._write_dashboard(spot_usdt, futures_usdt, margin_total, total_usdt,
                              position_usdt, df, has_position)

        # 结算后决策：当无持仓处理，找最优可开候选
        if scan_for_switch:
            # 找最优候选（与无持仓开仓逻辑一致：阈值 + 进场窗口）
            best = None
            if not df.empty:
                for _, row in df.iterrows():
                    net = float(row["net_rate"])
                    direction = str(row.get("direction", "forward"))
                    if direction == "reverse":
                        threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER
                    else:
                        threshold = MIN_NET_RATE
                    if net <= threshold:
                        continue
                    nft = float(row.get("next_funding_time_ms", 0))
                    same = (str(row["base"]) == self.state.base and direction == self.state.direction)
                    if direction != "reverse":
                        if not self._within_entry_window(nft) and nft > 0 and not same:
                            continue
                    best = row
                    break

            if best is None:
                # 只有手持币在结算窗口内才平仓，否则继续持有等待
                if self.state.next_funding_time_ms > 0 and not self._within_entry_window(self.state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(self.state.next_funding_time_ms / 1000, tz=self.tz)
                    logger.info("无合格候选，但手持币未到结算窗口 (结算 %s)，继续持有。",
                               settle_dt.strftime("%m-%d %H:%M"))
                    return
                logger.info("结算窗口内无合格候选，平仓。")
                await self.close_arbitrage_position()
                return

            best_base = str(best["base"])
            best_direction = str(best.get("direction", "forward"))
            same_coin = (best_base == self.state.base and best_direction == self.state.direction)

            if same_coin:
                keep_value = float(best["net_rate"])
                nft = float(best.get("next_funding_time_ms", 0))
                if keep_value > 0:
                    self.state.next_funding_time_ms = nft if nft > 0 else 0
                    self.state.locked = False
                    self.state.opened_at = datetime.now(tz=self.tz).isoformat()
                    self._save_state()
                    if nft > 0:
                        settle_dt = datetime.fromtimestamp(nft / 1000, tz=self.tz)
                        logger.info("当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式，下轮结算 %s。",
                                   self.state.base, keep_value * 100,
                                   settle_dt.strftime("%m-%d %H:%M"))
                    else:
                        logger.info("当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式。",
                                   self.state.base, keep_value * 100)
                    return
                # 手持币净利 ≤ 0，需平仓（仅结算窗口内）
                if self.state.next_funding_time_ms > 0 and not self._within_entry_window(self.state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(self.state.next_funding_time_ms / 1000, tz=self.tz) if self.state.next_funding_time_ms > 0 else None
                    logger.info("当前 %s 净利=%.4f%% 但未到结算窗口 (结算 %s)，继续持有等待。",
                               self.state.base, keep_value * 100,
                               settle_dt.strftime("%m-%d %H:%M") if settle_dt else "?")
                    return
                logger.info("当前 %s 结算窗口内净利=%.4f%%，平仓。", self.state.base, keep_value * 100)
                await self.close_arbitrage_position()
                return

            # 不同币：算 keep_value 和 switch_value，有利差才换
            current_mask = (df["base"] == self.state.base) & (df["direction"] == self.state.direction)
            if current_mask.any():
                cr = df[current_mask].iloc[0]
                keep_value = float(cr["predicted_funding_rate"]) - float(cr.get("borrow_hourly_rate", 0)) \
                    if self.state.direction == "reverse" else float(cr["predicted_funding_rate"])
            else:
                keep_value = -1.0

            close_fee_rate = float(best["spot_taker_fee"]) + float(best["futures_taker_fee"])
            switch_value = float(best["open_only_net_rate"]) - close_fee_rate

            if switch_value > keep_value + MIN_SWITCH_SPREAD and switch_value > 0:
                logger.info("切换: %s→%s | 保持净利=%.4f%% < 切换净利=%.4f%% (利差>%.3f%%)",
                           self.state.base, best_base, keep_value * 100, switch_value * 100,
                           MIN_SWITCH_SPREAD * 100)
                if await self.close_arbitrage_position():
                    await self.open_arbitrage_position(best)
                return

            if keep_value > 0:
                nft = float(cr.get("next_funding_time_ms", 0))
                self.state.next_funding_time_ms = nft if nft > 0 else 0
                self.state.locked = False
                self.state.opened_at = datetime.now(tz=self.tz).isoformat()
                self._save_state()
                logger.info("当前 %s 仍有正收益 (净利=%.4f%%)，换仓不划算，解锁自由人模式。",
                           self.state.base, keep_value * 100)
                return

            # 只有手持币在结算窗口内才平仓，否则继续持有等费率回升
            if self.state.next_funding_time_ms > 0 and not self._within_entry_window(self.state.next_funding_time_ms):
                settle_dt = datetime.fromtimestamp(self.state.next_funding_time_ms / 1000, tz=self.tz)
                logger.info("当前 %s 净利=%.4f%% 但未到结算窗口 (结算 %s)，继续持有等待。",
                           self.state.base, keep_value * 100, settle_dt.strftime("%m-%d %H:%M"))
                return
            logger.info("当前 %s 结算窗口内净利=%.4f%%，平仓。", self.state.base, keep_value * 100)
            await self.close_arbitrage_position()
            return

        # 无持仓时：按净收益降序遍历，选第一个在结算窗口内且超阈值的机会
        if not has_position:
            for _, candidate in df.iterrows():
                net = float(candidate["net_rate"])
                direction = str(candidate.get("direction", "forward"))
                if direction == "reverse":
                    threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER
                else:
                    threshold = MIN_NET_RATE

                if net <= threshold:
                    continue
                nft = float(candidate.get("next_funding_time_ms", 0))
                holding_h = (nft - time.time() * 1000) / 3_600_000 if nft > 0 else 0
                # 反向不限进场窗口（借币池先到先得），正向仍需窗口检查
                if direction != "reverse":
                    within = self._within_entry_window(nft)
                    if not within and nft > 0:
                        mins = max(0, (nft - time.time() * 1000) / 60000)
                        logger.info("跳过 %s [%s]: 距结算 %.0f min > 窗口 %d min",
                                    candidate["base"], direction, mins, ENTRY_WINDOW_MINUTES)
                        continue
                if direction == "reverse":
                    borrow_h = float(candidate.get("borrow_hourly_rate", 0))
                    borrow_cost = borrow_h * max(1.0, holding_h)
                    logger.info(
                        "选中: %s [reverse] | 费率=%.4f%% | 净收益=%.4f%% | "
                        "持有=%.1fh | 借币费=%.4f%% | 阈值=%.3f%%",
                        candidate["base"],
                        float(candidate["predicted_funding_rate"]) * 100,
                        net * 100,
                        max(1.0, holding_h),
                        borrow_cost * 100,
                        (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100,
                    )
                else:
                    logger.info(
                        "选中: %s [forward] | 费率=%.4f%% | 净收益=%.4f%% | 距结算 %s",
                        candidate["base"],
                        float(candidate["predicted_funding_rate"]) * 100, net * 100,
                        (f"{int(max(0, holding_h * 60))}min" if nft > 0 else "?"),
                    )
                if await self.open_arbitrage_position(candidate):
                    return
                logger.info("开仓失败，尝试下一个候选币。")

        if has_position:
            logger.info("未到结算决策窗口，继续持有。")
        else:
            logger.info("所有机会均不在结算窗口内或开仓失败，本轮不开仓。")

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
    def _print_position_summary(self) -> None:
        position_notional = self.state.amount * self.state.entry_price
        estimated_profit = position_notional * self.state.net_rate
        if self.state.direction == "reverse":
            side_label = "long"
            source_label = "margin"
        else:
            side_label = "short"
            source_label = self.state.spot_source
        logger.info(
            "当前套利持仓 [%s]: %s [%s] + %s %s | amount=%s | entry=%s "
            "| 预测单期净利润≈%.4f USDT | opened_at=%s",
            self.state.direction,
            self.state.spot_symbol, source_label,
            self.state.futures_symbol, side_label,
            self.state.amount, self.state.entry_price,
            estimated_profit, self.state.opened_at,
        )

    async def run_forever(self) -> None:
        """打卡模式主循环: 有持仓按实际结算时间平仓, 无持仓定时扫描。"""
        await self.initialize()

        logger.info("启动完成，进入主循环。")
        while True:
            sleep_seconds = self._seconds_until_next_wakeup()
            wakeup_at = datetime.now(tz=self.tz) + timedelta(seconds=sleep_seconds)
            has_pos = await self.has_open_arbitrage_position()
            logger.info(
                "休眠 %.0f 分钟, 下次唤醒: %s (当前%s持仓)",
                sleep_seconds / 60,
                wakeup_at.strftime("%m-%d %H:%M:%S"),
                "有" if has_pos else "无",
            )
            await asyncio.sleep(sleep_seconds)
            await self.run_once()

    def _write_dashboard(
        self,
        spot_usdt: float,
        futures_usdt: float,
        margin_total: float,
        total_usdt: float,
        position_usdt: float,
        df: "pd.DataFrame",
        has_position: bool,
    ) -> None:
        """生成手机端自适应 Dashboard HTML，写入 data/dashboard.html。"""
        now_str = datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S")
        top10 = df.head(10) if not df.empty else df

        # --- 余额卡片 ---
        balance_cards = f"""
        <div class="balances">
            <div class="card"><div class="label">现货</div><div class="value">{spot_usdt:,.2f}</div></div>
            <div class="card"><div class="label">合约</div><div class="value">{futures_usdt:,.2f}</div></div>
            <div class="card"><div class="label">逐仓杠杆</div><div class="value">{margin_total:,.2f}</div></div>
            <div class="card total"><div class="label">合计</div><div class="value">{total_usdt:,.2f}</div></div>
        </div>"""

        # --- 持仓状态 ---
        if has_position and self.state.is_open:
            pos_notional = self.state.amount * self.state.entry_price
            est_profit = pos_notional * self.state.net_rate
            dir_label = "反向" if self.state.direction == "reverse" else "正向"
            dir_cls = "reverse" if self.state.direction == "reverse" else "forward"
            settle_dt = ""
            if self.state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.state.next_funding_time_ms / 1000, tz=self.tz
                ).strftime("%m-%d %H:%M:%S")
            position_html = f"""
        <div class="section">
            <h2>当前持仓</h2>
            <div class="position {dir_cls}">
                <span class="badge {dir_cls}">{dir_label}</span>
                <strong>{self.state.base}</strong>
                <span>数量: {self.state.amount}</span>
                <span>入场价: {self.state.entry_price:.4f}</span>
                <span>预估净利润: <span class="{'positive' if est_profit > 0 else 'negative'}">{est_profit:+.4f} USDT</span></span>
                <span>净费率: <span class="{'positive' if self.state.net_rate > 0 else 'negative'}">{self.state.net_rate*100:+.4f}%</span></span>
                <span>下次结算: {settle_dt}</span>
            </div>
        </div>"""
        else:
            position_html = """
        <div class="section">
            <h2>当前持仓</h2>
            <p class="muted">无持仓</p>
        </div>"""

        # --- Top 10 表格 ---
        rows_html = ""
        if not top10.empty:
            for _, row in top10.iterrows():
                nft = float(row.get("next_funding_time_ms", 0))
                if nft > 0:
                    mins = max(0, (nft - time.time() * 1000) / 60000)
                    countdown = f"{int(mins)}min" if mins < 120 else f"{mins/60:.0f}h"
                else:
                    countdown = "N/A"
                direction = str(row.get("direction", "forward"))
                dir_label = "反向" if direction == "reverse" else "正向"
                dir_cls = "reverse" if direction == "reverse" else "forward"
                net_rate = float(row["net_rate"])
                net_cls = "positive" if net_rate > 0 else "negative"
                borrow = float(row.get("borrow_hourly_rate", 0))
                held = has_position and str(row["base"]) == self.state.base and direction == self.state.direction
                held_mark = " ★" if held else ""
                rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="badge {dir_cls}">{dir_label}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td>{str(row.get('spot_source', ''))[:5]}</td>
                    <td class="right">{float(row['predicted_funding_rate'])*100:+.3f}%</td>
                    <td class="right">{float(row['spot_taker_fee'])*100:.3f}%</td>
                    <td class="right">{float(row['futures_taker_fee'])*100:.3f}%</td>
                    <td class="right">{borrow*100:.3f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td class="right">{countdown}</td>
                </tr>"""
        else:
            rows_html = '<tr><td colspan="9" class="muted center">暂无套利机会</td></tr>'

        table_html = f"""
        <div class="section">
            <h2>Top 10 机会 <span class="muted">(每腿 {position_usdt:,.0f} USDT)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>方向</th><th>币种</th><th>来源</th>
                        <th class="right">费率</th><th class="right">现货费</th><th class="right">合约费</th>
                        <th class="right">借币费</th><th class="right">净收益</th><th class="right">结算</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            </div>
        </div>"""

        html = f"""<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="30">
<title>bazfbot 套利看板</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', system-ui, sans-serif; background: #0f0f1a; color: #e0e0e0; padding: 12px; max-width: 800px; margin: 0 auto; }}
h1 {{ font-size: 1.3rem; margin-bottom: 4px; }}
h2 {{ font-size: 1rem; margin-bottom: 8px; color: #aaa; }}
.header {{ text-align: center; margin-bottom: 16px; }}
.header .ts {{ color: #666; font-size: 0.8rem; }}
.section {{ background: #1a1a2e; border-radius: 10px; padding: 14px; margin-bottom: 12px; }}
.balances {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; margin-bottom: 12px; }}
.card {{ background: #1a1a2e; border-radius: 10px; padding: 12px 8px; text-align: center; }}
.card.total {{ background: #16213e; border: 1px solid #30305a; }}
.card .label {{ font-size: 0.7rem; color: #888; text-transform: uppercase; }}
.card .value {{ font-size: 1.05rem; font-weight: 700; margin-top: 2px; }}
.position {{ display: flex; flex-wrap: wrap; gap: 8px 16px; align-items: center; font-size: 0.9rem; }}
.position.reverse {{ border-left: 3px solid #f0a030; padding-left: 10px; }}
.position.forward {{ border-left: 3px solid #4090e0; padding-left: 10px; }}
.badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 0.7rem; font-weight: 700; }}
.badge.reverse {{ background: #f0a030; color: #1a1a2e; }}
.badge.forward {{ background: #4090e0; color: #fff; }}
.table-wrap {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
table {{ width: 100%; border-collapse: collapse; font-size: 0.78rem; white-space: nowrap; }}
th, td {{ padding: 6px 8px; text-align: left; border-bottom: 1px solid #252540; }}
th {{ color: #888; font-weight: 600; position: sticky; top: 0; background: #1a1a2e; }}
.right {{ text-align: right; }}
.center {{ text-align: center; }}
.positive {{ color: #4caf84; font-weight: 600; }}
.negative {{ color: #e05560; font-weight: 600; }}
.muted {{ color: #666; }}
tr.held {{ background: #1e2a1e; }}
.footer {{ text-align: center; color: #444; font-size: 0.7rem; margin-top: 16px; }}
@media (max-width: 500px) {{
    .balances {{ grid-template-columns: repeat(2, 1fr); }}
    table {{ font-size: 0.7rem; }}
    th, td {{ padding: 4px 5px; }}
}}
</style>
</head>
<body>
<div class="header">
    <h1>bazfbot 套利看板</h1>
    <div class="ts">更新: {now_str} | 每30秒自动刷新</div>
</div>
{balance_cards}
{position_html}
{table_html}
<div class="footer">bazfbot · funding arbitrage</div>
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
            f"  {'排名':<4} {'币种':<10} {'来源':<6} {'链':<8} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {source:<6} {chain:<8} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% {borrow:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )
    else:
        header = (
            f"  正向套利机会 (Positive Funding) — 做多现货 + 做空合约，收取多头费率\n"
            f"  共 {len(df)} 个机会, 显示 Top 20 | 利润按每 {ref_usdt:.0f} USDT/腿 参考值"
        )
        col_header = (
            f"  {'排名':<4} {'币种':<10} {'来源':<6} {'链':<12} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {source:<6} {chain:<12} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )

    print(f"\n{'='*90}")
    print(header)
    print(f"{'='*90}")
    print(col_header)
    print(f"  {'-'*88}")

    for rank, (_, row) in enumerate(top20.iterrows(), 1):
        kwargs = dict(
            rank=rank,
            base=str(row["base"])[:10],
            source=str(row["spot_source"])[:6],
            chain=str(row.get("chain", ""))[:12] if direction == "positive" else str(row.get("chain", ""))[:8],
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
    args = parser.parse_args()

    global USE_TESTNET
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
