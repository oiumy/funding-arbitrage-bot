from __future__ import annotations
"""Shared constants and logger for all modules."""

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

def _detect_proxy() -> str | None:
    """探测本地是否有 HTTP 代理可用。有则返回代理地址，无则返回 None。"""
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(("127.0.0.1", 7892))
        s.close()
    except (socket.error, OSError):
        return None
    os.environ.setdefault("HTTP_PROXY", _LOCAL_PROXY)
    os.environ.setdefault("HTTPS_PROXY", _LOCAL_PROXY)
    return _LOCAL_PROXY

HTTP_PROXY = _detect_proxy()

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
# QQ 邮箱通知 → 配置见 config.py
STATE_FILE = Path("data/funding_arbitrage_state.json")
GATE_STATE_FILE = Path("data/gate_arbitrage_state.json")
MARGIN_STATE_FILE = Path("data/margin_state.json")
CROSS_STATE_FILE = Path("data/cross_arbitrage_state.json")

# Binance 官方 REST API 基础地址（跳过 ccxt 直连）
BINANCE_SPOT_API = "https://api.binance.com"
BINANCE_FUTURES_API = "https://fapi.binance.com"

# ── Gate.io ──
GATE_SPOT_API = "https://api.gateio.ws/api/v4"
GATE_FUTURES_API = "https://api.gateio.ws/api/v4"
DEFAULT_GATE_SPOT_TAKER_FEE = 0.001      # Gate VIP0 现货 taker 基础费率 0.10%
DEFAULT_GATE_FUTURES_TAKER_FEE = 0.0005  # Gate 永续 taker 基础费率 0.05%
GATE_SPOT_REBATE = 0.55                  # 现货返佣 55% → 有效费率 = 基础 × 0.45
GATE_FUTURES_REBATE = 0.64               # 合约返佣 64% → 有效费率 = 基础 × 0.36
BINANCE_TRADING_ENABLED = True           # Binance 实盘交易总开关
GATE_TRADING_ENABLED = True              # Gate.io 实盘交易总开关

# ── 跨交易所资金费率套利 ──
CROSS_EXCHANGE_ENABLED = False           # 跨所套利开关（--cross 开启）
CROSS_SHOW_SINGLE = True                # 跨所模式下同时展示单交易所套利机会（仅扫描，不下单）
CROSS_POSITION_SIZE_RATIO = 0.98        # 跨所资金利用率（纯期货对冲，价格风险中性，可更高）
CROSS_SNIPER_SCAN_OFFSET_SEC = 10         # 结算前N秒扫描（获取最新费率）
CROSS_SNIPER_OPEN_OFFSET_MS = 1000         # 结算前N毫秒发出开仓（预留足够网络延迟）
CROSS_SNIPER_CLOSE_DELAY_MS = 200          # 结算后N毫秒发出平仓（确保资费到账）
CROSS_LEVERAGE = 1                        # 跨所套利用的合约杠杆倍数
CROSS_SNIPE_WINDOW_SEC = 60              # 距结算≤N秒时跳过单所扫描，直接狙击
CROSS_MIN_RATE_SPREAD = 0.0003          # 最小原始费率差 0.03%
CROSS_MIN_NET_RATE = 0.0001             # 最小净收益率 0.01%（扣双边手续费后）

# ── Binance 资金费率狙击 (裸合约，无对冲) ──
BN_SNIPE_ENABLED = False                # 狙击开关（--bn-snipe 开启）
BN_SNIPE_MIN_ABS_RATE = 0.003           # 最小绝对费率 0.3%
BN_SNIPE_POSITION_SIZE_RATIO = 0.95     # 合约余额使用率（裸仓单腿，留 5% 缓冲）
BN_SNIPE_OPEN_OFFSET_MS = -100          # 开仓相对结算点偏移：正=结算前N ms，负=结算后N ms。-100=结算后100ms开仓
BN_SNIPE_TAKE_PROFIT_ENABLED = True     # 开仓同时挂交易所侧止盈单(closePosition，命中即锁≥资费收益)
BN_SNIPE_TP_RATE_MULT = 1.5             # 止盈触发距 = abs(费率)×此倍数（1.5=只吃≥1.5倍资费的大插针，留足抗滑点缓冲；1.0=价格收益≈毛资费，易被噪声误触）

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
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
LOG_DIR = Path("logs")

logger = logging.getLogger("funding-arbitrage-bot")


def setup_logging() -> None:
    """初始化日志：创建目录 + 配置 handler。幂等，多次调用无副作用。"""
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


