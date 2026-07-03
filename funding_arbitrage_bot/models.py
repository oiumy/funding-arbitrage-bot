"""Data models for funding arbitrage bot."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Any

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




def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0
