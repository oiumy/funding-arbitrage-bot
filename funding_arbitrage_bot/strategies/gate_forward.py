"""Gate.io 正向期现套利: 买入现货 + 做空合约，收取多头费率。"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime
from typing import Any, TYPE_CHECKING

import pandas as pd

from ..constants import *
from ..models import ArbitrageState, _safe_float

if TYPE_CHECKING:
    from ..bot import FundingArbitrageBot


class GateForwardStrategy:
    """Gate.io 正向期现套利: 买入现货 + 做空合约，收取多头费率。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot

    async def scan(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """Gate.io 正向扫描：现货买入 + 合约做空。无 Alpha 回退。"""
        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self.bot._fetch_gate_spot_tickers_direct(),
            self.bot._fetch_gate_futures_tickers_direct(),
            self.bot._fetch_gate_funding_rates_direct(),
            self.bot._fetch_gate_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        rows: list[dict[str, Any]] = []
        futures_index = self.bot._build_gate_futures_market_index()

        for base, futures_market in futures_index.items():
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_qv = _safe_float(futures_ticker.get("quoteVolume"))
            if not self.bot._futures_passes_volume(futures_qv):
                continue

            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, _ = self.bot._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None:
                continue

            next_ft = self.bot._extract_next_funding_time(funding_item)

            spot_symbol = f"{base}/USDT"
            spot_market = (getattr(self.bot.gate_spot, "markets", None) or {}).get(spot_symbol)
            if not spot_market or not spot_market.get("active", True):
                continue
            spot_ticker = spot_tickers.get(spot_symbol, {})
            spot_last = _safe_float(spot_ticker.get("last"))
            spot_qv = _safe_float(spot_ticker.get("quoteVolume"))
            if spot_last <= 0:
                continue
            if not self.bot._passes_liquidity_filter(spot_qv, futures_qv):
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

    async def open_position(self, row: pd.Series) -> bool:
        """Gate 正向开仓：现货买入 + 合约做空。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])

        await self.bot._gate_rebalance_accounts()
        position_usdt = await self.bot._gate_get_position_size()
        if position_usdt <= 0:
            return False

        next_ft_ms = float(row.get("next_funding_time_ms", 0))
        if not self.bot._within_entry_window(next_ft_ms):
            logger.info("Gate [%s] 不在结算窗口内，跳过。", base)
            return False

        await self.bot._gate_set_leverage(futures_symbol)

        price = self.bot._select_reference_price(row)
        if not price or price <= 0:
            return False
        amount = await self.bot._gate_calculate_amount(spot_symbol, futures_symbol, price, position_usdt)
        if amount <= 0:
            logger.warning("[gate] 开仓失败(%s): 金额不足最小下单量，跳过", base)
            return False

        logger.info("Gate 正向开仓: %s %s buy spot + short futures | 费率=%+.4f%%",
                    base, amount, float(row["predicted_funding_rate"]) * 100)
        spot_task = self.bot._open_gate_spot_leg(spot_symbol, amount)
        futures_task = self.bot._open_gate_futures_short_leg(futures_symbol, amount)
        spot_result, futures_result = await asyncio.gather(spot_task, futures_task)

        if spot_result.ok and futures_result.ok:
            next_ft = await self.bot._gate_fetch_next_funding_time(futures_symbol)
            self.bot.gate_state = ArbitrageState(
                is_open=True, exchange="gate",
                spot_symbol=spot_symbol, futures_symbol=futures_symbol,
                base=base, amount=amount, entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.bot.tz).isoformat(),
                spot_source="spot", next_funding_time_ms=next_ft,
                direction="forward",
            )
            self.bot._save_state("gate")
            self.bot._print_position_summary()
            self.bot._send_email(f"[Gate 开仓] {base} 正向套利",
                             f"费率={float(row['predicted_funding_rate'])*100:.4f}% 净收益={float(row['net_rate'])*100:.4f}%")
            return True

        if spot_result.ok or futures_result.ok:
            await self.emergency_close(spot_result, futures_result)
        return False

    async def close_position(self) -> bool:
        """Gate 正向平仓：现货卖出 + 合约平空。"""
        symbol = self.bot.gate_state.spot_symbol
        fsymbol = self.bot.gate_state.futures_symbol
        amount = self.bot.gate_state.amount
        logger.info("Gate 正向平仓: spot=%s futures=%s amount=%s", symbol, fsymbol, amount)

        spot_task = self.bot._close_gate_spot_leg(symbol, amount)
        futures_task = self.bot._close_gate_futures_short_leg(fsymbol, amount)
        spot_r, fut_r = await asyncio.gather(spot_task, futures_task)

        for _ in range(3):
            if spot_r.ok and fut_r.ok:
                break
            if not spot_r.ok:
                spot_r = await self.bot._close_gate_spot_leg(symbol, amount)
            if not fut_r.ok:
                fut_r = await self.bot._close_gate_futures_short_leg(fsymbol, amount)
            await asyncio.sleep(1.0)

        if spot_r.ok and fut_r.ok:
            logger.info("Gate 正向平仓成功。")
            self.bot._record_close_trade(self.bot.gate_state)
            self.bot.gate_state = ArbitrageState()
            self.bot._save_state("gate")
            self.bot._send_email("[Gate 平仓] 正向套利", "平仓成功")
            return True

        logger.critical("Gate 平仓部分失败，恢复对冲。")
        if spot_r.ok and not fut_r.ok:
            await self.bot._open_gate_spot_leg(symbol, amount)
        elif fut_r.ok and not spot_r.ok:
            await self.bot._open_gate_futures_short_leg(fsymbol, amount)
        return False

    async def emergency_close(self, spot_result: Any, futures_result: Any) -> None:
        """Gate 正向部分成交应急平仓。"""
        tasks = []
        if spot_result.ok and not futures_result.ok:
            tasks.append(self.bot._close_gate_spot_leg(spot_result.symbol, spot_result.amount))
        if futures_result.ok and not spot_result.ok:
            tasks.append(self.bot._close_gate_futures_short_leg(futures_result.symbol, futures_result.amount))
        if tasks:
            await asyncio.gather(*tasks)

    def should_exit(self) -> bool:
        """持仓决策时机: 自由人模式随时触发，锁定期等结算过后才触发。"""
        state = self.bot.gate_state
        if not state.is_open:
            return False
        if not state.locked:
            return True
        if state.next_funding_time_ms <= 0:
            return False
        return time.time() * 1000 >= state.next_funding_time_ms

    async def check_position(self) -> bool:
        """验证 Gate 正向持仓是否仍然存在。"""
        if not self.bot.gate_state.is_open:
            return False
        if self.bot.gate_state.direction != "forward":
            return False
        spot_ok, futures_ok = await asyncio.gather(
            self.bot._has_gate_spot_balance(),
            self.bot._has_gate_futures_position(),
        )
        if spot_ok or futures_ok:
            return True
        logger.warning("Gate 状态文件显示有正向持仓，但交易所未发现对应仓位，重置状态。")
        self.bot.gate_state = ArbitrageState()
        self.bot._save_state("gate")
        return False

