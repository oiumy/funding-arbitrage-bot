"""Gate.io 反向套利: 借币卖出 + 做多合约，收取空头费率。"""
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


class GateReverseStrategy:
    """Gate.io 反向套利: 借币卖出 + 做多合约，收取空头费率。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot

    async def scan(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """Gate.io 反向扫描：借币卖出 + 合约做多，负费率方向。"""
        if not REVERSE_ENABLED:
            return pd.DataFrame()

        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self.bot._fetch_gate_spot_tickers_direct(),
            self.bot._fetch_gate_futures_tickers_direct(),
            self.bot._fetch_gate_funding_rates_direct(),
            self.bot._fetch_gate_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        futures_index = self.bot._build_gate_futures_market_index()

        negative_bases: list[str] = []
        base_funding_info: dict[str, tuple[float, bool, float]] = {}
        for base in futures_index:
            futures_symbol = futures_index[base]["symbol"]
            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, is_predicted = self.bot._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None or predicted_rate >= 0:
                continue
            next_ft = self.bot._extract_next_funding_time(funding_item)
            negative_bases.append(base)
            base_funding_info[base] = (predicted_rate, is_predicted, next_ft)

        if not negative_bases:
            return pd.DataFrame()

        borrow_rates = await self.bot._fetch_gate_margin_borrow_rates(negative_bases)

        rows: list[dict[str, Any]] = []
        for base in negative_bases:
            borrow_rate = borrow_rates.get(base)
            if borrow_rate is None:
                continue

            predicted_rate, is_predicted, next_ft = base_funding_info[base]
            futures_market = futures_index[base]
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_qv = _safe_float(futures_ticker.get("quoteVolume"))

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

    async def open_position(self, row: pd.Series) -> bool:
        """Gate 反向开仓：划转保证金 → 借币卖出 + 合约做多。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])

        if self.bot._borrow_blacklist.get(base.upper(), 0) > time.time():
            logger.info("Gate [%s] 借币池黑名单中，跳过。", base)
            return False

        total = await self.bot._gate_spot_balance()
        half = total / 2  # 一半做保证金抵押，一半做合约保证金
        if half <= 0:
            return False

        position_usdt = half * POSITION_SIZE_RATIO

        # 划转保证金到逐仓账户
        transfer_amt = self.bot._floor_usdt(position_usdt)
        if transfer_amt >= 1.0:
            if not await self.bot._gate_transfer_usdt(transfer_amt, "spot", "margin", symbol=spot_symbol):
                return False

        price = self.bot._select_reference_price(row)
        if not price or price <= 0:
            logger.warning("[gate] 开仓失败(%s): 无参考价格", base)
            await self.bot._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False
        amount = await self.bot._gate_calculate_amount(spot_symbol, futures_symbol, price, position_usdt)
        if amount <= 0:
            logger.warning("[gate] 开仓失败(%s): 金额不足最小下单量，跳过", base)
            await self.bot._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False

        if not await self.bot._gate_margin_borrow(spot_symbol, base, amount):
            logger.warning("[gate] 开仓失败(%s): 借币失败", base)
            await self.bot._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
            return False

        await self.bot._gate_set_leverage(futures_symbol)
        logger.info("Gate 反向开仓: %s %s margin sell + futures long | 费率=%+.4f%%",
                    base, amount, float(row["predicted_funding_rate"]) * 100)
        margin_task = self.bot._open_gate_margin_spot_leg(spot_symbol, amount)
        futures_task = self.bot._open_gate_futures_long_leg(futures_symbol, amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        if margin_result.ok and futures_result.ok:
            next_ft = await self.bot._gate_fetch_next_funding_time(futures_symbol)
            self.bot.gate_state = ArbitrageState(
                is_open=True, exchange="gate",
                spot_symbol=spot_symbol, futures_symbol=futures_symbol,
                base=base, amount=amount, entry_price=price,
                predicted_funding_rate=float(row["predicted_funding_rate"]),
                net_rate=float(row["net_rate"]),
                opened_at=datetime.now(tz=self.bot.tz).isoformat(),
                spot_source="margin", next_funding_time_ms=next_ft,
                direction="reverse",
            )
            self.bot._save_state("gate")
            self.bot._print_position_summary()
            self.bot._send_email(f"[Gate 开仓] {base} 反向套利",
                             f"费率={float(row['predicted_funding_rate'])*100:.4f}% 净收益={float(row['net_rate'])*100:.4f}%")
            return True

        if margin_result.ok or futures_result.ok:
            logger.warning("[gate] 开仓失败(%s): 部分成交 margin_ok=%s futures_ok=%s",
                          base, margin_result.ok, futures_result.ok)
            await self.emergency_close(margin_result, futures_result, spot_symbol, base)
        else:
            logger.warning("[gate] 开仓失败(%s): 下单失败 margin_err=%s futures_err=%s",
                          base, margin_result.error, futures_result.error)
            await self.bot._gate_margin_repay(spot_symbol, base, amount)
            await self.bot._gate_transfer_usdt(transfer_amt, "margin", "spot", symbol=spot_symbol)
        return False

    async def close_position(self) -> bool:
        """Gate 反向平仓：margin 买回 + 合约平多 → 还款 → USDT 划回 spot。"""
        symbol = self.bot.gate_state.spot_symbol
        fsymbol = self.bot.gate_state.futures_symbol
        base = self.bot.gate_state.base
        amount = self.bot.gate_state.amount
        logger.info("Gate 反向平仓: margin=%s futures=%s amount=%s", symbol, fsymbol, amount)

        margin_task = self.bot._close_gate_margin_spot_leg(symbol, amount)
        futures_task = self.bot._close_gate_futures_long_leg(fsymbol, amount)
        margin_r, fut_r = await asyncio.gather(margin_task, futures_task)

        for _ in range(3):
            if margin_r.ok and fut_r.ok:
                break
            if not margin_r.ok:
                margin_r = await self.bot._close_gate_margin_spot_leg(symbol, amount)
            if not fut_r.ok:
                fut_r = await self.bot._close_gate_futures_long_leg(fsymbol, amount)
            await asyncio.sleep(1.0)

        if not margin_r.ok or not fut_r.ok:
            logger.critical("Gate 反向平仓部分失败，恢复对冲。")
            if margin_r.ok and not fut_r.ok:
                await self.bot._open_gate_margin_spot_leg(symbol, amount)
            elif fut_r.ok and not margin_r.ok:
                await self.bot._open_gate_futures_long_leg(fsymbol, amount)
            return False

        acct = await self.bot._gate_query_margin_account(symbol)
        borrowed = acct.get("base_borrowed", amount)
        await self.bot._gate_margin_repay(symbol, base, max(borrowed, amount))

        acct = await self.bot._gate_query_margin_account(symbol)
        quote_net = acct.get("quote_net", 0)
        transfer_out = self.bot._floor_usdt(quote_net)
        if transfer_out > 0:
            await self.bot._gate_transfer_usdt(transfer_out, "margin", "spot", symbol=symbol)

        logger.info("Gate 反向平仓完成。")
        self.bot._record_close_trade(self.bot.gate_state)
        self.bot.gate_state = ArbitrageState()
        self.bot._save_state("gate")
        self.bot._send_email("[Gate 平仓] 反向套利", "平仓成功")
        return True

    async def emergency_close(self, margin_result: Any, futures_result: Any, symbol: str = "", base: str = "") -> None:
        """Gate 反向部分成交应急平仓。"""
        tasks = []
        if margin_result.ok and not futures_result.ok:
            tasks.append(self.bot._close_gate_margin_spot_leg(symbol, margin_result.amount))
        if futures_result.ok and not margin_result.ok:
            tasks.append(self.bot._close_gate_futures_long_leg(futures_result.symbol, futures_result.amount))
        if tasks:
            await asyncio.gather(*tasks)
        if not margin_result.ok:
            await self.bot._gate_margin_repay(symbol, base, margin_result.amount)

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
        """验证 Gate 反向持仓是否仍然存在。"""
        if not self.bot.gate_state.is_open:
            return False
        if self.bot.gate_state.direction != "reverse":
            return False
        margin_ok, futures_ok = await asyncio.gather(
            self.bot._has_gate_margin_loan(),
            self.bot._has_gate_futures_position(),
        )
        if margin_ok or futures_ok:
            return True
        logger.warning("Gate 状态文件显示有反向持仓，但交易所未发现对应仓位，重置状态。")
        self.bot.gate_state = ArbitrageState()
        self.bot._save_state("gate")
        return False

