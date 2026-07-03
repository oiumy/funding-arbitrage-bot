"""Binance 正向期现套利: 买入现货 + 做空合约，收取多头费率."""
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


class BnForwardStrategy:
    """Binance 正向期现套利策略。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot

    # ── 扫描 ───────────────────────────────────────────────

    async def scan(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """扫描全市场 USDT 现货(主站+Alpha)与 U 本位永续，计算单期净收益率。"""
        alpha_tokens = await self.bot._get_alpha_tokens()

        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self.bot._safe_request(
                "spot.fetch_tickers",
                lambda: self.bot.spot.fetch_tickers(),
                default={},
            ),
            self.bot._safe_request(
                "futures.fetch_tickers",
                lambda: self.bot.futures.fetch_tickers(),
                default={},
            ),
            self.bot._safe_request(
                "futures.fetch_funding_rates",
                lambda: self.bot.futures.fetch_funding_rates(),
                default={},
            ),
            self.bot.fetch_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        rows: list[dict[str, Any]] = []
        futures_markets_by_base = self.bot._build_futures_market_index()

        for base, futures_market in futures_markets_by_base.items():
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_quote_volume = _safe_float(
                futures_ticker.get("quoteVolume")
            )

            if not self.bot._futures_passes_volume(futures_quote_volume):
                continue

            funding_item = funding_rates.get(futures_symbol, {})
            predicted_rate, is_predicted = self.bot._extract_predicted_funding_rate(funding_item)
            if predicted_rate is None:
                continue

            if not is_predicted and not getattr(self.bot, '_warned_fallback_rate', False):
                logger.info("使用 lastFundingRate 作为预测费率（币安不提供 nextFundingRate 字段）。")
                self.bot._warned_fallback_rate = True

            next_ft = self.bot._extract_next_funding_time(funding_item)

            spot_symbol, spot_source, spot_last, spot_quote_volume, chain, alpha_fee_ratio = (
                self.bot._resolve_spot_leg(
                    base,
                    spot_tickers,
                    alpha_tokens,
                    position_usdt,
                )
            )
            if spot_symbol is None:
                continue

            if not self.bot._passes_liquidity_filter(
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
                        self.bot._effective_spot_taker_fee(DEFAULT_SPOT_TAKER_FEE),
                    ),
                )
            futures_fee = futures_taker_fees.get(
                futures_symbol,
                futures_taker_fees.get(
                    "__default__",
                    self.bot._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE),
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

    # ── 开仓 ───────────────────────────────────────────────

    async def open_position(self, row: pd.Series) -> bool:
        """并发执行现货买入和合约开空。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        spot_source = str(row.get("spot_source", "spot"))

        await self.bot._reclaim_all_usdt()
        await self.bot._rebalance_accounts()
        position_usdt = await self.bot._get_position_size()
        if position_usdt <= 0:
            logger.error("可用余额不足，放弃开仓。")
            return False

        next_ft_ms = float(row.get("next_funding_time_ms", 0))
        if not self.bot._within_entry_window(next_ft_ms):
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
            if expected_gas > 0 and not await self.bot._check_alpha_gas(chain, expected_gas):
                logger.info("Alpha Gas 超标，放弃本次开仓。")
                return False

        price = self.bot._select_reference_price(row)

        if not price or price <= 0:
            logger.error("参考价格无效，放弃开仓: %s", row.to_dict())
            return False

        amount = self.bot._calculate_precise_amount(
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

        spot_task = self.bot._open_spot_leg(spot_symbol, amount, spot_source)
        futures_task = self.bot._open_futures_short_leg(futures_symbol, amount)
        spot_result, futures_result = await asyncio.gather(
            spot_task,
            futures_task,
        )

        if spot_result.ok and futures_result.ok:
            next_ft = await self.bot._fetch_next_funding_time(futures_symbol)
            next_ft_dt = datetime.fromtimestamp(next_ft / 1000, tz=self.bot.tz) if next_ft > 0 else None
            if next_ft_dt:
                logger.info(
                    "合约 %s 下次结算: %s (距今 %.0f 分钟)",
                    futures_symbol,
                    next_ft_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    (next_ft - time.time() * 1000) / 60000,
                )

            self.bot.binance_state = ArbitrageState(
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
                opened_at=datetime.now(tz=self.bot.tz).isoformat(),
                spot_source=spot_source,
                next_funding_time_ms=next_ft,
            )
            self.bot._save_state()
            self.bot._print_position_summary()
            await asyncio.to_thread(
                self.bot._send_email,
                f"bazfbot 开仓: {row['base']}",
                f"币种: {row['base']} [{spot_source}]\n"
                f"数量: {amount}\n"
                f"费率: {float(row['predicted_funding_rate'])*100:.3f}%\n"
                f"净收益: {float(row['net_rate'])*100:.3f}%\n"
                f"结算: {datetime.fromtimestamp(next_ft/1000, tz=self.bot.tz).strftime('%H:%M')}",
            )
            return True

        if spot_result.ok or futures_result.ok:
            logger.critical(
                "双腿下单未同时成功，触发应急平仓。spot=%s futures=%s",
                spot_result, futures_result,
            )
            await self.bot._emergency_close_exposed_leg(spot_result, futures_result)
        else:
            logger.warning("双腿均未成交: spot=%s futures=%s — 无需应急。", spot_result, futures_result)
        return False

    # ── 平仓 ───────────────────────────────────────────────

    async def close_position(self) -> bool:
        """结算后双腿并发平仓：卖出现货 + 买入平空。"""
        logger.info(
            "开始平仓套利头寸: spot=%s futures=%s amount=%s",
            self.bot.binance_state.spot_symbol,
            self.bot.binance_state.futures_symbol,
            self.bot.binance_state.amount,
        )

        spot_task = self.bot._close_spot_leg(
            self.bot.binance_state.spot_symbol,
            self.bot.binance_state.amount,
            self.bot.binance_state.spot_source,
        )
        futures_task = self.bot._close_futures_short_leg(
            self.bot.binance_state.futures_symbol,
            self.bot.binance_state.amount,
        )
        spot_result, futures_result = await asyncio.gather(spot_task, futures_task)

        for attempt in range(3):
            if spot_result.ok and futures_result.ok:
                break
            if not spot_result.ok:
                logger.warning("现货卖出失败，重试 %d/3", attempt + 1)
                spot_result = await self.bot._close_spot_leg(
                    self.bot.binance_state.spot_symbol, self.bot.binance_state.amount, self.bot.binance_state.spot_source,
                )
            if not futures_result.ok:
                logger.warning("合约平空失败，重试 %d/3", attempt + 1)
                futures_result = await self.bot._close_futures_short_leg(
                    self.bot.binance_state.futures_symbol, self.bot.binance_state.amount,
                )
            await asyncio.sleep(1.0)

        if spot_result.ok and futures_result.ok:
            logger.info("套利平仓成功。现货卖出+合约买入平空均已完成。")
            self.bot._record_close_trade(self.bot.binance_state)
            self.bot.binance_state = ArbitrageState()
            self.bot._save_state()
            await asyncio.to_thread(
                self.bot._send_email, "bazfbot 平仓", "双腿已平，仓位已清空。"
            )
            return True

        logger.critical("平仓失败（重试3次），尝试恢复对冲。spot=%s futures=%s", spot_result, futures_result)
        if spot_result.ok and not futures_result.ok:
            logger.critical("合约平空失败，买回现货恢复对冲")
            await self.bot._open_spot_leg(
                self.bot.binance_state.spot_symbol, self.bot.binance_state.amount, self.bot.binance_state.spot_source,
            )
        elif futures_result.ok and not spot_result.ok:
            logger.critical("现货卖出失败，重开空单恢复对冲")
            await self.bot._open_futures_short_leg(
                self.bot.binance_state.futures_symbol, self.bot.binance_state.amount,
            )
        await asyncio.to_thread(
            self.bot._send_email,
            "bazfbot 平仓失败！",
            "平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    # ── 决策 ───────────────────────────────────────────────

    def should_exit(self) -> bool:
        """持仓决策时机: 自由人模式随时触发，锁定期等结算过后才触发。"""
        state = self.bot.binance_state
        if not state.is_open:
            return False
        if not state.locked:
            return True
        if state.next_funding_time_ms <= 0:
            return False
        return time.time() * 1000 >= state.next_funding_time_ms

    # ── 持仓检查 ───────────────────────────────────────────

    async def check_position(self) -> bool:
        """验证 Binance 正向持仓是否仍然存在，不一致则重置状态。"""
        if not self.bot.binance_state.is_open:
            return False

        spot_ok, futures_ok = await asyncio.gather(
            self.bot._has_spot_balance(),
            self.bot._has_futures_short_position(),
        )
        if spot_ok or futures_ok:
            return True

        logger.warning("状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.bot.binance_state = ArbitrageState()
        self.bot._save_state()
        return False
