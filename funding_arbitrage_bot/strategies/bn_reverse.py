"""Binance 反向套利: 借币卖出 + 做多合约，收取空头费率。"""
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


class BnReverseStrategy:
    """Binance 反向套利: 借币卖出 + 做多合约，收取空头费率。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot

    async def scan(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """扫描负费率反向套利机会：借币卖出 + 做多合约，收取空头费率。

        Returns:
            DataFrame 按 net_rate 降序排列，含 borrow_hourly_rate 列。
        """
        if not REVERSE_ENABLED:
            return pd.DataFrame()

        alpha_tokens = await self.bot._get_alpha_tokens()
        spot_tickers, futures_tickers, funding_rates, fee_pair = await asyncio.gather(
            self.bot._safe_request("spot.fetch_tickers", lambda: self.bot.spot.fetch_tickers(), default={}),
            self.bot._safe_request("futures.fetch_tickers", lambda: self.bot.futures.fetch_tickers(), default={}),
            self.bot._safe_request("futures.fetch_funding_rates", lambda: self.bot.futures.fetch_funding_rates(), default={}),
            self.bot.fetch_taker_fees(),
        )
        spot_taker_fees, futures_taker_fees = fee_pair

        # 预筛选：只找负费率币种，减少借币 API 查询量
        futures_index = self.bot._build_futures_market_index()
        negative_bases: list[str] = []
        base_funding_info: dict[str, tuple[float, bool, float]] = {}  # base -> (rate, is_predicted, next_ft)
        for base, futures_market in futures_index.items():
            futures_ticker_data = futures_tickers.get(futures_market["symbol"], {})
            futures_quote_volume = _safe_float(futures_ticker_data.get("quoteVolume"))
            if not self.bot._futures_passes_volume(futures_quote_volume):
                continue
            funding_item = funding_rates.get(futures_market["symbol"], {})
            rate, is_predicted = self.bot._extract_predicted_funding_rate(funding_item)
            if rate is None or rate >= 0:
                continue
            next_ft = self.bot._extract_next_funding_time(funding_item)
            negative_bases.append(base)
            base_funding_info[base] = (rate, is_predicted, next_ft)

        if not negative_bases:
            return pd.DataFrame()

        # 批量查询借币利率（只查负费率币种）
        borrow_rates = await self.bot._fetch_margin_borrow_rates(negative_bases)

        rows: list[dict[str, Any]] = []
        for base in negative_bases:
            borrow_rate = borrow_rates.get(base)
            if borrow_rate is None:
                continue  # 不可借贷或利率为 0

            predicted_rate, is_predicted, next_ft = base_funding_info[base]
            futures_market = futures_index[base]
            futures_symbol = futures_market["symbol"]
            futures_ticker = futures_tickers.get(futures_symbol, {})
            futures_quote_volume = _safe_float(futures_ticker.get("quoteVolume"))

            resolved = self.bot._resolve_spot_leg(
                base, spot_tickers, alpha_tokens, position_usdt,
            )
            if resolved is None:
                continue
            spot_symbol, spot_source, spot_last, spot_quote_volume, chain, alpha_fee_ratio = resolved

            # Alpha 代币不能做杠杆交易，反向套利只走主站现货
            if spot_source == "alpha":
                continue

            if not self.bot._passes_liquidity_filter(spot_quote_volume, futures_quote_volume):
                continue

            spot_fee = spot_taker_fees.get(
                    spot_symbol,
                    spot_taker_fees.get("__default__", self.bot._effective_spot_taker_fee(DEFAULT_SPOT_TAKER_FEE)),
                )
            futures_fee = futures_taker_fees.get(
                futures_symbol,
                futures_taker_fees.get("__default__", self.bot._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE)),
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

    async def open_position(self, row: pd.Series) -> bool:
        """反向套利开仓: 借币 → margin卖出 → 合约开多。任一环节失败则全部撤销。"""
        spot_symbol = str(row["spot_symbol"])
        futures_symbol = str(row["futures_symbol"])
        base = str(row["base"])
        margin_symbol = spot_symbol  # same pair, e.g. "BTC/USDT"

        if self.bot._borrow_blacklist.get(base, 0) > time.time():
            remaining = int(self.bot._borrow_blacklist[base] - time.time())
            logger.info("%s 借币池黑名单中 (剩余 %ds)，跳过。", base, remaining)
            return False

        # [1/5] 回收闲置资金（跳过目标交易对，避免划走又划回）+ 50/50 分配
        await self.bot._reclaim_all_usdt(keep_symbol=margin_symbol)
        spot_bal, futures_bal, target_margin_acct = await asyncio.gather(
            self.bot._safe_request("spot.fetch_balance", lambda: self.bot.spot.fetch_balance(), default={}),
            self.bot._safe_request("futures.fetch_balance", lambda: self.bot.futures.fetch_balance(), default={}),
            self.bot._get_isolated_margin_account(margin_symbol),
        )
        spot_usdt = self.bot._free_balance(spot_bal, "USDT")
        futures_usdt = self.bot._free_balance(futures_bal, "USDT")
        target_quote = target_margin_acct.get("quoteAsset", {})
        target_margin_usdt = float(target_quote.get("netAsset", 0)) if target_quote else 0.0
        total_usdt = spot_usdt + futures_usdt + target_margin_usdt
        half = total_usdt / 2
        if half <= 0:
            logger.error("[1/5] 总余额不足，放弃反向开仓。")
            return False

        # 如果目标逐仓交易对 USDT 超过 half（上次失败遗留），先划回现货用于合约侧
        if target_margin_usdt > half + 1.0:
            excess = self.bot._floor_usdt(target_margin_usdt - half)
            logger.info("[1/5] 逐仓杠杆 USDT 过多 (%.2f > half %.2f)，划回 %.2f 到现货",
                        target_margin_usdt, half, excess)
            try:
                await self.bot._binance_isolated_margin_transfer(
                    "USDT", excess, margin_symbol, "margin_to_spot",
                )
                target_margin_usdt = half
                spot_usdt += excess
            except Exception as exc:
                logger.warning("[1/5] 划回失败: %s，跳过。", exc)

        # 必要时补足合约侧（做多保证金），只划差额
        if futures_usdt < half:
            need = self.bot._floor_usdt(min(half - futures_usdt, spot_usdt))
            if need >= 1.0:
                logger.info("[1/5] 划转 %.2f USDT: spot → futures（补做多保证金）", need)
                try:
                    await self.bot._safe_request(
                        "transfer_spot_to_futures",
                        lambda: self.bot.spot.transfer("USDT", need, "spot", "future"),
                        raise_error=True,
                    )
                    spot_usdt -= need
                    futures_usdt += need
                except Exception as exc:
                    logger.error("[1/5] spot→futures 划转失败: %s，放弃开仓。", exc)
                    return False
        # 合约有多余且现货不够抵押时，划回 spot
        elif spot_usdt < half and futures_usdt > half:
            need = self.bot._floor_usdt(min(half - spot_usdt, futures_usdt - half))
            if need >= 1.0:
                logger.info("[1/5] 划转 %.2f USDT: futures → spot（补抵押金）", need)
                try:
                    await self.bot._safe_request(
                        "transfer_futures_to_spot",
                        lambda: self.bot.futures.transfer("USDT", need, "future", "spot"),
                        raise_error=True,
                    )
                    spot_usdt += need
                    futures_usdt -= need
                except Exception as exc:
                    logger.error("[1/5] futures→spot 划转失败: %s，放弃开仓。", exc)
                    return False

        position_usdt = half * POSITION_SIZE_RATIO

        # [2/5] 确保逐仓交易对已启用 + 划转 50% 资金 → 逐仓杠杆
        if not await self.bot._ensure_margin_pair_enabled(margin_symbol):
            logger.error("[2/5] 无法启用逐仓交易对 %s（已达上限且无法释放）", base)
            return False

        margin_acct = await self.bot._get_isolated_margin_account(margin_symbol)
        quote_asset = margin_acct.get("quoteAsset", {})
        margin_usdt = float(quote_asset.get("netAsset", 0)) if quote_asset else 0.0

        # 直接查现货账户 USDT 余额（绕过 ccxt 缓存，避免 _reclaim_all_usdt 划转后数据过期）
        spot_actual = spot_usdt
        try:
            spot_acct = await self.bot._binance_request(BINANCE_SPOT_API, "/api/v3/account")
            for b in (spot_acct.get("balances", []) if isinstance(spot_acct, dict) else []):
                if b.get("asset") == "USDT":
                    spot_actual = float(b.get("free", 0))
                    break
        except Exception:
            pass

        shortfall = self.bot._floor_usdt(min(max(0.0, half - margin_usdt), spot_actual))
        # 留 0.01 缓冲，避免余额刚好等于划转金额时被币安拒绝
        transfer_amt = max(0.0, shortfall - 0.01)
        if transfer_amt < 1.0:
            logger.info("[2/5] 逐仓杠杆已有 %.2f USDT ≥ %.2f，跳过划转。", margin_usdt, half)
        else:
            logger.info("[2/5] 划转 %.2f USDT 到逐仓杠杆 [%s]（50%%抵押 + 另50%%在合约做多）",
                        transfer_amt, base)
            try:
                await self.bot._binance_isolated_margin_transfer(
                    "USDT", transfer_amt, margin_symbol, "spot_to_margin",
                )
            except Exception as exc:
                logger.error("[2/5] 划转抵押失败: %s", exc)
                return False

        # [3/5] 计算数量 + 借币（反向不限进场窗口）
        price = self.bot._select_reference_price(row)
        if not price or price <= 0:
            logger.error("[3/5] 参考价格无效，划回 USDT 并放弃开仓。")
            await self.bot._drain_margin_to_spot(margin_symbol)
            return False

        amount = self.bot._calculate_precise_amount(
            spot_symbol=spot_symbol, futures_symbol=futures_symbol,
            reference_price=price, spot_source="margin", total_usdt=position_usdt,
        )
        if amount <= 0:
            logger.error("[3/5] 精度处理后的下单数量无效，划回 USDT 并放弃开仓。")
            await self.bot._drain_margin_to_spot(margin_symbol)
            return False

        logger.info("[3/5] 价格=%.6f 数量=%s 名义=%.2f USDT", price, amount, amount * price)

        # 检查是否已预借 (pre-borrow)
        base_asset = margin_acct.get("baseAsset", {}) if margin_acct else {}
        already_borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0
        if already_borrowed >= amount:
            logger.info("[3/5] 已预借 %s x%s (borrowed=%.4f)，跳过借币。", base, amount, already_borrowed)
        else:
            need_borrow = amount - already_borrowed if already_borrowed > 0 else amount
            can_borrow, max_borrowable = await self.bot._binance_margin_max_borrowable(base, margin_symbol)
            if not can_borrow or max_borrowable < need_borrow:
                logger.error("[3/5] 无法借够 %s: need=%s max=%s，划回 USDT 并放弃开仓。",
                             base, need_borrow, max_borrowable)
                await self.bot._drain_margin_to_spot(margin_symbol)
                return False

            try:
                await self.bot._binance_margin_loan(base, need_borrow, margin_symbol)
            except Exception as exc:
                logger.error("[3/5] 借币 %s API 失败: %s，划回 USDT 并放弃开仓。", base, exc)
                await self.bot._drain_margin_to_spot(margin_symbol)
                return False

        # [4/5] 并发下单 — margin 卖出 + 合约开多
        logger.info(
            "[4/5] 下单: sell %s %s [margin] + buy %s %s [long] | 费率=%.4f%%",
            amount, base, amount, futures_symbol,
            float(row["predicted_funding_rate"]) * 100,
        )
        margin_task = self.bot._open_margin_spot_leg(spot_symbol, amount)
        futures_task = self.bot._open_futures_long_leg(futures_symbol, amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        # [5/5] 处理结果
        if margin_result.ok and futures_result.ok:
            next_ft = await self.bot._fetch_next_funding_time(futures_symbol)
            next_ft_dt = datetime.fromtimestamp(next_ft / 1000, tz=self.bot.tz) if next_ft > 0 else None
            if next_ft_dt:
                logger.info("合约 %s 下次结算: %s (距今 %.0f 分钟)",
                            futures_symbol, next_ft_dt.strftime("%Y-%m-%d %H:%M:%S"),
                            (next_ft - time.time() * 1000) / 60000)

            self.bot.binance_state = ArbitrageState(
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
                opened_at=datetime.now(tz=self.bot.tz).isoformat(),
                spot_source="margin",
                next_funding_time_ms=next_ft,
                direction="reverse",
            )
            self.bot._save_state()
            self.bot._record_margin_used(margin_symbol)
            self.bot._print_position_summary()
            await asyncio.to_thread(
                self.bot._send_email,
                f"bazfbot 反向开仓: {base}",
                f"币种: {base} [margin]\n数量: {amount}\n负费率: {float(row['predicted_funding_rate'])*100:.3f}%\n"
                f"净收益: {float(row['net_rate'])*100:.3f}%\n"
                f"结算: {datetime.fromtimestamp(next_ft/1000, tz=self.bot.tz).strftime('%H:%M')}",
            )
            return True

        # 应急撤销：仅当至少一腿成交时才需要
        if margin_result.ok or futures_result.ok:
            logger.critical("[!!] 反向开仓双腿未同时成交！margin=%s futures=%s", margin_result, futures_result)
            await self.bot._emergency_close_exposed_leg(
                margin_result, futures_result,
                direction="reverse", margin_symbol=margin_symbol, base=base,
            )
            await self.bot._cleanup_margin_pair(base, margin_symbol, position_usdt)
        else:
            logger.warning("反向双腿均未成交: margin=%s futures=%s — 无需应急。", margin_result, futures_result)
            # USDT 留在逐仓杠杆，不划回，下轮可直接重试
        return False

    async def close_position(self) -> bool:
        """平仓反向套利: margin买回 + 合约平多 + 还款 + 划回 USDT。"""
        if not self.bot.binance_state.is_open or self.bot.binance_state.direction != "reverse":
            return True

        logger.info(
            "[平仓 1/4] 反向平仓: margin=%s futures=%s amount=%s",
            self.bot.binance_state.spot_symbol, self.bot.binance_state.futures_symbol, self.bot.binance_state.amount,
        )

        # [1] 并发 — margin买回 + 合约平多
        margin_task = self.bot._close_margin_spot_leg(self.bot.binance_state.spot_symbol, self.bot.binance_state.amount)
        futures_task = self.bot._close_futures_long_leg(self.bot.binance_state.futures_symbol, self.bot.binance_state.amount)
        margin_result, futures_result = await asyncio.gather(margin_task, futures_task)

        # 重试失败的腿（最多3次）
        for attempt in range(3):
            if margin_result.ok and futures_result.ok:
                break
            if not margin_result.ok:
                logger.warning("Margin 买回失败，重试 %d/3", attempt + 1)
                margin_result = await self.bot._close_margin_spot_leg(
                    self.bot.binance_state.spot_symbol, self.bot.binance_state.amount,
                )
            if not futures_result.ok:
                logger.warning("合约平多失败，重试 %d/3", attempt + 1)
                futures_result = await self.bot._close_futures_long_leg(
                    self.bot.binance_state.futures_symbol, self.bot.binance_state.amount,
                )
            await asyncio.sleep(1.0)

        if not margin_result.ok or not futures_result.ok:
            logger.critical(
                "[平仓 1/4] 下单异常（重试3次），尝试恢复对冲。margin=%s futures=%s",
                margin_result, futures_result,
            )
            if margin_result.ok and not futures_result.ok:
                logger.critical("合约平多失败，卖出 margin 现货恢复对冲")
                await self.bot._open_margin_spot_leg(self.bot.binance_state.spot_symbol, self.bot.binance_state.amount)
            elif futures_result.ok and not margin_result.ok:
                logger.critical("Margin 买回失败，重开多单恢复对冲")
                await self.bot._open_futures_long_leg(self.bot.binance_state.futures_symbol, self.bot.binance_state.amount)
            await asyncio.to_thread(
                self.bot._send_email,
                "bazfbot 平仓失败！",
                "反向平仓一条腿失败（重试3次），已尝试恢复对冲。请立即检查持仓！",
            )
            return False

        # [2] 查询负债 + 还款
        base = self.bot.binance_state.base
        amount = self.bot.binance_state.amount
        margin_symbol = self.bot.binance_state.spot_symbol
        logger.info("[平仓 2/4] 查询逐仓负债...")
        margin_acct = await self.bot._get_isolated_margin_account(margin_symbol)
        base_asset = margin_acct.get("baseAsset", {})
        borrowed = float(base_asset.get("borrowed", 0)) if base_asset else 0.0
        repay_amount = max(borrowed, amount)
        logger.info("[平仓 2/4] 还款 %s %s (借入=%s 负债=%s)", repay_amount, base, amount, borrowed)
        try:
            await self.bot._binance_margin_repay(base, repay_amount, margin_symbol)
        except Exception as exc:
            logger.error("还款失败需手动处理: %s", exc)
            # 尝试用买入的全部余额还款
            try:
                net_asset = float(base_asset.get("netAsset", amount)) if base_asset else amount
                await self.bot._binance_margin_repay(base, net_asset, margin_symbol)
            except Exception as exc2:
                logger.error("二次还款也失败: %s", exc2)

        # [3] 划回所有 USDT（逐仓杠杆 → spot）
        margin_acct = await self.bot._get_isolated_margin_account(margin_symbol)
        quote_asset = margin_acct.get("quoteAsset", {})
        quote_net = float(quote_asset.get("netAsset", 0)) if quote_asset else 0.0
        transfer_out = self.bot._floor_usdt(quote_net)
        if transfer_out > 0:
            logger.info("[平仓 3/4] 划回 %.2f USDT: margin → spot", transfer_out)
            try:
                await self.bot._binance_isolated_margin_transfer(
                    "USDT", transfer_out, margin_symbol, "margin_to_spot",
                )
            except Exception:
                pass

        logger.info("[平仓 4/4] 反向套利平仓完成: 平多+买回+还款。")
        self.bot._record_close_trade(self.bot.binance_state)
        self.bot.binance_state = ArbitrageState()
        self.bot._save_state()
        await asyncio.to_thread(
            self.bot._send_email, "bazfbot 反向平仓", "双腿已平，借款已归还，仓位已清空。"
        )
        return True

    async def execute_pre_borrow(self, row: pd.Series) -> bool:
        """预借: 划转 USDT 到逐仓 + 借币，但不卖出。等费率到位后秒开。"""
        base = str(row["base"])
        if self.bot._borrow_blacklist.get(base, 0) > time.time():
            logger.info("[预借] %s 在黑名单中，跳过。", base)
            return False
        margin_symbol = str(row["spot_symbol"])
        t_start = time.perf_counter()
        price = self.bot._select_reference_price(row)
        if not price or price <= 0:
            logger.warning("[预借] %s: 参考价格无效", base)
            return False

        # 计算 50/50 分配（ccxt + 直连 REST + 逐仓杠杆 三查）
        spot_bal, futures_bal, spot_rest, futures_rest = await asyncio.gather(
            self.bot._safe_request("spot.fetch_balance", lambda: self.bot.spot.fetch_balance(), default={}),
            self.bot._safe_request("futures.fetch_balance", lambda: self.bot.futures.fetch_balance(), default={}),
            self.bot._safe_binance_balance(BINANCE_SPOT_API, "/api/v3/account"),
            self.bot._safe_binance_balance(BINANCE_FUTURES_API, "/fapi/v2/balance"),
        )
        spot_usdt_ccxt = self.bot._free_balance(spot_bal, "USDT")
        futures_usdt_ccxt = self.bot._free_balance(futures_bal, "USDT")
        spot_usdt = max(spot_usdt_ccxt, spot_rest)
        futures_usdt = max(futures_usdt_ccxt, futures_rest)

        # 也查逐仓杠杆 USDT（上次失败的回收可能留在了逐仓）
        margin_usdt = 0.0
        try:
            resp = await self.bot._binance_request(
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
                    await self.bot._drain_margin_to_spot(sym)
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
        if not await self.bot._ensure_margin_pair_enabled(margin_symbol):
            logger.warning("[预借] %s: 无法启用逐仓交易对", base)
            return False

        need = self.bot._floor_usdt(min(half, spot_usdt))
        if need >= 1.0:
            logger.info("[预借 1/3] 划转 %.2f USDT: 现货 → 逐仓 [%s]", need, base)
            try:
                await self.bot._binance_isolated_margin_transfer(
                    "USDT", need, margin_symbol, "spot_to_margin",
                )
            except Exception as exc:
                logger.error("[预借] %s: 划转失败 %s", base, exc)
                return False
        else:
            logger.info("[预借 1/3] 逐仓已有足够 USDT，跳过划转。")

        # 借币
        amount = self.bot._calculate_precise_amount(
            spot_symbol=margin_symbol, futures_symbol=str(row["futures_symbol"]),
            reference_price=price, spot_source="margin", total_usdt=position_usdt,
        )
        if amount <= 0:
            logger.warning("[预借] %s: 数量计算失败", base)
            return False

        nominal = amount * price
        can_borrow, max_borrowable = await self.bot._binance_margin_max_borrowable(base, margin_symbol)
        if not can_borrow or max_borrowable < amount:
            logger.warning("[预借] %s: 借币池不足 need=%s max=%s 池剩余=%.0f%%",
                         base, amount, max_borrowable,
                         max_borrowable / amount * 100 if amount > 0 else 0)
            return False

        logger.info("[预借 2/3] 借币 %s x%s | 价格=%.6f 名义=%.2f USDT | 池剩余=%.0f%%",
                    base, amount, price, nominal,
                    (1 - amount / max_borrowable) * 100 if max_borrowable > 0 else 0)
        try:
            await self.bot._binance_margin_loan(base, amount, margin_symbol)
        except Exception as exc:
            logger.error("[预借] %s: 借币失败 %s", base, exc)
            return False

        # 记录状态
        self.bot.binance_state.pre_borrow_base = base
        self.bot.binance_state.pre_borrow_margin_symbol = margin_symbol
        self.bot.binance_state.pre_borrow_amount = amount
        self.bot.binance_state.pre_borrow_at = datetime.now(tz=self.bot.tz).isoformat()
        self.bot._save_state()
        elapsed = (time.perf_counter() - t_start) * 1000
        target = (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100
        logger.info(
            "[预借 3/3] 完成! %s x%s | 费率=%+.4f%% 目标=%.3f%% | "
            "耗时 %.0fms | 等待费率到位秒开",
            base, amount, float(row["predicted_funding_rate"]) * 100,
            target, elapsed,
        )
        return True

    async def cancel_pre_borrow(self) -> None:
        """归还预借的币 + 划回 USDT。"""
        base = self.bot.binance_state.pre_borrow_base
        margin_symbol = self.bot.binance_state.pre_borrow_margin_symbol
        amount = self.bot.binance_state.pre_borrow_amount
        elapsed = (datetime.now(tz=self.bot.tz) - datetime.fromisoformat(
            self.bot.binance_state.pre_borrow_at)).total_seconds() / 60 if self.bot.binance_state.pre_borrow_at else 0
        logger.info("[预借] 取消 | %s x%s | 已等待 %.0f min | 归还 + 划回 USDT", base, amount, elapsed)
        try:
            await self.bot._binance_margin_repay(base, amount, margin_symbol)
        except Exception as exc:
            logger.error("[预借] 归还失败 %s: %s", base, exc)
        await self.bot._drain_margin_to_spot(margin_symbol)
        self.bot.binance_state.pre_borrow_base = ""
        self.bot.binance_state.pre_borrow_margin_symbol = ""
        self.bot.binance_state.pre_borrow_amount = 0.0
        self.bot.binance_state.pre_borrow_at = ""
        self.bot._save_state()

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

    async def check_position(self) -> bool:
        """验证 Binance 反向持仓是否仍然存在，不一致则重置状态。"""
        if not self.bot.binance_state.is_open:
            return False
        if self.bot.binance_state.direction != "reverse":
            return False

        futures_ok = await self.bot._has_futures_long_position()
        if futures_ok:
            return True
        logger.warning("状态文件显示有反向持仓，但交易所未发现合约多仓，重置状态。")
        self.bot.binance_state = ArbitrageState()
        self.bot._save_state()
        return False

