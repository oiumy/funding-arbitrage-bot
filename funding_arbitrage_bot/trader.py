"""Mixin: TraderMixin — 共享 leg/余额/转账/下单."""
from __future__ import annotations
from typing import Any
from .constants import *
from .models import LegResult

class TraderMixin:

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
        return 0



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
            logger.info("Alpha 现货买入: symbol=%s amount=%s", symbol, amount)
            try:
                order = await self._binance_spot_order(symbol, "buy", amount)
                return LegResult(True, "alpha_spot", symbol, "buy", amount, order=order)
            except Exception as exc:
                logger.error("Alpha 现货买入失败 %s: %s", symbol, exc)
                return LegResult(False, "alpha_spot", symbol, "buy", amount, error=str(exc))
        if spot_source == "margin":
            return await self._open_margin_spot_leg(symbol, amount)
        try:
            order = await self._binance_spot_order(symbol, "buy", amount)
            return LegResult(True, "spot", symbol, "buy", amount, order=order)
        except Exception as exc:
            logger.error("现货买入失败 %s: %s", symbol, exc)
            return LegResult(False, "spot", symbol, "buy", amount, error=str(exc))



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
            logger.info("Alpha 现货卖出: symbol=%s amount=%s", symbol, amount)
            precise = math.floor(amount * 100) / 100
            try:
                order = await self._binance_spot_order(symbol, "sell", precise)
                return LegResult(True, "alpha_spot", symbol, "sell", precise, order=order)
            except Exception as exc:
                logger.error("Alpha 现货卖出失败 %s: %s", symbol, exc)
                return LegResult(False, "alpha_spot", symbol, "sell", precise, error=str(exc))
        if spot_source == "margin":
            return await self._close_margin_spot_leg(symbol, amount)
        precise = float(self.spot.amount_to_precision(symbol, amount))
        try:
            order = await self._binance_spot_order(symbol, "sell", precise)
            return LegResult(True, "spot", symbol, "sell", precise, order=order)
        except Exception as exc:
            logger.error("现货卖出失败 %s: %s", symbol, exc)
            return LegResult(False, "spot", symbol, "sell", precise, error=str(exc))



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
                # 轮询等 WS 持仓缓存刷新（ACCOUNT_UPDATE 在成交回报后 ~2ms 到），
                # 确认即走，免固定睡 50ms；worst-case 仍 ~50ms 后走 REST 兜底
                for _ in range(50):
                    await asyncio.sleep(0.001)
                    ok, pos = self._ws_position_check("binance", symbol, "SHORT", expect_zero=False, amount=amount)
                    if ok:
                        break
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
            try:
                order = await self._gate_trade_ws_order(symbol, "sell", amount)
                ok = order.get("status") == "closed"
                if ok:
                    logger.info("[跨交易所] Gate开空成功 %s: filled=%.4f", symbol, order.get("filled", 0))
                else:
                    logger.error("[跨交易所] Gate开空未成交 %s: status=%s", symbol, order.get("status"))
                return LegResult(ok, "futures", symbol, "sell", order.get("filled", amount),
                                 order=order, error=None if ok else "Gate short not filled")
            except Exception as exc:
                logger.warning("Gate开空WS异常 %s: %s，以持仓验证为准", symbol, exc)
                ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
                if not ok:
                    await asyncio.sleep(0.05)
                    ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
                if not ok:
                    pos = await self._cross_verify_gate_position(symbol)
                    ok = pos > 0
                if ok:
                    logger.warning("Gate开空持仓验证通过 %s: filled=%.4f (API异常但持仓存在)", symbol, pos)
                    return LegResult(True, "futures", symbol, "sell", pos,
                                     order={"id": "verified", "symbol": symbol, "side": "sell",
                                            "amount": pos, "filled": pos, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("Gate开空确认失败 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "sell", amount, error=str(exc))



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
                # 轮询等 WS 持仓缓存刷新（ACCOUNT_UPDATE 在成交回报后 ~2ms 到），
                # 确认即走，免固定睡 50ms；worst-case 仍 ~50ms 后走 REST 兜底
                for _ in range(50):
                    await asyncio.sleep(0.001)
                    ok, pos = self._ws_position_check("binance", symbol, "LONG", expect_zero=False, amount=amount)
                    if ok:
                        break
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
            try:
                order = await self._gate_trade_ws_order(symbol, "buy", amount)
                ok = order.get("status") == "closed"
                if ok:
                    logger.info("[跨交易所] Gate开多成功 %s: filled=%.4f", symbol, order.get("filled", 0))
                else:
                    logger.error("[跨交易所] Gate开多未成交 %s: status=%s", symbol, order.get("status"))
                return LegResult(ok, "futures", symbol, "buy", order.get("filled", amount),
                                 order=order, error=None if ok else "Gate long not filled")
            except Exception as exc:
                logger.warning("Gate开多WS异常 %s: %s，以持仓验证为准", symbol, exc)
                ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
                if not ok:
                    await asyncio.sleep(0.05)
                    ok, pos = self._ws_position_check("gate", symbol, "", expect_zero=False, amount=amount)
                if not ok:
                    pos = await self._cross_verify_gate_position(symbol)
                    ok = pos > 0
                if ok:
                    logger.warning("Gate开多持仓验证通过 %s: filled=%.4f (API异常但持仓存在)", symbol, pos)
                    return LegResult(True, "futures", symbol, "buy", pos,
                                     order={"id": "verified", "symbol": symbol, "side": "buy",
                                            "amount": pos, "filled": pos, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("Gate开多确认失败 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))



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
            try:
                order = await self._gate_trade_ws_order(symbol, "buy", amount, reduce_only=True)
                ok = order.get("status") == "closed"
                return LegResult(ok, "futures", symbol, "buy", order.get("filled", amount),
                                 order=order, error=None if ok else "Gate close short not filled")
            except Exception as exc:
                logger.warning("Gate平空WS异常 %s: %s，以持仓验证为准", symbol, exc)
                ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
                if not ok:
                    await asyncio.sleep(0.05)
                    ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
                if not ok:
                    remaining = await self._cross_verify_gate_position(symbol)
                    ok = remaining < amount * 0.1
                if ok:
                    logger.warning("Gate平空持仓验证通过 %s: 仓位已消失 (API异常但持仓已平)", symbol)
                    return LegResult(True, "futures", symbol, "buy", amount,
                                     order={"id": "verified_close", "symbol": symbol, "side": "buy",
                                            "amount": amount, "filled": amount, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("Gate平空确认失败 %s: %s", symbol, exc)
                return LegResult(False, "futures", symbol, "buy", amount, error=str(exc))



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
            try:
                order = await self._gate_trade_ws_order(symbol, "sell", amount, reduce_only=True)
                ok = order.get("status") == "closed"
                return LegResult(ok, "futures", symbol, "sell", order.get("filled", amount),
                                 order=order, error=None if ok else "Gate close long not filled")
            except Exception as exc:
                logger.warning("Gate平多WS异常 %s: %s，以持仓验证为准", symbol, exc)
                ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
                if not ok:
                    await asyncio.sleep(0.05)
                    ok, remaining = self._ws_position_check("gate", symbol, "", expect_zero=True, amount=amount)
                if not ok:
                    remaining = await self._cross_verify_gate_position(symbol)
                    ok = remaining < amount * 0.1
                if ok:
                    logger.warning("Gate平多持仓验证通过 %s: 仓位已消失 (API异常但持仓已平)", symbol)
                    return LegResult(True, "futures", symbol, "sell", amount,
                                     order={"id": "verified_close", "symbol": symbol, "side": "sell",
                                            "amount": amount, "filled": amount, "status": "closed",
                                            "info": {"verified_after_error": str(exc)}})
                logger.error("Gate平多确认失败 %s: %s", symbol, exc)
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



    async def _cross_notional_step(self, exchange: str, symbol: str,
                                    reference_price: float) -> float:
        """单笔最小名义金额步长（USDT）。值越大的那侧精度越粗，应优先计算。"""
        ex = self.futures if exchange == "binance" else self.gate_futures
        try:
            market = ex.market(symbol)
            contract_size = float(market.get("contractSize", 1) or 1)
            min_qty = float(market.get("limits", {}).get("amount", {}).get("min", 0) or 0)
            if min_qty <= 0:
                min_qty = 0.01 if exchange == "binance" else 1.0
            return contract_size * min_qty * reference_price
        except Exception:
            return reference_price



    async def _cross_actual_notional(self, exchange: str, symbol: str,
                                      qty: float, reference_price: float) -> float:
        """从下单数量反算实际 USDT 名义金额（考虑 contractSize）。"""
        ex = self.futures if exchange == "binance" else self.gate_futures
        try:
            market = ex.market(symbol)
            contract_size = float(market.get("contractSize", 1) or 1)
            return qty * contract_size * reference_price
        except Exception:
            return qty * reference_price



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
                                       close_info: dict | None = None) -> tuple[float, float]:
        """查询实际成交手续费 + 成交均价。返回 (fee, avg_price)。
        - Binance: GET /fapi/v1/userTrades?orderId=... 累加 commission + 加权均价
        - Gate: 优先用 close_info，否则 GET /futures/usdt/orders/{id}
        """
        if not order_id or order_id in ("", "0", "verified", "verified_close"):
            return 0.0, 0.0
        if exchange == "gate":
            fee = 0.0
            avg_price = 0.0
            if close_info:
                fee = float(close_info.get("fee", 0) or 0)
                avg_price = float(close_info.get("fill_price", 0) or 0)
                if fee > 0 and avg_price > 0:
                    return fee, avg_price
            try:
                resp = await self._gate_request(f"/futures/usdt/orders/{order_id}")
                fee = float(resp.get("fee", 0) or 0)
                avg_price = float(resp.get("fill_price", 0) or 0)
                return fee, avg_price
            except Exception:
                return fee, avg_price
        # Binance
        try:
            clean = self._clean_futures_symbol(symbol)
            trades = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/userTrades",
                {"symbol": clean, "orderId": int(order_id)},
            )
            total_fee = 0.0
            total_qty = 0.0
            total_quote = 0.0
            for t in (trades if isinstance(trades, list) else []):
                total_fee += abs(float(t.get("commission", 0) or 0))
                qty = abs(float(t.get("qty", 0) or 0))
                price = float(t.get("price", 0) or 0)
                total_qty += qty
                total_quote += qty * price
            avg_price = total_quote / total_qty if total_qty > 0 else 0.0
            return total_fee, avg_price
        except Exception:
            return 0.0, 0.0


