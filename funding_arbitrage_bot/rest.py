"""Mixin: ExchangeRestMixin — Binance + Gate 直连 REST API."""
from __future__ import annotations
from typing import Any
from .constants import *

class ExchangeRestMixin:

    # ── 核心 REST 签名/请求原语 ────────────────────────────

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



    def _within_entry_window(self, next_funding_time_ms: float) -> bool:
        """开仓时间窗口: 距 settlement 不足 ENTRY_WINDOW_MINUTES 分钟才允许入场。"""
        if next_funding_time_ms <= 0:
            return True  # 获取失败不阻止，允许入场
        now_ms = time.time() * 1000
        remaining_ms = next_funding_time_ms - now_ms
        return 0 < remaining_ms <= ENTRY_WINDOW_MINUTES * 60_000

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
        logger.debug("Gate 统一账户: USDT可用=%.2f 总计=%.2f",
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
        # WS 下单响应可能返回 "NEW"（市价单已受理但 WS 尚未回报 fill）
        # 此时 executedQty 通常为 0，但持仓实际已建立
        if filled <= 0 and status in ("FILLED", "NEW"):
            filled = amount
        ok = status in ("FILLED", "NEW") and filled >= amount * 0.9
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
                logger.debug("Binance 现货费率: 基础=%.3f%% → BNB折扣后=%.3f%%",
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
                logger.debug("Binance 合约费率: VIP基础=%.3f%% → BNB折扣后=%.3f%%",
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


