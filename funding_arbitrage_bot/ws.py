"""WebSocketMixin — Binance & Gate WebSocket 长连接管理 + WS 下单."""
from __future__ import annotations
import asyncio
import json
import socket
import time
from typing import Any

from .constants import *


class WebSocketMixin:
    """Binance & Gate WebSocket: 费率监听、交易 WS、WS 下单、持仓缓存。"""

    # ── from bot.py lines 359-557 ──
    async def _fetch_premium_index_all(self) -> list[dict[str, Any]]:
        """查询全部 USDT 永续合约溢价指数 (含资金费率). 公开接口无需签名.
        返回 [{"symbol": "BTCUSDT", "markPrice": "...", "lastFundingRate": "...", ...}, ...]"""
        url = f"{BINANCE_FUTURES_API}/fapi/v1/premiumIndex"
        def _do():
            proxies = {"http": HTTP_PROXY, "https": HTTP_PROXY} if HTTP_PROXY else None
            resp = self._rest_session.get(url, proxies=proxies, timeout=15)
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
                await self._bn_reverse.cancel_pre_borrow()
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
                await self._bn_reverse.cancel_pre_borrow()
                return
            current_rate = float(pb_rows.iloc[0]["rate"]) * 100
            elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                self.binance_state.pre_borrow_at)).total_seconds() / 60
            target = (REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER) * 100
            if elapsed > PRE_BORROW_TIMEOUT_MINUTES:
                await self._bn_reverse.cancel_pre_borrow()
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
            ok = await self._bn_reverse.execute_pre_borrow(minimal_row)
            if not ok:
                logger.info("[异动] 预借失败，120s 冷却。")




    # ── from bot.py lines 839-854 ──
    def _ws_position_check(self, exchange: str, symbol: str, position_side: str,
                           expect_zero: bool, amount: float) -> tuple[bool, float]:
        """WS 缓存快速查持仓（免 REST）。返回 (confirmed, position_size)。
        cache miss 返回 (False, -1) 表示需等待或 REST 兜底。"""
        if exchange == "binance":
            clean = self._clean_futures_symbol(symbol)
            pos = self._bn_ws_positions.get(f"{clean}|{position_side}")
        else:
            pos = self._gate_ws_positions.get(self._to_gate_contract(symbol))
        if pos is None:
            return False, -1.0
        if expect_zero:
            return pos < amount * 0.1, pos
        return pos > 0, pos



    # ── from bot.py lines 913-1466 ──
    async def _ensure_bn_funding_ws(self) -> None:
        """确保 BN 用户数据流 WS 长连接存活（断线自动重连）。
        启动时调用一次，之后持续监听 FUNDING_FEE 事件。"""
        ws = getattr(self, "_funding_ws", None)
        if ws and not ws.closed:
            # 每 45min 刷新 listenKey（60min 有效期）
            if time.time() - getattr(self, "_bn_listen_key_ts", 0) < 2700:
                return
            logger.info("[资费监听] listenKey 即将过期，刷新")
            await self._close_bn_funding_ws()
        try:
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="POST",
            )
            self._bn_listen_key = resp.get("listenKey", "")
            self._bn_listen_key_ts = time.time()
            if not self._bn_listen_key:
                logger.warning("[资费监听] listenKey 创建失败")
                return
            if not self._funding_session:
                class _Resolver:
                    async def resolve(self, host, port=0, family=0):
                        if host == "fstream.binance.com":
                            return [{"hostname": host, "host": "52.69.16.71", "port": port,
                                     "family": socket.AF_INET, "proto": 6, "flags": socket.AI_NUMERICHOST}]
                        return [{"hostname": host, "host": host, "port": port,
                                 "family": socket.AF_INET, "proto": 6, "flags": 0}]
                    async def close(self): pass
                connector = aiohttp.TCPConnector(resolver=_Resolver(), ssl=False)
                self._funding_session = aiohttp.ClientSession(connector=connector)
            ws_url = f"wss://fstream.binance.com/ws/{self._bn_listen_key}"
            self._funding_ws = await self._funding_session.ws_connect(ws_url)
            logger.info("[资费监听] BN WS 长连接已建立")
            asyncio.create_task(self._read_bn_funding_stream())
        except Exception as exc:
            logger.warning("[资费监听] BN WS 建立失败: %s", exc)



    async def _close_bn_funding_ws(self) -> None:
        """关闭 BN WS 长连接。"""
        try:
            ws = getattr(self, "_funding_ws", None)
            if ws:
                await ws.close()
                self._funding_ws = None
        except Exception:
            pass
        try:
            if self._bn_listen_key:
                await self._binance_request(
                    BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="DELETE",
                )
                self._bn_listen_key = None
        except Exception:
            pass



    async def _read_bn_funding_stream(self) -> None:
        """持久读 BN 用户数据流，检测 FUNDING_FEE 设 event，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_funding_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        if data.get("e") == "ACCOUNT_UPDATE":
                            # 缓存所有持仓到 WS cache（开平仓验证免 REST）
                            for p in (data.get("a", {}).get("P", []) or []):
                                sym = p.get("s", "")
                                ps = p.get("ps", "")
                                if sym and ps:
                                    self._bn_ws_positions[f"{sym}|{ps}"] = abs(float(p.get("pa", 0) or 0))
                            if data.get("a", {}).get("m") == "FUNDING_FEE":
                                logger.info("[资费监听] BN 资费已到账 @ %s",
                                           datetime.now(self.tz).strftime("%H:%M:%S.%f")[:-3])
                                self._funding_event.set()
                        elif data.get("e") == "listenKeyExpired":
                            logger.warning("[资费监听] listenKey 过期，重连")
                            break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[资费监听] BN WS 读取异常: %s", exc)
            # 断线重连
            self._funding_ws = None
            await asyncio.sleep(3)
            try:
                await self._ensure_bn_funding_ws()
                break  # _ensure 会创建新的 reader task
            except Exception:
                logger.warning("[资费监听] BN WS 重连失败，3s后重试")
                await asyncio.sleep(3)



    async def _gate_subscribe_position(self, contract: str) -> bool:
        ws = getattr(self, "_gate_trade_ws", None)
        if not ws or ws.closed:
            logger.warning("Gate subscribe fail %s: trade WS not connected", contract)
            return False
        try:
            t = int(time.time())
            # 订阅持仓变动（用于开平仓验证）
            ch_pos = "futures.positions"
            sign_pos = hmac.new(GATE_API_SECRET.encode(),
                                f"channel={ch_pos}&event=subscribe&time={t}".encode(),
                                hashlib.sha512).hexdigest()
            await ws.send_json({
                "time": t, "channel": ch_pos, "event": "subscribe",
                "payload": [contract],
                "auth": {"method": "api_key", "KEY": GATE_API_KEY, "SIGN": sign_pos},
            })
            # 订阅资费收付通知（专门用于资费结算检测，不依赖 pnl_fund 变动）
            ch_fund = "futures.funding_payments"
            sign_fund = hmac.new(GATE_API_SECRET.encode(),
                                 f"channel={ch_fund}&event=subscribe&time={t}".encode(),
                                 hashlib.sha512).hexdigest()
            await ws.send_json({
                "time": t, "channel": ch_fund, "event": "subscribe",
                "payload": [contract],
                "auth": {"method": "api_key", "KEY": GATE_API_KEY, "SIGN": sign_fund},
            })
            logger.info("Gate subscribed %s on trade WS (positions + funding_payments)", contract)
            return True
        except Exception as exc:
            logger.warning("Gate subscribe %s error: %s", contract, exc)
            return False


    # ════════════════════════════════════════════════════════════════
    # BN + Gate 交易 WS（持久长连接，下单省 HTTP 握手 + to_thread）
    # ════════════════════════════════════════════════════════════════



    async def _ensure_bn_trade_ws(self) -> None:
        """确保 BN 交易 WS 长连接存活（wss://ws-fapi.binance.com/ws-fapi/v1），断线自动重连。"""
        ws = getattr(self, "_bn_trade_ws", None)
        if ws and not ws.closed:
            return
        await self._close_bn_trade_ws()
        try:
            if not self._bn_trade_session:
                self._bn_trade_session = aiohttp.ClientSession()
            self._bn_trade_ws = await self._bn_trade_session.ws_connect(
                "wss://ws-fapi.binance.com/ws-fapi/v1"
            )
            logger.info("[交易WS] BN 交易 WS 长连接已建立")
            asyncio.create_task(self._read_bn_trade_ws())
        except Exception as exc:
            logger.warning("[交易WS] BN 交易 WS 建立失败: %s", exc)



    async def _close_bn_trade_ws(self) -> None:
        """关闭 BN 交易 WS，取消所有等待中的订单。"""
        try:
            ws = getattr(self, "_bn_trade_ws", None)
            if ws:
                await ws.close()
                self._bn_trade_ws = None
        except Exception:
            pass
        for fut in getattr(self, "_bn_trade_futures", {}).values():
            if not fut.done():
                fut.set_exception(Exception("BN trade WS closed"))



    async def _read_bn_trade_ws(self) -> None:
        """持久读 BN 交易 WS 响应，按 id 分发到对应 Future，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_bn_trade_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        rid = data.get("id", "")
                        if rid in getattr(self, "_bn_trade_futures", {}):
                            fut = self._bn_trade_futures.pop(rid)
                            if not fut.done():
                                if data.get("status") == 200:
                                    fut.set_result(data.get("result", {}))
                                else:
                                    err = data.get("error", {})
                                    fut.set_exception(
                                        Exception(f"BN WS order failed: {err}"))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[交易WS] BN 交易 WS 读取异常: %s", exc)
            # 断线重连
            self._bn_trade_ws = None
            for fut in getattr(self, "_bn_trade_futures", {}).values():
                if not fut.done():
                    fut.set_exception(Exception("BN trade WS disconnected"))
            self._bn_trade_futures.clear()
            await asyncio.sleep(3)
            try:
                await self._ensure_bn_trade_ws()
                break
            except Exception:
                await asyncio.sleep(3)



    async def _bn_trade_ws_order(self, symbol: str, side: str, quantity: float,
                                  reduce_only: bool = False,
                                  position_side: str | None = None) -> dict[str, Any]:
        """BN 合约下单 — 走持久 WS 长连接，省 HTTP 握手 + headers 开销。"""
        import uuid as _uuid
        await self._ensure_bn_trade_ws()
        clean = self._clean_futures_symbol(symbol)
        ts = int(time.time() * 1000)
        params: dict[str, Any] = {
            "apiKey": BINANCE_API_KEY,
            "symbol": clean,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": ts,
            "recvWindow": 5000,
        }
        if position_side:
            params["positionSide"] = position_side.upper()
        elif reduce_only:
            params["reduceOnly"] = "true"
        # 签名（标准 Binance HMAC-SHA256，按 key 排序后 URL-encode）
        sorted_items = sorted((str(k), str(v)) for k, v in params.items())
        qs = "&".join(f"{k}={v}" for k, v in sorted_items)
        params["signature"] = self._binance_sign(qs)

        rid = str(_uuid.uuid4())[:8]
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._bn_trade_futures[rid] = fut
        try:
            t0 = time.perf_counter()
            t0_ms = int(time.time() * 1000)
            await self._bn_trade_ws.send_json(
                {"id": rid, "method": "order.place", "params": params})
            result = await asyncio.wait_for(fut, timeout=15.0)
            t1 = time.perf_counter()
            raw = result.get("result", {})
            transact_time = raw.get("transactTime", 0) or raw.get("updateTime", 0)
            fill_ms = transact_time - t0_ms if transact_time > 0 else 0
            logger.info("[延迟] BN WS 下单 %s %s: 往返 %.0fms | 成交 %dms",
                        symbol, side, (t1 - t0) * 1000, fill_ms)
            # 传内层 result（含 executedQty / status:FILLED），非外层信封（status:200）
            order = self._normalize_order_response(raw, symbol, side, quantity)
            if order["status"] == "open":
                raise ExchangeError(
                    f"合约{side}未成交: {symbol} filled=0/{quantity}")
            if order["status"] == "partial":
                logger.warning("合约%s部分成交: filled=%.4f/%.4f (%.0f%%)",
                               symbol, order["filled"], quantity,
                               order["filled"] / quantity * 100 if quantity > 0 else 0)
            return order
        except asyncio.TimeoutError:
            self._bn_trade_futures.pop(rid, None)
            raise Exception(f"BN WS order timeout: {symbol} {side}")
        except Exception:
            self._bn_trade_futures.pop(rid, None)
            raise

    # ── Gate 交易 WS（futures.order_place 频道直连下单） ──



    async def _ensure_gate_trade_ws(self) -> None:
        """确保 Gate 交易 WS 长连接存活（futures.order_place），断线自动重连，先 login 再就绪。"""
        ws = getattr(self, "_gate_trade_ws", None)
        if ws and not ws.closed:
            return
        await self._close_gate_trade_ws()
        try:
            if not self._gate_trade_session:
                self._gate_trade_session = aiohttp.ClientSession()
            self._gate_trade_ws = await self._gate_trade_session.ws_connect(
                "wss://fx-ws.gateio.ws/v4/ws/usdt"
            )
            logger.info("[交易WS] Gate 交易 WS 长连接已建立")
            await self._gate_ws_login(self._gate_trade_ws, label="交易WS")
            asyncio.create_task(self._read_gate_trade_ws())
            asyncio.create_task(self._gate_trade_ws_ping())
        except Exception as exc:
            logger.warning("[交易WS] Gate 交易 WS 建立失败: %s", exc)



    async def _gate_ws_login(self, ws=None, label: str = "交易WS") -> None:
        """Gate WS request_private 登录 futures.login，登录后可发 request/subscribe。
        ws 默认使用 _gate_trade_ws，也可传入其他 WS 连接。"""
        import uuid as _uuid
        if ws is None:
            ws = getattr(self, "_gate_trade_ws", None)
        if not ws or ws.closed:
            raise Exception(f"Gate WS login: ws 未连接")
        channel = "futures.login"
        event = "api"
        req_id = _uuid.uuid4().hex[:16]
        t = int(time.time())
        req_params: dict[str, Any] = {}
        sign_msg = f"{event}\n{channel}\n{json.dumps(req_params, separators=(',', ':'))}\n{t}"
        sign = hmac.new(
            GATE_API_SECRET.encode(), sign_msg.encode(), hashlib.sha512,
        ).hexdigest()
        msg = {
            "id": req_id, "time": t, "channel": channel, "event": event,
            "payload": {
                "req_id": req_id, "timestamp": str(t),
                "api_key": GATE_API_KEY, "signature": sign,
                "req_param": req_params,
                "req_header": {"X-Gate-Channel-Id": "ccxt"},
            },
        }
        await ws.send_json(msg)
        t_deadline = time.perf_counter() + 10.0
        while time.perf_counter() < t_deadline:
            try:
                raw = await asyncio.wait_for(ws.receive(), timeout=t_deadline - time.perf_counter())
            except asyncio.TimeoutError:
                raise Exception("Gate WS login timeout")
            if raw.type == aiohttp.WSMsgType.TEXT:
                d = json.loads(raw.data)
                hdr = d.get("header", {})
                if hdr.get("channel") == channel and hdr.get("event") == event:
                    if hdr.get("status") == "200":
                        logger.info("[%s] Gate WS 登录成功", label)
                        return
                    raise Exception(f"Gate WS login failed: status={hdr.get('status')}")
            elif raw.type == aiohttp.WSMsgType.CLOSED:
                raise Exception(f"Gate WS closed during login: code={raw.data}")
        raise Exception("Gate WS login timeout")



    async def _read_gate_trade_ws(self) -> None:
        """持久读 Gate 交易 WS 响应：下单回执 + 持仓变动（资费结算监听），断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_gate_trade_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = _json.loads(msg.data)
                        hdr = data.get("header", {})
                        ch = hdr.get("channel", "") or data.get("channel", "")
                        ev = hdr.get("event", "") or data.get("event", "")
                        if ch == "futures.ping":
                            await ws.send_json({"time": int(time.time()), "channel": "futures.pong"})
                        # ── 持仓变动（资费结算监听 + 持仓缓存）──
                        elif ch == "futures.positions" and ev == "update":
                            result = data.get("data", {}).get("result", data.get("result", []))
                            for pos in (result if isinstance(result, list) else [result]):
                                ctr = pos.get("contract", "")
                                if ctr:
                                    # 缓存所有合约持仓（开平仓验证免 REST）
                                    self._gate_ws_positions[ctr] = abs(float(pos.get("size", 0) or 0))
                            # 资费结算检测
                            symbol = getattr(self, "_gate_funding_symbol", "")
                            baseline = getattr(self, "_gate_funding_baseline", 0.0)
                            contract = self._to_gate_contract(symbol) if symbol else ""
                            if contract:
                                for pos in (result if isinstance(result, list) else [result]):
                                    if pos.get("contract") == contract:
                                        pnl = float(pos.get("pnl_fund", 0) or 0)
                                        if abs(pnl - baseline) > 0.0001:
                                            self._gate_funding_amount = round(pnl - baseline, 6)
                                            logger.info("[资费监听] Gate 资费到账: %s pnl_fund %.4f→%.4f (资费=%.4f) @ %s",
                                                        symbol, baseline, pnl, self._gate_funding_amount,
                                                        datetime.now(self.tz).strftime("%H:%M:%S.%f")[:-3])
                                            self._gate_funding_event.set()
                        # ── 资费收付通知（专门通道，不依赖 pnl_fund 变动）──
                        elif ch == "futures.funding_payments" and ev == "update":
                            symbol = getattr(self, "_gate_funding_symbol", "")
                            contract = self._to_gate_contract(symbol) if symbol else ""
                            if contract:
                                result = data.get("data", {}).get("result", data.get("result", []))
                                for item in (result if isinstance(result, list) else [result]):
                                    if item.get("contract") == contract:
                                        logger.info("[资费监听] Gate 资费到账: %s (funding_payments) @ %s",
                                                    symbol,
                                                    datetime.now(self.tz).strftime("%H:%M:%S.%f")[:-3])
                                        self._gate_funding_event.set()
                                        break
                        # ── 下单回执 ──
                        elif ch == "futures.order_place" and ev == "api":
                            rid = data.get("request_id", "")
                            result = data.get("data", {}).get("result", {})
                            order = result[0] if isinstance(result, list) else result
                            for key in (rid, order.get("text", ""), str(order.get("id", ""))):
                                if key and key in getattr(self, "_gate_trade_futures", {}):
                                    fut = self._gate_trade_futures[key]  # 不 pop，ack 消息不解析
                                    if not fut.done():
                                        errs = data.get("data", {}).get("errs", {})
                                        if errs:
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_exception(
                                                Exception(f"Gate WS order error: {errs.get('message', '') or str(errs)}"))
                                        elif hdr.get("status") and hdr.get("status") != "200":
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_exception(
                                                Exception(f"Gate WS order failed: status={hdr.get('status')}"))
                                        elif "id" in order:
                                            self._gate_trade_futures.pop(key, None)
                                            fut.set_result(order)
                                        # else: ack-only 回执 (无 id)，不解析，等第二条消息
                                    break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[交易WS] Gate 交易 WS 读取异常: %s", exc)
            # 断线重连
            self._gate_trade_ws = None
            for fut in getattr(self, "_gate_trade_futures", {}).values():
                if not fut.done():
                    fut.set_exception(Exception("Gate trade WS disconnected"))
            self._gate_trade_futures.clear()
            await asyncio.sleep(3)
            try:
                await self._ensure_gate_trade_ws()
                break
            except Exception:
                await asyncio.sleep(3)



    async def _gate_trade_ws_order(self, symbol: str, side: str,
                                    amount: float, reduce_only: bool = False,
                                    ) -> dict[str, Any]:
        """Gate 合约市价单 — 走 WS futures.order_place 持久连接。
        使用 request_private 认证模式（api_key + signature 嵌入 payload），
        而非 subscribe_private 的 auth.method 模式。"""
        import uuid as _uuid
        await self._ensure_gate_trade_ws()
        contract = self._to_gate_contract(symbol)
        precise = int(float(self.gate_futures.amount_to_precision(symbol, amount)))
        size = precise if side == "buy" else -precise
        t = int(time.time())
        text = f"t-{_uuid.uuid4().hex[:8]}"
        req_id = _uuid.uuid4().hex[:16]
        req_params: dict[str, Any] = {
            "contract": contract, "size": size, "price": "0", "tif": "ioc",
            "text": text, "settle": "usdt",
        }
        if reduce_only:
            req_params["reduce_only"] = True
        channel = "futures.order_place"
        event = "api"
        # request_private 签名: HMAC-SHA512("{event}\n{channel}\n{json(reqParams)}\n{time}")
        sign_msg = f"{event}\n{channel}\n{json.dumps(req_params, separators=(',', ':'))}\n{t}"
        sign = hmac.new(
            GATE_API_SECRET.encode(), sign_msg.encode(), hashlib.sha512,
        ).hexdigest()
        msg = {
            "id": req_id,
            "time": t, "channel": channel, "event": event,
            "payload": {
                "req_id": req_id,
                "timestamp": str(t),
                "api_key": GATE_API_KEY,
                "signature": sign,
                "req_param": req_params,
                "req_header": {"X-Gate-Channel-Id": "ccxt"},
            },
        }
        fut: asyncio.Future = asyncio.get_event_loop().create_future()
        self._gate_trade_futures[req_id] = fut
        self._gate_trade_futures[text] = fut  # fallback 用 text 也能匹配
        try:
            t0 = time.perf_counter()
            t0_sec = int(time.time())
            await self._gate_trade_ws.send_json(msg)
            result = await asyncio.wait_for(fut, timeout=15.0)
            t1 = time.perf_counter()
            rtt_ms = (t1 - t0) * 1000
            if isinstance(result, list):
                result = result[0] if result else {}
            # Gate order_place 响应含 create_time (秒)，可算成交延迟
            fill_info = ""
            if "status" in result:
                ct = result.get("create_time", 0) or result.get("finish_time", 0)
                if ct > 0:
                    fill_ms = int((ct - t0_sec) * 1000)
                    fill_info = f" | 成交 {fill_ms}ms"
            logger.info("[延迟] Gate WS 下单 %s %s: 往返 %.0fms%s", symbol, side, rtt_ms, fill_info)
            # Gate order_place 返回 ack (接单回执)，不含 fill 数据
            # result 可能是 req_param echo (无 status 字段) 或真实订单对象 (有 status 字段)
            if "status" in result:
                filled = abs(float(result.get("size", 0) or 0)) - abs(float(result.get("left", 0) or 0))
                finished = result.get("status") == "finished"
            else:
                # ack 回执模式: IOC 市价单已送达，假设全部成交，由上层调用者 REST 验证
                filled = precise
                finished = True
            return {
                "id": str(result.get("id", "")), "symbol": symbol, "side": side,
                "amount": precise, "filled": filled,
                "status": "closed" if finished else ("partial" if filled > 0 else "open"),
                "info": result,
            }
        except asyncio.TimeoutError:
            self._gate_trade_futures.pop(req_id, None)
            self._gate_trade_futures.pop(text, None)
            raise Exception(f"Gate WS order timeout: {symbol} {side}")
        except Exception:
            self._gate_trade_futures.pop(req_id, None)
            self._gate_trade_futures.pop(text, None)
            raise



    async def _gate_trade_ws_ping(self) -> None:
        """Gate 交易 WS 心跳，每 25 秒 ping 一次防止断线。"""
        while True:
            await asyncio.sleep(25)
            ws = getattr(self, "_gate_trade_ws", None)
            if not ws or ws.closed:
                break
            try:
                await ws.send_json({"time": int(time.time()), "channel": "futures.ping"})
            except Exception:
                break



    async def _close_gate_trade_ws(self) -> None:
        """关闭 Gate 交易 WS，取消所有等待中的订单。"""
        try:
            ws = getattr(self, "_gate_trade_ws", None)
            if ws:
                await ws.close()
                self._gate_trade_ws = None
        except Exception:
            pass
        for fut in getattr(self, "_gate_trade_futures", {}).values():
            if not fut.done():
                fut.set_exception(Exception("Gate trade WS closed"))




