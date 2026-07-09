"""WebSocketMixin — Binance & Gate WebSocket 长连接管理 + WS 下单."""
from __future__ import annotations
import asyncio
import json
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



    # ════════════════════════════════════════════════════════════════
    # BN 交易 WS（下单专用，wss://ws-fapi.binance.com/ws-fapi/v1）
    # ════════════════════════════════════════════════════════════════

    async def _ensure_bn_trade_ws(self) -> None:
        """确保 BN 交易 WS 长连接（仅用于下单），断线自动重连。"""
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
            asyncio.create_task(self._bn_trade_ws_ping())
        except Exception as exc:
            logger.warning("[交易WS] BN 交易 WS 建立失败: %s", exc)


    # ════════════════════════════════════════════════════════════════
    # BN 用户数据流 WS（资费/账户事件推送，wss://fstream.binance.com/private/ws）
    # 币安 2026-04 起废弃旧 URL，新 URL 需显式指定 events 参数 太扫码了
    # ════════════════════════════════════════════════════════════════

    async def _ensure_bn_user_data_ws(self) -> None:
        """确保 BN 用户数据流 WS 连接，断线自动重连。"""
        ws = getattr(self, "_bn_user_data_ws", None)
        if ws and not ws.closed:
            return
        await self._close_bn_user_data_ws()
        try:
            # Step 1: 获取 listenKey
            if not self._bn_listen_key or time.time() - getattr(self, "_bn_listen_key_ts", 0) >= 2700:
                resp = await self._binance_request(
                    BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="POST",
                )
                self._bn_listen_key = resp.get("listenKey", "")
                self._bn_listen_key_ts = time.time()
                if not self._bn_listen_key:
                    logger.warning("[用户数据WS] listenKey 创建失败")
                    return

            # Step 2: 连接用户数据流 WS（新 URL，显式指定 events）
            if not self._bn_user_data_session:
                self._bn_user_data_session = aiohttp.ClientSession()
            url = (f"wss://fstream.binance.com/private/ws"
                   f"?listenKey={self._bn_listen_key}"
                   f"&events=ORDER_TRADE_UPDATE/ACCOUNT_UPDATE")
            self._bn_user_data_ws = await self._bn_user_data_session.ws_connect(url)
            logger.info("[用户数据WS] BN 用户数据流已连接")

            # Step 3: 启动后台任务
            asyncio.create_task(self._read_bn_user_data_ws())
            asyncio.create_task(self._bn_user_data_ping())
            asyncio.create_task(self._renew_bn_listen_key_loop())
        except Exception as exc:
            logger.warning("[用户数据WS] 连接失败: %s", exc)


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


    async def _bn_trade_ws_ping(self) -> None:
        """BN 交易 WS 心跳，每 50 秒 ping 一次防断线。"""
        while True:
            await asyncio.sleep(50)
            ws = getattr(self, "_bn_trade_ws", None)
            if not ws or ws.closed:
                break
            try:
                await ws.ping()
            except Exception:
                break

    async def _renew_bn_listen_key_loop(self) -> None:
        """每 45 分钟 PUT /fapi/v1/listenKey 续期，防止 listenKey 过期（60min 有效期）。"""
        while True:
            await asyncio.sleep(2700)
            if not self._bn_listen_key:
                break
            try:
                await self._binance_request(
                    BINANCE_FUTURES_API, "/fapi/v1/listenKey", {}, method="PUT",
                )
                self._bn_listen_key_ts = time.time()
                logger.info("[用户数据WS] listenKey 续期成功")
            except Exception as exc:
                logger.warning("[用户数据WS] listenKey 续期失败: %s", exc)

    async def _close_bn_trade_ws(self) -> None:
        """关闭 BN 交易 WS。"""
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

    async def _close_bn_user_data_ws(self) -> None:
        """关闭 BN 用户数据流 WS + 清理 listenKey。"""
        try:
            ws = getattr(self, "_bn_user_data_ws", None)
            if ws:
                await ws.close()
                self._bn_user_data_ws = None
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



    async def _read_bn_trade_ws(self) -> None:
        """读取 BN 交易 WS 响应：仅处理下单回执，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_bn_trade_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    data = _json.loads(msg.data)

                    # 下单回执（id 匹配订单 Future）
                    rid = data.get("id", "")
                    if rid and rid in getattr(self, "_bn_trade_futures", {}):
                        fut = self._bn_trade_futures.pop(rid)
                        if not fut.done():
                            if data.get("status") == 200:
                                fut.set_result(data.get("result", {}))
                            else:
                                err = data.get("error", {})
                                fut.set_exception(
                                    Exception(f"BN WS order failed: {err}"))

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

    async def _read_bn_user_data_ws(self) -> None:
        """读取 BN 用户数据流：ACCOUNT_UPDATE 资费检测 + 持仓缓存，断线自动重连。"""
        import json as _json
        while True:
            try:
                ws = getattr(self, "_bn_user_data_ws", None)
                if not ws or ws.closed:
                    break
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
                        continue
                    data = _json.loads(msg.data)

                    e_type = data.get("e", "")
                    if not e_type:
                        continue

                    # 诊断: 事件类型去重日志
                    seen = getattr(self, "_funding_ws_seen_types", set())
                    if e_type not in seen:
                        seen.add(e_type)
                        self._funding_ws_seen_types = seen
                        logger.info("[资费诊断-WS存活] 新事件类型: %s | 累计: %s",
                                   e_type, sorted(seen))

                    # RAW dump（结算窗口内）
                    if time.time() < getattr(self, "_funding_raw_dump_until", 0):
                        logger.info("[资费诊断-RAW] %s", msg.data)

                    if e_type == "ACCOUNT_UPDATE":
                        a = data.get("a", {})
                        positions = a.get("P") or []
                        for p in positions:
                            sym = p.get("s", "")
                            ps = p.get("ps", "")
                            if sym and ps:
                                self._bn_ws_positions[f"{sym}|{ps}"] = abs(
                                    float(p.get("pa", 0) or 0))

                        reason = a.get("m", "")
                        balances = a.get("B") or []
                        binance_tx_ms = data.get("T") or data.get("E")

                        # ── 双轨资费检测 ──
                        funding_detected = False
                        detection_label = ""

                        if reason == "FUNDING_FEE":
                            funding_detected = True
                            detection_label = "m=FUNDING_FEE"
                        elif balances:
                            for b in balances:
                                bc = float(b.get("bc", 0) or 0)
                                if bc != 0.0 and reason not in (
                                    "ORDER", "DEPOSIT", "WITHDRAW",
                                    "MARGIN_TRANSFER", "ASSET_TRANSFER",
                                ):
                                    funding_detected = True
                                    detection_label = f"m={reason}, bc={bc}"
                                    break

                        if funding_detected:
                            recv_wall_ms = int(time.time() * 1000)
                            # 资费金额直接从 bc 抓（USDT-M 结算币种为 USDT），最准，免 REST 竞态
                            funding_amount = sum(
                                float(b.get("bc", 0) or 0) for b in balances)
                            recv_time_str = time.strftime(
                                "%H:%M:%S", time.localtime(recv_wall_ms / 1000))
                            recv_time_str += f".{recv_wall_ms % 1000:03d}"
                            logger.info("[资费监听] BN 资费已到账 @ %s | 检测方式=%s",
                                       datetime.now(self.tz).strftime("%H:%M:%S.%f")[:-3],
                                       detection_label)
                            self._funding_event.set()
                            snipe = getattr(self, "_bn_snipe", None)
                            if snipe is not None:
                                snipe.last_funding_tx_ms = int(binance_tx_ms) if binance_tx_ms else 0
                                snipe.last_funding_event_ms = int(data.get("E", 0))
                                snipe.last_funding_recv_ms = recv_wall_ms
                                snipe.last_funding_amount_usdt = funding_amount
                                snipe.ws_funding_arrived_event.set()
                            if binance_tx_ms:
                                bn_time_str = time.strftime(
                                    "%H:%M:%S", time.localtime(int(binance_tx_ms) / 1000))
                                bn_time_str += f".{int(binance_tx_ms) % 1000:03d}"
                                logger.info("[资费对账审计] 币安结算记账: %s | 我方收到: %s | 跨系统时差: %dms",
                                           bn_time_str, recv_time_str,
                                           recv_wall_ms - int(binance_tx_ms))
                            else:
                                logger.info("[资费对账审计] 币安结算记账: N/A | 我方收到: %s",
                                           recv_time_str)
                        else:
                            seen_reasons = getattr(self, "_funding_seen_reasons", set())
                            if reason and reason not in seen_reasons:
                                seen_reasons.add(reason)
                                self._funding_seen_reasons = seen_reasons
                                logger.info("[资费诊断-ACCOUNT_UPDATE] m=%s B=%s P=%s | 完整: %s",
                                           reason,
                                           _json.dumps(balances, ensure_ascii=False)[:200],
                                           _json.dumps(positions, ensure_ascii=False)[:200],
                                           _json.dumps(data, ensure_ascii=False)[:400])

                    elif e_type == "ORDER_TRADE_UPDATE":
                        o = data.get("o", {})
                        ws_lat = int(time.time() * 1000) - int(data.get("E", 0))
                        logger.info("[用户数据WS] 订单推送: %s %s status=%s | 延迟 %dms",
                                   o.get("s", ""), o.get("S", ""), o.get("X", ""), ws_lat)
                        # 实时抓取成交回报(rp价格盈亏 + n手续费)，最准，免平仓后 REST userTrades 竞态。
                        # 拥堵结算下 REST 可能只返回单腿，rp/手续费漏半 → 净利记错。WS 每笔成交都推。
                        snipe = getattr(self, "_bn_snipe", None)
                        if snipe is not None and o.get("x") == "TRADE":
                            target = getattr(snipe, "_clean_symbol", "")
                            if not target or o.get("s") == target:
                                snipe.last_realized_pnl_usdt += float(o.get("rp", 0) or 0)
                                comm = float(o.get("n", 0) or 0)
                                if comm:
                                    snipe.last_commission += comm
                                    snipe.last_commission_asset = (
                                        o.get("N", "") or snipe.last_commission_asset)
                                snipe.last_fill_count += 1
                                # 开仓腿(非 reduce)全部成交(X=FILLED)→ 记真实开仓均价(ap)，
                                # 供止盈按真实成交价而非陈旧扫描价计算触发距。
                                # 卡 FILLED 而非任一笔 TRADE：确保 ap 是完整成交的加权均价，
                                # 而非中途某笔部分成交的均价。
                                if not o.get("R") and o.get("X") == "FILLED":
                                    ap = float(o.get("ap", 0) or 0)
                                    if ap > 0:
                                        snipe.last_open_avg_price = ap
                                # 止盈成交标记：算法条件单触发生成的子市价单，clientOrderId(c)
                                # 由交易所自动生成(不带我方 clientAlgoId)，但会带
                                # ot(原始类型)=TAKE_PROFIT_MARKET 或 st(策略类型)=C_TAKE_PROFIT。
                                # 普通市价平仓 ot=MARKET 不含 TAKE_PROFIT，不会误判。
                                # → 供账本打标签 + 平仓时跳过白等资费。
                                otype = (str(o.get("ot", "")) + "|"
                                         + str(o.get("st", ""))).upper()
                                if ("TAKE_PROFIT" in otype
                                        or str(o.get("c", "")).startswith("TPSNIPE")):
                                    snipe.tp_filled = True
                                    # 机房侧被动止盈已成交、仓位已归零 → 提前拉响信号弹，
                                    # 唤醒主循环 run_cycle 的 ws_funding_arrived_event.wait()，
                                    # 免其空等到 400/600ms 硬死线（此时资费必不会到账）。
                                    snipe.ws_funding_arrived_event.set()

                    elif e_type == "listenKeyExpired":
                        logger.warning("[用户数据WS] listenKey 过期，重连")
                        break

            except Exception as exc:
                if "Connection" not in str(exc) and "closed" not in str(exc).lower():
                    logger.warning("[用户数据WS] 读取异常: %s", exc)
            # 断线重连
            self._bn_user_data_ws = None
            self._funding_ws_lost_at = time.time()
            await asyncio.sleep(3)
            try:
                await self._ensure_bn_user_data_ws()
                self._funding_ws_lost_at = 0.0
                break
            except Exception:
                await asyncio.sleep(3)

    async def _bn_user_data_ping(self) -> None:
        """BN 用户数据 WS 心跳，每 50 秒 ping 一次防断线。"""
        while True:
            await asyncio.sleep(50)
            ws = getattr(self, "_bn_user_data_ws", None)
            if not ws or ws.closed:
                break
            try:
                await ws.ping()
            except Exception:
                break



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
            raw = result
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

    async def _bn_trade_ws_stop_order(self, symbol: str, side: str,
                                       stop_price: str | float, position_side: str,
                                       client_id: str) -> bool:
        """BN 止盈条件单 — TAKE_PROFIT_MARKET + closePosition，走持久 WS 的 algoOrder.place。
        条件单不能走 order.place（-4120），须用 algo 端点，触发价字段为 triggerPrice。
        无 quantity/reduceOnly：挂着时不占用仓位/保证金，仓位归零时交易所自动撤销，
        故不会挤占后续收资费的市价全平。尽力而为：受理(有 algoId)即成功，
        失败返回 False，绝不抛给交易主流程。"""
        import uuid as _uuid
        rid = None
        try:
            await self._ensure_bn_trade_ws()
            clean = self._clean_futures_symbol(symbol)
            ts = int(time.time() * 1000)
            params: dict[str, Any] = {
                "apiKey": BINANCE_API_KEY,
                "algoType": "CONDITIONAL",
                "symbol": clean,
                "side": side.upper(),
                "type": "TAKE_PROFIT_MARKET",
                "triggerPrice": stop_price,
                "closePosition": True,   # 原生 JSON 布尔：WS 是强类型 JSON 帧，官方定义 closePosition 为 boolean，
                                         # 传字符串 "true" 可能在类型反序列化阶段被 -1102 拒（走不到验签）
                "positionSide": position_side.upper(),
                "workingType": "CONTRACT_PRICE",
                "clientAlgoId": client_id,
                "timestamp": ts,
                "recvWindow": 5000,
            }
            # 签名须与服务器对 JSON 值的字符串化一致：布尔 True/False → 小写 "true"/"false"。
            # 若用 str(True) 会得到大写 "True"，与 send_json 序列化出的小写 true 不符 → -1022 签名失效。
            sorted_items = sorted(
                (str(k), "true" if v is True else "false" if v is False else str(v))
                for k, v in params.items()
            )
            qs = "&".join(f"{k}={v}" for k, v in sorted_items)
            params["signature"] = self._binance_sign(qs)

            rid = str(_uuid.uuid4())[:8]
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._bn_trade_futures[rid] = fut
            await self._bn_trade_ws.send_json(
                {"id": rid, "method": "algoOrder.place", "params": params})
            raw = await asyncio.wait_for(fut, timeout=5.0)
            aid = raw.get("algoId", 0) if isinstance(raw, dict) else 0
            if aid:
                logger.info("[狙击] 止盈挂单成功 %s %s @ %s (closePosition, algoId=%s)",
                            symbol, side, stop_price, aid)
                return True
            logger.warning("[狙击] 止盈挂单无 algoId: %s", raw)
            return False
        except Exception as exc:
            if rid is not None:
                self._bn_trade_futures.pop(rid, None)
            logger.warning("[狙击] 止盈挂单失败 %s: %s", symbol, exc)
            return False

    async def _bn_cancel_order(self, symbol: str, client_id: str) -> None:
        """尽力撤单：按 clientAlgoId 撤掉一张算法条件单（开仓失败后清理残留止盈单）。
        算法单须走 /fapi/v1/algoOrder（普通 /fapi/v1/order 不认），冷路径不计延迟；
        失败只 warning，不抛。正常平仓时仓位归零 → 交易所自动撤销(EXPIRED)，无需调用此法。"""
        try:
            await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/algoOrder",
                {"clientAlgoId": client_id},
                method="DELETE",
            )
            logger.info("[狙击] 已撤残留止盈单 %s %s", symbol, client_id)
        except Exception as exc:
            logger.warning("[狙击] 撤残留止盈单失败 %s %s: %s", symbol, client_id, exc)

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




