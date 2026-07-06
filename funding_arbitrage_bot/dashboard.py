"""Mixin: DashboardMixin."""
from __future__ import annotations
import string as _string_mod
from pathlib import Path
from typing import Any
from .constants import *
from .models import _safe_float, ArbitrageState, LegResult, CrossArbitrageState

_TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"

class DashboardMixin:

    def _load_trade_history(self) -> list[dict[str, Any]]:
        if not self.TRADE_HISTORY_FILE.exists():
            return []
        try:
            return json.loads(self.TRADE_HISTORY_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []



    def _record_close_trade(self, state: ArbitrageState) -> None:
        if not state.is_open:
            return
        profit = state.amount * state.entry_price * state.net_rate
        self._record_trade(
            state.base or "???", state.direction, profit, state.net_rate,
        )



    def _record_trade(self, coin: str, direction: str,
                      profit_usdt: float, net_rate: float,
                      *, amount: float = 0.0,
                      price_pnl: float = 0.0,
                      funding_pnl: float = 0.0,
                      fee_total: float = 0.0) -> None:
        history = self._load_trade_history()
        record: dict[str, Any] = {
            "ts": datetime.now(tz=self.tz).isoformat(),
            "coin": coin,
            "direction": direction,
            "profit_usdt": round(profit_usdt, 6),
            "net_rate": round(net_rate, 6),
        }
        if amount:
            record["amount"] = round(amount, 4)
        if price_pnl or funding_pnl or fee_total:
            record["price_pnl"] = round(price_pnl, 6)
            record["funding_pnl"] = round(funding_pnl, 6)
            record["fee_total"] = round(fee_total, 6)
        history.append(record)
        if len(history) > 365:
            history = history[-365:]
        self.TRADE_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.TRADE_HISTORY_FILE.write_text(
            json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8",
        )



    def _record_balance_snapshot(self, bn_total: float, gt_total: float) -> None:
        snaps = []
        if self.BALANCE_SNAPSHOT_FILE.exists():
            try:
                snaps = json.loads(self.BALANCE_SNAPSHOT_FILE.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                pass
        ts = datetime.now(tz=self.tz).isoformat()
        if snaps and snaps[-1].get("ts", "")[:16] == ts[:16]:
            snaps[-1] = {"ts": ts, "binance": round(bn_total, 2), "gate": round(gt_total, 2)}
        else:
            snaps.append({"ts": ts, "binance": round(bn_total, 2), "gate": round(gt_total, 2)})
        if len(snaps) > 60000:
            snaps = snaps[-60000:]
        self.BALANCE_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        self.BALANCE_SNAPSHOT_FILE.write_text(
            json.dumps(snaps, ensure_ascii=False), encoding="utf-8",
        )



    async def _record_cross_trade(self, short_close_order: dict | None = None,
                                   long_close_order: dict | None = None,
                                   state_snapshot: CrossArbitrageState | None = None) -> None:
        """平仓后从交易所查询实际资费+手续费+成交价，不做估算。"""
        cs = state_snapshot if state_snapshot is not None else self.cross_state
        if not cs.is_open or cs.short_amount <= 0 or cs.long_amount <= 0:
            return
        short_amount = cs.short_amount
        long_amount = cs.long_amount
        short_entry = cs.short_entry_price
        long_entry = cs.long_entry_price
        short_notional = short_amount * short_entry if short_entry else 0
        long_notional = long_amount * long_entry if long_entry else 0

        # 提取平仓成交价（先用 WS 订单响应，失败则 REST 补齐）
        short_exit = self._extract_fill_price(short_close_order) if short_close_order else 0.0
        long_exit = self._extract_fill_price(long_close_order) if long_close_order else 0.0

        # ── 查实际手续费 + 平仓均价（4 单并发：开空+平空+开多+平多）──
        short_close_id = str(short_close_order.get("id", "")) if short_close_order else ""
        long_close_id = str(long_close_order.get("id", "")) if long_close_order else ""
        short_close_info = short_close_order.get("info") if short_close_order else None
        long_close_info = long_close_order.get("info") if long_close_order else None

        (s_open_fee, _), (s_close_fee, s_close_avg), \
        (l_open_fee, _), (l_close_fee, l_close_avg) = await asyncio.gather(
            self._query_order_actual_fee(cs.short_exchange, cs.short_symbol, cs.short_order_id or ""),
            self._query_order_actual_fee(cs.short_exchange, cs.short_symbol, short_close_id, short_close_info),
            self._query_order_actual_fee(cs.long_exchange, cs.long_symbol, cs.long_order_id or ""),
            self._query_order_actual_fee(cs.long_exchange, cs.long_symbol, long_close_id, long_close_info),
        )
        short_fee_actual = round(s_open_fee + s_close_fee, 6)
        long_fee_actual = round(l_open_fee + l_close_fee, 6)

        # REST 补齐平仓均价（WS 响应可能缺失 fill_price）
        if short_exit <= 0 and s_close_avg > 0:
            short_exit = s_close_avg
        if long_exit <= 0 and l_close_avg > 0:
            long_exit = l_close_avg

        # 费率估算（仅当交易所查询失败时回退）
        bn_fee, gt_fee = getattr(self, "_dash_cross_fees", (0.00045, 0.00018))
        _fee_of = {"binance": bn_fee, "gate": gt_fee}
        _dsc_of = {"binance": BN_FEE_DISCOUNT_FACTOR, "gate": GT_FEE_DISCOUNT_FACTOR}
        _rbt_of = {"binance": BN_FEE_REBATE_FACTOR, "gate": GT_FEE_REBATE_FACTOR}
        if short_fee_actual <= 0:
            short_fee_est = round(short_notional * _fee_of.get(cs.short_exchange, bn_fee) * 2, 6)
            short_fee_actual = round(short_fee_est * _dsc_of.get(cs.short_exchange, 1.0) * _rbt_of.get(cs.short_exchange, 1.0), 6)
        if long_fee_actual <= 0:
            long_fee_est = round(long_notional * _fee_of.get(cs.long_exchange, gt_fee) * 2, 6)
            long_fee_actual = round(long_fee_est * _dsc_of.get(cs.long_exchange, 1.0) * _rbt_of.get(cs.long_exchange, 1.0), 6)

        # ── 查实际资费（BN income API / Gate WS 监听结果）──
        short_actual_funding = 0.0
        long_actual_funding = 0.0
        if cs.short_exchange == "binance":
            short_actual_funding = await self._query_bn_funding_amount(
                cs.short_symbol, int(cs.short_next_funding_time_ms))
        elif cs.short_exchange == "gate":
            short_actual_funding = self._gate_funding_amount
        if cs.long_exchange == "binance":
            long_actual_funding = await self._query_bn_funding_amount(
                cs.long_symbol, int(cs.long_next_funding_time_ms))
        elif cs.long_exchange == "gate":
            long_actual_funding = self._gate_funding_amount

        # ── 做空侧 PnL ──
        short_price_pnl = round(short_amount * (short_entry - short_exit), 6)
        short_funding_pnl = round(short_actual_funding, 6) if short_actual_funding != 0.0 else round(short_notional * cs.short_rate, 6)
        short_net = round(short_price_pnl + short_funding_pnl - short_fee_actual, 6)

        # ── 做多侧 PnL ──
        long_price_pnl = round(long_amount * (long_exit - long_entry), 6)
        # API 返回的 FUNDING_FEE income 已自带 PnL 符号（正=收到, 负=付出），无需再取反
        long_funding_pnl = round(long_actual_funding, 6) if long_actual_funding != 0.0 else round(-long_notional * cs.long_rate, 6)
        long_net = round(long_price_pnl + long_funding_pnl - long_fee_actual, 6)

        net_pnl = round(short_net + long_net, 6)

        self._write_cross_trade_record(
            cs.base, f"cross({cs.short_exchange}空+{cs.long_exchange}多)",
            cs.short_amount, cs.short_entry_price, cs.total_net_rate, cs.rate_spread,
            short_entry=short_entry, short_exit=short_exit,
            long_entry=long_entry, long_exit=long_exit,
            net_pnl=net_pnl,
            short_notional=round(short_notional, 2),
            long_notional=round(long_notional, 2),
            short_exchange=cs.short_exchange, short_price_pnl=short_price_pnl,
            short_funding_pnl=short_funding_pnl,
            short_fee=short_fee_actual, short_net=short_net,
            long_exchange=cs.long_exchange, long_price_pnl=long_price_pnl,
            long_funding_pnl=long_funding_pnl,
            long_fee=long_fee_actual, long_net=long_net)



    def _write_cross_trade_record(self, coin: str, direction: str, amount: float,
                                   entry_price: float, net_rate: float, rate_spread: float,
                                   **extra) -> None:
        """写入一条跨所交易记录。profit_usdt 优先用真实 net_pnl。"""
        try:
            history = self._load_trade_history()
            real_pnl = extra.get("net_pnl")
            record = {
                "time": datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S"),
                "coin": coin,
                "direction": direction,
                "profit_usdt": round(real_pnl, 6) if real_pnl is not None else round(amount * entry_price * net_rate, 6),
                "net_rate": net_rate,
                "rate_spread": rate_spread,
                "amount": amount,
            }
            record.update({k: v for k, v in extra.items() if v is not None})  # 保留零值
            history.append(record)
            with self.TRADE_HISTORY_FILE.open("w", encoding="utf-8") as f:
                json.dump(history[-500:], f, indent=2, ensure_ascii=False)
        except Exception as exc:
            logger.warning("记录跨所交易历史失败: %s", exc)



    def _print_cross_opportunity_table(self, df: pd.DataFrame, position_usdt: float) -> None:
        """打印跨交易所套利机会表。"""
        if df.empty:
            return
        est_profit = position_usdt * POSITION_SIZE_RATIO
        has_col = "passes" in df.columns
        lines = []
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════════════════")
        lines.append(f"  跨交易所资金费率套利 (纯期货多空对冲)  —  每腿 ≈{est_profit:.0f} USDT")
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════════════════")
        header = (f"  {'':1s}{'币种':<8s} {'BN价':>10s} {'GT价':>10s} {'费率差':>7s} {'净收益':>7s} "
                  f"{'做空所':>7s} {'空费率':>8s} {'空结算':>7s} "
                  f"{'做多所':>7s} {'多费率':>8s} {'多结算':>7s}")
        lines.append(header)
        lines.append("  " + "-" * 110)
        now_ms = time.time() * 1000
        for _, r in df.head(20).iterrows():
            if has_col:
                mark = "✓" if r.get("passes", False) else "✗"
            else:
                mark = " "
            bn_p = float(r.get("bn_price", 0))
            gt_p = float(r.get("gt_price", 0))
            short_nft = float(r.get("short_next_funding_time_ms", 0))
            long_nft = float(r.get("long_next_funding_time_ms", 0))
            short_m = max(0, (short_nft - now_ms) / 60000) if short_nft > 0 else -1
            long_m = max(0, (long_nft - now_ms) / 60000) if long_nft > 0 else -1
            short_wait = f"{int(short_m)}m" if short_m >= 0 else "N/A"
            long_wait = f"{int(long_m)}m" if long_m >= 0 else "N/A"
            lines.append(
                f"  {mark}{str(r['base']):<8s} {bn_p:>10.6f} {gt_p:>10.6f} {float(r['rate_spread'])*100:>6.4f}% {float(r['net_rate'])*100:>6.4f}% "
                f"{str(r['short_exchange']):>7s} {float(r['short_rate'])*100:>7.4f}% {short_wait:>7s} "
                f"{str(r['long_exchange']):>7s} {float(r['long_rate'])*100:>7.4f}% {long_wait:>7s}"
            )
        passes_any = any(r.get("passes", False) for _, r in df.head(20).iterrows())
        if has_col and not passes_any:
            lines.append("  (无候选通过严格筛选 — 费率差/净收益/结算窗口未达标)")
        lines.append(f"  ═══════════════════════════════════════════════════════════════════════════════")
        for line in lines:
            logger.info(line)



    def _print_cross_position_summary(self) -> None:
        """打印跨交易所持仓摘要（费率、结算时间、预估收益）。"""
        cs = self.cross_state
        if not cs.is_open:
            return
        now_ms = time.time() * 1000
        short_wait = max(0, (cs.short_next_funding_time_ms - now_ms) / 60000) if cs.short_next_funding_time_ms > 0 else -1
        long_wait = max(0, (cs.long_next_funding_time_ms - now_ms) / 60000) if cs.long_next_funding_time_ms > 0 else -1

        short_settle = f"{short_wait:.0f}min后" if short_wait >= 0 else "未知"
        long_settle = f"{long_wait:.0f}min后" if long_wait >= 0 else "未知"

        notional = cs.amount * cs.short_entry_price if cs.short_entry_price > 0 else 0
        est_profit = notional * cs.total_net_rate if notional > 0 else 0

        logger.info("  ═══════════════════════════════════════════════════════════════")
        logger.info("  跨交易所持仓  —  %s 空×%.4f张 多×%.4f张  (≈%.0f USDT/腿)",
                     cs.base, cs.short_amount, cs.long_amount, notional)
        logger.info("  ═══════════════════════════════════════════════════════════════")
        logger.info("    做空: %6s  %-12s  费率=%+7.4f%%  结算: %s",
                     cs.short_exchange.upper(), cs.short_symbol, cs.short_rate * 100, short_settle)
        logger.info("    做多: %6s  %-12s  费率=%+7.4f%%  结算: %s",
                     cs.long_exchange.upper(), cs.long_symbol, cs.long_rate * 100, long_settle)
        logger.info("    ───────────────────────────────────────────────────────────")
        logger.info("    原始费率差: %+7.4f%%    净收益率: %+7.4f%%",
                     cs.rate_spread * 100, cs.total_net_rate * 100)
        if est_profit > 0:
            logger.info("    预估单期收益: %7.2f USDT", est_profit)
        logger.info("    开仓时间: %s", cs.opened_at or '未知')
        logger.info("  ═══════════════════════════════════════════════════════════════")



    def _print_position_summary(self, exchange: str = "binance") -> None:
        state = self.gate_state if exchange == "gate" else self.binance_state
        if not state.is_open:
            return
        position_notional = state.amount * state.entry_price
        estimated_profit = position_notional * state.net_rate
        if state.direction == "reverse":
            side_label = "long"
            source_label = "margin"
        else:
            side_label = "short"
            source_label = state.spot_source
        tag = "Gate" if exchange == "gate" else "Binance"
        logger.info(
            "[%s] 套利持仓 [%s]: %s [%s] + %s %s | amount=%s | entry=%s "
            "| 预测单期净利润≈%.4f USDT | opened_at=%s",
            tag, state.direction,
            state.spot_symbol, source_label,
            state.futures_symbol, side_label,
            state.amount, state.entry_price,
            estimated_profit, state.opened_at,
        )


    def _write_dashboard(self) -> None:
        """生成手机端自适应 Dashboard。含盈亏日历、资金曲线。"""
        futures_usdt = getattr(self, "_dash_futures_usdt", 0.0)
        total_usdt = getattr(self, "_dash_total_usdt", 0.0)
        df = getattr(self, "_dash_df", pd.DataFrame())
        gate_spot = getattr(self, "_dash_gate_spot", 0.0)
        gate_df = getattr(self, "_dash_gate_df", pd.DataFrame())
        cross_df = getattr(self, "_dash_cross_df", pd.DataFrame())
        snipe_df = getattr(self, "_dash_snipe_df", pd.DataFrame())

        now_str = datetime.now(tz=self.tz).strftime("%Y-%m-%d %H:%M:%S")
        now = datetime.now(tz=self.tz)

        # ── 合并扫描结果 ──
        combined_rows = []
        if not df.empty:
            for _, row in df.iterrows():
                d = row.to_dict()
                d["exchange"] = "BN"
                combined_rows.append(d)
        if not gate_df.empty:
            for _, row in gate_df.iterrows():
                d = row.to_dict()
                d["exchange"] = "GT"
                combined_rows.append(d)
        combined_rows.sort(key=lambda r: float(r.get("net_rate", -999)), reverse=True)
        top10 = combined_rows[:10]

        # ── 跨交易所 Top 10 ──
        cross_top10 = []
        if not cross_df.empty:
            for _, row in cross_df.head(10).iterrows():
                cross_top10.append(row.to_dict())

        # ── 交易历史 & 累计盈亏 ──
        history = self._load_trade_history()

        def _get_real_pnl(h: dict) -> float:
            """真实净盈亏：优先用 net_pnl，旧记录回退 profit_usdt。"""
            n = h.get("net_pnl")
            if n is not None:
                return float(n)
            return float(h.get("profit_usdt", 0) or 0)

        total_profit = sum(_get_real_pnl(h) for h in history)
        profit_cls = "positive" if total_profit >= 0 else "negative"

        # 累计分项：平仓盈亏 / 资费 / 手续费
        cum_price = 0.0
        cum_funding = 0.0
        cum_fee = 0.0
        for h in history:
            if "short_exchange" in h:
                cum_price += (h.get("short_price_pnl", 0) or 0) + (h.get("long_price_pnl", 0) or 0)
                cum_funding += (h.get("short_funding_pnl", 0) or 0) + (h.get("long_funding_pnl", 0) or 0)
                sf = (h.get("short_actual_fee") or h.get("short_fee")) or 0
                lf = (h.get("long_actual_fee") or h.get("long_fee")) or 0
                cum_fee += sf + lf
            elif "price_pnl" in h:
                cum_price += (h.get("price_pnl", 0) or 0)
                cum_funding += (h.get("funding_pnl", 0) or 0)
                cum_fee += (h.get("fee_total", 0) or 0)

        # 月度盈亏：按日期汇总（使用真实净盈亏）
        month_pnl: dict[str, float] = {}
        for h in history:
            ts = h.get("ts") or h.get("time", "")
            try:
                d = ts[:10]  # YYYY-MM-DD
            except (ValueError, KeyError):
                continue
            month_pnl[d] = month_pnl.get(d, 0) + _get_real_pnl(h)

        # 当日盈亏
        today_str = now.strftime("%Y-%m-%d")
        today_pnl = month_pnl.get(today_str, 0.0)
        today_cls = "positive" if today_pnl >= 0 else "negative"

        # ── 统计 ──
        total_trades = len(history)
        if total_trades > 0:
            wins = sum(1 for h in history if _get_real_pnl(h) > 0)
            win_rate = wins / total_trades * 100
            best = max(_get_real_pnl(h) for h in history)
            worst = min(_get_real_pnl(h) for h in history)
            stats_html = (
                f'<div class="stats">'
                f'<span>总交易 <b>{total_trades}</b> 笔</span>'
                f'<span>胜率 <b>{win_rate:.0f}%</b></span>'
                f'<span>累计 <b class="{profit_cls}">{total_profit:+.4f}</b></span>'
                f'<span>最佳 <b class="positive">+{best:.4f}</b></span>'
                f'<span>最差 <b class="negative">{worst:.4f}</b></span>'
                f'</div>')
        else:
            stats_html = '<div class="stats"><span>暂无交易记录</span></div>'

        # ── 余额快照（资金曲线）──
        snaps = []
        if self.BALANCE_SNAPSHOT_FILE.exists():
            try:
                with self.BALANCE_SNAPSHOT_FILE.open("r", encoding="utf-8") as f:
                    snaps = json.load(f)
            except (OSError, json.JSONDecodeError):
                pass
        curve_labels, curve_total, curve_bn, curve_gt = [], [], [], []
        if snaps:
            step = max(1, len(snaps) // 200)
            first_date = snaps[0].get("ts", "")[:10]
            last_date = snaps[-1].get("ts", "")[:10]
            same_day = first_date == last_date
            for s in snaps[::step]:
                ts = s.get("ts", "")
                if same_day:
                    label = ts[11:16]
                else:
                    label = ts[5:16].replace("T", " ")
                bn_v = s.get("binance", 0)
                gt_v = s.get("gate", 0)
                curve_labels.append(label)
                curve_total.append(round(bn_v + gt_v, 2))
                curve_bn.append(round(bn_v, 2))
                curve_gt.append(round(gt_v, 2))

        # ── 月历数据（JS 动态渲染）──
        pnl_json = json.dumps(month_pnl, ensure_ascii=False)
        today_iso = now.strftime("%Y-%m-%d")

        # ── 余额卡片 ──
        grand_total = futures_usdt + (gate_spot if GATE_TRADING_ENABLED else 0)
        gate_card = ""
        if GATE_TRADING_ENABLED:
            gate_card = f"""<div class="card"><div class="label">Gate 交易</div><div class="value" style="color:#2955E7">{gate_spot:,.2f}</div></div>
            """
        balance_cards = f"""
        <div class="balances">
            <div class="card"><div class="label">BN 合约</div><div class="value" style="color:#f0b90b">{futures_usdt:,.2f}</div></div>
            {gate_card}<div class="card"><div class="label">总计</div><div class="value" style="color:#e0e0e0">{grand_total:,.2f}</div></div>
            <div class="card"><div class="label">累计盈亏</div><div class="value {profit_cls}">{total_profit:+.4f}</div></div>
            <div class="card"><div class="label">今日盈亏</div><div class="value {today_cls}">{today_pnl:+.4f}</div></div>
            <div class="card"><div class="label">平仓盈亏</div><div class="value {"positive" if cum_price>=0 else "negative"}">{cum_price:+.4f}</div></div>
            <div class="card"><div class="label">累计资费</div><div class="value {"positive" if cum_funding>=0 else "negative"}">{cum_funding:+.4f}</div></div>
            <div class="card"><div class="label">手续费</div><div class="value negative">{cum_fee:.4f}</div></div>
        </div>"""

        # ── 当前持仓 ──
        def _pos_html(state, tag):
            if not state.is_open:
                return ""
            pos_notional = state.amount * state.entry_price
            est_profit = pos_notional * state.net_rate
            dir_label = "反向" if state.direction == "reverse" else "正向"
            dir_cls = "reverse" if state.direction == "reverse" else "forward"
            settle_dt = ""
            if state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    state.next_funding_time_ms / 1000, tz=self.tz
                ).strftime("%m-%d %H:%M:%S")
            hold_str = ""
            if state.opened_at:
                try:
                    opened = datetime.fromisoformat(state.opened_at)
                    elapsed = now - opened
                    h = int(elapsed.total_seconds() // 3600)
                    m = int((elapsed.total_seconds() % 3600) // 60)
                    hold_str = f"已持有: {h}h{m:02d}m"
                except (ValueError, TypeError):
                    pass
            return f"""
            <h3>{tag} <span class="muted" style="font-size:0.7rem">{hold_str}</span></h3>
            <div class="position {dir_cls}">
                <span class="badge {dir_cls}">{dir_label}</span>
                <strong>{state.base}</strong>
                <span>数量: {state.amount:.4f}</span>
                <span>入场价: {state.entry_price:.4f}</span>
                <span>预估净利润: <span class="{'positive' if est_profit > 0 else 'negative'}">{est_profit:+.4f} USDT</span></span>
                <span>净费率: <span class="{'positive' if state.net_rate > 0 else 'negative'}">{state.net_rate*100:+.4f}%</span></span>
                <span>下次结算: {settle_dt}</span>
            </div>"""

        if self.binance_state.is_open or self.gate_state.is_open or self.cross_state.is_open:
            cross_html = ""
            if self.cross_state.is_open:
                cs = self.cross_state
                cs_notional = cs.short_amount * cs.short_entry_price if cs.short_entry_price > 0 else cs.amount
                cs_est = cs_notional * cs.total_net_rate
                hold_str = ""
                if cs.opened_at:
                    try:
                        opened = datetime.fromisoformat(cs.opened_at)
                        elapsed = now - opened
                        h = int(elapsed.total_seconds() // 3600)
                        m = int((elapsed.total_seconds() % 3600) // 60)
                        hold_str = f"已持有: {h}h{m:02d}m"
                    except (ValueError, TypeError):
                        pass
                short_settle = ""
                if cs.short_next_funding_time_ms > 0:
                    short_settle = datetime.fromtimestamp(cs.short_next_funding_time_ms / 1000, tz=self.tz).strftime("%m-%d %H:%M:%S")
                long_settle = ""
                if cs.long_next_funding_time_ms > 0:
                    long_settle = datetime.fromtimestamp(cs.long_next_funding_time_ms / 1000, tz=self.tz).strftime("%m-%d %H:%M:%S")
                cross_html = f"""
            <h3>🌐 跨交易所套利 <span class="muted" style="font-size:0.7rem">{hold_str}</span></h3>
            <div class="position cross" style="border:2px solid #f0b90b; background:linear-gradient(135deg,#1a2a1a,#1a1a2a)">
                <strong>{cs.base}</strong>
                <span>数量: 空{cs.short_amount:.4f}张 / 多{cs.long_amount:.4f}张</span>
                <span>费率差: <span class="positive">{cs.rate_spread*100:+.4f}%</span></span>
                <span>预估利润: <span class="{'positive' if cs_est > 0 else 'negative'}">{cs_est:+.4f} USDT</span></span>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:8px">
                <div style="padding:8px;background:rgba(240,75,75,0.15);border-radius:6px">
                    <span style="color:#e74c3c">🔻 做空 @ {cs.short_exchange.upper()}</span><br>
                    <span style="font-size:0.8rem">费率: {cs.short_rate*100:+.4f}% | 结算: {short_settle}</span>
                </div>
                <div style="padding:8px;background:rgba(14,203,129,0.15);border-radius:6px">
                    <span style="color:#0ecb81">🔺 做多 @ {cs.long_exchange.upper()}</span><br>
                    <span style="font-size:0.8rem">费率: {cs.long_rate*100:+.4f}% | 结算: {long_settle}</span>
                </div>
                </div>
            </div>"""
            position_html = f"""
        <div class="section">
            <h2>当前持仓</h2>
            {cross_html}
            {_pos_html(self.binance_state, "Binance")}
            {_pos_html(self.gate_state, "Gate")}
        </div>"""
        else:
            pre_borrow_html = ""
            if self.binance_state.pre_borrow_base:
                elapsed = (now - datetime.fromisoformat(
                    self.binance_state.pre_borrow_at)).total_seconds() / 60
                pre_borrow_html = f"""
        <div class="section">
            <h2>预借等待中</h2>
            <div class="position reverse">
                <span class="badge reverse">反向</span>
                <strong>{self.binance_state.pre_borrow_base}</strong>
                <span>数量: {self.binance_state.pre_borrow_amount}</span>
                <span>已等待: {elapsed:.0f} / {PRE_BORROW_TIMEOUT_MINUTES} min</span>
            </div>
        </div>"""
            position_html = f"""
        <div class="section">
            <h2>当前持仓</h2>
            <p class="muted">无持仓</p>
        </div>
        {pre_borrow_html}"""

        # ── 资金曲线 ──
        has_curve = bool(curve_labels)
        labels_json = json.dumps(curve_labels, ensure_ascii=False) if has_curve else "[]"
        total_json = json.dumps(curve_total, ensure_ascii=False) if has_curve else "[]"
        bn_json = json.dumps(curve_bn, ensure_ascii=False) if has_curve else "[]"
        gt_json = json.dumps(curve_gt, ensure_ascii=False) if has_curve else "[]"

        # ── 月历（JS 动态）──
        calendar_html = f"""
        <div class="section">
            <h2><button class="cal-nav" onclick="navCal(-1)">&#9664;</button> <span id="calTitle">--</span> <button class="cal-nav" onclick="navCal(1)">&#9654;</button></h2>
            <div class="table-wrap">
            <table class="calendar" id="calTable"></table>
            </div>
        </div>"""

        # ── 最近交易 ──
        recent_html = ""
        if history:
            recent = history[-20:][::-1]
            rows = ""
            has_detailed = any("price_pnl" in h or "short_exchange" in h for h in history)
            for h in recent:
                ts_raw = h.get("ts") or h.get("time", "")
                ts_short = ts_raw[:16].replace("T", " ")
                direction = str(h.get("direction", ""))
                if "cross" in direction or "orphan" in direction:
                    tag = "跨所"
                    tag_cls = "cross"
                else:
                    tag = "反" if direction == "reverse" else "正"
                    tag_cls = "reverse" if direction == "reverse" else "forward"
                amount = float(h.get("amount", 0))
                amount_str = f"{amount:.4f}" if amount else "-"
                net_pnl = h.get("net_pnl", h.get("profit_usdt", 0)) or 0
                pnl_cls = "positive" if net_pnl >= 0 else "negative"
                # 新版跨所记录：按交易所拆分
                if "short_exchange" in h:
                    short_ex = h.get("short_exchange", "")
                    long_ex = h.get("long_exchange", "")
                    sp = h.get("short_price_pnl", 0) or 0
                    sf = h.get("short_funding_pnl", 0) or 0
                    sfee = (h.get("short_actual_fee") or h.get("short_fee")) or 0
                    sn = h.get("short_net", 0) or 0
                    lp = h.get("long_price_pnl", 0) or 0
                    lf = h.get("long_funding_pnl", 0) or 0
                    lfee = (h.get("long_actual_fee") or h.get("long_fee")) or 0
                    ln = h.get("long_net", 0) or 0
                    s_exit = h.get("short_exit") or 0
                    l_exit = h.get("long_exit") or 0
                    cd1 = f"{s_exit:.4f}" if s_exit else "-"
                    cd2 = f"{l_exit:.4f}" if l_exit else "-"
                    # 名义价值：优先用记录的 notional，否则用 amount * entry 估算
                    snl = h.get("short_notional") or (float(h.get("amount", 0)) * float(h.get("short_entry", 0)))
                    lnl = h.get("long_notional") or (float(h.get("amount", 0)) * float(h.get("long_entry", 0)))
                    notional_str = f"~{snl:.0f}" if snl else "-"
                    # 分组头行：时间/类型/币种/名义价值 + 汇总净盈亏
                    rows += (f'<tr class="cross-group"><td>{ts_short}</td>'
                             f'<td><span class="badge cross">跨所</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{notional_str}</td>'
                             f'<td class="right muted" colspan="3">平仓价 {short_ex}空{cd1} / {long_ex}多{cd2}</td>'
                             f'<td class="right {pnl_cls}"><b>{net_pnl:+.4f}</b></td></tr>')
                    s_cls = "bn" if short_ex == "binance" else "gt"
                    l_cls = "bn" if long_ex == "binance" else "gt"
                    # 做空侧: 价格盈亏 / 资费 / 手续费 / 净盈亏
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {s_cls}">{short_ex}空</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if sp>=0 else "negative"}">{sp:+.4f}</td>'
                             f'<td class="right {"positive" if sf>=0 else "negative"}">{sf:+.4f}</td>'
                             f'<td class="right negative">{sfee:.4f}</td>'
                             f'<td class="right {"positive" if sn>=0 else "negative"}">{sn:+.4f}</td></tr>')
                    # 做多侧
                    rows += (f'<tr class="cross-sub"><td></td>'
                             f'<td class="right"><span class="badge {l_cls}">{long_ex}多</span></td>'
                             f'<td></td><td class="right"></td>'
                             f'<td class="right {"positive" if lp>=0 else "negative"}">{lp:+.4f}</td>'
                             f'<td class="right {"positive" if lf>=0 else "negative"}">{lf:+.4f}</td>'
                             f'<td class="right negative">{lfee:.4f}</td>'
                             f'<td class="right {"positive" if ln>=0 else "negative"}">{ln:+.4f}</td></tr>')
                elif "price_pnl" in h:
                    # 旧版跨所记录（无拆分）：直接展示汇总
                    price_pnl = h.get("price_pnl", 0) or 0
                    funding_pnl = h.get("funding_pnl", 0) or 0
                    fee_total = h.get("fee_total", 0) or 0
                    rows += (f'<tr><td>{ts_short}</td>'
                             f'<td><span class="badge {tag_cls}">{tag}</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{amount_str}</td>'
                             f'<td class="right {"positive" if price_pnl>=0 else "negative"}">{price_pnl:+.4f}</td>'
                             f'<td class="right positive">{funding_pnl:+.4f}</td>'
                             f'<td class="right negative">{fee_total:.4f}</td>'
                             f'<td class="right {pnl_cls}">{net_pnl:+.4f}</td></tr>')
                else:
                    pnl = float(h.get("profit_usdt", 0))
                    rows += (f'<tr><td>{ts_short}</td>'
                             f'<td><span class="badge {tag_cls}">{tag}</span></td>'
                             f'<td>{h["coin"]}</td>'
                             f'<td class="right">{amount_str}</td>'
                             f'<td class="right">-</td><td class="right">-</td><td class="right">-</td>'
                             f'<td class="right {pnl_cls}">{pnl:+.4f}</td></tr>')
            header = '<tr><th>时间</th><th>类型</th><th>币种</th><th class="right">价值(USDT)</th><th class="right">价格盈亏</th><th class="right">资费</th><th class="right">手续费</th><th class="right">净盈亏</th></tr>'
            recent_html = f"""
        <div class="section">
            <h2>历史交易 <span class="muted">(最近20笔)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>{header}</thead>
                <tbody>{rows}</tbody>
            </table>
            </div>
        </div>"""

        # ── Top 10 表格 ──
        rows_html = ""
        if top10:
            for row in top10:
                nft = float(row.get("next_funding_time_ms", 0))
                if nft > 0:
                    mins = max(0, (nft - time.time() * 1000) / 60000)
                    countdown = f"{int(mins)}min" if mins < 120 else f"{mins/60:.0f}h"
                else:
                    countdown = "N/A"
                direction = str(row.get("direction", "forward"))
                dir_label = "反" if direction == "reverse" else "正"
                dir_cls = "reverse" if direction == "reverse" else "forward"
                net_rate = float(row.get("net_rate", 0))
                net_cls = "positive" if net_rate > 0 else "negative"
                borrow = float(row.get("borrow_hourly_rate", 0))
                exchange = str(row.get("exchange", ""))
                exc_cls = "gt" if exchange == "GT" else "bn"
                held = False
                if self.binance_state.is_open:
                    held = held or (str(row["base"]) == self.binance_state.base
                                   and direction == self.binance_state.direction)
                if self.gate_state.is_open:
                    held = held or (str(row["base"]) == self.gate_state.base
                                   and direction == self.gate_state.direction)
                held_mark = " ★" if held else ""
                spot_p = float(row.get("spot_last", 0) or 0)
                fut_p = float(row.get("futures_last", 0) or 0)
                rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="badge {exc_cls}">{exchange}</span></td>
                    <td><span class="badge {dir_cls}">{dir_label}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td class="right">{spot_p:.6f}</td>
                    <td class="right">{fut_p:.6f}</td>
                    <td class="right">{float(row['predicted_funding_rate'])*100:+.3f}%</td>
                    <td class="right">{float(row['spot_taker_fee'])*100:.3f}%</td>
                    <td class="right">{float(row['futures_taker_fee'])*100:.3f}%</td>
                    <td class="right">{borrow*100:.3f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td class="right">{countdown}</td>
                </tr>"""
        else:
            rows_html = '<tr><td colspan="11" class="muted center">暂无套利机会</td></tr>'

        table_html = f"""
        <div class="section">
            <h2>套利机会 Top 10 <span class="muted">(Binance+Gate)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>所</th><th>方向</th><th>币种</th><th class="right">现货价</th><th class="right">合约价</th>
                        <th class="right">费率</th><th class="right">现货费</th><th class="right">合约费</th>
                        <th class="right">借币费</th><th class="right">净收益</th><th class="right">结算</th>
                    </tr>
                </thead>
                <tbody>{rows_html}</tbody>
            </table>
            </div>
        </div>"""

        # ── 跨交易所机会表格 ──
        cross_rows_html = ""
        if cross_top10:
            for row in cross_top10:
                rate_spread = float(row.get("rate_spread", 0))
                net_rate = float(row.get("net_rate", 0))
                short_ex = str(row.get("short_exchange", ""))
                long_ex = str(row.get("long_exchange", ""))
                short_rate = float(row.get("short_rate", 0))
                long_rate = float(row.get("long_rate", 0))
                net_cls = "positive" if net_rate > 0 else "negative"
                passes = row.get("passes", True)
                short_nft = float(row.get("short_next_funding_time_ms", 0))
                long_nft = float(row.get("long_next_funding_time_ms", 0))
                now_ms = time.time() * 1000
                s_mins = max(0, (short_nft - now_ms) / 60000) if short_nft > 0 else -1
                l_mins = max(0, (long_nft - now_ms) / 60000) if long_nft > 0 else -1
                s_countdown = f"{int(s_mins)}min" if s_mins >= 0 else "N/A"
                l_countdown = f"{int(l_mins)}min" if l_mins >= 0 else "N/A"
                held = self.cross_state.is_open and str(row.get("base", "")) == self.cross_state.base
                held_mark = " ★" if held else ""
                pass_mark = "✓" if passes else "✗"
                pass_cls = "pass" if passes else "fail"
                bn_p = float(row.get("bn_price", 0))
                gt_p = float(row.get("gt_price", 0))
                short_cls = "bn" if short_ex == "binance" else "gt"
                long_cls = "bn" if long_ex == "binance" else "gt"
                cross_rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="{pass_cls}">{pass_mark}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td class="right">{bn_p:.6f}</td>
                    <td class="right">{gt_p:.6f}</td>
                    <td class="right">{rate_spread*100:+.4f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td><span class="badge {short_cls}">{short_ex.upper()}</span></td>
                    <td class="right">{short_rate*100:+.4f}%</td>
                    <td class="right">{s_countdown}</td>
                    <td><span class="badge {long_cls}">{long_ex.upper()}</span></td>
                    <td class="right">{long_rate*100:+.4f}%</td>
                    <td class="right">{l_countdown}</td>
                </tr>"""
        else:
            cross_rows_html = '<tr><td colspan="12" class="muted center">暂无跨所套利机会</td></tr>'

        cross_table_html = f"""
        <div class="section">
            <h2>跨交易所套利机会 <span class="muted">(BN↔GT 纯期货对冲)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>通过</th><th>币种</th><th class="right">BN价</th><th class="right">GT价</th><th class="right">费率差</th><th class="right">净收益</th>
                        <th>做空所</th><th class="right">空费率</th><th class="right">空结算</th>
                        <th>做多所</th><th class="right">多费率</th><th class="right">多结算</th>
                    </tr>
                </thead>
                <tbody>{cross_rows_html}</tbody>
            </table>
            </div>
        </div>"""

        chart_section = ""
        chart_js = ""
        if has_curve:
            chart_section = """
        <div class="section">
            <h2>资金曲线</h2>
            <div style="position:relative;height:320px"><canvas id="equityChart"></canvas></div>
        </div>"""
            chart_js = f"""
<script>
(function() {{
    try {{
        var ctx = document.getElementById('equityChart');
        if (!ctx) return;
        new Chart(ctx, {{
            type: 'line',
            data: {{
                labels: {labels_json},
                datasets: [
                    {{
                        label: '总计',
                        data: {total_json},
                        borderColor: '#4caf84',
                        backgroundColor: 'rgba(76,175,132,0.05)',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 2.0,
                        tension: 0.3,
                    }},
                    {{
                        label: 'BN',
                        data: {bn_json},
                        borderColor: '#f0b90b',
                        backgroundColor: 'transparent',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 1.0,
                        borderDash: [5, 5],
                        tension: 0.3,
                    }},
                    {{
                        label: 'Gate',
                        data: {gt_json},
                        borderColor: '#17c9a4',
                        backgroundColor: 'transparent',
                        fill: false,
                        pointRadius: 0,
                        borderWidth: 1.0,
                        borderDash: [3, 3],
                        tension: 0.3,
                    }}
                ]
            }},
            options: {{
                responsive: true,
                maintainAspectRatio: false,
                plugins: {{ legend: {{ display: true, position: 'top', labels: {{ color: '#aaa', font: {{ size: 10 }}, boxWidth: 16 }} }} }},
                scales: {{
                    x: {{ ticks: {{ maxTicksLimit: 12, maxRotation: 0, color: '#666', font: {{ size: 10 }} }}, grid: {{ color: '#252540' }} }},
                    y: {{ ticks: {{ color: '#666', font: {{ size: 10 }} }}, grid: {{ color: '#252540' }} }}
                }},
                interaction: {{ intersect: false, mode: 'index' }}
            }}
        }});
    }} catch(e) {{}}
}})();
</script>"""

        # ── 狙击机会表格 ──
        snipe_rows_html = ""
        if not snipe_df.empty:
            for _, row in snipe_df.head(10).iterrows():
                rate = float(row.get("funding_rate", 0))
                net_rate = float(row.get("net_rate", 0))
                direction = str(row.get("direction", ""))
                net_cls = "positive" if net_rate > 0 else "negative"
                passes = row.get("passes", True)
                pass_mark = "✓" if passes else "✗"
                pass_cls = "pass" if passes else "fail"
                nft = float(row.get("next_funding_time_ms", 0))
                now_ms = time.time() * 1000
                mins = max(0, (nft - now_ms) / 60000) if nft > 0 else -1
                cd = f"{int(mins)}min" if mins >= 0 else "N/A"
                settle_local = str(row.get("settle_local", "?")) if nft > 0 else "?"
                vol = float(row.get("quote_volume", 0) or 0)
                if vol >= 1_000_000:
                    vol_str = f"{vol/1e6:.1f}M"
                elif vol >= 1_000:
                    vol_str = f"{vol/1e3:.0f}K"
                else:
                    vol_str = f"{vol:.0f}"
                held = self._bn_snipe.is_open if getattr(self, '_bn_snipe', None) else False
                held_mark = " ★" if held else ""
                snipe_rows_html += f"""
                <tr class="{'held' if held else ''}">
                    <td><span class="{pass_cls}">{pass_mark}</span></td>
                    <td>{str(row['base'])[:8]}{held_mark}</td>
                    <td class="right">{float(row['futures_price']):.6f}</td>
                    <td class="right">{rate*100:+.4f}%</td>
                    <td class="right">{abs(rate)*100:.4f}%</td>
                    <td class="right {net_cls}">{net_rate*100:+.4f}%</td>
                    <td><span class="badge {'short' if direction=='short' else 'long'}">{direction.upper()}</span></td>
                    <td class="right">{vol_str}</td>
                    <td class="right">{cd}</td>
                    <td class="right">{settle_local}</td>
                </tr>"""
        else:
            snipe_rows_html = '<tr><td colspan="10" class="muted center">暂无狙击机会</td></tr>'

        snipe_table_html = f"""
        <div class="section">
            <h2>Binance 资金费率狙击 <span class="muted">(裸合约, 无对冲)</span></h2>
            <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>通过</th><th>币种</th><th class="right">合约价</th><th class="right">费率</th><th class="right">abs费率</th><th class="right">净收益</th><th>方向</th><th class="right">24h成交</th><th class="right">距结算</th><th class="right">结算</th>
                    </tr>
                </thead>
                <tbody>{snipe_rows_html}</tbody>
            </table>
            </div>
        </div>"""

        template = (_TEMPLATE_DIR / "dashboard.html").read_text(encoding="utf-8")
        html = _string_mod.Template(template).substitute(
            now_str=now_str,
            balance_cards=balance_cards,
            stats_html=stats_html,
            position_html=position_html,
            calendar_html=calendar_html,
            chart_section=chart_section,
            recent_html=recent_html,
            table_html=table_html,
            cross_table_html=cross_table_html,
            snipe_table_html=snipe_table_html,
            pnl_json=pnl_json,
            today_iso=today_iso,
            labels_json=labels_json,
            total_json=total_json,
            bn_json=bn_json,
            gt_json=gt_json,
            chart_js=chart_js,
        )

        dashboard_path = Path("data/dashboard.html")
        dashboard_path.parent.mkdir(parents=True, exist_ok=True)
        dashboard_path.write_text(html, encoding="utf-8")


def _print_opportunity_table(df: pd.DataFrame, ref_usdt: float, direction: str = "positive") -> None:
    """格式化打印套利机会表格。"""
    top20 = df.head(20)
    if direction == "reverse":
        header = (
            f"  反向套利机会 (Negative Funding) — 借币卖出 + 做多合约，收取空头费率\n"
            f"  借币成本已计入净收益（按 1 小时利息计算）\n"
            f"  共 {len(df)} 个机会, 显示 Top 20 | 利润按每 {ref_usdt:.0f} USDT/腿 参考值"
        )
        col_header = (
            f"  {'排名':<4} {'币种':<10} {'现货价':>10} {'合约价':>10} {'来源':<6} {'链':<8} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {spot_p:>10.6f} {fut_p:>10.6f} {source:<6} {chain:<8} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% {borrow:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )
    else:
        header = (
            f"  正向套利机会 (Positive Funding) — 做多现货 + 做空合约，收取多头费率\n"
            f"  共 {len(df)} 个机会, 显示 Top 20 | 利润按每 {ref_usdt:.0f} USDT/腿 参考值"
        )
        col_header = (
            f"  {'排名':<4} {'币种':<10} {'现货价':>10} {'合约价':>10} {'来源':<6} {'链':<12} "
            f"{'费率':>8} {'现货费':>8} {'合约费':>8} {'净收益':>8} {'利润(USDT)':>10}"
        )
        row_fmt = (
            "  {rank:<4} {base:<10} {spot_p:>10.6f} {fut_p:>10.6f} {source:<6} {chain:<12} "
            "{rate:>7.3f}% {spot_fee:>7.3f}% {fut_fee:>7.3f}% "
            "{net:>7.3f}% {profit:>9.4f}"
        )

    print(f"\n{'='*110}")
    print(header)
    print(f"{'='*110}")
    print(col_header)
    print(f"  {'-'*108}")

    for rank, (_, row) in enumerate(top20.iterrows(), 1):
        kwargs = dict(
            rank=rank,
            base=str(row["base"])[:10],
            source=str(row["spot_source"])[:6],
            chain=str(row.get("chain", ""))[:12] if direction == "positive" else str(row.get("chain", ""))[:8],
            spot_p=float(row.get("spot_last", 0) or 0),
            fut_p=float(row.get("futures_last", 0) or 0),
            rate=float(row["predicted_funding_rate"]) * 100,
            spot_fee=float(row["spot_taker_fee"]) * 100,
            fut_fee=float(row["futures_taker_fee"]) * 100,
            net=float(row["net_rate"]) * 100,
            profit=float(row["net_rate"]) * ref_usdt,
        )
        if direction == "reverse":
            kwargs["borrow"] = float(row.get("borrow_hourly_rate", 0)) * 100
        print(row_fmt.format(**kwargs))

    print(f"{'='*90}\n")

