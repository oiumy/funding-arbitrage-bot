"""Binance 资金费率狙击: 裸合约单向持仓，收取极端费率后立即平仓。"""
from __future__ import annotations
import asyncio
import gc
import time
from typing import Any, TYPE_CHECKING

import pandas as pd

from ..constants import *
from .. import constants as _c
from ..models import LegResult, _safe_float

if TYPE_CHECKING:
    from ..bot import FundingArbitrageBot


class BnSnipeStrategy:
    """Binance 资金费率狙击策略。扫描全市场 USDT 永续，选取 abs(费率) 最高、
    覆盖 2x 手续费且通过流动性筛选的币种，在结算前瞬间开裸单，
    结算时间到立即平仓。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot
        self.is_open: bool = False
        self.symbol: str = ""
        self.amount: float = 0.0
        self.direction: str = ""  # "short" 或 "long"
        self.next_funding_time_ms: float = 0.0
        self.entry_price: float = 0.0
        self.funding_rate: float = 0.0
        self._next_snipe_settle_ms: float = 0.0
        self._snipe_loop_guard: bool = False
        self.is_sniping: bool = False       # 并发穿透锁，防外部定时器重入
        self.current_delay_ms: float = _c.CROSS_SNIPER_CLOSE_DELAY_MS

    # ── 扫描 ───────────────────────────────────────────────

    async def scan(self, position_usdt: float = 100.0) -> pd.DataFrame:
        """扫描全市场 USDT 永续，按 abs(资金费率) 降序排列。"""
        futures_tickers, funding_rates, fee_pair = await asyncio.gather(
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
        _spot_fees, futures_taker_fees = fee_pair
        default_futures_fee = futures_taker_fees.get("__default__", DEFAULT_FUTURES_TAKER_FEE)

        futures_market_index = self.bot._build_futures_market_index()

        rows: list[dict[str, Any]] = []
        for base, market in futures_market_index.items():
            symbol = market["symbol"]
            ticker = futures_tickers.get(symbol, {})
            quote_volume = _safe_float(ticker.get("quoteVolume"))
            if not self.bot._futures_passes_volume(quote_volume):
                continue

            funding_item = funding_rates.get(symbol, {})
            rate, _is_predicted = self.bot._extract_predicted_funding_rate(funding_item)
            if rate is None:
                continue

            next_ft = self.bot._extract_next_funding_time(funding_item)
            abs_rate = abs(rate)
            futures_fee = futures_taker_fees.get(symbol, default_futures_fee)
            net_rate = abs_rate - 2 * futures_fee
            direction = "short" if rate > 0 else "long"

            passes = (
                net_rate > 0
                and abs_rate >= _c.BN_SNIPE_MIN_ABS_RATE
            )

            remain_s = (next_ft - time.time() * 1000) / 1000 if (next_ft is not None and next_ft > 0) else 0
            settle_str = time.strftime("%H:%M", time.localtime(next_ft / 1000)) if (next_ft is not None and next_ft > 0) else "?"

            interval_hours = int(market["info"].get("fundingIntervalHours", DEFAULT_FUNDING_INTERVAL_HOURS))

            rows.append({
                "symbol": symbol,
                "base": base,
                "futures_price": _safe_float(ticker.get("last")),
                "funding_rate": rate,
                "abs_rate": abs_rate,
                "net_rate": net_rate,
                "direction": direction,
                "next_funding_time_ms": next_ft,
                "futures_taker_fee": futures_fee,
                "passes": passes,
                "quote_volume": quote_volume,
                "remain_min": remain_s / 60,
                "settle_local": settle_str,
                "interval_hours": interval_hours,
            })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.sort_values("abs_rate", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    # ── 开仓 ───────────────────────────────────────────────

    async def open_position_fast(self, args: dict) -> bool:
        """极速开仓：接收 T-10s 预解包的原生 dict，零 Pandas 开销直发 WS。"""
        symbol = args["symbol"]
        direction = args["direction"]
        amount = args["amount"]
        rate = args["rate"]
        nft = args["nft"]
        price = args["price"]

        send_ts = time.time()
        t0 = time.perf_counter()
        if direction == "short":
            result = await self.bot._cross_open_short_leg("binance", symbol, amount)
        else:
            result = await self.bot._cross_open_long_leg("binance", symbol, amount)
        rtt_ms = (time.perf_counter() - t0) * 1000
        send_str = time.strftime("%H:%M:%S", time.localtime(send_ts))
        send_str += f".{int((send_ts % 1) * 1000):03d}"

        if not result.ok:
            logger.warning("[狙击] 开仓失败 %s: %s | 发送=%s | RTT=%.1fms",
                           symbol, result.error, send_str, rtt_ms)
            return False

        self.is_open = True
        self.symbol = symbol
        self.amount = amount
        self.direction = direction
        self.next_funding_time_ms = nft
        self.entry_price = price
        self.funding_rate = rate
        logger.info("[狙击] 开仓成功 %s: %s %.4f张 @ %.6f 费率=%.4f%% | 发送=%s | RTT=%.1fms",
                     symbol, direction, amount, price, rate * 100, send_str, rtt_ms)
        return True

    # ── 平仓 ───────────────────────────────────────────────

    async def close_position_fast(self) -> tuple[bool, float]:
        """极速平仓。先开枪后说话，重试间隔 2ms。返回 (成功, 纯净RTT_ms)。
        ReduceOnly 拒绝 = 首次已成交，视为成功。"""
        if not self.is_open:
            return True, 0.0

        sym, amt, d = self.symbol, self.amount, self.direction
        send_ts = time.time()
        t0 = time.perf_counter()
        for attempt in range(3):
            if d == "short":
                result = await self.bot._cross_close_short_leg("binance", sym, amt)
            else:
                result = await self.bot._cross_close_long_leg("binance", sym, amt)
            if result.ok:
                break
            # ReduceOnly = 仓位已被首次平掉，视为成功
            if result.error and "-2022" in str(result.error):
                logger.info("[狙击] 平仓重试命中 ReduceOnly，首次已成交，视为成功")
                result = LegResult(True, result.market_type, result.symbol, result.side, result.amount)
                break
            if attempt < 2:
                logger.warning("[狙击] 平仓受阻！第 %d/3 次极速重试", attempt + 1)
                await asyncio.sleep(0.002)
        rtt_ms = (time.perf_counter() - t0) * 1000
        send_str = time.strftime("%H:%M:%S", time.localtime(send_ts))
        send_str += f".{int((send_ts % 1) * 1000):03d}"

        if not result.ok:
            logger.critical("[狙击] 平仓失败！%s | 发送=%s", result.error, send_str)
            return False, rtt_ms

        # 只有确认平仓成功后才清状态，防止幽灵裸仓
        self.is_open = False
        self.symbol = ""
        self.amount = 0.0
        self.direction = ""
        self.next_funding_time_ms = 0.0
        self.entry_price = 0.0
        self.funding_rate = 0.0

        logger.info("[狙击] 平仓成功 %s %s %.4f张 | 发送=%s | RTT=%.1fms",
                     sym, d, amt, send_str, rtt_ms)
        return True, rtt_ms

    # ── 辅助 ───────────────────────────────────────────────

    def should_exit(self) -> bool:
        """结算时间+延时已过则退出。与自旋锁共用同一延时，防止顶部持仓管理提前抢跑。"""
        if not self.is_open:
            return False
        if self.next_funding_time_ms <= 0:
            return True
        return time.time() * 1000 >= (self.next_funding_time_ms + self.current_delay_ms)

    async def check_position(self) -> bool:
        """WS 缓存验证裸仓是否存在。"""
        if not self.is_open:
            return False
        pos_side = "SHORT" if self.direction == "short" else "LONG"
        ok, _ = self.bot._ws_position_check("binance", self.symbol, pos_side, expect_zero=False, amount=self.amount)
        if not ok:
            logger.warning("[狙击] 仓位丢失 %s %s", self.symbol, pos_side)
            self.is_open = False
            return False
        return True

    # ── 主循环 ─────────────────────────────────────────────

    async def run_cycle(self) -> None:
        """狙击主循环。"""
        # ── 持仓管理 ──
        if self.is_open:
            if not await self.check_position():
                return
            if self.should_exit():
                success, _ = await self.close_position_fast()
                return
            remain_ms = self.next_funding_time_ms - time.time() * 1000
            logger.info("[狙击] 持有 %s %s, 距结算 %dms", self.symbol, self.direction, int(remain_ms))
            return

        # ── 狙击模式 ──
        snipe_ms = self._next_snipe_settle_ms
        if snipe_ms:
            remain_ms = snipe_ms - time.time() * 1000
            if remain_ms <= 0:
                self._next_snipe_settle_ms = 0.0
            elif remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000:
                # 防外部定时器并发穿透：同一时刻只允许一个狙击协程
                if self.is_sniping:
                    return
                self.is_sniping = True
                try:
                    # Step 1: T-10s 扫描 + 设杠杆
                    scan_at_ms = snipe_ms - CROSS_SNIPER_SCAN_OFFSET_SEC * 1000
                    await asyncio.sleep(max(0, (scan_at_ms - time.time() * 1000) / 1000))
                    logger.info("[狙击] 狙击扫描：距结算 %dms",
                                max(0, int(snipe_ms - time.time() * 1000)))

                    bal = await self.bot._cross_get_bn_futures_balance()
                    if bal <= 0:
                        logger.error("[狙击] 余额不足 %.2f", bal)
                        return
                    position_usdt = bal * _c.BN_SNIPE_POSITION_SIZE_RATIO * CROSS_LEVERAGE
                    try:
                        df = await asyncio.wait_for(self.scan(position_usdt), timeout=12)
                    except asyncio.TimeoutError:
                        logger.warning("[狙击] 扫描超时，放弃")
                        return

                    passing = df[df["passes"] == True] if not df.empty else pd.DataFrame()
                    if passing.empty:
                        logger.info("[狙击] 无合格候选")
                        return

                    # 遍历候选直到能开仓，预计算 amount
                    chosen = None
                    chosen_amount = 0.0
                    for idx in range(min(10, len(passing))):
                        candidate = passing.iloc[idx]
                        sym = str(candidate["symbol"])
                        price = float(candidate["futures_price"])
                        amount = await self.bot._cross_calculate_amount("binance", sym, price, position_usdt)
                        if amount > 0:
                            chosen = candidate
                            chosen_amount = amount
                            logger.info("[狙击] 目标 #%d %s: 费率=%.4f%% 方向=%s 张数=%.4f",
                                         idx + 1, sym, float(candidate["funding_rate"]) * 100,
                                         str(candidate["direction"]), amount)
                            break
                        logger.warning("[狙击] #%d %s 数量=%.4f，尝试下一位", idx + 1, sym, amount)

                    if chosen is None:
                        logger.info("[狙击] 所有候选数量不足")
                        return

                    # T-10s 预处理: 设杠杆 + 动态延时矩阵
                    await self.bot._binance_set_leverage(str(chosen["symbol"]))

                    rate_abs = abs(float(chosen["funding_rate"]))
                    nft_ms = float(chosen["next_funding_time_ms"])
                    settle_hour = time.localtime(nft_ms / 1000).tm_hour
                    interval_hours = int(chosen.get("interval_hours", DEFAULT_FUNDING_INTERVAL_HOURS))

                    # ── 两段式时延: 宏观大盘 + 微观妖币加固 ──
                    # 第一段: 按结算钟点锁定基础大盘延迟
                    if settle_hour == 0:
                        base_delay = 300.0  # 00:00 跨夜地狱: 全站日线收线+资产对账
                    elif settle_hour in (8, 16):
                        base_delay = 150.0  # 08/16 主力整点: 全站 8h+4h+2h+1h 齐结算
                    else:
                        base_delay = 80.0   # 4h/2h/1h 轻载节点

                    # 第二段: 妖币局部队列加固 — 撮合队列瘫痪 + 清算账本遍历延迟
                    if rate_abs >= 0.015:
                        self.current_delay_ms = base_delay + 50.0
                    else:
                        self.current_delay_ms = base_delay

                    logger.info("[狙击] 目标 %s | 节点 %02d:00 | 周期 %dh | 妖币=%s | 延时: %dms",
                                 str(chosen["symbol"]), settle_hour, interval_hours,
                                 "YES" if rate_abs >= 0.015 else "NO", int(self.current_delay_ms))

                    fast_args = {
                        "symbol": str(chosen["symbol"]),
                        "direction": str(chosen["direction"]),
                        "amount": chosen_amount,
                        "price": float(chosen["futures_price"]),
                        "nft": nft_ms,
                        "rate": float(chosen["funding_rate"]),
                    }

                    # Step 2: T-2s 战术网络热身 → T-1s 开仓
                    warmup_at_ms = snipe_ms - 2000
                    remain_to_warmup = warmup_at_ms - time.time() * 1000
                    if remain_to_warmup > 0:
                        await asyncio.sleep(remain_to_warmup / 1000)
                        try:
                            await asyncio.wait_for(self.bot.futures.fetch_time(), timeout=2)
                        except Exception:
                            pass

                    open_at_ms = snipe_ms - CROSS_SNIPER_OPEN_OFFSET_MS
                    logger.info("[狙击] 锁定 %s，T-%dms 准时开枪",
                                 fast_args["symbol"], CROSS_SNIPER_OPEN_OFFSET_MS)
                    await asyncio.sleep(max(0, (open_at_ms - time.time() * 1000) / 1000))

                    # 终极防线: 关闭 GC, 开仓→平仓这 1s 内绝不允许 GC 背刺
                    gc.disable()
                    try:
                        ok = await self.open_position_fast(fast_args)

                        if ok:
                            # Step 3: 结算后 +delay_ms 混合自旋平仓
                            snipe_sec = (snipe_ms + self.current_delay_ms) / 1000.0
                            now_perf = time.perf_counter()
                            now_wall = time.time()
                            target_perf = now_perf + (snipe_sec - now_wall)

                            get_perf = time.perf_counter  # 局部绑定, 零字典查找

                            time_left_ms = (target_perf - get_perf()) * 1000
                            if time_left_ms > 30:
                                await asyncio.sleep((time_left_ms - 30) / 1000)

                            # 用硬件单调时钟自旋, 比 time.time() 快数倍且零抖动
                            while get_perf() < target_perf:
                                pass
                            closed, pure_rtt = await self.close_position_fast()
                            logger.info("[延迟] 平仓纯净 RTT: %.1fms", pure_rtt)
                    finally:
                        gc.enable()
                finally:
                    self.is_sniping = False
                    self._next_snipe_settle_ms = 0.0
                return

        # ── 常规扫描 ──
        bal = await self.bot._cross_get_bn_futures_balance()
        self._dash_balance = bal
        if bal <= 0:
            logger.error("[狙击] 余额不足 %.2f，等待", bal)
            return
        position_usdt = bal * _c.BN_SNIPE_POSITION_SIZE_RATIO * CROSS_LEVERAGE
        logger.info("┏━━ [狙击] 主扫开始 | 余额=%.2f | 可用仓位=%.2f ━━", bal, position_usdt)
        df = await self.scan(position_usdt)
        self._dash_df = df

        if df.empty:
            logger.info("[狙击] 无符合条件的币种。")
            return

        passing = df[df["passes"] == True] if "passes" in df.columns else pd.DataFrame()
        self._print_opportunity_table(df, position_usdt)

        if not passing.empty:
            best = passing.iloc[0]
            self._next_snipe_settle_ms = float(best["next_funding_time_ms"])
            settle_local = str(best.get("settle_local", "?"))
            remain_min = float(best.get("remain_min", 0) or 0)
            remain_str = f"{int(remain_min//60)}h{int(remain_min%60):02d}m" if remain_min >= 60 else f"{int(remain_min)}m"
            logger.info("[狙击] 下次狙击目标: %s 费率=%.4f%% 方向=%s 结算=%s(北京时间) 还剩%s",
                         str(best["symbol"]), float(best["funding_rate"]) * 100,
                         str(best["direction"]), settle_local, remain_str)
        else:
            self._next_snipe_settle_ms = 0.0

        # 若刚发现机会且结算在 2*snipe_window 内 → 直接跳狙击
        if not self._snipe_loop_guard and self._next_snipe_settle_ms:
            remain_ms = self._next_snipe_settle_ms - time.time() * 1000
            if 0 < remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000 * 2:
                logger.info("[狙击] 距结算仅 %dms，直接进入狙击流程", int(remain_ms))
                self._snipe_loop_guard = True
                try:
                    await self.run_cycle()
                finally:
                    self._snipe_loop_guard = False

    def _print_opportunity_table(self, df: pd.DataFrame, position_usdt: float) -> None:
        """打印机会表格。"""
        top = df.head(10)
        if top.empty:
            return

        def _fmt_vol(v: float) -> str:
            if v >= 1_000_000:
                return f"{v/1e6:.1f}M"
            elif v >= 1_000:
                return f"{v/1e3:.0f}K"
            return f"{v:.0f}"

        def _fmt_remain(m: float) -> str:
            if m >= 60:
                return f"{int(m//60)}h{int(m%60):02d}m"
            return f"{int(m)}m"

        logger.info("  ═══════════════════════════════════════════════════════════════════")
        logger.info("  币安资金费率狙击 (裸合约, 无对冲) — 可用 ≈%.0f USDT", position_usdt)
        logger.info("  ═══════════════════════════════════════════════════════════════════")
        logger.info("    %-10s %9s %8s %7s %7s %5s %7s %7s %6s %5s",
                    "币种", "价格", "费率%", "abs%", "净收益%", "方向",
                    "24h成交", "距结算", "结算", "通过")
        logger.info("  ------------------------------------------------------------------")
        for _, row in top.iterrows():
            price_f = float(row["futures_price"])
            vol = float(row.get("quote_volume", 0) or 0)
            remain_m = float(row.get("remain_min", 0) or 0)
            settle = str(row.get("settle_local", "?"))
            logger.info("    %s%-10s %9s %7.2f%% %6.2f%% %6.2f%% %5s %7s %7s %6s %5s",
                        "✓" if row["passes"] else " ",
                        str(row["base"]),
                        f"{price_f:.6f}" if price_f < 1 else f"{price_f:.4f}",
                        float(row["funding_rate"]) * 100,
                        float(row["abs_rate"]) * 100,
                        float(row["net_rate"]) * 100,
                        str(row["direction"]),
                        _fmt_vol(vol),
                        _fmt_remain(remain_m),
                        settle,
                        "✓" if row["passes"] else "✗")
        logger.info("  ═══════════════════════════════════════════════════════════════════")
