"""跨交易所资金费率差套利: Binance vs Gate 合约对冲。"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime
from typing import Any, TYPE_CHECKING

import pandas as pd

from ..constants import *
from .. import constants as _c
from ..models import CrossArbitrageState

if TYPE_CHECKING:
    from ..bot import FundingArbitrageBot


class CrossExchangeStrategy:
    """跨交易所资金费率差套利: Binance vs Gate 合约对冲。"""

    def __init__(self, bot: FundingArbitrageBot) -> None:
        self.bot: FundingArbitrageBot = bot

    async def scan(self, position_usdt: float = 100.0, strict: bool = True, snipe: bool = False) -> pd.DataFrame:
        """扫描跨交易所费率差异机会：纯期货多空对冲。

        高费率所做空 + 低费率所做多 = delta 中性，赚费率差。
        strict=False: 放宽筛选，用于展示接近阈值的候选。
        snipe=True: 狙击刷新模式，只拉费率+Gate价格，复用缓存中的手续费和BN价格。
        """
        if not _c.CROSS_EXCHANGE_ENABLED:
            return pd.DataFrame()

        cache = getattr(self.bot, "_cross_scan_cache", {})

        if snipe and cache:
            # 狙击模式：只刷新 BN 资金费率 + Gate tickers（含费率），手续费/BN价格不变
            bn_funding, gt_tickers = await asyncio.gather(
                self.bot._safe_request("futures.fetch_funding_rates",
                                   lambda: self.bot.futures.fetch_funding_rates(), default={}),
                self.bot._safe_request("gate_futures.fetch_tickers_direct",
                                   lambda: self.bot._fetch_gate_futures_tickers_direct(), default={}),
            )
            gt_funding = await self.bot._fetch_gate_funding_rates_direct(gt_tickers)
            bn_fee_pair = cache["bn_fee_pair"]
            gt_fee_pair = cache["gt_fee_pair"]
            bn_tickers = cache["bn_tickers"]
            # 更新缓存中的费率和 GT tickers（但不改 ts，主扫缓存仍有效）
            cache["bn_funding"] = bn_funding
            cache["gt_funding"] = gt_funding
            cache["gt_tickers"] = gt_tickers
        else:
            # 复用缓存数据，避免严格+宽松两次扫描重复拉取
            now = time.time()
            if cache and (now - cache.get("ts", 0)) < 120:
                bn_fee_pair = cache["bn_fee_pair"]
                gt_fee_pair = cache["gt_fee_pair"]
                bn_funding = cache["bn_funding"]
                gt_funding = cache["gt_funding"]
                bn_tickers = cache["bn_tickers"]
                gt_tickers = cache["gt_tickers"]
            else:
                # 所有 API 并行拉取：tickers + 费率 + 资金费率
                gt_tickers_raw, bn_fee_pair, gt_fee_pair, bn_funding, bn_tickers = await asyncio.gather(
                    self.bot._safe_request("gate_futures.fetch_tickers_direct",
                                       lambda: self.bot._fetch_gate_futures_tickers_direct(), default={}),
                    self.bot.fetch_taker_fees(),
                    self.bot._fetch_gate_taker_fees(),
                    self.bot._safe_request("futures.fetch_funding_rates",
                                       lambda: self.bot.futures.fetch_funding_rates(), default={}),
                    self.bot._safe_request("futures.fetch_tickers",
                                       lambda: self.bot.futures.fetch_tickers(), default={}),
                )
                gt_tickers = gt_tickers_raw
                gt_funding = await self.bot._fetch_gate_funding_rates_direct(gt_tickers)
                self.bot._cross_scan_cache = {
                    "ts": time.time(),  # 数据就绪时间，非请求发起时间
                    "bn_fee_pair": bn_fee_pair,
                    "gt_fee_pair": gt_fee_pair,
                    "bn_funding": bn_funding,
                    "gt_funding": gt_funding,
                    "bn_tickers": bn_tickers,
                    "gt_tickers": gt_tickers,
                }
        _, bn_futures_fee_dict = bn_fee_pair
        _, gt_futures_fee_dict = gt_fee_pair
        bn_futures_fee = float(bn_futures_fee_dict.get("__default__",
            self.bot._effective_futures_taker_fee(DEFAULT_FUTURES_TAKER_FEE)))
        gt_futures_fee = float(gt_futures_fee_dict.get("__default__", DEFAULT_GATE_FUTURES_TAKER_FEE))
        self.bot._dash_cross_fees = (bn_futures_fee, gt_futures_fee)

        bn_market_index = self.bot._build_futures_market_index()
        gt_market_index = self.bot._build_gate_futures_market_index()

        # 构建 base → (symbol, rate, next_funding_time) 映射
        def _parse_rates(funding_dict: dict, market_index: dict, exchange: str) -> dict[str, tuple[str, float, float]]:
            result: dict[str, tuple[str, float, float]] = {}
            for symbol, item in funding_dict.items():
                market = market_index.get(symbol)
                if market is None:
                    # ccxt 可能用 slash 格式的 key，尝试匹配 base
                    for base, mkt in market_index.items():
                        if mkt["symbol"] == symbol:
                            market = mkt
                            break
                if market is None:
                    continue
                base = market["base"]
                rate, _ = self.bot._extract_predicted_funding_rate(item)
                if rate is None:
                    continue
                next_ft = self.bot._extract_next_funding_time(item)
                result[base] = (market["symbol"], rate, next_ft)
            return result

        bn_rates = _parse_rates(bn_funding, bn_market_index, "binance")
        gt_rates = _parse_rates(gt_funding, gt_market_index, "gate")

        # ── 找共同币种：同名 + 价格校验 ──
        # 两所同名且价差 <1% 才视为同一资产，否则跳过
        PRICE_DIVERGENCE_THRESHOLD = 0.01

        def _ticker_price(tickers: dict, symbol: str) -> float:
            t = tickers.get(symbol, {})
            return float(t.get("last") or 0)

        valid_pairs: dict[str, str] = {}  # bn_base → gt_base

        for base in set(bn_rates) & set(gt_rates):
            bn_sym = bn_rates[base][0]
            gt_sym = gt_rates[base][0]
            bp = _ticker_price(bn_tickers, bn_sym)
            gp = _ticker_price(gt_tickers, gt_sym)
            if bp > 0 and gp > 0 and abs(bp - gp) / min(bp, gp) >= PRICE_DIVERGENCE_THRESHOLD:
                logger.debug("[跨交易所] 价格拒绝 %s: BN=%.6f GT=%.6f 偏离=%.1f%%",
                           base, bp, gp, abs(bp - gp) / min(bp, gp) * 100)
                continue
            valid_pairs[base] = base


        if not valid_pairs:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for bn_base, gt_base in valid_pairs.items():
            if bn_base not in bn_rates or gt_base not in gt_rates:
                continue
            bn_sym, bn_rate, bn_nft = bn_rates[bn_base]
            gt_sym, gt_rate, gt_nft = gt_rates[gt_base]

            rate_spread = abs(bn_rate - gt_rate)
            if strict and rate_spread < CROSS_MIN_RATE_SPREAD:
                continue

            # 确定方向：费率高的做空，低的做多
            if bn_rate > gt_rate:
                short_ex, long_ex = "binance", "gate"
                short_sym, long_sym = bn_sym, gt_sym
                short_rate, long_rate = bn_rate, gt_rate
                short_fee, long_fee = bn_futures_fee, gt_futures_fee
                short_nft, long_nft = bn_nft, gt_nft
            else:
                short_ex, long_ex = "gate", "binance"
                short_sym, long_sym = gt_sym, bn_sym
                short_rate, long_rate = gt_rate, bn_rate
                short_fee, long_fee = gt_futures_fee, bn_futures_fee
                short_nft, long_nft = gt_nft, bn_nft

            # 净收益 = 费率差 - 双边往返手续费
            total_fees = 2 * (short_fee + long_fee)
            net_rate = rate_spread - total_fees

            # 判断是否通过严格筛选
            passes = True
            if rate_spread < CROSS_MIN_RATE_SPREAD:
                passes = False
            elif bn_nft > 0 and gt_nft > 0 and abs(bn_nft - gt_nft) > 5000:
                # 两所结算时间差>5s：如 BN 8h / GT 4h，不同步无法套利
                passes = False
            elif bn_nft > 0 and not self.bot._within_entry_window(bn_nft):
                passes = False
            elif gt_nft > 0 and not self.bot._within_entry_window(gt_nft):
                passes = False
            elif net_rate <= CROSS_MIN_NET_RATE:
                passes = False

            if strict and not passes:
                continue

            bn_price = float((bn_tickers.get(bn_sym) or {}).get("last") or 0)
            gt_price = float((gt_tickers.get(gt_sym) or {}).get("last") or 0)

            rows.append({
                "base": bn_base,
                "rate_spread": rate_spread,
                "net_rate": net_rate,
                "short_exchange": short_ex,
                "long_exchange": long_ex,
                "short_symbol": short_sym,
                "long_symbol": long_sym,
                "short_rate": short_rate,
                "long_rate": long_rate,
                "short_fee": short_fee,
                "long_fee": long_fee,
                "short_next_funding_time_ms": short_nft,
                "long_next_funding_time_ms": long_nft,
                "short_price": bn_price if short_ex == "binance" else gt_price,
                "long_price": bn_price if long_ex == "binance" else gt_price,
                "bn_price": bn_price,
                "gt_price": gt_price,
                "passes": passes,
            })

        df = pd.DataFrame(rows)
        if df.empty:
            return df
        df = df.sort_values("rate_spread", ascending=False).reset_index(drop=True)
        if strict:
            return df
        return df.head(20)

    async def open_position(self, *, base: str, short_ex: str, short_sym: str, long_ex: str, long_sym: str, short_amount: float, long_amount: float, short_price: float, long_price: float, short_rate: float, long_rate: float, rate_spread: float, net_rate: float, short_next_funding_time_ms: float, long_next_funding_time_ms: float) -> bool:
        """开仓跨交易所套利：并发下空单+多单，设置 cross_state。"""
        logger.info(
            "[跨交易所] 开仓 %s: %s空@%s(%.4f%%) × %.4f张 + %s多@%s(%.4f%%) × %.4f张 | 费率差=%.4f%% | 净收益=%.4f%%",
            base, base, short_ex, short_rate * 100, short_amount,
            base, long_ex, long_rate * 100, long_amount,
            rate_spread * 100, net_rate * 100,
        )

        short_task = self.bot._cross_open_short_leg(short_ex, short_sym, short_amount)
        long_task = self.bot._cross_open_long_leg(long_ex, long_sym, long_amount)
        short_result, long_result = await asyncio.gather(short_task, long_task)

        if short_result.ok and long_result.ok:
            # 用实际成交均价，不用扫描参考价
            short_fill = self.bot._extract_fill_price(short_result.order)
            long_fill = self.bot._extract_fill_price(long_result.order)
            self.bot.cross_state = CrossArbitrageState(
                is_open=True,
                base=base,
                amount=short_amount * short_price,  # 近似 USDT 名义值（显示用）
                short_amount=short_amount,
                long_amount=long_amount,
                short_exchange=short_ex,
                long_exchange=long_ex,
                short_symbol=short_sym,
                long_symbol=long_sym,
                short_order_id=str(short_result.order.get("id", "")),
                long_order_id=str(long_result.order.get("id", "")),
                short_entry_price=short_fill or short_price,
                long_entry_price=long_fill or long_price,
                short_rate=short_rate,
                long_rate=long_rate,
                rate_spread=rate_spread,
                total_net_rate=net_rate,
                opened_at=datetime.now(tz=self.bot.tz).isoformat(),
                short_next_funding_time_ms=short_next_funding_time_ms,
                long_next_funding_time_ms=long_next_funding_time_ms,
            )
            self.bot._save_cross_state()
            await asyncio.to_thread(
                self.bot._send_email,
                "bazfbot 跨所开仓",
                f"币种: {base}\n"
                f"空 {short_ex}: {short_sym} × {short_amount}张 费率 {short_rate*100:.4f}%\n"
                f"多 {long_ex}: {long_sym} × {long_amount}张 费率 {long_rate*100:.4f}%\n"
                f"数量: {short_amount}/{long_amount} | 费率差: {rate_spread*100:.4f}%\n"
                f"净收益: {net_rate*100:.4f}%",
            )
            return True

        # 一腿或两腿失败 → 应急平掉成功腿
        logger.critical("[跨交易所] 开仓未同时成功: short=%s long=%s", short_result, long_result)
        if short_result.ok:
            logger.critical("[跨交易所] 空单已成交但多单失败，立即平空恢复中性")
            await self.bot._cross_close_short_leg(short_ex, short_sym, short_amount)
        if long_result.ok:
            logger.critical("[跨交易所] 多单已成交但空单失败，立即平多恢复中性")
            await self.bot._cross_close_long_leg(long_ex, long_sym, long_amount)
        return False

    async def _close_post_process(self, cs_snapshot: CrossArbitrageState,
                                    short_r: Any, long_r: Any) -> None:
        """后台任务：记录交易 + 发邮件，不阻塞平仓主路径。"""
        try:
            await self.bot._record_cross_trade(short_close_order=short_r.order,
                                            long_close_order=long_r.order,
                                            state_snapshot=cs_snapshot)
        except Exception as exc:
            logger.error("[跨交易所] 后台记录交易失败: %s", exc)
        try:
            await asyncio.to_thread(
                self.bot._send_email, "bazfbot 跨所平仓",
                f"{cs_snapshot.base} 跨所套利已平仓。"
            )
        except Exception:
            pass

    async def close_position(self) -> bool:
        """平仓跨交易所套利：平空 + 平多。"""
        if not self.bot.cross_state.is_open:
            return True

        cs = self.bot.cross_state
        logger.info("[跨交易所] 平仓 %s: 平空@%s(%s) + 平多@%s(%s)",
                     cs.base, cs.short_exchange, cs.short_symbol,
                     cs.long_exchange, cs.long_symbol)

        short_task = self.bot._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
        long_task = self.bot._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
        short_r, long_r = await asyncio.gather(short_task, long_task)

        # 重试失败腿
        for attempt in range(3):
            if short_r.ok and long_r.ok:
                break
            if not short_r.ok:
                logger.warning("[跨交易所] 平空失败，重试 %d/3", attempt + 1)
                short_r = await self.bot._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
            if not long_r.ok:
                logger.warning("[跨交易所] 平多失败，重试 %d/3", attempt + 1)
                long_r = await self.bot._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
            await asyncio.sleep(0.5)

        if short_r.ok and long_r.ok:
            logger.info("[跨交易所] 平仓成功。")
            # 先清状态再记录交易，防止 _record_cross_trade 耗时期间
            # 其他协程调用 check_position 看到旧状态误判单腿异常
            cs_snapshot = self.bot.cross_state
            self.bot.cross_state = CrossArbitrageState()
            self.bot._save_cross_state()
            # 记账 + 邮件放后台，不阻塞平仓返回
            asyncio.create_task(self._close_post_process(cs_snapshot, short_r, long_r))
            return True

        # 部分失败：重开已平腿恢复对冲
        logger.critical("[跨交易所] 平仓部分失败！short=%s long=%s", short_r, long_r)
        if short_r.ok and not long_r.ok:
            logger.critical("[跨交易所] 重开多单恢复对冲")
            await self.bot._cross_open_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount)
        elif long_r.ok and not short_r.ok:
            logger.critical("[跨交易所] 重开空单恢复对冲")
            await self.bot._cross_open_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount)
        await asyncio.to_thread(
            self.bot._send_email,
            "bazfbot 跨所平仓失败！",
            f"{cs.base} 平仓单腿失败，已尝试恢复对冲。请立即检查持仓！",
        )
        return False

    def should_exit(self) -> bool:
        """跨所套利退出判断：两腿都过了结算时间且 unlocked。"""
        cs = self.bot.cross_state
        if not cs.is_open:
            return False
        if not cs.short_locked and not cs.long_locked:
            return True
        now_ms = time.time() * 1000
        short_ok = cs.short_next_funding_time_ms > 0 and now_ms >= cs.short_next_funding_time_ms
        long_ok = cs.long_next_funding_time_ms > 0 and now_ms >= cs.long_next_funding_time_ms
        return short_ok and long_ok

    async def check_position(self) -> bool:
        """验证两交易所实际仓位与 cross_state 一致。不一致则清理所有腿。"""
        cs = self.bot.cross_state
        if not cs.is_open:
            return False

        async def _check_futures_pos(exchange: str, symbol: str, expect_short: bool, expect_amount: float) -> tuple[bool, str]:
            """检查指定交易所是否有对应方向的合约持仓。返回 (ok, detail)。"""
            if exchange == "binance":
                try:
                    clean = self.bot._clean_futures_symbol(symbol)
                    resp = await self.bot._binance_request(
                        BINANCE_FUTURES_API, "/fapi/v2/positionRisk",
                        {"symbol": clean},
                    )
                    if isinstance(resp, list):
                        for pos in resp:
                            if not isinstance(pos, dict):
                                continue
                            ps = pos.get("positionSide", "")
                            if expect_short and ps != "SHORT":
                                continue
                            if not expect_short and ps != "LONG":
                                continue
                            amt = abs(float(pos.get("positionAmt", 0) or 0))
                            if amt >= expect_amount * 0.1:
                                detail = f"positionAmt={amt} side={ps}"
                                return True, detail
                        return False, f"no matching positionSide in {len(resp)} positions"
                    return False, f"unexpected resp type: {type(resp)}"
                except Exception as exc:
                    logger.warning("[跨交易所] 验证BN持仓异常 %s: %s", symbol, exc)
                    return True, f"API error, assume exists: {exc}"
            else:  # gate
                try:
                    positions = await self.bot._safe_request(
                        f"verify_pos_gate",
                        lambda: self.bot._fetch_gate_positions_direct(symbol),
                        default=[],
                    )
                    for pos in positions:
                        contracts = float(pos.get("contracts", 0) or 0)
                        if abs(contracts) < expect_amount * 0.1:
                            continue
                        if expect_short:
                            return contracts < 0, f"contracts={contracts}"
                        else:
                            return contracts > 0, f"contracts={contracts}"
                    return False, f"no position in {len(positions)} results"
                except Exception as exc:
                    logger.warning("[跨交易所] 验证GT持仓异常 %s: %s", symbol, exc)
                    return True, f"API error, assume exists: {exc}"

        short_ok, long_ok = await asyncio.gather(
            _check_futures_pos(cs.short_exchange, cs.short_symbol, expect_short=True, expect_amount=cs.short_amount),
            _check_futures_pos(cs.long_exchange, cs.long_symbol, expect_short=False, expect_amount=cs.long_amount),
        )
        logger.info("[跨交易所] 持仓验证: short(%s)=%s(%s) long(%s)=%s(%s)",
                    cs.short_exchange, short_ok[0], short_ok[1],
                    cs.long_exchange, long_ok[0], long_ok[1])

        if short_ok[0] and long_ok[0]:
            return True
        if not short_ok[0] and not long_ok[0]:
            logger.warning("[跨交易所] 两所均无实际持仓，重置状态。")
            self.bot.cross_state = CrossArbitrageState()
            self.bot._save_cross_state()
            return False

        # 应急：不管验证结果，两腿都尝试平仓，杜绝裸仓
        logger.critical("[跨交易所] 单腿持仓异常！short_ok=%s long_ok=%s → 尝试平两腿",
                        short_ok, long_ok)
        cs_snapshot = self.bot.cross_state
        s_close_r, l_close_r = await asyncio.gather(
            self.bot._cross_close_short_leg(cs.short_exchange, cs.short_symbol, cs.short_amount),
            self.bot._cross_close_long_leg(cs.long_exchange, cs.long_symbol, cs.long_amount),
        )
        self.bot.cross_state = CrossArbitrageState()
        self.bot._save_cross_state()
        asyncio.create_task(self._close_post_process(cs_snapshot, s_close_r, l_close_r))
        return False

    async def check_liquidation(self, exchange: str, symbol: str, short: bool) -> float | None:
        """查询跨所单腿距强平距离。复用 Binance 已有方法，Gate 单独实现。"""
        if exchange == "binance":
            return await self.bot._check_futures_liquidation_distance(symbol, short)
        # Gate: 通过 ccxt fetch_positions
        try:
            positions = await self.bot._safe_request(
                "gate_futures.fetch_positions_x",
                lambda: self.bot._fetch_gate_positions_direct(symbol),
                default=[],
            )
            for pos in positions:
                if pos.get("symbol") != symbol:
                    continue
                liq_price = float(pos.get("liquidationPrice") or 0)
                mark_price = float(pos.get("markPrice") or 0)
                if liq_price <= 0 or mark_price <= 0:
                    return None
                if short:
                    return (liq_price - mark_price) / mark_price
                return (mark_price - liq_price) / mark_price
        except Exception:
            pass
        return None

    async def detect_orphans(self) -> bool:
        """扫描两所所有持仓，检测 cross_state 为空时是否有游离仓位。
        有游离仓则平仓并返回 True，无则返回 False。"""
        found = False
        try:
            # 并行获取两所仓位
            bn_positions_raw = await self.bot._binance_request(
                BINANCE_FUTURES_API, "/fapi/v2/positionRisk", {}
            )
            gt_positions = await self.bot._safe_request(
                "orphan_gt_positions",
                lambda: self.bot._fetch_gate_positions_direct(),
                default=[],
            )

            # 解析 Binance 仓位：聚合 positionAmt（找有仓的）
            bn_positions: dict[str, dict[str, Any]] = {}  # base → {symbol, side, amount}
            if isinstance(bn_positions_raw, list):
                for pos in bn_positions_raw:
                    if not isinstance(pos, dict):
                        continue
                    amt = abs(float(pos.get("positionAmt", 0) or 0))
                    if amt < 1:  # 忽略极小/零仓位
                        continue
                    sym = str(pos.get("symbol", ""))
                    side = str(pos.get("positionSide", ""))
                    base = sym.replace("USDT", "")  # AGLDUSDT → AGLD
                    bn_positions[base] = {"symbol": f"{base}/USDT:USDT", "side": side,
                                          "amount": amt, "exchange": "binance"}

            # 解析 Gate 仓位
            gt_pos_map: dict[str, dict[str, Any]] = {}
            for pos in (gt_positions or []):
                contracts = abs(float(pos.get("contracts", 0) or 0))
                if contracts < 1:
                    continue
                sym = str(pos.get("symbol", ""))
                # sym format: "AGLD/USDT:USDT"
                base = sym.split("/")[0] if "/" in sym else sym.replace("USDT", "")
                side = "LONG" if float(pos.get("contracts", 0) or 0) > 0 else "SHORT"
                gt_pos_map[base] = {"symbol": sym, "side": side,
                                    "amount": contracts, "exchange": "gate"}

            # 找到两所都有仓位的币种 → 配对游离仓
            common = set(bn_positions.keys()) & set(gt_pos_map.keys())
            for base in common:
                bn = bn_positions[base]
                gt = gt_pos_map[base]
                if bn["side"] == gt["side"]:
                    continue
                close_amount = min(bn["amount"], gt["amount"])
                short_ex = "binance" if bn["side"] == "SHORT" else "gate"
                short_sym = bn["symbol"] if bn["side"] == "SHORT" else gt["symbol"]
                long_ex = "gate" if bn["side"] == "SHORT" else "binance"
                long_sym = gt["symbol"] if bn["side"] == "SHORT" else bn["symbol"]
                logger.critical("[跨交易所] ⚠ 检测到配对游离仓位 %s！立即平两腿", base)
                await asyncio.gather(
                    self.bot._cross_close_short_leg(short_ex, short_sym, close_amount),
                    self.bot._cross_close_long_leg(long_ex, long_sym, close_amount),
                )
                # 记录交易（无持仓期间费率数据，按 0 记录，至少历史可查）
                self.bot._write_cross_trade_record(
                    base, f"orphan_close({short_ex}空+{long_ex}多)",
                    close_amount, 0.0, 0.0, 0.0)
                logger.critical("[跨交易所] 配对游离仓位 %s 清理完成", base)
                found = True
                bn_positions.pop(base, None)
                gt_pos_map.pop(base, None)

            # 剩余单边仓位 — 并发平掉
            all_orphans = {**bn_positions, **gt_pos_map}
            if all_orphans:
                tasks = []
                for base, info in all_orphans.items():
                    logger.critical("[跨交易所] ⚠ 检测到孤立单腿仓位！%s %s %s %.0f张 → 立即平仓",
                                  info["exchange"], info["symbol"], info["side"], info["amount"])
                    self.bot._write_cross_trade_record(
                        base, f"orphan_single({info['exchange']}{'空' if info['side']=='SHORT' else '多'})",
                        info["amount"], 0.0, 0.0, 0.0)
                    if info["side"] == "SHORT":
                        tasks.append(self.bot._cross_close_short_leg(info["exchange"], info["symbol"], info["amount"]))
                    else:
                        tasks.append(self.bot._cross_close_long_leg(info["exchange"], info["symbol"], info["amount"]))
                await asyncio.gather(*tasks)
                found = True
            return found
        except Exception as exc:
            logger.warning("[跨交易所] 游离仓位检测异常: %s", exc)
            return False

    async def run_cycle(self, force_entry: bool) -> None:
        """跨交易所套利主循环：狙击模式 + 常规扫描 + 持仓管理。"""
        has_position = self.bot.cross_state.is_open

        # 启动/状态重置后先检查游离仓位，防止裸仓
        if not has_position:
            if await self.detect_orphans():
                return  # 发现孤仓并已清理，暂停本轮，下轮正常决策

        if has_position:
            valid = await self.check_position()
            if not valid:
                has_position = False

        # force_entry 时强平
        if force_entry and has_position:
            logger.info("[跨交易所] 即时模式：强制平仓。")
            await self.close_position()
            has_position = False

        if has_position:
            cs = self.bot.cross_state

            # ── 风控：强平距离监控 ──
            short_dist = await self.check_liquidation(
                cs.short_exchange, cs.short_symbol, short=True)
            long_dist = await self.check_liquidation(
                cs.long_exchange, cs.long_symbol, short=False)

            if short_dist is not None:
                logger.info("[跨交易所] 空单距强平 %.1f%% (阈值 %.0f%%)",
                          short_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
            if long_dist is not None:
                logger.info("[跨交易所] 多单距强平 %.1f%% (阈值 %.0f%%)",
                          long_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)

            if short_dist is not None and short_dist < CROSS_LIQ_DISTANCE_MIN:
                logger.critical("[跨交易所] 空单距强平 %.1f%% < %.0f%%，强制平仓！",
                              short_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
                await self.close_position()
                await asyncio.to_thread(self.bot._send_email, "bazfbot 跨所强平！",
                    f"{cs.base} 空单@{cs.short_exchange} 距强平仅 {short_dist*100:.1f}%，已强制平仓。")
                has_position = False

            if has_position and long_dist is not None and long_dist < CROSS_LIQ_DISTANCE_MIN:
                logger.critical("[跨交易所] 多单距强平 %.1f%% < %.0f%%，强制平仓！",
                              long_dist * 100, CROSS_LIQ_DISTANCE_MIN * 100)
                await self.close_position()
                await asyncio.to_thread(self.bot._send_email, "bazfbot 跨所强平！",
                    f"{cs.base} 多单@{cs.long_exchange} 距强平仅 {long_dist*100:.1f}%，已强制平仓。")
                has_position = False

        if has_position:
            should_exit = self.should_exit()
            if should_exit:
                logger.info("[跨交易所] 结算已过，直接平仓。")
                await self.close_position()
                has_position = False
            else:
                self.bot._print_cross_position_summary()
                cs = self.bot.cross_state
                now_ms = time.time() * 1000
                short_wait = max(0, (cs.short_next_funding_time_ms - now_ms) / 60000) if cs.short_next_funding_time_ms > 0 else 0
                long_wait = max(0, (cs.long_next_funding_time_ms - now_ms) / 60000) if cs.long_next_funding_time_ms > 0 else 0
                logger.info("[跨交易所] 持仓 %s | 空@%s %.0fmin后结算 | 多@%s %.0fmin后结算 | 费率差=%.4f%%",
                             cs.base, cs.short_exchange, short_wait, cs.long_exchange, long_wait,
                             cs.rate_spread * 100)

        if has_position:
            bn_bal, gt_bal = await asyncio.gather(
                self.bot._cross_get_bn_futures_balance(),
                self.bot._gate_futures_balance(),
            )
            await self.bot._update_cross_dashboard(bn_bal, gt_bal)
            return

        # ── 无持仓：狙击模式 ──
        # T-10s 扫描获取最新费率+设杠杆 → T-1s 直接开仓
        snipe_ms = getattr(self.bot, "_next_snipe_settle_ms", 0)
        if snipe_ms:
            remain_ms = snipe_ms - time.time() * 1000
            if remain_ms <= 0:
                self.bot._next_snipe_settle_ms = 0
            elif remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000:
                # Step 1: T-10s 扫描+设杠杆
                scan_at_ms = snipe_ms - CROSS_SNIPER_SCAN_OFFSET_SEC * 1000
                await asyncio.sleep(max(0, (scan_at_ms - time.time() * 1000) / 1000))
                logger.info("[跨交易所] 狙击扫描：距结算 %dms",
                             max(0, int(snipe_ms - time.time() * 1000)))
                bn_bal, gt_bal = await asyncio.gather(
                    self.bot._cross_get_bn_futures_balance(),
                    self.bot._gate_futures_balance(),
                )
                if bn_bal <= 0 or gt_bal <= 0:
                    logger.error("[跨交易所] 狙击模式余额不足")
                    self.bot._next_snipe_settle_ms = 0
                    return
                position_usdt = min(bn_bal, gt_bal) * CROSS_POSITION_SIZE_RATIO
                # 清 Gate tickers 5s 缓存，确保狙击用最新价格
                self.bot._gate_ft_cache = {}
                # 狙击扫描 12s 超时保护，超时回退主扫缓存
                try:
                    cross_df = await asyncio.wait_for(
                        self.scan(position_usdt, strict=True, snipe=True),
                        timeout=12,
                    )
                except asyncio.TimeoutError:
                    logger.warning("[跨交易所] 狙击扫描超时，回退主扫缓存数据")
                    cross_df = getattr(self.bot, "_dash_cross_df", pd.DataFrame())
                self.bot._dash_cross_df = cross_df
                passing = cross_df[cross_df["passes"] == True] if not cross_df.empty and "passes" in cross_df.columns else cross_df
                if passing.empty:
                    logger.info("[跨交易所] 狙击扫描无合格候选，放弃")
                    self.bot._next_snipe_settle_ms = 0
                    await self.bot._update_cross_dashboard(bn_bal, gt_bal)
                    return
                # 遍历通过筛选的候选，直到一个通过数量计算
                chosen = None
                for idx in range(min(10, len(passing))):
                    candidate = passing.iloc[idx]
                    logger.info("[跨交易所] 狙击目标 #%d %s: 费率差=%.4f%% 净收益=%.4f%%",
                                 idx + 1, str(candidate["base"]),
                                 float(candidate["rate_spread"]) * 100,
                                 float(candidate["net_rate"]) * 100)

                    # ── 计算开仓参数 ──
                    c_base = str(candidate["base"])
                    c_short_ex = str(candidate["short_exchange"])
                    c_long_ex = str(candidate["long_exchange"])
                    c_short_sym = str(candidate["short_symbol"])
                    c_long_sym = str(candidate["long_symbol"])
                    c_short_price = float(candidate["short_price"])
                    c_long_price = float(candidate["long_price"])
                    if c_short_price <= 0 or c_long_price <= 0:
                        logger.warning("[跨交易所] 狙击 #%d 价格为零，跳过", idx + 1)
                        continue
                    c_short_rate = float(candidate["short_rate"])
                    c_long_rate = float(candidate["long_rate"])
                    c_rate_spread = float(candidate["rate_spread"])
                    c_net_rate = float(candidate["net_rate"])
                    c_short_nft = float(candidate["short_next_funding_time_ms"])
                    c_long_nft = float(candidate["long_next_funding_time_ms"])

                    c_short_bal = bn_bal if c_short_ex == "binance" else gt_bal
                    c_long_bal = bn_bal if c_long_ex == "binance" else gt_bal
                    c_short_notional = c_short_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE
                    c_long_notional = c_long_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE
                    c_min_notional = min(c_short_notional, c_long_notional)
                    # 确定精度粗的那边（contractSize × 最小步长 更大的 = 更粗）
                    # 先算粗的那边，细的用粗的实际金额来对齐，一趟完成
                    c_short_step = await self.bot._cross_notional_step(c_short_ex, c_short_sym, c_short_price)
                    c_long_step = await self.bot._cross_notional_step(c_long_ex, c_long_sym, c_long_price)
                    if c_short_step >= c_long_step:
                        # 空单精度更粗，先算空单
                        c_short_qty = await self.bot._cross_calculate_amount(c_short_ex, c_short_sym, c_short_price, c_min_notional)
                        if c_short_qty <= 0:
                            logger.warning("[跨交易所] 狙击 #%d 空单数量不足，尝试下一位", idx + 1)
                            continue
                        short_actual = await self.bot._cross_actual_notional(c_short_ex, c_short_sym, c_short_qty, c_short_price)
                        c_long_qty = await self.bot._cross_calculate_amount(c_long_ex, c_long_sym, c_long_price, short_actual)
                        if c_long_qty <= 0:
                            logger.warning("[跨交易所] 狙击 #%d 多单对齐后数量不足，尝试下一位", idx + 1)
                            continue
                        long_actual = await self.bot._cross_actual_notional(c_long_ex, c_long_sym, c_long_qty, c_long_price)
                    else:
                        # 多单精度更粗，先算多单
                        c_long_qty = await self.bot._cross_calculate_amount(c_long_ex, c_long_sym, c_long_price, c_min_notional)
                        if c_long_qty <= 0:
                            logger.warning("[跨交易所] 狙击 #%d 多单数量不足，尝试下一位", idx + 1)
                            continue
                        long_actual = await self.bot._cross_actual_notional(c_long_ex, c_long_sym, c_long_qty, c_long_price)
                        c_short_qty = await self.bot._cross_calculate_amount(c_short_ex, c_short_sym, c_short_price, long_actual)
                        if c_short_qty <= 0:
                            logger.warning("[跨交易所] 狙击 #%d 空单对齐后数量不足，尝试下一位", idx + 1)
                            continue
                        short_actual = await self.bot._cross_actual_notional(c_short_ex, c_short_sym, c_short_qty, c_short_price)
                    est_short = min(short_actual, long_actual)
                    est_long = est_short
                    if est_short > c_short_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE * 1.1 or \
                       est_long > c_long_bal * CROSS_POSITION_SIZE_RATIO * CROSS_LEVERAGE * 1.1:
                        logger.warning("[跨交易所] 狙击 #%d 数量超限，跳过", idx + 1)
                        continue
                    if est_short < 5.5 or est_long < 5.5:
                        logger.warning("[跨交易所] 狙击 #%d 仓位太小(≈%.2f USDT)，跳过", idx + 1, est_short)
                        continue
                    chosen = candidate
                    break

                if chosen is None:
                    logger.warning("[跨交易所] 所有狙击候选均未通过数量计算，放弃本轮")
                    self.bot._next_snipe_settle_ms = 0
                    return
                candidate = chosen
                # 目标确认后立即订阅 Gate 资费 WS，不等开仓成功后再订
                gt_sym = c_short_sym if c_short_ex == "gate" else c_long_sym
                self.bot._gate_funding_symbol = gt_sym
                self.bot._gate_funding_baseline = 0.0
                try:
                    await self.bot._gate_subscribe_position(gt_sym)
                except Exception:
                    pass
                try:
                    existing = await self.bot._safe_request(
                        "gate_funding_baseline",
                        lambda: self.bot._fetch_gate_positions_direct(gt_sym),
                        default=[],
                    )
                    for pos in existing:
                        if pos.get("symbol") == gt_sym:
                            self.bot._gate_funding_baseline = float(pos.get("info", {}).get("pnl_fund", 0) or 0)
                            break
                except Exception:
                    pass
                logger.info("[资费监听] Gate 目标 %s baseline=%.4f (已提前订阅)", gt_sym, self.bot._gate_funding_baseline)
                # 提前设杠杆，省去开仓时 API 调用延迟 (~66ms)
                await asyncio.gather(
                    self.bot._cross_ensure_leverage(c_short_ex, c_short_sym),
                    self.bot._cross_ensure_leverage(c_long_ex, c_long_sym),
                )

                # Step 2: T-1s 只管发单
                open_at_ms = snipe_ms - CROSS_SNIPER_OPEN_OFFSET_MS
                await asyncio.sleep(max(0, (open_at_ms - time.time() * 1000) / 1000))
                logger.info("[跨交易所] 狙击开仓 %s: 距结算 %dms",
                             c_base, max(0, int(snipe_ms - time.time() * 1000)))
                ok = await self.open_position(
                    base=c_base,
                    short_ex=c_short_ex, short_sym=c_short_sym,
                    long_ex=c_long_ex, long_sym=c_long_sym,
                    short_amount=c_short_qty, long_amount=c_long_qty,
                    short_price=c_short_price, long_price=c_long_price,
                    short_rate=c_short_rate, long_rate=c_long_rate,
                    rate_spread=c_rate_spread, net_rate=c_net_rate,
                    short_next_funding_time_ms=c_short_nft,
                    long_next_funding_time_ms=c_long_nft,
                )
                if ok:
                    settle_ms = max(c_short_nft, c_long_nft)
                    try:
                        # 清除双 WS 事件标记，开仓后立刻开始等资费推送
                        self.bot._funding_event.clear()
                        self.bot._gate_funding_event.clear()
                        t_settle = time.perf_counter()
                        logger.info("[延迟] 开仓完毕，开始等 WS 资费推送")
                        bn_sym = c_short_sym if c_short_ex == "binance" else c_long_sym

                        async def _poll_rest():
                            """每 2s 轮询 REST 确认资费到账，WS 未检测到时可提前结束等待。"""
                            for _ in range(7):
                                await asyncio.sleep(2.0)
                                if self.bot._funding_event.is_set() and self.bot._gate_funding_event.is_set():
                                    return
                                if not self.bot._funding_event.is_set():
                                    if await self.bot._check_bn_funding_received(bn_sym, settle_ms):
                                        self.bot._funding_event.set()
                                if not self.bot._gate_funding_event.is_set():
                                    if await self.bot._check_gate_funding_received(gt_sym):
                                        self.bot._gate_funding_event.set()

                        bn_t = asyncio.create_task(self.bot._funding_event.wait())
                        gt_t = asyncio.create_task(self.bot._gate_funding_event.wait())
                        rest_t = asyncio.create_task(_poll_rest())
                        await asyncio.wait([bn_t, gt_t, rest_t], timeout=15.0,
                                          return_when=asyncio.FIRST_COMPLETED)
                        t_event = time.perf_counter()
                        bn_ok = self.bot._funding_event.is_set()
                        gt_ok = self.bot._gate_funding_event.is_set()
                        ws_latency_ms = (t_event - t_settle) * 1000
                        logger.info("[延迟] WS 推送检测耗时: %.1fms (BN=%s GT=%s)", ws_latency_ms, bn_ok, gt_ok)
                        # 超时后做最后一次 REST 确认（兜底）
                        if not bn_ok:
                            bn_ok = await self.bot._check_bn_funding_received(bn_sym, settle_ms)
                        if not gt_ok:
                            gt_ok = await self.bot._check_gate_funding_received(gt_sym)
                        logger.info("[资费监听] 确认完成: BN=%s GT=%s", bn_ok, gt_ok)
                        if not bn_ok or not gt_ok:
                            logger.warning("[资费监听] 未全部确认，强制平仓")
                    except Exception as exc:
                        logger.critical("[跨交易所] 等待资费异常: %s，直接平仓", exc)
                    finally:
                        t_close_start = time.perf_counter()
                        await self.close_position()
                        t_close_end = time.perf_counter()
                        logger.info("[延迟] 平仓执行耗时: %.1fms", (t_close_end - t_close_start) * 1000)
                self.bot._next_snipe_settle_ms = 0
                await self.bot._update_cross_dashboard(bn_bal, gt_bal)
                return

        bn_bal, gt_bal = await asyncio.gather(
            self.bot._cross_get_bn_futures_balance(),
            self.bot._gate_futures_balance(),
        )
        logger.info("[跨交易所] 账户余额: BN合约=%.2f | GT合约=%.2f USDT", bn_bal, gt_bal)
        await self.bot._update_cross_dashboard(bn_bal, gt_bal)

        if bn_bal <= 0 or gt_bal <= 0:
            logger.error("[跨交易所] 余额不足: BN=%.2f GT=%.2f", bn_bal, gt_bal)
            return

        position_usdt = min(bn_bal, gt_bal) * CROSS_POSITION_SIZE_RATIO
        logger.info("[跨交易所] 估算仓位: 每腿 ≈ %.2f USDT (使用率=%.0f%%)",
                     position_usdt, CROSS_POSITION_SIZE_RATIO * 100)

        logger.info("┏━━ [跨交易所] 主扫开始 ━━")
        cross_df = await self.scan(position_usdt, strict=True)
        bn_fee, gt_fee = getattr(self.bot, "_dash_cross_fees", (0.0, 0.0))
        logger.info("[跨交易所] 手续费: BN合约=%.4f%%(BNB折扣后) | GT合约=%.4f%%(返佣后) | 双边开平=%.4f%%",
                     bn_fee * 100, gt_fee * 100, 2 * (bn_fee + gt_fee) * 100)
        self.bot._dash_cross_df = cross_df

        if cross_df.empty:
            # 严格筛选没结果 → 宽松扫展示接近的候选
            cross_df = await self.scan(position_usdt, strict=False)
            self.bot._dash_cross_df = cross_df
            if cross_df.empty:
                logger.info("[跨交易所] 两所无共同币种，无法扫描。")
                self.bot._next_snipe_settle_ms = 0
                return

        # 更新下次狙击结算时间（只取 passes=True 中费率差最高的）
        if not cross_df.empty:
            passing = cross_df[cross_df["passes"] == True] if "passes" in cross_df.columns else cross_df
            if not passing.empty:
                best = passing.iloc[0]
                self.bot._next_snipe_settle_ms = min(
                    float(best.get("short_next_funding_time_ms", 0)),
                    float(best.get("long_next_funding_time_ms", 0)),
                )
            else:
                self.bot._next_snipe_settle_ms = 0

        self.bot._print_cross_opportunity_table(cross_df, position_usdt)

        # 若刚发现机会且结算在 20s 内 → 直接跳狙击，不再等下一轮
        if not getattr(self.bot, "_snipe_loop_guard", False) and self.bot._next_snipe_settle_ms:
            remain_ms = self.bot._next_snipe_settle_ms - time.time() * 1000
            if 0 < remain_ms <= CROSS_SNIPE_WINDOW_SEC * 1000 * 2:
                logger.info("[跨交易所] 距结算仅 %dms，直接进入狙击流程", int(remain_ms))
                self.bot._snipe_loop_guard = True
                try:
                    await self.run_cycle(False)
                finally:
                    self.bot._snipe_loop_guard = False
                gc.collect()
                return


