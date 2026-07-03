"""Mixin: ScannerMixin — 共享数据抓取/指标计算."""
from __future__ import annotations
from typing import Any
from .constants import *
from .models import _safe_float

class ScannerMixin:

    async def _fetch_alpha_token_list(self) -> dict[str, dict[str, Any]]:
        """Fetch Binance Alpha token list. Returns dict: base -> {alpha_symbol, volume, price, chain, gas_usd}."""

        url = f"{ALPHA_API_BASE}{ALPHA_TOKEN_LIST_PATH}"
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            url,
                            headers={"User-Agent": "funding-arbitrage-bot/1.0"},
                        )
                    ).read()
                )
            )
        except Exception as exc:
            logger.warning("Failed to fetch Alpha token list: %s", exc)
            return {}

        tokens: dict[str, dict[str, Any]] = {}
        for item in response.get("data", []):
            symbol = item.get("symbol", "")
            if not symbol:
                continue
            chain = item.get("chainName", "")
            if chain in SKIP_ALPHA_CHAINS:
                continue
            alpha_id = item.get("alphaId", "")
            gas_usd = ALPHA_CHAIN_GAS_USD.get(chain, ALPHA_GAS_USD_PER_TX)
            tokens[symbol.upper()] = {
                "alpha_symbol": f"{alpha_id}USDT",
                "alpha_id": alpha_id,
                "quote_volume": float(item.get("volume24h") or 0),
                "price": float(item.get("price") or 0),
                "chain": chain,
                "gas_usd": gas_usd,
            }
        return tokens



    async def _safe_request(
        self,
        name: str,
        request: Callable[[], Awaitable[Any]],
        default: Any = None,
        raise_error: bool = False,
    ) -> Any:
        """统一包裹所有 ccxt API 请求，便于日志和异常处理。"""
        try:
            return await request()
        except ExchangeError as exc:
            logger.error("交易所业务错误: %s | %s", name, exc)
            if raise_error:
                raise
            return default
        except BaseError as exc:
            logger.error("ccxt 请求错误: %s | %s: %s", name, type(exc).__name__, exc)
            if raise_error:
                raise
            return default



    def _resolve_spot_leg(
        self,
        base: str,
        spot_tickers: dict,
        alpha_tokens: dict[str, dict[str, Any]],
        position_usdt: float = 100.0,
    ) -> tuple[str | None, str, float | None, float, str, float]:
        """Resolve spot leg for a given base. Returns (symbol, source, last_price, quote_volume, chain, gas_fee_ratio).

        Checks main spot first, then falls back to Binance Alpha.
        """
        # Try main spot
        spot_symbol = f"{base}/USDT"
        spot_market = (getattr(self.spot, "markets", None) or {}).get(spot_symbol)
        if spot_market and self._is_valid_spot_usdt_market(spot_symbol, spot_market):
            ticker = spot_tickers.get(spot_symbol, {})
            return (
                spot_symbol,
                "spot",
                ticker.get("last"),
                _safe_float(ticker.get("quoteVolume")),
                "",
                0.0,
            )

        # Try Alpha
        alpha_info = alpha_tokens.get(base)
        if alpha_info:
            alpha_symbol = alpha_info["alpha_symbol"]
            gas_usd = alpha_info.get("gas_usd", ALPHA_GAS_USD_PER_TX)
            gas_fee_ratio = gas_usd / position_usdt
            return (
                alpha_symbol,
                "alpha",
                alpha_info.get("price"),
                alpha_info.get("quote_volume", 0.0),
                alpha_info.get("chain", ""),
                gas_fee_ratio,
            )

        return None, "", None, 0.0, "", 0.0

    @staticmethod
    def _futures_passes_volume(futures_quote_volume: float) -> bool:
        return futures_quote_volume >= MIN_FUTURES_24H_QUOTE_VOLUME



    async def _get_alpha_tokens(self) -> dict[str, dict[str, Any]]:
        """Get Alpha token list, cached for 5 minutes."""
        now = time.monotonic()
        if self._alpha_tokens and (now - self._alpha_last_fetch) < 300:
            return self._alpha_tokens
        self._alpha_tokens = await self._fetch_alpha_token_list()
        self._alpha_last_fetch = now
        return self._alpha_tokens



    async def _fetch_margin_borrow_rates(self, assets: list[str]) -> dict[str, float]:
        """批量查询逐仓杠杆借币小时利率。

        Returns:
            dict[base_upper, hourly_rate_float] — 只包含可借贷且 rate > 0 的币种。
            不可借贷的币种不会出现在返回结果中。
        """
        if not assets or not REVERSE_ENABLED:
            return {}
        rates: dict[str, float] = {}
        batch_size = 20
        for i in range(0, len(assets), batch_size):
            batch = assets[i : i + batch_size]
            try:
                resp = await self._safe_request(
                    "margin_next_hourly_interest",
                    lambda b=batch: self.spot.sapiGetMarginNextHourlyInterestRate(
                        {"assets": ",".join(b), "isIsolated": True}
                    ),
                    default=[],
                )
                for item in resp:
                    asset = item.get("asset", "").upper()
                    rate = _safe_float(item.get("nextHourlyInterestRate"))
                    if asset and rate > 0:
                        rates[asset] = rate
            except Exception:
                logger.warning(f"借币利率批量查询失败: {batch}", exc_info=True)
        return rates

    @staticmethod
    def _log_memory() -> None:
        """记录当前进程 RSS + VmData 到日志，并触发 malloc_trim 归还空闲堆内存。"""
        try:
            vm_data = vm_rss = 0
            with open("/proc/self/status") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        vm_rss = int(line.split()[1])
                    elif line.startswith("VmData:"):
                        vm_data = int(line.split()[1])
            logger.info("[内存] RSS: %.0f MB | Data: %.0f MB", vm_rss / 1024, vm_data / 1024)
            # 强制 glibc 把堆上空闲页归还 OS（pandas/numpy free 后 glibc arena 囤着不还）
            try:
                ctypes.CDLL("libc.so.6").malloc_trim(0)
            except Exception:
                pass
        except Exception:
            pass



    def _build_futures_market_index(self) -> dict[str, dict[str, Any]]:
        index: dict[str, dict[str, Any]] = {}
        bn_markets = getattr(self.futures, "markets", None)
        if not bn_markets:
            return index
        tradfi_skipped = 0
        for market in bn_markets.values():
            is_usdt_swap = (
                market.get("swap")
                and market.get("linear")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            )
            if is_usdt_swap:
                if market.get("info", {}).get("underlyingType") == "STOCK":
                    tradfi_skipped += 1
                    continue
                index[market["base"]] = market
        if tradfi_skipped:
            logger.debug("Binance 跳过 %d 个 TradFi 股票代币", tradfi_skipped)
        return index



    def _build_gate_futures_market_index(self) -> dict[str, dict[str, Any]]:
        """Gate.io 版: 构建 USDT 永续合约 base → market 索引。"""
        index: dict[str, dict[str, Any]] = {}
        gt_markets = getattr(self.gate_futures, "markets", None)
        if not gt_markets:
            return index
        for market in gt_markets.values():
            is_usdt_swap = (
                market.get("swap")
                and market.get("linear")
                and market.get("quote") == "USDT"
                and market.get("active", True)
            )
            if is_usdt_swap:
                index[market["base"]] = market
        return index



    async def _fetch_gate_taker_fees(self) -> tuple[dict[str, float], dict[str, float]]:
        """Gate 现货+合约 taker 费率（带缓存+锁，同轮扫描只查一次）。"""
        cache_key = "gate_fees"
        now = time.time()
        if cache_key in self._fee_cache:
            ts, cached = self._fee_cache[cache_key]
            if now - ts < 10:
                return cached
        async with self._fee_lock:
            if cache_key in self._fee_cache:
                ts, cached = self._fee_cache[cache_key]
                if now - ts < 10:
                    return cached
            result = await self._fetch_gate_taker_fees_impl()
            self._fee_cache[cache_key] = (time.time(), result)
            return result



    async def _fetch_gate_taker_fees_impl(self) -> tuple[dict[str, float], dict[str, float]]:
        """Gate 现货+合约 taker 费率（已扣除返佣）。失败用默认值。"""
        spot_base = DEFAULT_GATE_SPOT_TAKER_FEE
        fut_base = DEFAULT_GATE_FUTURES_TAKER_FEE
        spot_fees: dict[str, float] = {"__default__": spot_base * (1 - GATE_SPOT_REBATE)}
        fut_fees: dict[str, float] = {"__default__": fut_base * (1 - GATE_FUTURES_REBATE)}

        # 现货费率 — 直连 REST
        spot_raw_rate: float = 0.0
        try:
            spot_raw = await self._gate_request("/spot/fee", timeout=10)
            taker = float((spot_raw.get("taker_fee", 0) or 0))
            if taker > 0:
                eff = taker * (1 - GATE_SPOT_REBATE)
                spot_fees["__default__"] = eff
                spot_raw_rate = taker
        except Exception:
            pass
        if spot_raw_rate > 0:
            logger.debug("Gate 现货费率: 基础=%.3f%% → 返佣%.0f%%后=%.3f%%",
                        spot_raw_rate * 100, GATE_SPOT_REBATE * 100,
                        spot_fees["__default__"] * 100)
        else:
            logger.warning("Gate 现货费率查询失败，使用默认值 %.3f%%",
                           spot_fees["__default__"] * 100)

        # 合约费率 — Gate v4 无独立 fee 端点，回退 ccxt
        fut_raw_rate: float = 0.0
        gt_markets = getattr(self.gate_futures, "markets", None)
        valid_fut_symbols = {
            m["symbol"] for m in (gt_markets or {}).values()
            if m.get("swap") and m.get("linear") and m.get("quote") == "USDT"
        } if gt_markets else set()
        try:
            fut_raw = await self._safe_request(
                "gate_futures.fetch_trading_fees",
                lambda: self.gate_futures.fetch_trading_fees(),
                default=None,
            )
            if fut_raw:
                for sym, info in fut_raw.items():
                    taker = _safe_float(info.get("taker", 0))
                    if taker > 0 and sym in valid_fut_symbols:
                        eff = taker * (1 - GATE_FUTURES_REBATE)
                        fut_fees[sym] = eff
                        fut_fees["__default__"] = eff
                        fut_raw_rate = taker
        except Exception:
            pass
        if fut_raw_rate > 0:
            logger.debug("Gate 合约费率: 基础=%.3f%% → 返佣%.0f%%后=%.3f%%",
                        fut_raw_rate * 100, GATE_FUTURES_REBATE * 100,
                        fut_fees["__default__"] * 100)
        else:
            logger.warning("Gate 合约费率查询失败，使用默认值 %.3f%%",
                           fut_fees["__default__"] * 100)

        return spot_fees, fut_fees



    async def _load_gate_margin_bases(self) -> list[str]:
        """拉取 Gate 所有可逐仓借贷的币种列表（公开接口，缓存 24h，直连 REST）。"""
        now = time.monotonic()
        if self._gate_margin_bases and (now - self._gate_margin_bases_ts) < 86_400:
            return self._gate_margin_bases
        try:
            resp = await self._gate_request("/margin/uni/currency_pairs", timeout=15)
            bases: set[str] = set()
            for item in (resp if isinstance(resp, list) else []):
                pair = str(item.get("currency_pair", "") if isinstance(item, dict) else "")
                if pair.endswith("_USDT") and pair.count("_") == 1:
                    bases.add(pair.replace("_USDT", "").upper())
            self._gate_margin_bases = sorted(bases)
            self._gate_margin_bases_ts = now
            logger.info("Gate 可借贷币种: %d 个（缓存 24h）", len(bases))
        except Exception:
            pass
        return self._gate_margin_bases



    async def _fetch_gate_margin_borrow_rates(self, assets: list[str]) -> dict[str, float]:
        """Gate 逐仓借币利率。
        先加载 Gate 支持借贷的币种列表，只查已知可借贷的币，避免大量无意义查询。
        返回 {base_upper: hourly_rate_float}。"""
        if not assets or not GATE_API_KEY:
            return {}

        # 预先筛掉 Gate 不支持借币的币种（大部分都不可借，筛掉后批量请求不会整批失败）
        margin_bases = await self._load_gate_margin_bases()
        margin_set = set(margin_bases)
        query_assets = [a for a in assets if a.upper() in margin_set]
        if not query_assets:
            return {}

        rates: dict[str, float] = {}

        def _parse_response(data) -> None:
            if isinstance(data, dict):
                for currency, rate_str in data.items():
                    base = currency.upper()
                    rate_val = _safe_float(rate_str)
                    if rate_val > 0:
                        rates[base] = rate_val
            elif isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        pair = str(entry.get("currency_pair", ""))
                        base = pair.replace("_USDT", "").upper()
                        if base:
                            rate_val = _safe_float(entry.get("hourly_rate", entry.get("rate", 0)))
                            if rate_val > 0:
                                rates[base] = rate_val

        batch_size = 10
        for i in range(0, len(query_assets), batch_size):
            chunk = query_assets[i:i + batch_size]
            currencies_str = ",".join(chunk)
            try:
                resp = await self.gate_spot.private_margin_get_uni_estimate_rate(
                    {"currencies": currencies_str}
                )
                _parse_response(resp)
            except Exception:
                pass  # 理论上不应再失败；若失败静默跳过

        if rates:
            logger.debug("Gate 借币利率: %d/%d 币种可借贷", len(rates), len(assets))
        return rates

    # ── Gate.io 交易基础设施 ──



    def _compute_funding_stats(self, df: pd.DataFrame) -> tuple[float, float]:
        """计算全市场资金费率中位数和标准差，用于异动检测。"""
        rates = pd.to_numeric(df["predicted_funding_rate"], errors="coerce").dropna()
        if rates.empty:
            return 0.0, 0.0
        return float(rates.median()), float(rates.std())



    def _detect_rate_anomaly(self, df: pd.DataFrame, median: float, std: float
                             ) -> pd.Series | None:
        """检测费率异动: 负向偏离中位数 > PRE_BORROW_SIGMA 个标准差 且 费率已为负。
        返回最优异动币（最低费率），无则返回 None。
        仅扫描反向可做的币（有 margin 交易对）。
        """
        if std <= 0 or median >= 0:
            return None
        threshold = median - PRE_BORROW_SIGMA * std
        best: pd.Series | None = None
        best_rate = 0.0
        for _, row in df.iterrows():
            rate = float(row["predicted_funding_rate"])
            direction = str(row.get("direction", "forward"))
            if direction != "reverse":
                continue
            if rate < PRE_BORROW_MIN_RATE and rate < threshold:
                if best is None or rate < best_rate:
                    best = row
                    best_rate = rate
        return best


