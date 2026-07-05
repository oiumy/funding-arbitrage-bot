"""FundingArbitrageBot — main class with multiple inheritance."""
from __future__ import annotations
from typing import Any, TYPE_CHECKING
from .constants import *
from . import constants as _c
from .models import ArbitrageState, LegResult, CrossArbitrageState
from .ws import WebSocketMixin
from .rest import ExchangeRestMixin
from .scanner import ScannerMixin
from .trader import TraderMixin
from .dashboard import DashboardMixin

if TYPE_CHECKING:
    from .strategies import (BnForwardStrategy, BnReverseStrategy,
                             GateForwardStrategy, GateReverseStrategy,
                             CrossExchangeStrategy, BnSnipeStrategy)

class FundingArbitrageBot(WebSocketMixin, ExchangeRestMixin, ScannerMixin, TraderMixin, DashboardMixin):
    """Cross-exchange funding rate arbitrage bot."""


    def __init__(self) -> None:
        self.tz = ZoneInfo(LOCAL_TIMEZONE)
        self.binance_state = self._load_state("binance")
        self.gate_state = self._load_state("gate")
        self.margin_state = self._load_margin_state()
        self.cross_state = self._load_cross_state()
        self._next_snipe_settle_ms = 0
        self.spot = self._create_spot_exchange()
        self.futures = self._create_futures_exchange()
        self.gate_spot = self._create_gate_spot_exchange()
        self.gate_futures = self._create_gate_futures_exchange()
        self._alpha_tokens: dict[str, dict[str, Any]] = {}
        self._alpha_last_fetch: float = 0.0
        self._rest_session = requests.Session()  # Binance 直连 REST 持久连接池
        self._scan_lock = asyncio.Lock()  # 防止预借和主扫描冲突
        self._borrow_blacklist: dict[str, float] = {}  # base → 过期时间戳, 借币池空后暂避
        self._fee_cache: dict[str, tuple[float, Any]] = {}  # key → (ts, result), 避免同轮重复查询
        self._gate_margin_bases: list[str] = []  # Gate 可借贷币种列表缓存
        self._gate_margin_bases_ts: float = 0.0
        self._leverage_set: set[tuple[str, str, int]] = set()  # 已设杠杆缓存 (exchange, symbol, leverage)
        self._fee_lock = asyncio.Lock()
        self._bn_listen_key: str | None = None
        self._bn_listen_key_ts: float = 0.0
        self._funding_event = asyncio.Event()
        self._funding_session: aiohttp.ClientSession | None = None
        self._funding_ws: Any = None
        self._gate_funding_event = asyncio.Event()
        self._gate_funding_symbol: str = ""
        self._gate_funding_baseline: float = 0.0
        # 策略实例（initialize 末尾创建）
        self._bn_forward: BnForwardStrategy | None = None
        self._bn_reverse: BnReverseStrategy | None = None
        self._gate_forward: GateForwardStrategy | None = None
        self._gate_reverse: GateReverseStrategy | None = None
        self._cross: CrossExchangeStrategy | None = None
        self._bn_snipe: BnSnipeStrategy | None = None
        self._gate_funding_amount: float = 0.0  # WS 检测到的实际资费
        # BN + Gate 交易 WS（持久连接，下单省 HTTP 握手 + to_thread 开销）
        self._bn_trade_ws: Any = None
        self._bn_trade_session: aiohttp.ClientSession | None = None
        self._bn_trade_futures: dict[str, asyncio.Future] = {}
        self._gate_trade_ws: Any = None
        self._gate_trade_session: aiohttp.ClientSession | None = None
        self._gate_trade_futures: dict[str, asyncio.Future] = {}
        # WS 持仓缓存（开平仓验证用，免 REST 网络往返）
        self._bn_ws_positions: dict[str, float] = {}  # "SYMBOL|SHORT" → abs(positionAmt)
        self._gate_ws_positions: dict[str, float] = {}  # "CONTRACT" → abs(size)

    @staticmethod
    def _ccxt_config(options: dict | None = None) -> dict[str, Any]:
        """构建 ccxt 配置，自动判断是否启用代理。"""
        cfg: dict[str, Any] = {
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
            "timeout": 30000,  # 30s, ccxt 默认 10s 有时不够
        }
        if HTTP_PROXY:
            cfg["aiohttp_proxy"] = HTTP_PROXY
            cfg["proxies"] = {"http": HTTP_PROXY, "https": HTTP_PROXY}
        if options:
            cfg.setdefault("options", {}).update(options)
        else:
            cfg.setdefault("options", {})
        return cfg

    @staticmethod
    def _create_spot_exchange() -> ccxt_async.binance:
        exchange = ccxt_async.binance(
            FundingArbitrageBot._ccxt_config(
                {"defaultType": "spot", "createMarketBuyOrderRequiresPrice": False, "adjustForTimeDifference": True}
            )
        )
        return exchange

    @staticmethod
    def _create_futures_exchange() -> ccxt_async.binanceusdm:
        exchange = ccxt_async.binanceusdm(
            FundingArbitrageBot._ccxt_config(
                {"defaultType": "future", "adjustForTimeDifference": True}
            )
        )
        return exchange

    @staticmethod
    def _gate_ccxt_config() -> dict[str, Any]:
        cfg: dict[str, Any] = {
            "apiKey": GATE_API_KEY,
            "secret": GATE_API_SECRET,
            "enableRateLimit": True,
            "timeout": 60000,
        }
        if HTTP_PROXY:
            cfg["aiohttp_proxy"] = HTTP_PROXY
            cfg["proxies"] = {"http": HTTP_PROXY, "https": HTTP_PROXY}
        return cfg

    @staticmethod
    def _create_gate_spot_exchange() -> ccxt_async.gate:
        return ccxt_async.gate(
            FundingArbitrageBot._gate_ccxt_config()
            | {"options": {"defaultType": "spot"}},
        )

    @staticmethod
    def _create_gate_futures_exchange() -> ccxt_async.gate:
        return ccxt_async.gate(
            FundingArbitrageBot._gate_ccxt_config()
            | {"options": {"defaultType": "swap"}},
        )



    async def initialize(self) -> None:
        """加载市场元数据，建立资费监听 WS 长连接。"""
        # 大块内存(>128KB)走 mmap，释放时直接归还 OS，不经过 glibc arena 囤积
        try:
            ctypes.CDLL("libc.so.6").mallopt(-8, 128 * 1024)  # M_MMAP_THRESHOLD
        except Exception:
            pass
        # 全部交易所 load_markets 加重试（VPS 时钟偏差/网络抖动可导致 InvalidNonce/超时）
        for name, ex, fatal in [("spot", self.spot, True), ("futures", self.futures, True),
                                ("gate_spot", self.gate_spot, False), ("gate_futures", self.gate_futures, False)]:
            for attempt in range(3):
                try:
                    await asyncio.wait_for(ex.load_markets(), timeout=30)
                    break
                except Exception as e:
                    if attempt < 2:
                        logger.warning("[初始化] %s load_markets 失败 (attempt %d/3): %s，3s 后重试...", name, attempt + 1, e)
                        await asyncio.sleep(3)
                    elif fatal:
                        raise
                    else:
                        logger.error("[初始化] %s load_markets 3 次均失败: %s", name, e)
        # 建立资费监听 + 交易 WS 长连接（Gate 资费监听复用交易 WS，不再单开连接）
        if _c.CROSS_EXCHANGE_ENABLED or _c.BN_SNIPE_ENABLED:
            await asyncio.gather(
                self._ensure_bn_funding_ws(),
                self._ensure_bn_trade_ws(),
                self._ensure_gate_trade_ws(),
            )
        # 创建策略实例
        from .strategies import (BnForwardStrategy, BnReverseStrategy,
                                 GateForwardStrategy, GateReverseStrategy,
                                 CrossExchangeStrategy, BnSnipeStrategy)
        if BINANCE_TRADING_ENABLED:
            self._bn_forward = BnForwardStrategy(self)
            self._bn_reverse = BnReverseStrategy(self)
        if GATE_TRADING_ENABLED:
            self._gate_forward = GateForwardStrategy(self)
            self._gate_reverse = GateReverseStrategy(self)
        if _c.CROSS_EXCHANGE_ENABLED:
            self._cross = CrossExchangeStrategy(self)
        if _c.BN_SNIPE_ENABLED:
            self._bn_snipe = BnSnipeStrategy(self)


    async def close(self) -> None:
        """关闭 ccxt 异步会话、REST 连接池、WS 长连接。"""
        self._rest_session.close()
        await asyncio.gather(
            self._safe_request("spot.close", lambda: self.spot.close()),
            self._safe_request("futures.close", lambda: self.futures.close()),
            self._safe_request("gate_spot.close", lambda: self.gate_spot.close()),
            self._safe_request("gate_futures.close", lambda: self.gate_futures.close()),
        )
        await self._close_bn_funding_ws()
        await self._close_bn_trade_ws()
        await self._close_gate_trade_ws()
        # 清理 aiohttp session
        for attr in ("_funding_session",
                      "_bn_trade_session", "_gate_trade_session"):
            sess = getattr(self, attr, None)
            if sess:
                try:
                    await sess.close()
                except Exception:
                    pass
        # 取消所有等待中的 WS 订单 futures
        for fut_dict_attr in ("_bn_trade_futures", "_gate_trade_futures"):
            for fut in getattr(self, fut_dict_attr, {}).values():
                if not fut.done():
                    fut.set_exception(Exception("bot shutting down"))

    @staticmethod
    def _load_state(exchange: str = "binance") -> ArbitrageState:
        file = GATE_STATE_FILE if exchange == "gate" else STATE_FILE
        if not file.exists():
            return ArbitrageState()

        try:
            with file.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return ArbitrageState(**payload)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            logger.warning("读取%s状态文件失败，将按无持仓启动: %s", exchange, exc)
            return ArbitrageState()



    def _save_state(self, exchange: str = "binance") -> None:
        state = self.gate_state if exchange == "gate" else self.binance_state
        file = GATE_STATE_FILE if exchange == "gate" else STATE_FILE
        with file.open("w", encoding="utf-8") as f:
            json.dump(asdict(state), f, indent=2, ensure_ascii=False)

    @staticmethod
    def _load_margin_state() -> dict:
        if not MARGIN_STATE_FILE.exists():
            return {"last_used": {}, "last_disabled": {}}
        try:
            with MARGIN_STATE_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"last_used": {}, "last_disabled": {}}



    def _save_margin_state(self) -> None:
        with MARGIN_STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(self.margin_state, f, indent=2, ensure_ascii=False)



    def _load_cross_state(self) -> CrossArbitrageState:
        if not CROSS_STATE_FILE.exists():
            return CrossArbitrageState()
        try:
            with CROSS_STATE_FILE.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            return CrossArbitrageState(**payload)
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            logger.warning("读取跨交易所状态文件失败: %s", exc)
            return CrossArbitrageState()



    def _save_cross_state(self) -> None:
        with CROSS_STATE_FILE.open("w", encoding="utf-8") as f:
            json.dump(asdict(self.cross_state), f, indent=2, ensure_ascii=False)



    def _record_margin_used(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_used", {})[clean] = time.time()
        self._save_margin_state()



    def _record_margin_disabled(self, margin_symbol: str) -> None:
        clean = self._clean_spot_symbol(margin_symbol)
        self.margin_state.setdefault("last_disabled", {})[clean] = time.time()
        self._save_margin_state()

    # ------------------------------------------------------------------
    # 交易历史 & 余额快照
    # ------------------------------------------------------------------

    TRADE_HISTORY_FILE: Path = Path("data/trade_history.json")
    BALANCE_SNAPSHOT_FILE: Path = Path("data/balance_snapshots.json")



    async def _has_gate_spot_balance(self) -> bool:
        """检查 Gate 现货是否持有仓位对应的 base 币。"""
        if not self.gate_state.base:
            return False
        bal = await self._safe_request(
            "gate_spot.fetch_balance_for_position",
            lambda: self._fetch_gate_spot_balance_direct(),
            default={},
        )
        free = float((bal.get("free", {}) or {}).get(self.gate_state.base, 0))
        return free > self.gate_state.amount * 0.5



    async def _has_gate_futures_position(self) -> bool:
        """检查 Gate 合约是否有对应方向的持仓。"""
        if not self.gate_state.futures_symbol:
            return False
        positions = await self._safe_request(
            "gate_futures.fetch_positions",
            lambda: self._fetch_gate_positions_direct(self.gate_state.futures_symbol),
            default=[],
        )
        for pos in positions:
            if pos.get("symbol") != self.gate_state.futures_symbol:
                continue
            contracts = self._position_contracts(pos)
            if self.gate_state.direction == "reverse":
                return contracts < 0  # 反向=做空合约
            return contracts > 0      # 正向=做多合约
        return False



    async def _has_gate_margin_loan(self) -> bool:
        """检查 Gate 逐仓是否有借币。"""
        if not self.gate_state.base or self.gate_state.direction != "reverse":
            return False
        acct = await self._gate_query_margin_account(self.gate_state.spot_symbol)
        return acct.get("base_borrowed", 0) > self.gate_state.amount * 0.1



    async def _has_gate_open_position(self) -> bool:
        """聚合检查 Gate 持仓是否仍然存在。不一致则重置状态。"""
        if not self.gate_state.is_open or self.gate_state.exchange != "gate":
            return False
        if self.gate_state.direction == "reverse":
            margin_ok = await self._has_gate_margin_loan()
            futures_ok = await self._has_gate_futures_position()
            if margin_ok or futures_ok:
                return True
        else:
            spot_ok = await self._has_gate_spot_balance()
            futures_ok = await self._has_gate_futures_position()
            if spot_ok or futures_ok:
                return True
        logger.warning("Gate 状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.gate_state = ArbitrageState()
        self._save_state("gate")
        return False

    # ── Gate.io 交易流程 ──



    async def _check_binance_position(self) -> bool:
        """验证 Binance 持仓是否仍然存在，不一致则重置状态。"""
        if not self.binance_state.is_open:
            return False

        if self.binance_state.direction == "reverse":
            futures_ok = await self._has_futures_long_position()
            if futures_ok:
                return True
            logger.warning("状态文件显示有反向持仓，但交易所未发现合约多仓，重置状态。")
            self.binance_state = ArbitrageState()
            self._save_state()
            return False

        spot_ok, futures_ok = await asyncio.gather(
            self._has_spot_balance(),
            self._has_futures_short_position(),
        )
        if spot_ok or futures_ok:
            return True

        logger.warning("状态文件显示有持仓，但交易所未发现对应仓位，重置状态。")
        self.binance_state = ArbitrageState()
        self._save_state()
        return False



    async def has_open_arbitrage_position(self) -> bool:
        """检查是否任一交易所有套利持仓。"""
        if self.cross_state.is_open:
            return True
        binance_ok, gate_ok = await asyncio.gather(
            self._check_binance_position(),
            self._has_gate_open_position(),
        )
        return binance_ok or gate_ok



    async def _rebalance_accounts(self) -> None:
        """开仓前划转 USDT：让 spot 和 futures 余额尽量均等，最大化可用仓位。

        如果差异 < REBALANCE_THRESHOLD 则不操作。
        资金费率收入打到 futures 账户，长期运行后 spot 侧会成为瓶颈，
        所以需要定期从 futures 划回 spot。
        """
        spot_free, futures_free = await asyncio.gather(
            self._safe_request(
                "spot.fetch_balance_for_rebalance",
                lambda: self.spot.fetch_balance(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_balance_for_rebalance",
                lambda: self.futures.fetch_balance(),
                default={},
            ),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")
        if spot_usdt <= 0 and futures_usdt > 0:
            logger.warning("现货余额获取失败（测试网现货 API 可能不可用），跳过划转。")
            return
        total = spot_usdt + futures_usdt
        if total <= 0:
            return

        diff = abs(spot_usdt - futures_usdt)
        if diff / total < REBALANCE_THRESHOLD:
            return  # 差异不大，不划转

        half_diff = self._floor_usdt(diff / 2)
        if half_diff < 1.0:
            return  # 金额太小

        if spot_usdt > futures_usdt:
            logger.info("划转 %.2f USDT: spot → futures", half_diff)
            await self._safe_request(
                "transfer_spot_to_futures",
                lambda: self.spot.transfer("USDT", half_diff, "spot", "future"),
                raise_error=True,
            )
        else:
            logger.info("划转 %.2f USDT: futures → spot", half_diff)
            await self._safe_request(
                "transfer_futures_to_spot",
                lambda: self.futures.transfer("USDT", half_diff, "future", "spot"),
                raise_error=True,
            )



    async def _get_position_size(self) -> float:
        """获取动态仓位大小：取 spot 和 futures 可用 USDT 的最小值 × 比例。

        无杠杆：spot 买币 + futures 保证金各占一份，所以取两者最小值。
        """
        spot_free, futures_free = await asyncio.gather(
            self._safe_request(
                "spot.fetch_balance_for_size",
                lambda: self.spot.fetch_balance(),
                default={},
            ),
            self._safe_request(
                "futures.fetch_balance_for_size",
                lambda: self.futures.fetch_balance(),
                default={},
            ),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")

        size = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO

        if size <= 0:
            logger.error("可用 USDT 余额为 0: spot=%s futures=%s", spot_usdt, futures_usdt)
            return 0.0

        logger.info(
            "动态仓位: spot可用=%.2f futures可用=%.2f → 每腿名义=%.2f USDT",
            spot_usdt, futures_usdt, size,
        )
        return size



    async def _safe_binance_balance(self, base_url: str, path: str) -> float:
        """直连 Binance REST 查 USDT 余额，ccxt 查不到时的 fallback。"""
        try:
            data = await self._binance_request(base_url, path)
            if path == "/api/v3/account":
                for b in data.get("balances", []):
                    if b.get("asset") == "USDT":
                        return float(b.get("free", 0))
            else:
                for b in (data if isinstance(data, list) else []):
                    if b.get("asset") == "USDT":
                        return float(b.get("balance", 0))
        except Exception:
            pass
        return 0.0

    @staticmethod
    def _free_balance(balance: dict[str, Any], asset: str) -> float:
        value = balance.get("free", {}).get(asset, 0)
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0



    async def _has_spot_balance(self) -> bool:
        if not self.binance_state.base:
            return False

        balance = await self._safe_request(
            "spot.fetch_balance",
            lambda: self.spot.fetch_balance(),
            default={},
        )
        free = balance.get("free", {}).get(self.binance_state.base, 0)
        used = balance.get("used", {}).get(self.binance_state.base, 0)
        try:
            return float(free) + float(used) > self.binance_state.amount * 0.5
        except (TypeError, ValueError):
            return False



    async def _has_futures_short_position(self) -> bool:
        if not self.binance_state.futures_symbol:
            return False

        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.binance_state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.binance_state.futures_symbol:
                continue
            contracts = self._position_contracts(position)
            return contracts < 0 or (
                position.get("side") == "short" and abs(contracts) > 0
            )
        return False



    async def _has_futures_long_position(self) -> bool:
        if not self.binance_state.futures_symbol:
            return False
        positions = await self._safe_request(
            "futures.fetch_positions",
            lambda: self.futures.fetch_positions([self.binance_state.futures_symbol]),
            default=[],
        )
        for position in positions:
            if position.get("symbol") != self.binance_state.futures_symbol:
                continue
            contracts = self._position_contracts(position)
            return contracts > 0 or (
                position.get("side") == "long" and abs(contracts) > 0
            )
        return False

    @staticmethod
    def _position_contracts(position: dict[str, Any]) -> float:
        for key in ("contracts", "contractSize"):
            value = position.get(key)
            if value is None:
                continue
            try:
                contracts = float(value)
                if key == "contracts":
                    return contracts
            except (TypeError, ValueError):
                pass

        info = position.get("info") or {}
        raw_amount = info.get("positionAmt", 0)
        try:
            return float(raw_amount)
        except (TypeError, ValueError):
            return 0.0



    async def _fetch_next_funding_time(self, futures_symbol: str) -> float:
        """获取合约的 nextFundingTime (毫秒时间戳)。失败则按 8h 估算。"""
        try:
            info = await self._safe_request(
                f"futures.fetch_funding_rate({futures_symbol})",
                lambda: self.futures.fetch_funding_rate(futures_symbol),
                default={},
            )
            nft = info.get("nextFundingTime") or info.get("nextFundingTimestamp")
            if nft and float(nft) > 0:
                return float(nft)
        except Exception as e:
            logger.warning("获取 %s 结算时间失败: %s，按 8h 估算", futures_symbol, e)

        # fallback: 现在 + 8 小时
        return time.time() * 1000 + DEFAULT_FUNDING_INTERVAL_HOURS * 3600_000




    async def _query_bn_funding_amount(self, symbol: str, settle_ms: int) -> float:
        """查币安结算时刻的实际 FUNDING_FEE 金额。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/income",
                {"symbol": clean, "incomeType": "FUNDING_FEE",
                 "startTime": settle_ms - 5000, "endTime": settle_ms + 60000, "limit": 1},
            )
            if isinstance(resp, list) and resp:
                return float(resp[0].get("income", 0) or 0)
        except Exception:
            pass
        return 0.0



    async def _check_bn_funding_received(self, symbol: str, after_ms: float) -> bool:
        """检查币安是否已收到指定币种的资金费（结算时间之后）。"""
        try:
            clean = self._clean_futures_symbol(symbol)
            resp = await self._binance_request(
                BINANCE_FUTURES_API, "/fapi/v1/income",
                {"symbol": clean, "incomeType": "FUNDING_FEE",
                 "startTime": int(after_ms) - 5000, "limit": 3},
            )
            if isinstance(resp, list):
                for item in resp:
                    if isinstance(item, dict) and item.get("time", 0) >= after_ms - 1000:
                        ft = datetime.fromtimestamp(item["time"] / 1000, tz=self.tz)
                        logger.info("[资费] BN 资费到账时间: %s", ft.strftime("%H:%M:%S.%f")[:-3])
                        return True
        except Exception:
            pass
        return False



    async def _check_gate_funding_received(self, symbol: str) -> bool:
        """检查Gate是否已收到指定币种的资金费（通过持仓 pnl_fund 变化判断）。"""
        try:
            positions = await self._safe_request(
                "gate_funding_check",
                lambda: self._fetch_gate_positions_direct(symbol),
                default=[],
            )
            for pos in positions:
                if pos.get("symbol") == symbol:
                    pnl_fund = float(pos.get("info", {}).get("pnl_fund", 0) or 0)
                    if abs(pnl_fund) > 0.0001:
                        return True
        except Exception:
            pass
        return False




    async def _update_cross_dashboard(self, bn_fut_bal: float, gt_fut_bal: float) -> None:
        """在跨所模式下填充看板余额变量。"""
        self._dash_futures_usdt = bn_fut_bal
        # 用总余额(含锁定保证金)，避免开仓后曲线骤降
        bn_total, gt_total = await asyncio.gather(
            self._cross_get_bn_total_balance(),
            self._cross_get_gate_total_balance(),
        )
        self._dash_total_usdt = bn_total
        self._dash_gate_spot = gt_total
        self._dash_gate_df = pd.DataFrame()



    async def run_once(self, force_entry: bool = False) -> None:
        """执行一次持仓检查、全市场扫描和开仓/平仓判断。

        Binance 和 Gate.io 各自独立运行，互不阻挡。
        force_entry=True: 跳过结算时间检查，允许随时开仓。
        """
        # ── 跨交易所套利模式 ──
        if _c.CROSS_EXCHANGE_ENABLED:
            await self._cross.run_cycle(force_entry)

            # ── 单交易所扫描（仅展示，不下单）──
            if CROSS_SHOW_SINGLE:
                await self._scan_single_for_dashboard()

            bn_total = getattr(self, "_dash_total_usdt", 0.0)
            gt_total = getattr(self, "_dash_gate_spot", 0.0)
            self._record_balance_snapshot(bn_total, gt_total)
            self._write_dashboard()
            # 强制回收内存，防止 RSS 持续上涨
            gc.collect()
            self._log_memory()
            return

        # ── Binance 资金费率狙击模式 ──
        if _c.BN_SNIPE_ENABLED:
            await self._bn_snipe.run_cycle()
            # 更新看板（狙击模式专用）
            bal = getattr(self._bn_snipe, "_dash_balance", 0.0)
            self._dash_futures_usdt = bal
            self._dash_total_usdt = bal
            self._dash_snipe_df = getattr(self._bn_snipe, "_dash_df", pd.DataFrame())
            self._dash_df = pd.DataFrame()  # 单所表留空，狙击用独立表
            self._dash_gate_spot = 0.0
            self._dash_gate_df = pd.DataFrame()
            self._write_dashboard()
            gc.collect()
            self._log_memory()
            return

        # ── 并行检查两交易所持仓 ──
        has_binance = False
        has_gate = False
        if BINANCE_TRADING_ENABLED:
            fwd_ok, rev_ok = await asyncio.gather(
                self._bn_forward.check_position(),
                self._bn_reverse.check_position(),
            )
            has_binance = fwd_ok or rev_ok
        if GATE_TRADING_ENABLED:
            fwd_ok, rev_ok = await asyncio.gather(
                self._gate_forward.check_position(),
                self._gate_reverse.check_position(),
            )
            has_gate = fwd_ok or rev_ok

        # ── 风险监控: Binance ──
        if has_binance and self.binance_state.direction == "reverse":
            margin_level = await self._check_margin_level(self.binance_state.spot_symbol)
            if margin_level is not None and margin_level < MARGIN_LEVEL_MIN:
                logger.critical("保证金率过低 %.2f < %.1f，强制平仓！",
                              margin_level, MARGIN_LEVEL_MIN)
                await self._bn_reverse.close_position()
                await asyncio.to_thread(self._send_email, "bazfbot 强制平仓！",
                    f"逐仓保证金率 {margin_level:.2f} 低于阈值 {MARGIN_LEVEL_MIN}，已强制平仓。")
                has_binance = False

        if has_binance and self.binance_state.direction == "forward":
            dist = await self._check_futures_liquidation_distance(
                self.binance_state.futures_symbol, short=True)
            if dist is not None and dist < FUTURES_LIQ_DISTANCE_MIN:
                logger.critical("合约空单距强平 %.1f%% < %.0f%%，强制平仓！",
                              dist * 100, FUTURES_LIQ_DISTANCE_MIN * 100)
                await self._bn_forward.close_position()
                await asyncio.to_thread(self._send_email, "bazfbot 强制平仓！",
                    f"合约空单距强平仅 {dist*100:.1f}%，已强制平仓。")
                has_binance = False

        if has_gate:
            logger.info("Gate 持仓中，风险监控 (Gate 暂不细粒度风控)。")

        # ── force_entry 时强平所有持仓 ──
        if force_entry and (has_binance or has_gate):
            logger.info("即时模式：强制平仓所有持仓。")
            if has_binance:
                s = self._bn_reverse if self.binance_state.direction == "reverse" else self._bn_forward
                await s.close_position()
                has_binance = False
            if has_gate:
                s = self._gate_reverse if self.gate_state.direction == "reverse" else self._gate_forward
                await s.close_position()
                has_gate = False

        # ── Binance 交易循环 ──
        spot_usdt = futures_usdt = margin_total = 0.0
        if BINANCE_TRADING_ENABLED:
            await self._run_once_binance(force_entry, has_binance, has_gate)

        # ── Gate.io 交易循环 ──
        if GATE_TRADING_ENABLED:
            await self._run_once_gate(has_binance, has_gate)

        # 记录余额快照
        bn_total = getattr(self, "_dash_total_usdt", 0.0)
        gt_total = getattr(self, "_dash_gate_spot", 0.0)
        self._record_balance_snapshot(bn_total, gt_total)

        # 写看板
        self._write_dashboard()



    async def _run_once_binance(self, force_entry: bool, has_binance: bool, has_gate: bool) -> None:
        """Binance 独立交易循环。"""
        has_position = has_binance

        active_strat = self._bn_reverse if self.binance_state.direction == "reverse" else self._bn_forward
        scan_for_switch = has_position and active_strat.should_exit()
        if scan_for_switch:
            if not self.binance_state.locked:
                logger.info("[Binance] 自由人模式，扫描全市场寻找换仓机会...")
            else:
                logger.info("[Binance] 锁定期结算已过，扫描全市场对比择优...")
        elif has_position:
            if self.binance_state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.binance_state.next_funding_time_ms / 1000, tz=self.tz)
                wait_min = max(0, (self.binance_state.next_funding_time_ms - time.time() * 1000) / 60000)
                logger.info("[Binance] 锁定期，结算时间 %s (%.0f 分钟后)，持仓待涨。",
                            settle_dt.strftime("%H:%M:%S"), wait_min)
            else:
                logger.info("[Binance] 锁定期，持仓待涨。")
            self._print_position_summary("binance")

        logger.info("┏━━ [Binance] 主扫开始 ━━")
        spot_free, futures_free = await asyncio.gather(
            self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
            self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
        )
        spot_usdt = self._free_balance(spot_free, "USDT")
        futures_usdt = self._free_balance(futures_free, "USDT")

        margin_total = 0.0
        try:
            resp = await self._binance_request(BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account")
            for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                q = acct.get("quoteAsset", {})
                if q:
                    margin_total += float(q.get("netAsset", 0))
        except Exception:
            pass

        total_usdt = spot_usdt + futures_usdt + margin_total
        logger.info("[Binance] 账户余额: 现货=%.2f | 合约=%.2f | 逐仓杠杆=%.2f | 合计=%.2f USDT",
                    spot_usdt, futures_usdt, margin_total, total_usdt)
        if total_usdt <= 0:
            logger.error("[Binance] 可用余额为 0，请充值。")
            return
        estimate_forward = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
        estimate_reverse = total_usdt / 2 * POSITION_SIZE_RATIO
        position_usdt = max(estimate_forward, estimate_reverse)
        if position_usdt <= 0:
            logger.error("[Binance] 估算仓位为 0，跳过本轮。")
            return

        df_forward, df_reverse = await asyncio.gather(
            self._bn_forward.scan(position_usdt),
            self._bn_reverse.scan(position_usdt),
        )
        frames = []
        if not df_forward.empty:
            frames.append(df_forward)
        if not df_reverse.empty:
            frames.append(df_reverse)

        if not frames:
            logger.info("[Binance] 本轮未扫描到满足条件的机会。")
            return

        df = pd.concat(frames, ignore_index=True).sort_values(
            "net_rate", ascending=False).reset_index(drop=True)

        # 预借逻辑
        if REVERSE_ENABLED and not has_position:
            if self.binance_state.pre_borrow_base:
                pre_row = df[(df["base"] == self.binance_state.pre_borrow_base)
                             & (df.get("direction", "forward") == "reverse")]
                if not pre_row.empty:
                    r = pre_row.iloc[0]
                    net = float(r["net_rate"])
                    target = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER
                    if net > target:
                        elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                            self.binance_state.pre_borrow_at)).total_seconds() / 60 if self.binance_state.pre_borrow_at else 0
                        logger.info("[预借→秒开] %s | 净利=%+.4f%% 已到开仓线 | 等待 %.0f min",
                                   self.binance_state.pre_borrow_base, net * 100, elapsed)
                        self.binance_state.pre_borrow_base = ""
                        self.binance_state.pre_borrow_margin_symbol = ""
                        self.binance_state.pre_borrow_amount = 0.0
                        self.binance_state.pre_borrow_at = ""
                        self._save_state()
                        if await self._bn_reverse.open_position(r):
                            return
                        logger.warning("[预借→秒开] 开仓失败，下轮重试。")
                    else:
                        elapsed = (datetime.now(tz=self.tz) - datetime.fromisoformat(
                            self.binance_state.pre_borrow_at)).total_seconds() / 60
                        if elapsed > PRE_BORROW_TIMEOUT_MINUTES:
                            await self._bn_reverse.cancel_pre_borrow()
                else:
                    logger.info("[预借] %s 已不在候选列表 → 取消", self.binance_state.pre_borrow_base)
                    await self._bn_reverse.cancel_pre_borrow()
            elif total_usdt > 10:
                median, std = self._compute_funding_stats(df)
                anomaly = self._detect_rate_anomaly(df, median, std)
                if anomaly is not None:
                    base = str(anomaly["base"])
                    rate = float(anomaly["predicted_funding_rate"]) * 100
                    logger.info("费率异动: %s → %.4f%% (中位数=%.4f%%, σ=%.4f%%)，触发预借！",
                               base, rate, median * 100, std * 100)
                    await self._bn_reverse.execute_pre_borrow(anomaly)

        # 打印 Binance Top 10
        top10 = df.head(10)
        sep = "  " + "-" * 113
        header = (f"  {'#':<3} {'方向':<4} {'币种':<8} {'来源':<5} {'资金费率':>8} "
                  f"{'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'结算倒计时':>10}")
        logger.info("\n" + "=" * 113 + "\n"
                    "  Binance 套利机会 Top 10 — 每腿名义: %.2f USDT\n" % position_usdt
                    + "=" * 113 + "\n" + header + "\n" + sep)
        for rank, (_, row) in enumerate(top10.iterrows(), 1):
            nft = float(row.get("next_funding_time_ms", 0))
            if nft > 0:
                mins = max(0, (nft - time.time() * 1000) / 60000)
                countdown = f"{int(mins)}min后" if mins < 120 else f"{mins/60:.0f}h后"
            else:
                countdown = "N/A"
            direction_label = "反向" if str(row.get("direction")) == "reverse" else "正向"
            borrow_fee = float(row.get("borrow_hourly_rate") or 0)
            held = has_position and str(row["base"]) == self.binance_state.base and str(row.get("direction", "forward")) == self.binance_state.direction
            held_tag = " ← 持仓" if held else ""
            logger.info("  %-3d %-4s %-8s %-5s %7.3f%% %7.3f%% %7.3f%% %7.3f%% %7.3f%% %10s%s",
                       rank, direction_label, str(row["base"])[:8], str(row["spot_source"])[:5],
                       float(row["predicted_funding_rate"]) * 100,
                       float(row["spot_taker_fee"]) * 100,
                       float(row["futures_taker_fee"]) * 100,
                       borrow_fee * 100, float(row["net_rate"]) * 100, countdown, held_tag)
        logger.info(sep + "\n" + "=" * 113)

        # Binance 交易决策
        await self._trade_decision(df, "binance", self._bn_forward, self._bn_reverse,
                                   self.binance_state, has_position,
                                   scan_for_switch, force_entry, spot_usdt,
                                   futures_usdt, margin_total, total_usdt, position_usdt)

        # 缓存仪表盘数据
        self._dash_futures_usdt = futures_usdt
        self._dash_total_usdt = total_usdt
        self._dash_df = df



    async def _run_once_gate(self, has_binance: bool, has_gate: bool) -> None:
        """Gate.io 独立交易循环。"""
        has_position = has_gate

        active_gate = self._gate_reverse if self.gate_state.direction == "reverse" else self._gate_forward
        scan_for_switch = has_position and active_gate.should_exit()
        if scan_for_switch:
            if not self.gate_state.locked:
                logger.info("[Gate] 自由人模式，扫描全市场寻找换仓机会...")
            else:
                logger.info("[Gate] 锁定期结算已过，扫描全市场对比择优...")
        elif has_position:
            if self.gate_state.next_funding_time_ms > 0:
                settle_dt = datetime.fromtimestamp(
                    self.gate_state.next_funding_time_ms / 1000, tz=self.tz)
                wait_min = max(0, (self.gate_state.next_funding_time_ms - time.time() * 1000) / 60000)
                logger.info("[Gate] 锁定期，结算时间 %s (%.0f 分钟后)，持仓待涨。",
                            settle_dt.strftime("%H:%M:%S"), wait_min)
            else:
                logger.info("[Gate] 锁定期，持仓待涨。")
            self._print_position_summary("gate")

        # 估算 Gate 仓位（单币种保证金模式：现货=合约=统一余额）
        try:
            gate_spot_usdt = await self._gate_spot_balance()
        except Exception:
            gate_spot_usdt = 0.0
        gate_position_usdt = (gate_spot_usdt / 2) * POSITION_SIZE_RATIO if gate_spot_usdt > 0 else 100.0

        logger.debug("[Gate] 账户余额: %.2f USDT (统一账户)", gate_spot_usdt)

        try:
            df_gf, df_gr = await asyncio.gather(
                self._gate_forward.scan(gate_position_usdt),
                self._gate_reverse.scan(gate_position_usdt),
            )
        except Exception as exc:
            logger.warning("Gate.io 扫描失败: %s", exc)
            return

        gframes = []
        if not df_gf.empty:
            gframes.append(df_gf)
        if not df_gr.empty:
            gframes.append(df_gr)

        if not gframes:
            logger.info("[Gate] 本轮未扫描到满足条件的机会。")
            return

        gdf = pd.concat(gframes, ignore_index=True).sort_values(
            "net_rate", ascending=False).reset_index(drop=True)

        # 打印 Gate Top 10
        gtop = gdf.head(10)
        gsep = "  " + "-" * 113
        gheader = (f"  {'#':<3} {'方向':<4} {'币种':<8} {'来源':<5} {'资金费率':>8} "
                   f"{'现货费':>8} {'合约费':>8} {'借币费':>8} {'净收益':>8} {'结算倒计时':>10}")
        logger.info("\n" + "=" * 113 + "\n"
                    "  Gate.io 套利机会 Top 10 — 每腿名义: %.2f USDT\n" % gate_position_usdt
                    + "=" * 113 + "\n" + gheader + "\n" + gsep)
        for rank, (_, row) in enumerate(gtop.iterrows(), 1):
            nft = float(row.get("next_funding_time_ms", 0))
            if nft > 0:
                mins = max(0, (nft - time.time() * 1000) / 60000)
                countdown = f"{int(mins)}min后" if mins < 120 else f"{mins/60:.0f}h后"
            else:
                countdown = "N/A"
            direction_label = "反向" if str(row.get("direction")) == "reverse" else "正向"
            borrow_fee = float(row.get("borrow_hourly_rate") or 0)
            logger.info("  %3d  %s  %-8s %-5s %+8.3f%% %7.3f%% %7.3f%% %7.3f%% %+8.3f%% %10s",
                       rank, direction_label, str(row["base"])[:8],
                       str(row.get("spot_source", "spot"))[:5],
                       float(row["predicted_funding_rate"]) * 100,
                       float(row["spot_taker_fee"]) * 100,
                       float(row["futures_taker_fee"]) * 100,
                       borrow_fee * 100, float(row["net_rate"]) * 100, countdown)
        logger.info(gsep + "\n" + "=" * 113)

        # Gate 交易决策
        await self._trade_decision(gdf, "gate", self._gate_forward, self._gate_reverse,
                                   self.gate_state, has_position,
                                   scan_for_switch, False,
                                   gate_spot_usdt, 0.0, 0.0,
                                   gate_spot_usdt, gate_position_usdt)

        # 缓存 Gate 仪表盘数据（单币种保证金：现货即总余额）
        self._dash_gate_spot = gate_spot_usdt
        self._dash_gate_df = gdf



    async def _trade_decision(self, df: pd.DataFrame, exchange: str,
                              forward_strat: Any, reverse_strat: Any,
                              state: ArbitrageState, has_position: bool,
                              scan_for_switch: bool, force_entry: bool,
                              spot_usdt: float, futures_usdt: float,
                              margin_total: float, total_usdt: float,
                              position_usdt: float) -> None:
        """统一交易决策：换仓/平仓/开仓。exchange="binance"|"gate"。

        forward_strat/reverse_strat 为策略实例，需实现 close_position() 和 open_position(row)。
        """
        if scan_for_switch:
            best = None
            if not df.empty:
                for _, row in df.iterrows():
                    net = float(row["net_rate"])
                    direction = str(row.get("direction", "forward"))
                    threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER if direction == "reverse" else MIN_NET_RATE
                    if net <= threshold:
                        continue
                    nft = float(row.get("next_funding_time_ms", 0))
                    same = (str(row["base"]) == state.base and direction == state.direction)
                    if direction != "reverse":
                        if not self._within_entry_window(nft) and nft > 0 and not same:
                            continue
                    best = row
                    break

            if best is None:
                if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                    logger.info("[%s] 无合格候选，手持币未到结算窗口 (结算 %s)，继续持有。",
                               exchange, settle_dt.strftime("%m-%d %H:%M"))
                    return
                logger.info("[%s] 结算窗口内无合格候选，平仓。", exchange)
                await (reverse_strat if state.direction == "reverse" else forward_strat).close_position()
                return

            best_base = str(best["base"])
            best_direction = str(best.get("direction", "forward"))
            same_coin = (best_base == state.base and best_direction == state.direction)

            if same_coin:
                keep_value = float(best["net_rate"])
                nft = float(best.get("next_funding_time_ms", 0))
                if keep_value > 0:
                    state.next_funding_time_ms = nft if nft > 0 else 0
                    state.locked = False
                    state.opened_at = datetime.now(tz=self.tz).isoformat()
                    self._save_state(exchange)
                    if nft > 0:
                        settle_dt = datetime.fromtimestamp(nft / 1000, tz=self.tz)
                        logger.info("[%s] 当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式，下轮结算 %s。",
                                   exchange, state.base, keep_value * 100,
                                   settle_dt.strftime("%m-%d %H:%M"))
                    else:
                        logger.info("[%s] 当前 %s 仍是最优 (净利=%.4f%%)，解锁自由人模式。",
                                   exchange, state.base, keep_value * 100)
                    return
                if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                    settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                    logger.info("[%s] 当前 %s 净利=%.4f%% 未到结算窗口 (结算 %s)，继续持有。",
                               exchange, state.base, keep_value * 100,
                               settle_dt.strftime("%m-%d %H:%M") if settle_dt else "?")
                    return
                logger.info("[%s] 当前 %s 结算窗口内净利=%.4f%%，平仓。", exchange, state.base, keep_value * 100)
                await (reverse_strat if state.direction == "reverse" else forward_strat).close_position()
                return

            # 不同币
            current_mask = (df["base"] == state.base) & (df["direction"] == state.direction)
            if current_mask.any():
                cr = df[current_mask].iloc[0]
                keep_value = float(cr["predicted_funding_rate"]) - float(cr.get("borrow_hourly_rate", 0)) \
                    if state.direction == "reverse" else float(cr["predicted_funding_rate"])
            else:
                keep_value = -1.0

            close_fee_rate = float(best["spot_taker_fee"]) + float(best["futures_taker_fee"])
            switch_value = float(best["open_only_net_rate"]) - close_fee_rate

            if switch_value > keep_value + MIN_SWITCH_SPREAD and switch_value > 0:
                logger.info("[%s] 切换: %s→%s | 保持净利=%.4f%% < 切换净利=%.4f%% (利差>%.3f%%)",
                           exchange, state.base, best_base, keep_value * 100,
                           switch_value * 100, MIN_SWITCH_SPREAD * 100)
                if await (reverse_strat if state.direction == "reverse" else forward_strat).close_position():
                    await (reverse_strat if str(best.get("direction")) == "reverse" else forward_strat).open_position(best)
                return

            if keep_value > 0:
                nft = float(cr.get("next_funding_time_ms", 0))
                state.next_funding_time_ms = nft if nft > 0 else 0
                state.locked = False
                state.opened_at = datetime.now(tz=self.tz).isoformat()
                self._save_state(exchange)
                logger.info("[%s] 当前 %s 仍有正收益 (净利=%.4f%%)，解锁自由人模式。",
                           exchange, state.base, keep_value * 100)
                return

            if state.next_funding_time_ms > 0 and not self._within_entry_window(state.next_funding_time_ms):
                settle_dt = datetime.fromtimestamp(state.next_funding_time_ms / 1000, tz=self.tz)
                logger.info("[%s] 当前 %s 净利=%.4f%% 未到结算窗口 (结算 %s)，继续持有。",
                           exchange, state.base, keep_value * 100, settle_dt.strftime("%m-%d %H:%M"))
                return
            logger.info("[%s] 当前 %s 结算窗口内净利=%.4f%%，平仓。", exchange, state.base, keep_value * 100)
            await (reverse_strat if state.direction == "reverse" else forward_strat).close_position()
            return

        # 无持仓：找最优开仓候选
        if not has_position:
            for _, candidate in df.iterrows():
                net = float(candidate["net_rate"])
                direction = str(candidate.get("direction", "forward"))
                threshold = REVERSE_MIN_NET_RATE + REVERSE_RATE_BUFFER if direction == "reverse" else MIN_NET_RATE
                if net <= threshold:
                    break
                nft = float(candidate.get("next_funding_time_ms", 0))
                if direction != "reverse":
                    if not self._within_entry_window(nft) and nft > 0 and not force_entry:
                        continue
                logger.info("[%s] 开仓 %s: net=%.4f%% > threshold=%.4f%%",
                           exchange, candidate["base"], net * 100, threshold * 100)
                ok = await (reverse_strat if str(candidate.get("direction")) == "reverse" else forward_strat).open_position(candidate)
                if ok:
                    return
                logger.warning("[%s] 开仓失败，尝试下一个候选。", exchange)
            logger.info("[%s] 所有候选均不符合开仓条件。", exchange)





    async def _query_evm_gas_price(self, chain: str) -> float | None:
        """Query current gas price for an EVM chain via JSON-RPC. Returns gas price in USD per tx."""

        rpc_url = CHAIN_RPC_URL.get(chain)
        if not rpc_url:
            return None
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_gasPrice",
            "params": [],
        }).encode("utf-8")
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            rpc_url,
                            data=payload,
                            headers={
                                "Content-Type": "application/json",
                                "User-Agent": "funding-arbitrage-bot/1.0",
                            },
                        )
                    ).read()
                )
            )
            gas_wei = int(response.get("result", "0x0"), 16)
            if gas_wei <= 0:
                return None
            # Standard ERC-20 transfer: ~65000 gas. Simple swap: ~150000 gas.
            # Alpha DEX trade: estimate ~200000 gas.
            estimated_gas_units = 200_000
            gas_eth = (gas_wei * estimated_gas_units) / 1e18
            # Approximate ETH price for gas chains (BSC uses BNB ~$600, others use ETH ~$2500)
            eth_price = 600 if chain == "BSC" else 2500
            return gas_eth * eth_price
        except Exception as exc:
            logger.warning("Failed to query gas for %s: %s", chain, exc)
            return None



    async def _query_sui_gas_price(self) -> float | None:
        """Query Sui reference gas price. Returns gas price in USD per tx."""

        rpc_url = CHAIN_RPC_URL.get("Sui")
        if not rpc_url:
            return None
        payload = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sui_getReferenceGasPrice",
            "params": [],
        }).encode("utf-8")
        try:
            response = await asyncio.to_thread(
                lambda: json.loads(
                    urllib.request.urlopen(
                        urllib.request.Request(
                            rpc_url,
                            data=payload,
                            headers={
                                "Content-Type": "application/json",
                                "User-Agent": "funding-arbitrage-bot/1.0",
                            },
                        )
                    ).read()
                )
            )
            ref_price_mist = int(response.get("result", "0"), 10)
            if ref_price_mist <= 0:
                return None
            # Sui: ~5M gas units for a simple move call. SUI price ~$2.
            estimated_gas_units = 5_000_000
            gas_sui = (ref_price_mist * estimated_gas_units) / 1e9  # MIST -> SUI
            return gas_sui * 2.0  # SUI ~$2
        except Exception as exc:
            logger.warning("Failed to query Sui gas: %s", exc)
            return None



    async def _check_alpha_gas(self, chain: str, expected_gas_usd: float) -> bool:
        """Check if current on-chain gas is within acceptable range. Returns True if safe to proceed."""
        if not chain or chain not in CHAIN_RPC_URL:
            return True  # Solana or unknown chain — gas is stable enough, skip check

        if chain in EVM_CHAINS:
            actual_gas = await self._query_evm_gas_price(chain)
        elif chain == "Sui":
            actual_gas = await self._query_sui_gas_price()
        else:
            return True

        if actual_gas is None:
            logger.warning("无法查询 %s 链上实时 Gas，跳过 Gas 监测，允许开仓。", chain)
            return True

        limit = expected_gas_usd * MAX_ALPHA_GAS_MULTIPLIER
        if actual_gas > limit:
            logger.warning(
                "Alpha Gas 超标: chain=%s 实际=$%.4f 预期=$%.4f 上限=$%.4f 跳过开仓",
                chain, actual_gas, expected_gas_usd, limit,
            )
            return False

        logger.info(
            "Alpha Gas 正常: chain=%s 实际=$%.4f 预期=$%.4f",
            chain, actual_gas, expected_gas_usd,
        )
        return True



    async def run_scan(self) -> None:
        """扫描模式：拉取全市场机会并打印，不下单。"""
        from .dashboard import _print_opportunity_table

        ref_usdt = 100.0  # 扫描模式按固定参考值显示
        if BINANCE_TRADING_ENABLED:
            spot_free, futures_free = await asyncio.gather(
                self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
                self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
            )
            spot_usdt = self._free_balance(spot_free, "USDT")
            futures_usdt = self._free_balance(futures_free, "USDT")
            bn_ref = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
            if bn_ref > 0:
                ref_usdt = bn_ref

        logger.info("扫描模式：拉取全市场套利机会...")

        # ── Binance 扫描 ──
        if BINANCE_TRADING_ENABLED:
            df_pos = await self._bn_forward.scan(ref_usdt)
            if df_pos.empty:
                print("\n  [Binance] 未发现正向套利机会。")
            else:
                _print_opportunity_table(df_pos, ref_usdt, direction="positive")

            if REVERSE_ENABLED:
                df_rev = await self._bn_reverse.scan(ref_usdt)
                if df_rev.empty:
                    print("\n  [Binance] 未发现反向套利机会（无可借贷币种或净收益不足）。")
                else:
                    _print_opportunity_table(df_rev, ref_usdt, direction="reverse")

        # ── Gate.io 扫描 ──
        if GATE_TRADING_ENABLED:
            try:
                df_gf = await self._gate_forward.scan(ref_usdt)
                df_gr = await self._gate_reverse.scan(ref_usdt)
                print("\n  " + "=" * 63)
                print("  Gate.io 套利机会")
                print("  " + "=" * 63)
                if not df_gf.empty:
                    _print_opportunity_table(df_gf, ref_usdt, direction="positive")
                else:
                    print("\n  [Gate 正向] 未发现机会。")
                if not df_gr.empty:
                    _print_opportunity_table(df_gr, ref_usdt, direction="reverse")
                else:
                    print("\n  [Gate 反向] 未发现机会（无可借贷币种或 API Key 未配置）。")
            except Exception as exc:
                print(f"\n  [Gate.io] 扫描失败: {exc}")

        # ── 跨交易所费率差扫描 ──
        if _c.CROSS_EXCHANGE_ENABLED:
            try:
                cross_df = await self._cross.scan(ref_usdt, strict=False)
                if cross_df.empty:
                    print("\n  [跨交易所] 两所无共同币种，无法扫描。")
                else:
                    self._print_cross_opportunity_table(cross_df, ref_usdt)
            except Exception as exc:
                print(f"\n  [跨交易所] 扫描失败: {exc}")

        # ── Binance 资金费率狙击扫描 ──
        if _c.BN_SNIPE_ENABLED:
            bn_bal = await self._cross_get_bn_futures_balance()
            ref_usdt = bn_bal * _c.BN_SNIPE_POSITION_SIZE_RATIO * CROSS_LEVERAGE if bn_bal > 0 else 100.0
            try:
                df = await self._bn_snipe.scan(ref_usdt)
                if df.empty:
                    print("\n  [狙击] 未发现符合条件的币种。")
                else:
                    print(f"\n  [狙击] Binance 合约余额={bn_bal:.2f} USDT, 可用仓位≈{ref_usdt:.2f} USDT")
                    self._bn_snipe._print_opportunity_table(df, ref_usdt)
            except Exception as exc:
                print(f"\n  [狙击] 扫描失败: {exc}")

    async def run_forever(self) -> None:
        """打卡模式主循环: 有持仓按实际结算时间平仓, 无持仓定时扫描。
        双循环: REST 快速轮询费率 (预借) + 60s 全量扫 (开平仓决策)。
        有持仓时 1s 密集监控，发现单腿消失立即平另一腿。"""
        await self.initialize()

        if _c.CROSS_EXCHANGE_ENABLED:
            logger.info("启动完成，进入跨交易所套利循环：60s 全量决策（有持仓时 1s 守护）。")
        elif _c.BN_SNIPE_ENABLED:
            logger.info("启动完成，进入 Binance 资金费率狙击循环。")
        else:
            logger.info("启动完成，进入双循环: REST 费率轮询 + 60s 全量决策。")

        async def poller_loop():
            if not _c.CROSS_EXCHANGE_ENABLED and not _c.BN_SNIPE_ENABLED:
                await self._run_ws_monitor()

        async def position_watchdog():
            """有跨所持仓时每秒验证，单腿消失 → 立即平另一腿。"""
            while True:
                if _c.CROSS_EXCHANGE_ENABLED and self.cross_state.is_open:
                    await self._cross.check_position()
                    await asyncio.sleep(1)
                else:
                    await asyncio.sleep(5)  # 无持仓时低频率

        async def main_loop():
            first_run = True
            while True:
                if not first_run:
                    sleep_seconds = self._seconds_until_next_wakeup()
                    wakeup_at = datetime.now(tz=self.tz) + timedelta(seconds=sleep_seconds)
                    has_pos = await self.has_open_arbitrage_position()
                    logger.info(
                        "┗━━ 主扫结束 | 休眠 %.0f 分钟, 下次: %s (%s持仓)",
                        sleep_seconds / 60,
                        wakeup_at.strftime("%m-%d %H:%M:%S"),
                        "有" if has_pos else "无",
                    )
                    await asyncio.sleep(sleep_seconds)
                else:
                    first_run = False
                async with self._scan_lock:
                    await self.run_once()

        poller_task = asyncio.create_task(poller_loop())
        watchdog_task = asyncio.create_task(position_watchdog())
        main_task = asyncio.create_task(main_loop())

        try:
            await asyncio.gather(poller_task, watchdog_task, main_task)
        except Exception:
            poller_task.cancel()
            watchdog_task.cancel()
            main_task.cancel()
            raise



    async def _scan_single_for_dashboard(self) -> None:
        """跨所模式下扫描单交易所套利机会（仅展示，不交易）。
        BN 和 Gate 并行扫描，互不依赖。"""
        async def _scan_bn() -> None:
            try:
                spot_free, futures_free = await asyncio.gather(
                    self._safe_request("spot.fetch_balance", lambda: self.spot.fetch_balance(), default={}),
                    self._safe_request("futures.fetch_balance", lambda: self.futures.fetch_balance(), default={}),
                )
                spot_usdt = self._free_balance(spot_free, "USDT")
                futures_usdt = self._free_balance(futures_free, "USDT")

                margin_total = 0.0
                try:
                    resp = await self._binance_request(BINANCE_SPOT_API, "/sapi/v1/margin/isolated/account")
                    for acct in (resp.get("assets", []) if isinstance(resp, dict) else []):
                        q = acct.get("quoteAsset", {})
                        if q:
                            margin_total += float(q.get("netAsset", 0))
                except Exception:
                    pass

                total_usdt = spot_usdt + futures_usdt + margin_total
                estimate_fwd = min(spot_usdt, futures_usdt) * POSITION_SIZE_RATIO if min(spot_usdt, futures_usdt) > 0 else 0
                estimate_rev = total_usdt / 2 * POSITION_SIZE_RATIO
                position_usdt = max(estimate_fwd, estimate_rev, 100)

                df_forward, df_reverse = await asyncio.gather(
                    self._bn_forward.scan(position_usdt),
                    self._bn_reverse.scan(position_usdt),
                )
                frames = [d for d in [df_forward, df_reverse] if not d.empty]
                if frames:
                    df = pd.concat(frames, ignore_index=True).sort_values(
                        "net_rate", ascending=False).reset_index(drop=True)
                else:
                    df = pd.DataFrame()

                self._dash_futures_usdt = futures_usdt
                self._dash_total_usdt = total_usdt
                self._dash_df = df
            except Exception:
                pass

        async def _scan_gate() -> None:
            if not GATE_TRADING_ENABLED:
                return
            try:
                gate_spot_usdt = await self._gate_spot_balance()
                gate_position_usdt = (gate_spot_usdt / 2) * POSITION_SIZE_RATIO if gate_spot_usdt > 0 else 100.0
                df_gf, df_gr = await asyncio.gather(
                    self._gate_forward.scan(gate_position_usdt),
                    self._gate_reverse.scan(gate_position_usdt),
                )
                gframes = [d for d in [df_gf, df_gr] if not d.empty]
                if gframes:
                    gdf = pd.concat(gframes, ignore_index=True).sort_values(
                        "net_rate", ascending=False).reset_index(drop=True)
                else:
                    gdf = pd.DataFrame()
                self._dash_gate_spot = gate_spot_usdt
                self._dash_gate_df = gdf
            except Exception:
                pass

        await asyncio.gather(_scan_bn(), _scan_gate())



    def _seconds_until_next_wakeup(self) -> float:
        """每分钟唤醒扫描，持有期间检查是否有更优机会可换仓。"""
        return SCAN_INTERVAL_MINUTES * 60
