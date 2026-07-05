"""CLI entry point for funding arbitrage bot."""
from __future__ import annotations
import asyncio
from .constants import *
from . import constants as _c
from .bot import FundingArbitrageBot


async def main() -> None:
    setup_logging()  # 必须在一切日志输出前调用

    import argparse
    parser = argparse.ArgumentParser(description="资金费率套利机器人 (Binance + Gate)")
    parser.add_argument("--scan", action="store_true", help="扫描模式：打印机会，不下单")
    parser.add_argument("--binance-only", action="store_true", help="单所期现套利，仅 Binance")
    parser.add_argument("--gate-only", action="store_true", help="单所期现套利，仅 Gate")
    parser.add_argument("--cross", action="store_true", help="跨交易所资金费率差套利")
    parser.add_argument("--bn-snipe", action="store_true", help="Binance 资金费率狙击：裸合约单向持仓")
    args = parser.parse_args()

    single_exchange = args.binance_only or args.gate_only
    mode_count = sum([args.cross, single_exchange, args.bn_snipe])

    if mode_count != 1:
        parser.error("请选择一种运行模式: --binance-only / --gate-only / --cross / --bn-snipe")

    if args.bn_snipe:
        _c.BN_SNIPE_ENABLED = True
        _c.BINANCE_TRADING_ENABLED = False
        _c.GATE_TRADING_ENABLED = False
        _c.CROSS_EXCHANGE_ENABLED = False
        logger.info("运行模式: Binance 资金费率狙击")
    elif args.cross:
        _c.BINANCE_TRADING_ENABLED = True
        _c.GATE_TRADING_ENABLED = True
        _c.CROSS_EXCHANGE_ENABLED = True
        logger.info("运行模式: 跨交易所资金费率差套利")
    else:
        _c.CROSS_EXCHANGE_ENABLED = False
        _c.BINANCE_TRADING_ENABLED = args.binance_only
        _c.GATE_TRADING_ENABLED = args.gate_only
        parts = []
        if args.binance_only:
            parts.append("Binance")
        if args.gate_only:
            parts.append("Gate")
        logger.info("运行模式: %s 单所期现套利", " + ".join(parts))

    bot = FundingArbitrageBot()

    try:
        await bot.initialize()

        if args.scan:
            await bot.run_scan()
        else:
            await bot.run_forever()
    finally:
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到 Ctrl+C，程序退出。")
