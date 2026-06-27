# 复制此文件为 config.py，填入你的真实信息
# config.py 已被 .gitignore 忽略，不会上传到 GitHub

# ── Binance ──
BINANCE_API_KEY = "你的币安 API Key"
BINANCE_API_SECRET = "你的币安 API Secret"

# ── Gate.io ──
GATE_API_KEY = "你的 Gate.io API Key（仅扫描可以留空）"
GATE_API_SECRET = "你的 Gate.io API Secret"

# ── 通知 ──
NOTIFY_EMAIL = "你的QQ号@qq.com"
NOTIFY_EMAIL_AUTH = "QQ邮箱SMTP授权码"

# ── 策略模式 ──
# 跨交易所资金费率差异套利：--cross 启动
# 单交易所套利（BN/GT 独立）：不加 --cross 即可（默认模式）
# 用法示例：
#   python -m funding_arbitrage_bot.main --cross --scan   # 查看跨所机会
#   python -m funding_arbitrage_bot.main --cross --live    # 跨所实盘
