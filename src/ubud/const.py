from datetime import datetime

import parse
import pendulum

__all__ = [
    "KST",
    "UTC",
    "TICKER",
    "TRADE",
    "ORDERBOOK",
    "DATETIME",
    "MARKET",
    "CHANNEL",
    "SYMBOL",
    "CURRENCY",
    "ORDERTYPE",
    "ASK",
    "BID",
    "PRICE",
    "QUANTITY",
    "AMOUNT",
    "TRADE_DATETIME",
    "TRADE_SID",
    "TOTAL_QUANTITY",
    "BOOK_COUNT",
    "TS_MARKET",
    "TS_WS_SEND",
    "TS_WS_RECV",
    "TS_MQ_SEND",
    "TS_MQ_RECV",
]

KST = pendulum.timezone("Asia/Seoul")
UTC = pendulum.timezone("UTC")

# QUOTATION COMMON
TOPIC = "topic"
TICKER = "ticker"
TRADE = "trade"
ORDERBOOK = "orderbook"
CATEGORY = "category"
DATETIME = "datetime"
MARKET = "market"
CHANNEL = "channel"
SYMBOL = "symbol"
CURRENCY = "currency"
ORDERTYPE = "orderType"
RANK = "rank"
ASK = "ask"
BID = "bid"
PRICE = "price"
QUANTITY = "quantity"
AMOUNT = "amount"
BOOKCOUNT = "bookcount"

# TRADE ONLY
TRADE_DATETIME = "trade_dt"
TRADE_SID = "trade_sid"

# ORDERBOOK ONLY
TOTAL_QUANTITY = "total_qty"
BOOK_COUNT = "book_count"

# FOREX
CODES = "codes"

# MQ TIMESTAMP
TS_MARKET = "_ts_market"
TS_WS_SEND = "_ts_ws_send"
TS_WS_RECV = "_ts_ws_recv"
TS_MQ_SEND = "_ts_mq_send"
TS_MQ_RECV = "_ts_mq_recv"

# KEY RULE & PARSER
QUOTATION_KEY_RULE = [TOPIC, CATEGORY, CHANNEL, MARKET, SYMBOL, CURRENCY, ORDERTYPE, RANK]
QUOTATION_KEY_PARSER = parse.compile("/".join(["{{{}}}".format(x) for x in QUOTATION_KEY_RULE]))

PREMIUM_KEY_RULE = [TOPIC, CATEGORY, CHANNEL, MARKET, SYMBOL, CURRENCY]
PREMIUM_KEY_PARSER = parse.compile("/".join(["{{{}}}".format(x) for x in PREMIUM_KEY_RULE]))

BALANCE_KEY_RULE = [TOPIC, CATEGORY, MARKET, SYMBOL]
BALANCE_KEY_PARSER = parse.compile("/".join(["{{{}}}".format(x) for x in BALANCE_KEY_RULE]))

FOREX_KEY_RULE = [TOPIC, CATEGORY, CODES]
FOREX_KEY_PARSER = parse.compile("/".join(["{{{}}}".format(x) for x in FOREX_KEY_RULE]))

KEY_RULE = {
    "quotation": QUOTATION_KEY_RULE,
    "balance": BALANCE_KEY_RULE,
    "forex": FOREX_KEY_RULE,
    "premium": PREMIUM_KEY_RULE,
}

KEY_PARSER = {
    "quotation": QUOTATION_KEY_PARSER.parse,
    "balance": BALANCE_KEY_PARSER.parse,
    "forex": FOREX_KEY_PARSER.parse,
    "premium": PREMIUM_KEY_PARSER.parse,
}
