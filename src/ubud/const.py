from datetime import datetime

import pendulum

__all__ = [
    "KST",
    "UTC",
    "TICKER",
    "TRADE",
    "ORDERBOOK",
    "CHANNEL_PARAMS",
    "DATETIME",
    "MARKET",
    "CHANNEL",
    "SYMBOL",
    "SYMBOLS",
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
    "DT_FMT",
    "DT_FMT_FLOAT",
    "ts_to_strdt",
]

KST = pendulum.timezone("Asia/Seoul")
UTC = pendulum.timezone("UTC")

# CHANNEL PARAMS (TYPE PARAMS)
TICKER = "ticker"
TRADE = "trade"
ORDERBOOK = "orderbook"

CHANNEL_PARAMS = {
    "upbit": {
        TICKER: "ticker",
        TRADE: "trade",
        ORDERBOOK: "orderbook",
    },
    "bithumb": {
        TICKER: "ticker",
        TRADE: "transaction",
        ORDERBOOK: "orderbookdepth",
    },
}

# COMMON
API_CATEGORY = "api_category"
DATETIME = "datetime"
MARKET = "market"
CHANNEL = "channel"
SYMBOL = "symbol"
SYMBOLS = "symbols"
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

# MQ TIMESTAMP
TS_MARKET = "_ts_market"
TS_WS_SEND = "_ts_ws_send"
TS_WS_RECV = "_ts_ws_recv"
TS_MQ_SEND = "_ts_mq_send"
TS_MQ_RECV = "_ts_mq_recv"

# KEY RULES
# [NOTE] MQ SUBTOPIC RULES will be deprecated
MQ_SUBTOPICS = [API_CATEGORY, CHANNEL, MARKET, SYMBOL, CURRENCY, ORDERTYPE, RANK]
QUOTATION_KEY_RULE = [API_CATEGORY, CHANNEL, MARKET, SYMBOL, CURRENCY, ORDERTYPE, RANK]
EXCHANGE_KEY_RULE = [API_CATEGORY, CHANNEL, MARKET, SYMBOL]
FOREX_KEY_RULE = [API_CATEGORY, CHANNEL, "codes"]

# DATETIME FORMAT
DT_FMT = "%Y-%m-%dT%H:%M:%S%z"
DT_FMT_FLOAT = "%Y-%m-%dT%H:%M:%S.%f%z"


# timestamp to string datetime (w/ ISO format)
def ts_to_strdt(ts, _float=True):
    # _flaot is deprecated
    return datetime.fromtimestamp(ts).astimezone(KST).isoformat(timespec="microseconds")
