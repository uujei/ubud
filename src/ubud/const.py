from datetime import datetime
import pendulum

KST = pendulum.timezone("Asia/Seoul")

# QUOTE PARAMS (TYPE PARAMS)
TICKER = "ticker"
TRADE = "trade"
ORDERBOOK = "orderbook"

QUOTE_PARAMS = {
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
DATETIME = "datetime"
MARKET = "market"
QUOTE = "type"
SYMBOL = "symbol"
SYMBOLS = "symbols"
CURRENCY = "currency"
ORDERTYPE = "orderType"
ASK = "ask"
BID = "bid"
PRICE = "price"
QUANTITY = "quantity"
AMOUNT = "amount"

# TRADE ONLY
TRADE_DATETIME = "trade_dt"
TRADE_SID = "trade_sid"

# ORDERBOOK ONLY
TOTAL_QUANTITY = "total_qty"
BOOK_COUNT = "book_count"

# MQ TIMESTAMP
TS_MARKET = "ts_market"
TS_WS_SEND = "ts_ws_send"
TS_WS_RECV = "ts_ws_recv"
TS_MQ_SEND = "ts_mq_send"
TS_MQ_RECV = "ts_mq_recv"

# DATETIME FORMAT
DT_FMT = "%Y-%m-%dT%H:%M:%S%z"
DT_FMT_FLOAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# timestamp to string datetime (w/ ISO format)
def ts_to_strdt(ts, _float=False):
    _dt_fmt = DT_FMT_FLOAT if _float else DT_FMT
    return datetime.fromtimestamp(ts).astimezone(KST).strftime(_dt_fmt)