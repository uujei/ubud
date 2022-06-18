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

# timestamp to string datetime (w/ ISO format)
def ts_to_strdt(ts, _float=False):
    DTFMT = "%Y-%m-%dT%H:%M:%S%z"
    if _float:
        DTFMT = DTFMT.replace("%z", ".%f%z")
    return datetime.fromtimestamp(ts).astimezone(KST).strftime(DTFMT)
