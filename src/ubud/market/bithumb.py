import json
import logging
from time import time

from .common import (
    AbsConnector,
    ASK,
    BID,
    DATETIME,
    TRADE,
    TICKER,
    AMOUNT,
    MARKET,
    ORDERBOOK,
    TRADE_DATETIME,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    CURRENCY,
    SYMBOL,
    BOOK_COUNT,
    TYPE,
    ts_to_strdt,
)

logger = logging.getLogger(__name__)

THIS_MARKET = "BITHUMB"
URL = "wss://pubwss.bithumb.com/pub/ws"
TYPE_PARAMS = {
    TICKER: "ticker",
    TRADE: "transaction",
    ORDERBOOK: "orderbookdepth",
}

# after receive 1st message
def register(
    ws,
    quote: str,
    symbols: list,
    tickTypes: list = ["30M", "1H", "12H", "24H", "MID"],
):
    assert quote in TYPE_PARAMS.keys(), f"[ERROR] unknown type {quote}"
    params = json.dumps(
        {
            "type": TYPE_PARAMS[quote],
            "symbols": symbols if isinstance(symbols, list) else [symbols],
            "tickTypes": tickTypes,
        }
    )
    ws.send(params)
    logger.info(f"[WEBSOCKET] Register Params: {params}")


# Handler Class
class Connector(AbsConnector):
    # TRADE
    def _call_trade(self, ws, msg):
        # messages in
        _received_dt = ts_to_strdt(time(), _float=True)
        logger.info(f"[WEBSOCKET] Receive Message from TRADE {_received_dt}")

        # load body
        body = json.loads(msg)
        logger.debug(f"[WEBSOKET] Body: {body}")

        # parse and pub
        if "content" in body.keys():
            try:
                content = body["content"]
                base_msg = {
                    DATETIME: _received_dt,
                    MARKET: THIS_MARKET,
                    TYPE: self.type,
                }
                messages = [
                    {
                        **base_msg,
                        **self._parse_symbol(r["symbol"]),
                        TRADE_DATETIME: r["contDtm"].replace(" ", "T") + "+0900",
                        ORDERTYPE: ASK if r["buySellGb"] == "1" else BID,
                        PRICE: float(r["contPrice"]),
                        QUANTITY: float(r["contQty"]),
                        AMOUNT: float(r["contAmt"]),
                    }
                    for r in content["list"]
                ]
                if self.broker is not None:
                    self.publish(messages)

            except Exception as ex:
                logger.warn(f"[Connector.__call__] {ex}")

    # ORDERBOOK
    def _call_orderbook(self, ws, msg):
        # messages in
        _received_dt = ts_to_strdt(time(), _float=True)
        logger.info(f"[WEBSOCKET] Receive Message from ORDERBOOK {_received_dt}")

        # load body
        body = json.loads(msg)
        logger.debug(f"[WEBSOKET] Body: {body}")

        # parse and pub
        if "content" in body.keys():
            try:
                content = body["content"]
                base_msg = {
                    DATETIME: ts_to_strdt(int(content["datetime"]) / 1e6),
                    MARKET: THIS_MARKET,
                    TYPE: self.type,
                }
                messages = [
                    {
                        **base_msg,
                        **self._parse_symbol(r["symbol"]),
                        ORDERTYPE: r["orderType"],
                        PRICE: float(r["price"]),
                        QUANTITY: float(r["quantity"]),
                        BOOK_COUNT: int(r["total"]),
                    }
                    for r in content["list"]
                ]
                if self.broker is not None:
                    self.publish(messages)

            except Exception as ex:
                logger.warn(f"[Connector.__call__] {ex}")

    @staticmethod
    def _parse_symbol(symbol):
        if "_" in symbol:
            symbol, cur = symbol.split("_")
            return {SYMBOL: symbol, CURRENCY: cur}
        return {SYMBOL: symbol, CURRENCY: "unknown"}
