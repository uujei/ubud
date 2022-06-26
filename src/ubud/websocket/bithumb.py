import asyncio
import json
import logging
import sys
from time import time
from datetime import datetime
from typing import Callable

from .base import BaseWebsocket
from ..const import (
    AMOUNT,
    ASK,
    BID,
    BOOK_COUNT,
    CURRENCY,
    DATETIME,
    MARKET,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    QUOTE,
    SYMBOL,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    DT_FMT,
    DT_FMT_FLOAT,
    TS_WS_SEND,
    TS_MARKET,
    TS_WS_RECV,
    ts_to_strdt,
)

logger = logging.getLogger(__name__)

################################################################
# Market Conf
################################################################
THIS_MARKET = "bithumb"
API_CATEGORY = "quotation"
URL = "wss://pubwss.bithumb.com/pub/ws"
QUOTE_PARAMS = {
    TICKER: "ticker",
    TRADE: "transaction",
    ORDERBOOK: "orderbookdepth",
}


################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    """Override Required
    Example is for BITHUMB
    """
    return f"{symbol}_{currency}".upper()


async def _split_symbol(symbol):
    """Override Required
    Example is for BITHUMB
    """
    if "_" in symbol:
        symbol, cur = symbol.split("_")
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


################################################################
# Market Parsers
################################################################
# [TRADE]
async def trade_parser(body, handler=None, ts_ws_recv=None):
    # parse and pub
    if "content" in body.keys():
        try:
            content = body["content"]
            base_msg = {
                MARKET: THIS_MARKET,
                QUOTE: TRADE,
            }
            for r in content["list"]:
                symbol_currency = await _split_symbol(r["symbol"])
                trade_datetime = r["contDtm"].replace(" ", "T") + "+0900"
                ts_market = datetime.strptime(trade_datetime, DT_FMT_FLOAT).timestamp()
                msg = {
                    **base_msg,
                    **symbol_currency,
                    TRADE_DATETIME: trade_datetime,
                    ORDERTYPE: ASK if r["buySellGb"] == "1" else BID,
                    PRICE: float(r["contPrice"]),
                    QUANTITY: float(r["contQty"]),
                    AMOUNT: float(r["contAmt"]),
                    TS_MARKET: ts_market,
                    TS_WS_RECV: ts_ws_recv,
                }
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
                if handler is not None:
                    handler(msg)

        except Exception as ex:
            logger.warn(f"[{__name__}] {ex}")


# [ORDERBOOK]
async def orderbook_parser(body, handler=None, ts_ws_recv=None):
    # parse and pub
    if "content" in body.keys():
        try:
            content = body["content"]
            ts_ws_send = int(content["datetime"]) / 1e6
            base_msg = {
                DATETIME: ts_to_strdt(ts_ws_send),
                MARKET: THIS_MARKET,
                QUOTE: ORDERBOOK,
            }
            for r in content["list"]:
                symbol_currency = await _split_symbol(r["symbol"])
                msg = {
                    **base_msg,
                    **symbol_currency,
                    ORDERTYPE: r["orderType"],
                    PRICE: float(r["price"]),
                    QUANTITY: float(r["quantity"]),
                    BOOK_COUNT: int(r["total"]),
                    TS_WS_SEND: ts_ws_send,
                    TS_WS_RECV: ts_ws_recv,
                }
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
                if handler is not None:
                    handler(msg)

        except Exception as ex:
            logger.warn(f"[{__name__}] {ex}")


# PARSER
PARSER = {
    TRADE: trade_parser,
    ORDERBOOK: orderbook_parser,
}


################################################################
# BithumbWebsocket
################################################################
class BithumbWebsocket(BaseWebsocket):
    # init
    def __init__(
        self,
        quote: str,
        symbols: list,
        currency: str = "KRW",
        handler: Callable = None,
    ):
        assert quote in QUOTE_PARAMS, f"[ERROR] unknown quote '{quote}'!"
        self.quote = QUOTE_PARAMS[quote]
        assert isinstance(symbols, (list, tuple)), "[ERROR] 'symbols' should be a list!"
        self.symbols = [_concat_symbol_currency(s, currency) for s in symbols]
        self.ws_url = URL
        self.ws_conf = {
            "ping_interval": None,
        }
        self.ws_params = self._generate_ws_params(self.quote, self.symbols)
        self.request = self._request
        self.parser = PARSER[quote]
        self.handler = handler

    @staticmethod
    def _generate_ws_params(quote, symbols):
        """Override Required
        Example is for BITHUMB
        """
        return {"type": quote, "symbols": symbols}

    @staticmethod
    async def _request(ws, params):
        """Override Required
        Example is for BITHUMB
        """
        msg = await ws.recv()
        msg = json.loads(msg)
        if msg["status"] != "0000":
            raise ConnectionError(msg)
        logger.info(f"[WEBSOCKET] {msg['resmsg']} with Status Code {msg['status']}")
        logger.info(f"[WEBSOCKET] Requsts with Parameters {params}")
        params = json.dumps(params)
        await ws.send(params)
        msg = await ws.recv()
        msg = json.loads(msg)
        if msg["status"] != "0000":
            raise ConnectionError(msg)
        logger.info(f"[WEBSOCKET] {msg['resmsg']} with Status Code {msg['status']}")


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    quote = sys.argv[1] if len(sys.argv) > 1 else "orderbook"
    ws = BithumbWebsocket(quote="orderbook", symbols=["BTC", "ETH", "WAVES"])
    ws.start()
