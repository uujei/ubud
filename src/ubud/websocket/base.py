import asyncio
import json
import logging
import sys
from time import time
from typing import Callable
import paho.mqtt.client as mqtt
import websockets

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
    SYMBOLS,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    ts_to_strdt,
)


logger = logging.getLogger(__name__)


################################################################
# Market Conf
################################################################
THIS_MARKET = "BITHUMB"
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
                DATETIME: ts_ws_recv,
                MARKET: THIS_MARKET,
                QUOTE: TRADE,
            }
            messages = []
            for r in content["list"]:
                symbol_currency = await _split_symbol(r["symbol"])
                msg = {
                    **base_msg,
                    **symbol_currency,
                    TRADE_DATETIME: r["contDtm"].replace(" ", "T") + "+0900",
                    ORDERTYPE: ASK if r["buySellGb"] == "1" else BID,
                    PRICE: float(r["contPrice"]),
                    QUANTITY: float(r["contQty"]),
                    AMOUNT: float(r["contAmt"]),
                }
                if handler is not None:
                    handler(messages)
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
        except Exception as ex:
            logger.warn(f"[{__name__}] {ex}")


# [ORDERBOOK]
async def orderbook_parser(body, handler=None, ts_ws_recv=None):
    # parse and pub
    if "content" in body.keys():
        try:
            content = body["content"]
            base_msg = {
                DATETIME: ts_to_strdt(int(content["datetime"]) / 1e6),
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
                }
                if handler is not None:
                    handler(msg)
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
        except Exception as ex:
            logger.warn(f"[{__name__}] {ex}")


# PARSER
PARSER = {
    TRADE: trade_parser,
    ORDERBOOK: orderbook_parser,
}


################################################################
# BaseWebsocket
################################################################
class BaseWebsocket:
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

    # run
    async def run(self):
        async with websockets.connect(self.ws_url, **self.ws_conf) as ws:
            logger.info(f"[WEBSOCKET] Try Connect to '{self.ws_url}'")
            await self.request(ws, params=self.ws_params)
            while True:
                _ = await self._recv(ws, parser=self.parser, handler=self.handler)

    # start
    def start(self):
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            logger.error("[WEBSOCKET] Keboard Interrupt.. EXIT!")
        finally:
            if isinstance(self.handler, mqtt.Client):
                self.handler.close()

    @staticmethod
    async def _recv(ws, parser=None, handler=None):
        recv = await ws.recv()
        if recv is None:
            return
        recv = json.loads(recv)
        ts_ws_recv = time()
        logger.info(f"[WEBSOCKET] Receive Message from ORDERBOOK @ {ts_to_strdt(ts_ws_recv, _float=True)}")
        logger.info(f"[WEBSOCKET] Body: {recv}")
        if parser is not None:
            try:
                _ = await parser(body=recv, handler=handler, ts_ws_recv=ts_ws_recv)
            except Exception as ex:
                logger.warn(f"[WEBSOCKET] Error Parsing {ts_ws_recv}: {ex}")

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
    ws = BaseWebsocket(quote="orderbook", symbols=["BTC", "ETH", "WAVES"])
    ws.start()
