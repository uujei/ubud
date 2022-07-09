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
    API_CATEGORY,
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
    CHANNEL,
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
THIS_API_CATEGORY = "quotation"
CHANNEL_PARAMS = {
    TICKER: "ticker",
    TRADE: "transaction",
    ORDERBOOK: "orderbookdepth",
}


################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    if "_" in symbol:
        return symbol.upper()
    return f"{symbol}_{currency}".upper()


def _split_symbol(symbol):
    if "_" in symbol:
        symbol, cur = symbol.split("_")
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


################################################################
# Market Parsers
################################################################
# [TRADE]
async def trade_parser(body, ts_ws_recv=None):
    messages = []

    # parse and pub
    if "content" in body.keys():
        try:
            content = body["content"]
            base_msg = {
                MARKET: THIS_MARKET,
                API_CATEGORY: THIS_API_CATEGORY,
                CHANNEL: TRADE,
            }
            for r in content["list"]:
                symbol_currency = _split_symbol(r["symbol"])
                trade_datetime = r["contDtm"].replace(" ", "T") + "+0900"
                ts_market = datetime.strptime(trade_datetime, DT_FMT_FLOAT).timestamp()
                msg = {
                    DATETIME: trade_datetime,
                    **base_msg,
                    **symbol_currency,
                    ORDERTYPE: ASK if r["buySellGb"] == "1" else BID,
                    PRICE: float(r["contPrice"]),
                    QUANTITY: float(r["contQty"]),
                    AMOUNT: float(r["contAmt"]),
                    TS_MARKET: ts_market,
                    TS_WS_RECV: ts_ws_recv,
                }
                messages += [msg]
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
        except Exception as ex:
            logger.warn(f"[{__name__}] {ex}")

    return messages


# [ORDERBOOK]
async def orderbook_parser(body, ts_ws_recv=None):
    try:
        # parse and pub
        if "content" in body.keys():
            messages = []
            content = body["content"]
            ts_ws_send = int(content["datetime"]) / 1e6
            base_msg = {
                DATETIME: ts_to_strdt(ts_ws_send),
                MARKET: THIS_MARKET,
                API_CATEGORY: THIS_API_CATEGORY,
                CHANNEL: ORDERBOOK,
            }
            for r in content["list"]:
                symbol_currency = _split_symbol(r["symbol"])
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
                messages += [msg]
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

    except Exception as ex:
        logger.warn(f"[{__name__}] {ex}")

    return messages


# PARSER
PARSER = {
    TRADE: trade_parser,
    ORDERBOOK: orderbook_parser,
}


################################################################
# BithumbWebsocket
################################################################
class BithumbWebsocket(BaseWebsocket):
    ws_url = "wss://pubwss.bithumb.com/pub/ws"
    ws_conf = {"ping_interval": None}

    # init
    def __init__(
        self,
        channel: str,
        symbols: list,
        currencies: list = ["KRW"],
        handler: Callable = None,
        apiKey: str = None,
        apiSecret: str = None,
    ):
        # dummy properties for api consistency
        self.apiKey = apiKey
        self.apiSecret = apiSecret

        assert channel in CHANNEL_PARAMS, f"[ERROR] unknown channel '{channel}'!"
        self.channel = CHANNEL_PARAMS[channel]
        assert isinstance(symbols, (list, tuple)), "[ERROR] 'symbols' should be a list!"
        assert isinstance(currencies, (list, tuple)), "[ERROR] 'currencies' should be a list!"
        self.symbols = [_concat_symbol_currency(s, c) for s in symbols for c in currencies]
        self.ws_params = self._generate_ws_params(self.channel, self.symbols)
        self.parser = PARSER[channel]
        self.handler = handler

    @staticmethod
    def _generate_ws_params(channel, symbols):
        return {"type": channel, "symbols": symbols}

    @staticmethod
    async def _request(ws, params):
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

    CHANNELS = ["orderbook", "trade"]

    async def tasks():
        coros = [BithumbWebsocket(channel=c, symbols=["BTC", "ETH", "WAVES"]).run() for c in CHANNELS]
        await asyncio.gather(*coros)

    asyncio.run(tasks())
