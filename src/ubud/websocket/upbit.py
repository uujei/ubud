import asyncio
import json
import logging
import sys
from time import time
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
    TRADE_SID,
    TS_WS_SEND,
    TS_WS_RECV,
    TS_MARKET,
    ts_to_strdt,
)

logger = logging.getLogger(__name__)


################################################################
# Market Conf
################################################################
THIS_MARKET = "upbit"
API_CATEGORY = "quotation"
URL = "wss://api.upbit.com/websocket/v1"
QUOTE_PARAMS = {
    TICKER: "ticker",
    TRADE: "trade",
    ORDERBOOK: "orderbook",
}


################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    return f"{currency}-{symbol}".upper()


async def _split_symbol(symbol):
    DELIM = "-"
    if DELIM in symbol:
        cur, symbol = symbol.split(DELIM)
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


################################################################
# Market Parsers
################################################################
# TRADE
async def trade_parser(body, handler=None, ts_ws_recv=None):
    # load body
    try:
        symbol_currency = await _split_symbol(body["cd"])
        ts_ws_send = int(body["tms"]) / 1e3
        msg = {
            DATETIME: ts_to_strdt(ts_ws_send),
            MARKET: THIS_MARKET,
            QUOTE: TRADE,
            **symbol_currency,
            TRADE_SID: body["sid"],
            TRADE_DATETIME: ts_to_strdt(int(body["ttms"]) / 1e3),
            ORDERTYPE: body["ab"].lower(),
            PRICE: body["tp"],
            QUANTITY: body["tv"],
            TS_WS_SEND: ts_ws_send,
            TS_WS_RECV: ts_ws_recv,
        }
        logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
        if handler is not None:
            handler(msg)
    except Exception as ex:
        logger.warn(f"[{__name__}] {ex}")


# ORDERBOOK
async def orderbook_parser(body, handler=None, ts_ws_recv=None):
    # parse and pub
    if "obu" in body.keys():
        # base message
        symbol_currency = await _split_symbol(body["cd"])
        ts_ws_send = int(body["tms"]) / 1e3
        base_msg = {
            DATETIME: ts_to_strdt(ts_ws_send),
            MARKET: THIS_MARKET,
            QUOTE: ORDERBOOK,
            **symbol_currency,
        }
        # parse and pub
        try:
            for r in body["obu"]:
                msg = {
                    **base_msg,
                    ORDERTYPE: ASK,
                    PRICE: r["ap"],
                    QUANTITY: r["as"],
                    TS_WS_SEND: ts_ws_send,
                    TS_WS_RECV: ts_ws_recv,
                }
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
                if handler is not None:
                    handler(msg)
                msg = {
                    **base_msg,
                    ORDERTYPE: BID,
                    PRICE: r["bp"],
                    QUANTITY: r["bs"],
                    TS_WS_SEND: ts_ws_send,
                    TS_WS_RECV: ts_ws_recv,
                }
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
                if handler is not None:
                    handler(msg)
            # Additional Info (total_ask_size, total_bid_size)
            base_msg[QUOTE] = f"{base_msg[QUOTE]}_total_qty"
            msg = {**base_msg, ORDERTYPE: ASK, QUANTITY: body["tas"]}
            logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
            if handler is not None:
                handler(msg)
            msg = {**base_msg, ORDERTYPE: BID, QUANTITY: body["tbs"]}
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
# UpbitWebsocket
################################################################
class UpbitWebsocket(BaseWebsocket):
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
        self.ws_conf = {}
        self.ws_params = self._generate_ws_params(self.quote, self.symbols)
        self.request = self._request
        self.parser = PARSER[quote]
        self.handler = handler

    @staticmethod
    def _generate_ws_params(quote, symbols):
        return [
            {"ticket": f"{quote}-{'|'.join(sorted(symbols))}".lower()},
            {
                "type": QUOTE_PARAMS[quote],
                "codes": symbols,
                "isOnlyRealtime": True,
            },
            {"format": "SIMPLE"},
        ]

    @staticmethod
    async def _request(ws, params):
        logger.info(f"[WEBSOCKET] Requsts with Parameters {params}")
        params = json.dumps(params)
        await ws.send(params)
        logger.info("[WEBSOCKET] Connected Successfully...")


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    quote = sys.argv[1] if len(sys.argv) > 1 else "orderbook"
    ws = UpbitWebsocket(quote="orderbook", symbols=["BTC", "ETH", "WAVES"])
    ws.start()
