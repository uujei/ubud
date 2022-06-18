import asyncio
import json
import logging
import sys
from time import time
from typing import Callable

from .base import BaseStreamer
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
    TRADE_SID,
    ts_to_strdt,
)
from ..publisher.mqtt_publisher import Publisher

logger = logging.getLogger(__name__)


################################################################
# Market Conf
################################################################
THIS_MARKET = "UPBIT"
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


def _split_symbol(symbol):
    DELIM = "-"
    if DELIM in symbol:
        cur, symbol = symbol.split(DELIM)
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


def _generate_ws_params(quote, symbols):
    """Override Required
    Example is for BITHUMB
    """
    return [
        {"ticket": f"{quote}-{'|'.join(sorted(symbols))}".lower()},
        {
            "type": QUOTE_PARAMS[quote],
            "codes": symbols,
            "isOnlyRealtime": True,
        },
        {"format": "SIMPLE"},
    ]


async def _request(ws, params):
    """Override Required
    Example is for BITHUMB
    """
    logger.info(f"[WEBSOCKET] Requsts with Parameters {params}")
    params = json.dumps(params)
    await ws.send(params)
    logger.info("[WEBSOCKET] Connected Successfully...")


################################################################
# Market Parsers
################################################################
# TRADE
async def trade_parser(body, handler=None, ts_recv=None):
    # load body
    try:
        symbol_currency = _split_symbol(body["cd"])
        msg = {
            DATETIME: ts_to_strdt(int(body["tms"]) / 1e3),
            MARKET: THIS_MARKET,
            QUOTE: TRADE,
            **symbol_currency,
            TRADE_SID: body["sid"],
            TRADE_DATETIME: ts_to_strdt(int(body["ttms"]) / 1e3),
            ORDERTYPE: body["ab"].lower(),
            PRICE: body["tp"],
            QUANTITY: body["tv"],
        }
        logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
        if handler is not None:
            handler(msg)
    except Exception as ex:
        logger.warn(f"[{__name__}] {ex}")


# ORDERBOOK
async def orderbook_parser(body, handler=None, ts_recv=None):
    # parse and pub
    if "obu" in body.keys():
        # base message
        symbol_currency = _split_symbol(body["cd"])
        base_msg = {
            DATETIME: ts_to_strdt(int(body["tms"]) / 1e3),
            MARKET: THIS_MARKET,
            QUOTE: ORDERBOOK,
            **symbol_currency,
        }
        # parse and pub
        try:
            for r in body["obu"]:
                msg = {**base_msg, ORDERTYPE: ASK, PRICE: r["ap"], QUANTITY: r["as"]}
                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
                if handler is not None:
                    handler(msg)
                msg = {**base_msg, ORDERTYPE: BID, PRICE: r["bp"], QUANTITY: r["bs"]}
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
# Streamer
################################################################
class Streamer(BaseStreamer):
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
        self.ws_params = _generate_ws_params(self.quote, self.symbols)
        self.request = _request
        self.parser = PARSER[quote]
        self.handler = handler


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    quote = sys.argv[1] if len(sys.argv) > 1 else "orderbook"
    publisher = Publisher()
    ws = Streamer(quote="orderbook", symbols=["BTC", "ETH", "WAVES"], handler=publisher)
    ws.start()
