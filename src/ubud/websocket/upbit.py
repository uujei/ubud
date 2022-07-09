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
    API_CATEGORY,
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
THIS_API_CATEGORY = "quotation"
CHANNEL_PARAMS = {
    TICKER: "ticker",
    TRADE: "trade",
    ORDERBOOK: "orderbook",
}


################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    if "-" in symbol:
        return symbol.upper()
    return f"{currency}-{symbol}".upper()


def _split_symbol(symbol):
    DELIM = "-"
    if DELIM in symbol:
        cur, symbol = symbol.split(DELIM)
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


################################################################
# Market Parsers
################################################################
# TRADE
async def trade_parser(body, ts_ws_recv=None):
    # load body
    try:
        symbol_currency = _split_symbol(body["cd"])
        ts_ws_send = int(body["tms"]) / 1e3
        msg = {
            DATETIME: ts_to_strdt(int(body["ttms"]) / 1e3),
            MARKET: THIS_MARKET,
            API_CATEGORY: THIS_API_CATEGORY,
            CHANNEL: TRADE,
            **symbol_currency,
            TRADE_SID: body["sid"],
            ORDERTYPE: body["ab"].lower(),
            PRICE: body["tp"],
            QUANTITY: body["tv"],
            TS_WS_SEND: ts_ws_send,
            TS_WS_RECV: ts_ws_recv,
        }
        logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
    except Exception as ex:
        logger.warn(f"[{__name__}] {ex}")

    return [msg]


# ORDERBOOK
async def orderbook_parser(body, ts_ws_recv=None):
    messages = []
    try:
        # parse
        if "obu" in body.keys():
            # base message
            symbol_currency = _split_symbol(body["cd"])
            ts_ws_send = int(body["tms"]) / 1e3
            base_msg = {
                DATETIME: ts_to_strdt(ts_ws_send),
                MARKET: THIS_MARKET,
                API_CATEGORY: THIS_API_CATEGORY,
                CHANNEL: ORDERBOOK,
                **symbol_currency,
            }
            # parse and pub
            for r in body["obu"]:
                # ASK
                for _p, _q, _TYPE in [("ap", "as", ASK), ("bp", "bs", BID)]:
                    msg = {
                        **base_msg,
                        ORDERTYPE: _TYPE,
                        PRICE: r[_p],
                        QUANTITY: r[_q],
                        TS_WS_SEND: ts_ws_send,
                        TS_WS_RECV: ts_ws_recv,
                    }
                    messages += [msg]
                    logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

            # TOTAL_ASK_SIZE
            base_msg[CHANNEL] = f"{base_msg[CHANNEL]}_total_qty"
            msg = {**base_msg, ORDERTYPE: ASK, QUANTITY: body["tas"]}
            logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
            messages += [msg]

            # TOTAL_BID_SIZE
            msg = {**base_msg, ORDERTYPE: BID, QUANTITY: body["tbs"]}
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
# UpbitWebsocket
################################################################
class UpbitWebsocket(BaseWebsocket):
    ws_url = "wss://api.upbit.com/websocket/v1"
    ws_conf = {}

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
        return [
            {"ticket": f"{channel}-{'|'.join(sorted(symbols))}".lower()},
            {
                "type": CHANNEL_PARAMS[channel],
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

    CHANNELS = ["orderbook", "trade"]

    async def tasks():
        coros = [UpbitWebsocket(channel=c, symbols=["BTC", "ETH", "WAVES"]).run() for c in CHANNELS]
        await asyncio.gather(*coros)

    asyncio.run(tasks())
