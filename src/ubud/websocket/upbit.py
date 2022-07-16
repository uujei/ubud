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
    BOOKCOUNT,
    MQ_SUBTOPICS,
    RANK,
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
# UpbitWebsocket
################################################################
class UpbitWebsocket(BaseWebsocket):
    ws_url = "wss://api.upbit.com/websocket/v1"

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

        # SELECT PARSER
        if channel == "trade":
            self.parser = self.trade_parser
        elif channel == "orderbook":
            self.parser = self.orderbook_parser
        else:
            raise ReferenceError(f"[WEBSOCKET] Unknown channel '{channel}'")

        # HANDLER
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

    # TRADE
    async def trade_parser(self, body, ts_ws_recv=None):
        messages = []

        # load body
        try:
            symbol_currency = _split_symbol(body["cd"])
            ts_ws_send = float(body["tms"]) / 1e3

            # Key (name)
            _key = {
                API_CATEGORY: THIS_API_CATEGORY,
                CHANNEL: TRADE,
                MARKET: THIS_MARKET,
                **symbol_currency,
                ORDERTYPE: body["ab"].lower(),
                RANK: 0,
            }
            name = "/".join([str(_key[k]) for k in MQ_SUBTOPICS])

            value = {
                DATETIME: ts_to_strdt(int(body["ttms"]) / 1e3),
                TRADE_SID: body["sid"],
                PRICE: body["tp"],
                QUANTITY: body["tv"],
                TS_WS_SEND: ts_ws_send,
                TS_WS_RECV: ts_ws_recv,
            }

            # Message
            msg = {"name": name, "value": value}
            messages += [msg]

            # logging
            logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

        except Exception as ex:
            logger.warn(f"[WEBSOCKET] Upbit Unknown Message: {body}")

        return [msg]

    # ORDERBOOK
    async def orderbook_parser(self, body, ts_ws_recv=None):
        messages = []
        try:
            # parse
            if "obu" in body.keys():
                # base message
                symbol_currency = _split_symbol(body["cd"])
                ts_ws_send = float(body["tms"]) / 1e3

                # parse and pub
                n = len(body["obu"])
                for i, r in enumerate(body["obu"]):
                    # ASK
                    for _p, _q, _ordertype in [("ap", "as", ASK), ("bp", "bs", BID)]:
                        # Key (name)
                        _key = {
                            API_CATEGORY: THIS_API_CATEGORY,
                            CHANNEL: ORDERBOOK,
                            MARKET: THIS_MARKET,
                            **symbol_currency,
                            ORDERTYPE: _ordertype,
                            RANK: i + 1,
                        }
                        name = "/".join([str(_key[k]) for k in MQ_SUBTOPICS])

                        value = {
                            DATETIME: ts_to_strdt(ts_ws_send),
                            PRICE: r[_p],
                            QUANTITY: r[_q],
                            TS_WS_SEND: ts_ws_send,
                            TS_WS_RECV: ts_ws_recv,
                        }
                        # Message
                        msg = {"name": name, "value": value}
                        messages += [msg]

                        # logging
                        logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")
            else:
                logger.warning(f"[WEBSOCKET] Upbit Unknown Message: {body}")
        except Exception as ex:
            logger.warn(f"[WEBSOCKET] Upbit Parse Error - {ex}")
        return messages


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
