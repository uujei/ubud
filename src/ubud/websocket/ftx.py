import asyncio
import hmac
import json
import logging
import sys
import time
from typing import Callable, DefaultDict, Deque, Dict, List, Optional, Tuple

import websockets

from ..const import (
    AMOUNT,
    API_CATEGORY,
    ASK,
    BID,
    BOOK_COUNT,
    CHANNEL,
    CURRENCY,
    DATETIME,
    MARKET,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    SYMBOL,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    TRADE_SID,
    TS_MARKET,
    TS_WS_RECV,
    TS_WS_SEND,
    ts_to_strdt,
)
from .base import BaseWebsocket

logger = logging.getLogger(__name__)

################################################################
# Market Conf
################################################################
THIS_MARKET = "ftx"
THIS_API_CATEGORY = "quotation"
CHANNEL_PARAMS = {
    TICKER: "ticker",
    TRADE: "trades",
    ORDERBOOK: "orderbook",
}
AVAILABLE_CURRENCIES = ["AUD", "BRZ", "BTC", "EUR", "JPY", "TRYB", "USD", "USDT"]

################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    if currency.startswith("-"):
        return f"{symbol}{currency}".upper()
    return f"{symbol}/{currency}".upper()


def _split_symbol(symbol):
    if "/" in symbol:
        symbol, cur = symbol.split("/", 1)
        return {SYMBOL: symbol, CURRENCY: cur}
    if "-" in symbol:
        symbol, cur = symbol.split("-", 1)
        return {SYMBOL: symbol, CURRENCY: cur}
    return {SYMBOL: symbol, CURRENCY: "unknown"}


################################################################
# Market Parsers
################################################################
# TRADE
async def trade_parser(body, ts_ws_recv=None):
    logger.info(body)
    return body


# ORDERBOOK
async def orderbook_parser(body, ts_ws_recv=None):
    logger.info(body)
    return body


# PARSER
PARSER = {
    TRADE: trade_parser,
    ORDERBOOK: orderbook_parser,
}


################################################################
# UpbitWebsocket
################################################################
class FtxWebsocket(BaseWebsocket):
    ws_url = "wss://ftx.com/ws/"
    ws_conf = {}

    def __init__(
        self,
        channel: str,
        symbols: list,
        currencies: list = ["USD"],
        handler: Callable = None,
        apiKey: str = None,
        apiSecret: str = None,
    ):
        """
        [NOTE] For API consistency,
         - currencies는 PERP가 될 수 있음. "-"로 시작하면 FUTURES로 인식
        """
        # FTX webocket requires api key and secret
        self.apiKey = apiKey
        self.apiSecret = apiSecret

        assert channel in CHANNEL_PARAMS, f"[ERROR] unknown channel '{channel}'!"
        self.channel = CHANNEL_PARAMS[channel]
        assert isinstance(symbols, (list, tuple)), "[ERROR] 'symbols' should be a list!"
        assert isinstance(currencies, (list, tuple)), "[ERROR] 'currencies' should be a list!"
        self.symbols = [_concat_symbol_currency(s, c) for s in symbols for c in currencies]
        self.ws_params = self._generate_ws_params()
        self.parser = PARSER[channel]
        self.handler = handler

    def _generate_ws_params(self):
        ts = int(time.time() * 1000)
        sign = hmac.new(self.apiSecret.encode(), f"{ts}websocket_login".encode(), "sha256").hexdigest()
        args = [{"op": "login", "args": {"key": self.apiKey, "sign": sign, "time": ts}}]
        for symbol in self.symbols:
            args += [{"op": "subscribe", "channel": self.channel, "market": symbol}]
        return args

    @staticmethod
    async def _request(ws, params):
        for _params in params:
            logger.info(f"[WEBSOCKET] Requsts with Parameters {_params}")
            await ws.send(json.dumps(_params))


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
    import os

    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    apiKey = os.getenv("FTX_KEY")
    apiSecret = os.getenv("FTX_SECRET")

    CHANNELS = ["orderbook", "trade"]

    async def tasks():
        coros = [
            FtxWebsocket(channel=c, symbols=["BTC", "ETH", "WAVES"], apiKey=apiKey, apiSecret=apiSecret).run()
            for c in CHANNELS
        ]
        await asyncio.gather(*coros)

    asyncio.run(tasks())
