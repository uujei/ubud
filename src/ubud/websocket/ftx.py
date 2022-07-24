import asyncio
import hmac
import json
import logging
import sys
import time
import traceback
from datetime import datetime, timedelta
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
    KST,
    MARKET,
    MQ_SUBTOPICS,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    RANK,
    SYMBOL,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    TRADE_SID,
    TS_MARKET,
    TS_WS_RECV,
    TS_WS_SEND,
    UTC,
    ts_to_strdt,
)
from ..models import Message
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

DT_FMT_FTX_SRC = "%Y-%m-%"

################################################################
# Market Helpers
################################################################
def _concat_symbol_currency(symbol, currency):
    if "-" in symbol or "/" in symbol:
        return symbol.upper()
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


def _buy_sell(x):
    if x == "buy":
        return BID
    if x == "sell":
        return ASK
    return x


################################################################
# UpbitWebsocket
################################################################
class FtxWebsocket(BaseWebsocket):
    ws_url = "wss://ftx.com/ws/"

    def __init__(
        self,
        channel: str,
        symbols: list,
        currencies: list = ["USD", "-PERP"],
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

        # SELECT PARSER
        if channel == "trade":
            self.parser = self.trade_parser
        elif channel == "orderbook":
            self.parser = self.orderbook_parser
        else:
            raise ReferenceError(f"[WEBSOCKET] Unknown channel '{channel}'")

        # HANDLER
        self.handler = handler

        # FTX는 milliseconds 내에서 수 건의 체결 발생 ~ 중복 datetime 있으면 1 millisecond씩 더해서 회피
        self._last_trade_dt = dict()  # datetime.now(tz=KST)
        self._n_duplicated_dt = dict()

        # Bithumb, FTX는 변경호가를 제공 ~ Store 필요
        self._orderbook_len = 15
        self._orderbook = dict()
        for sc in self.symbols:
            _sc = _split_symbol(sc)
            symbol, currency = _sc[SYMBOL], _sc[CURRENCY]
            if symbol not in self._orderbook:
                self._orderbook[symbol] = dict()
            if currency not in self._orderbook[symbol]:
                self._orderbook[symbol][currency] = dict()
            self._orderbook[symbol][currency] = {ASK: dict(), BID: dict()}

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

    # TRADE
    async def trade_parser(self, body, ts_ws_recv=None):
        messages = []
        try:
            if body["type"] == "update":
                symbol_currency = _split_symbol(body["market"])
                data = body["data"]
                for record in data:

                    # key
                    _key = {
                        API_CATEGORY: THIS_API_CATEGORY,
                        CHANNEL: TRADE,
                        MARKET: THIS_MARKET,
                        **symbol_currency,
                        ORDERTYPE: _buy_sell(record["side"]),
                        RANK: 0,
                    }
                    key = "/".join([str(_key[k]) for k in MQ_SUBTOPICS])

                    # avoid duplicated datetime
                    trade_dt = datetime.fromisoformat(record["time"]).astimezone(KST)

                    if key not in self._last_trade_dt.keys():
                        self._last_trade_dt[key] = trade_dt - timedelta(microseconds=1)

                    if key not in self._n_duplicated_dt.keys():
                        self._n_duplicated_dt[key] = 0

                    if trade_dt == self._last_trade_dt[key]:
                        self._n_duplicated_dt[key] += 1
                    else:
                        self._n_duplicated_dt[key] = 0

                    self._last_trade_dt[key] = trade_dt
                    seq_us = self._n_duplicated_dt[key]
                    dt = (trade_dt + timedelta(microseconds=seq_us)).isoformat(timespec="microseconds")

                    # update message
                    msg = Message(
                        key=key,
                        value={
                            DATETIME: dt,
                            TRADE_SID: record["id"],
                            PRICE: float(record["price"]),
                            QUANTITY: float(record["size"]),
                            TS_WS_SEND: datetime.fromisoformat(record["time"]).timestamp(),
                            TS_WS_RECV: ts_ws_recv,
                            "_side": record["side"],
                            "_liquidation": str(record["liquidation"]).lower(),
                        },
                    )
                    messages += [msg]

                    # logging
                    logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

            if body["type"] == "error":
                logger.warning(f"[WEBSOCKET] FTX Websocket Error - {body}")

        except Exception as ex:
            logger.warning(ex)
            traceback.print_exc()

        return messages

    # ORDERBOOK
    async def orderbook_parser(self, body, ts_ws_recv=None):
        messages = []
        try:
            if body["type"] == "update":
                symbol_currency = _split_symbol(body["market"])
                data = body["data"]
                for _type, orderType in [("asks", ASK), ("bids", BID)]:
                    for price, quantity in data[_type]:
                        # get rank of orderbook
                        rank = self._rank_orderbook(
                            symbol=symbol_currency[SYMBOL],
                            currency=symbol_currency[CURRENCY],
                            orderType=orderType,
                            price=float(price),
                            quantity=float(quantity),
                            reverse=True if orderType == BID else False,
                        )
                        if rank is None:
                            continue

                        # key
                        _key = {
                            API_CATEGORY: THIS_API_CATEGORY,
                            CHANNEL: ORDERBOOK,
                            MARKET: THIS_MARKET,
                            **symbol_currency,
                            ORDERTYPE: orderType,
                            RANK: rank,
                        }
                        dt = datetime.fromtimestamp(data["time"]).astimezone(KST).isoformat(timespec="microseconds")

                        # add message
                        msg = Message(
                            key="/".join([str(_key[k]) for k in MQ_SUBTOPICS]),
                            value={
                                DATETIME: dt,
                                PRICE: float(price),
                                QUANTITY: float(quantity),
                                TS_WS_SEND: float(data["time"]),
                                TS_WS_RECV: ts_ws_recv,
                            },
                        )
                        messages += [msg]

                        # logging
                        logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

            if body["type"] == "error":
                logger.warning(f"[WEBSOCKET] FTX Websocket Error - {body}")

        except Exception as ex:
            logger.error(f"[WEBSOCKET] Unknown Error EXIT! - {ex}")
            traceback.print_exc()

        return messages

    # ORDERBOOK HELPER
    def _rank_orderbook(self, symbol, currency, orderType, price, quantity, reverse=False):
        # pop zero quantity orderbook
        if quantity == 0.0:
            if price in self._orderbook[symbol][currency][orderType].keys():
                self._orderbook[symbol][currency][orderType].pop(price)
                self._orderbook[symbol][currency][orderType] = self._rank(
                    self._orderbook[symbol][currency][orderType], reverse=reverse, maxlen=self._orderbook_len
                )
            return

        # add new orderbook unit
        if price not in self._orderbook[symbol][currency][orderType].keys():
            self._orderbook[symbol][currency][orderType][price] = -1

        # sort orderbook ~ note orderbook_len + 1 is useful when some orderbook is popped
        self._orderbook[symbol][currency][orderType] = self._rank(
            self._orderbook[symbol][currency][orderType], reverse=reverse, maxlen=self._orderbook_len
        )

        # return
        if len(self._orderbook[symbol][currency][orderType]) < self._orderbook_len:
            return None

        rank = self._orderbook[symbol][currency][orderType].get(price)
        if rank is None or rank > self._orderbook_len:
            return
        return rank

    # rank
    @staticmethod
    def _rank(x, reverse, maxlen):
        return {k: i + 1 for i, (k, _) in enumerate(sorted(x.items(), reverse=reverse)) if i < maxlen + 1}


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
    import os
    import sys

    import dotenv

    dotenv.load_dotenv()
    apiKey = os.environ["FTX_API_KEY"]
    apiSecret = os.environ["FTX_API_SECRET"]

    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    # DEBUG EXAMPLE
    # python -m src.ubud.upbit trade,orderbook BTC,WAVES
    if len(sys.argv) > 1:
        channels = sys.argv[1].split(",")
    else:
        channels = ["orderbook", "trade"]
    if len(sys.argv) > 2:
        symbols = sys.argv[2].split(",")
    else:
        symbols = ["BTC", "WAVES"]

    async def tasks():
        coros = [FtxWebsocket(apiKey=apiKey, apiSecret=apiSecret, channel=c, symbols=symbols).run() for c in channels]
        await asyncio.gather(*coros)

    asyncio.run(tasks())
