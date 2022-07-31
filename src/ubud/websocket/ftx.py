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
from .base import BaseWebsocket, Orderbook

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
        orderbook_depth: int = 5,
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
        self.orderbook_depth = orderbook_depth

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

        # ORDERBOOKS
        self.orderbooks = {}
        for sc in self.symbols:
            _sc = _split_symbol(sc)
            symbol, currency = _sc[SYMBOL], _sc[CURRENCY]
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = dict()
            if currency not in self.orderbooks[symbol]:
                self.orderbooks[symbol][currency] = dict()
            self.orderbooks[symbol][currency] = {
                ASK: Orderbook(orderType=ASK, orderbook_depth=self.orderbook_depth),
                BID: Orderbook(orderType=BID, orderbook_depth=self.orderbook_depth),
            }

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
                symbol = symbol_currency[SYMBOL]
                currency = symbol_currency[CURRENCY]

                data = body["data"]
                for record in data:

                    # orderType
                    orderType = _buy_sell(record["side"])
                    price = float(record["price"])

                    # key
                    _key = {
                        API_CATEGORY: THIS_API_CATEGORY,
                        CHANNEL: TRADE,
                        MARKET: THIS_MARKET,
                        SYMBOL: symbol,
                        CURRENCY: currency,
                        ORDERTYPE: orderType,
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
                            PRICE: price,
                            QUANTITY: float(record["size"]),
                            TS_WS_SEND: datetime.fromisoformat(record["time"]).timestamp(),
                            TS_WS_RECV: ts_ws_recv,
                            "_side": record["side"],
                            "_liquidation": str(record["liquidation"]).lower(),
                        },
                    )
                    messages += [msg]

                    # Orderbook 정리 - 임의의 음수 QUANTITY를 입력
                    self.orderbooks[symbol][currency][orderType].update({PRICE: price, QUANTITY: -1.0})

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
                symbol = symbol_currency[SYMBOL]
                currency = symbol_currency[CURRENCY]

                data = body["data"]
                for _type, orderType in [("asks", ASK), ("bids", BID)]:
                    for price, quantity in data[_type]:
                        # update
                        ts = data["time"]
                        dt = datetime.fromtimestamp(ts).astimezone(KST).isoformat(timespec="microseconds")
                        self.orderbooks[symbol][currency][orderType].update(
                            {PRICE: float(price), QUANTITY: float(quantity), DATETIME: dt, TS_WS_SEND: float(ts)}
                        )

                    orderbooks = self.orderbooks[symbol][currency][orderType]()
                    for orderbook in orderbooks:
                        # key
                        _key = {
                            API_CATEGORY: THIS_API_CATEGORY,
                            CHANNEL: ORDERBOOK,
                            MARKET: THIS_MARKET,
                            SYMBOL: symbol,
                            CURRENCY: currency,
                            ORDERTYPE: orderType,
                            RANK: orderbook[RANK],
                        }

                        # add message
                        msg = Message(
                            key="/".join([str(_key[k]) for k in MQ_SUBTOPICS]),
                            value={
                                DATETIME: orderbook[DATETIME],
                                PRICE: orderbook[PRICE],
                                QUANTITY: orderbook[QUANTITY],
                                TS_WS_SEND: orderbook[TS_WS_SEND],
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
