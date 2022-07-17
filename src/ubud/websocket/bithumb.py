import asyncio
import json
import logging
import sys
import traceback
from datetime import datetime
from time import time
from typing import Callable

from ..const import (
    AMOUNT,
    API_CATEGORY,
    ASK,
    BID,
    BOOK_COUNT,
    CHANNEL,
    CURRENCY,
    DATETIME,
    DT_FMT,
    TS_MQ_SEND,
    DT_FMT_FLOAT,
    KST,
    MARKET,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    RANK,
    SYMBOL,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    TS_MARKET,
    TS_WS_RECV,
    TS_WS_SEND,
    MQ_SUBTOPICS,
    ts_to_strdt,
)
from .base import BaseWebsocket

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
# BithumbWebsocket
################################################################
class BithumbWebsocket(BaseWebsocket):
    ws_url = "wss://pubwss.bithumb.com/pub/ws"

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

    # [TRADE]
    async def trade_parser(self, body, ts_ws_recv=None):
        messages = []

        # parse and pub
        if "content" in body.keys():
            try:
                content = body["content"]
                for r in content["list"]:
                    symbol_currency = _split_symbol(r["symbol"])
                    _dt = datetime.fromisoformat(r["contDtm"]).astimezone(KST)
                    trade_datetime = _dt.isoformat(timespec="microseconds")
                    ts_market = _dt.timestamp()

                    # Key (name)
                    _key = {
                        API_CATEGORY: THIS_API_CATEGORY,
                        CHANNEL: TRADE,
                        MARKET: THIS_MARKET,
                        **symbol_currency,
                        ORDERTYPE: ASK if r["buySellGb"] == "1" else BID,
                        RANK: 0,
                    }
                    name = "/".join([str(_key[k]) for k in MQ_SUBTOPICS])

                    # Value (value)
                    value = {
                        DATETIME: trade_datetime,
                        PRICE: float(r["contPrice"]),
                        QUANTITY: float(r["contQty"]),
                        AMOUNT: float(r["contAmt"]),
                        TS_MARKET: ts_market,
                        TS_WS_RECV: ts_ws_recv,
                    }

                    # Message
                    msg = {"name": name, "value": value}
                    messages += [msg]

                    # logging
                    logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

            except Exception as ex:
                logger.warn(f"[WEBSOCKET] Bithumb Trade Parser Error - {ex}")
                traceback.print_exc()

        return messages

    # [ORDERBOOK]
    async def orderbook_parser(self, body, ts_ws_recv=None):
        messages = []
        try:
            # parse and pub
            if "content" in body.keys():
                content = body["content"]
                ts_ws_send = int(content["datetime"]) / 1e6

                for r in content["list"]:
                    symbol_currency = _split_symbol(r["symbol"])
                    price = float(r["price"])
                    orderType = r["orderType"]
                    quantity = float(r["quantity"])

                    # get rank of orderbook
                    rank = self._rank_orderbook(
                        symbol=symbol_currency[SYMBOL],
                        currency=symbol_currency[CURRENCY],
                        orderType=orderType,
                        price=price,
                        quantity=quantity,
                        reverse=True if orderType == BID else False,
                    )
                    if rank is None:
                        continue

                    # Key (name)
                    _key = {
                        API_CATEGORY: THIS_API_CATEGORY,
                        CHANNEL: ORDERBOOK,
                        MARKET: THIS_MARKET,
                        **symbol_currency,
                        ORDERTYPE: r["orderType"],
                        RANK: rank,
                    }
                    name = "/".join([str(_key[k]) for k in MQ_SUBTOPICS])

                    # generaete message
                    value = {
                        DATETIME: ts_to_strdt(ts_ws_send),
                        PRICE: price,
                        QUANTITY: quantity,
                        BOOK_COUNT: int(r["total"]),
                        TS_WS_SEND: ts_ws_send,
                        TS_WS_RECV: ts_ws_recv,
                    }

                    # Message
                    msg = {"name": name, "value": value}
                    messages += [msg]

                    # logging
                    logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

        except Exception as ex:
            logger.warn(f"[WEBSOCKET] Bithumb Orderbook Parser Error - {ex}")
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
    import sys

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
        coros = [BithumbWebsocket(channel=c, symbols=symbols).run() for c in channels]
        await asyncio.gather(*coros)

    asyncio.run(tasks())
