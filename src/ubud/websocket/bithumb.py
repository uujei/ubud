import asyncio
import json
import logging
import sys
import traceback
from datetime import datetime
from time import time
from typing import Callable

from ..utils.app import ts_to_strdt
from ..const import (
    AMOUNT,
    CATEGORY,
    ASK,
    BID,
    BOOK_COUNT,
    CHANNEL,
    CURRENCY,
    DATETIME,
    KST,
    MARKET,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    QUOTATION_KEY_RULE,
    RANK,
    SYMBOL,
    TICKER,
    TRADE,
    TRADE_DATETIME,
    TS_MARKET,
    TS_MQ_SEND,
    TS_WS_RECV,
    TS_WS_SEND,
)
from ..models import Message
from .base import BaseWebsocket, Orderbook
from ..utils.app import key_maker

logger = logging.getLogger(__name__)

################################################################
# Market Conf
################################################################
THIS_MARKET = "bithumb"
THIS_CATEGORY = "quotation"
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
        return symbol.split("_")
    return symbol, "unknown"


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
        orderbook_depth: int = 5,
        handler: Callable = None,
        apiKey: str = None,
        apiSecret: str = None,
    ):
        super().__init__()

        # dummy properties for api consistency
        self.apiKey = apiKey
        self.apiSecret = apiSecret

        assert channel in CHANNEL_PARAMS, f"[ERROR] unknown channel '{channel}'!"
        self.channel = CHANNEL_PARAMS[channel]
        assert isinstance(symbols, (list, tuple)), "[ERROR] 'symbols' should be a list!"
        assert isinstance(currencies, (list, tuple)), "[ERROR] 'currencies' should be a list!"
        self.symbols = [_concat_symbol_currency(s, c) for s in symbols for c in currencies]
        self.ws_params = self._generate_ws_params(self.channel, self.symbols)
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

        # ORDERBOOKS
        self.orderbooks = {}
        for sc in self.symbols:
            symbol, currency = _split_symbol(sc)
            if symbol not in self.orderbooks:
                self.orderbooks[symbol] = dict()
            if currency not in self.orderbooks[symbol]:
                self.orderbooks[symbol][currency] = dict()
            self.orderbooks[symbol][currency] = {
                ASK: Orderbook(orderType=ASK, orderbook_depth=self.orderbook_depth),
                BID: Orderbook(orderType=BID, orderbook_depth=self.orderbook_depth),
            }

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
                    symbol, currency = _split_symbol(r["symbol"])

                    price = float(r["contPrice"])
                    quantity = float(r["contQty"])
                    amount = float(r["contAmt"])
                    if r["buySellGb"] == "1":
                        orderType = ASK
                    elif r["buySellGb"] == "2":
                        orderType = BID
                    else:
                        logger.warning(f"[WEBOSKCET] Unknown buySellGb for bithumb {r['buySellGb']}!")

                    _dt = datetime.fromisoformat(r["contDtm"]).astimezone(KST)
                    trade_datetime = _dt.isoformat(timespec="microseconds")
                    ts_market = _dt.timestamp()

                    # key
                    _key = {
                        CATEGORY: THIS_CATEGORY,
                        CHANNEL: TRADE,
                        MARKET: THIS_MARKET,
                        SYMBOL: symbol,
                        CURRENCY: currency,
                        ORDERTYPE: orderType,
                        RANK: 0,
                    }

                    # update message
                    msg = Message(
                        key="/".join([str(_key[k]) for k in QUOTATION_KEY_RULE[1:]]),
                        value={
                            DATETIME: trade_datetime,
                            PRICE: price,
                            QUANTITY: quantity,
                            AMOUNT: amount,
                            TS_MARKET: ts_market,
                            TS_WS_RECV: ts_ws_recv,
                        },
                    )
                    messages += [msg]

                    # Orderbook 정리 - 임의의 음수 QUANTITY를 입력
                    self.orderbooks[symbol][currency][orderType].update({PRICE: price, QUANTITY: -1.0})

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
                dt = datetime.fromtimestamp(ts_ws_send).astimezone(KST).isoformat(timespec="microseconds")

                symbols = set()
                currencies = set()
                orderTypes = set()

                for record in content["list"]:
                    symbol, currency = _split_symbol(record["symbol"])
                    orderType = record[ORDERTYPE]
                    price = float(record[PRICE])
                    quantity = float(record[QUANTITY])

                    self.orderbooks[symbol][currency][orderType].update(
                        {
                            PRICE: price,
                            QUANTITY: quantity,
                            DATETIME: dt,
                            TS_WS_SEND: ts_ws_send,
                        }
                    )

                    # update
                    symbols |= {symbol}
                    currencies |= {currency}
                    orderTypes |= {orderType}

                # order and return orderbooks
                for symbol in symbols:
                    for currency in currencies:
                        for orderType in orderTypes:
                            orderbooks = self.orderbooks[symbol][currency][orderType]()
                            for orderbook in orderbooks:
                                key = key_maker(
                                    **{
                                        CATEGORY: THIS_CATEGORY,
                                        CHANNEL: ORDERBOOK,
                                        MARKET: THIS_MARKET,
                                        SYMBOL: symbol,
                                        CURRENCY: currency,
                                        ORDERTYPE: orderType,
                                        RANK: str(orderbook[RANK]),
                                    }
                                )
                                msg = Message(
                                    key=key,
                                    value={
                                        DATETIME: dt,
                                        PRICE: orderbook[PRICE],
                                        QUANTITY: orderbook[QUANTITY],
                                        TS_WS_SEND: orderbook[TS_WS_SEND],
                                        TS_WS_RECV: ts_ws_recv,
                                    },
                                )
                                messages += [msg]

                                # logging
                                logger.debug(f"[WEBSOCKET] Parsed Message: {msg}")

        except Exception as ex:
            logger.warn(f"[WEBSOCKET] Bithumb Orderbook Parser Error - {ex}")
            traceback.print_exc()

        return messages


################################################################
# DEBUG RUN
################################################################
if __name__ == "__main__":
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
