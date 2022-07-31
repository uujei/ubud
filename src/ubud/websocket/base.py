import abc
import asyncio
import json
import logging
import socket
from datetime import datetime
from time import time
from typing import Callable

import websockets

from ..const import ASK, BID, KST, PRICE, QUANTITY, RANK

logger = logging.getLogger(__name__)


################################################################
# Orderbook
################################################################
class Orderbook:
    def __init__(self, orderType, orderbook_depth=5):
        assert orderType in [ASK, BID], f"[ERROR] Wrong orderType {orderType}! - {ASK} or {BID} is available"
        self.orderType = orderType
        self.orderbook_depth = orderbook_depth

        # [NOTE]
        # reverse = False for ASK - 매도호가는 낮을수록 선순위
        # reverse = True for BID - 매수호가는 높을수록 선순위
        self.reverse = False if self.orderType == ASK else True

        # orderbook storage
        self.orderbooks = dict()

    def __call__(self):
        # [NOTE]
        # depth보다 원소 수가 하나 더 많은 것은 trade 처리 때문
        self.orderbooks = {
            k: self.orderbooks[k]
            for i, k in enumerate(sorted(self.orderbooks, reverse=self.reverse))
            if i < self.orderbook_depth + 1
        }
        if len(self.orderbooks) < self.orderbook_depth:
            return []
        return [{**v, RANK: i + 1} for i, (_, v) in enumerate(self.orderbooks.items()) if i < self.orderbook_depth]

    def update(self, order):
        # [NOTE] Trade 발생시 Quantity -1의 Order를 업데이트
        # Websocket 불안정하여 끊어질 경우 Orderbook 갱신이 제대로 되지 않는 경우 있음.
        # trade 발생하면 그보다 높거나(매도호가), 낮은(매수호가) 주문만 남김.
        if order[QUANTITY] < 0.0:
            if self.orderType == ASK:
                self.orderbooks = {k: v for k, v in self.orderbooks if k > order[PRICE]}
            else:
                self.orderbooks = {k: v for k, v in self.orderbooks if k < order[PRICE]}
            return

        # Quantity가 0으로 변경된 호가를 저장된 Orderbook에서 삭제
        if order[QUANTITY] == 0.0:
            if order[PRICE] in self.orderbooks:
                self.orderbooks.pop(order[PRICE])
            return

        # 위 두 경우 외에는 Orderbook 업데이트
        self.orderbooks.update({order[PRICE]: order})


################################################################
# BaseWebsocket
################################################################
class BaseWebsocket(abc.ABC):
    ws_conf = {"ping_timeout": None}

    # run
    async def run(self):
        while True:
            logger.info(f"[WEBSOCKET] Try Connect to '{self.ws_url}'")
            try:
                async with websockets.connect(self.ws_url, **self.ws_conf) as ws:
                    await self._request(ws, params=self.ws_params)
                    while True:
                        try:
                            _ = await self._recv(ws, parser=self.parser, handler=self.handler)
                        except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed):
                            try:
                                pong = await ws.ping()
                                await asyncio.wait_for(pong, timeout=self.ping_timeout)
                                continue
                            except Exception as ex:
                                logger.warning(
                                    f"[WEBSOCKET] Ping Error '{ex}' - retrying connection in 1 sec (Ctrl-C to quit)"
                                )
                                await asyncio.sleep(1)
                                break
            except socket.gaierror:
                logger.warning(
                    "[WEBSOCKET] Socket Get Address Info Error - retrying connection in 1 sec (Ctrl-C to quit)"
                )
                await asyncio.sleep(1)
                continue
            except ConnectionRefusedError:
                logger.error("Nobody seems to listen to this endpoint. Please check the URL.")
                await asyncio.sleep(1)
                continue

    @staticmethod
    async def _recv(ws, parser: Callable = None, handler: Callable = None):
        recv = await ws.recv()
        if recv is None:
            return
        msg = json.loads(recv)
        ts_ws_recv = time()
        logger.debug(
            "[WEBSOCKET] Receive Message @ {}".format(
                datetime.fromtimestamp(ts_ws_recv).astimezone(KST).isoformat(timespec="microseconds")
            )
        )
        logger.debug("[WEBSOCKET] Body: {}".format(msg))

        # parser
        if parser is not None:
            try:
                msg = await parser(body=msg, ts_ws_recv=ts_ws_recv)
            except Exception as ex:
                logger.warn("[WEBSOCKET] Error Parsing {0}: {1}".format(ts_ws_recv, ex))

        # handler
        if handler is not None:
            try:
                await handler(msg)
            except Exception as ex:
                logger.warn("[WEBSOCKET] Error Execute Handler: {}".format(ex))

    @abc.abstractstaticmethod
    async def _request(ws, params):
        pass
