import abc
import json
import logging
from time import time
from typing import Callable

import websockets

from ..const import ts_to_strdt

logger = logging.getLogger(__name__)


################################################################
# BaseWebsocket
################################################################
class BaseWebsocket(abc.ABC):
    # run
    async def run(self):
        async with websockets.connect(self.ws_url, **self.ws_conf) as ws:
            logger.info(f"[WEBSOCKET] Try Connect to '{self.ws_url}'")
            await self._request(ws, params=self.ws_params)
            while True:
                _ = await self._recv(ws, parser=self.parser, handler=self.handler)

    @staticmethod
    async def _recv(ws, parser: Callable = None, handler: Callable = None):
        recv = await ws.recv()
        if recv is None:
            return
        msg = json.loads(recv)
        ts_ws_recv = time()
        logger.debug(f"[WEBSOCKET] Receive Message from ORDERBOOK @ {ts_to_strdt(ts_ws_recv, _float=True)}")
        logger.debug(f"[WEBSOCKET] Body: {msg}")

        # parser
        if parser is not None:
            try:
                msg = await parser(body=msg, ts_ws_recv=ts_ws_recv)
            except Exception as ex:
                logger.warn(f"[WEBSOCKET] Error Parsing {ts_ws_recv}: {ex}")

        # handler
        if handler is not None:
            try:
                await handler(msg)
            except Exception as ex:
                logger.warn(f"[WEBSOCKET] Error Execute Handler: {ex}")

    @abc.abstractstaticmethod
    async def _request(ws, params):
        pass
