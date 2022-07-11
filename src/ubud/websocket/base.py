import abc
import asyncio
import json
import logging
import socket
from time import time
from typing import Callable

import websockets

from ..const import ts_to_strdt

logger = logging.getLogger(__name__)


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
        logger.debug(f"[WEBSOCKET] Receive Message @ {ts_to_strdt(ts_ws_recv, _float=True)}")
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
