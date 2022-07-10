import abc
import asyncio
import logging
from typing import Callable, List

import aiohttp
import redis.asyncio as redis

from ..const import KST

logger = logging.getLogger(__name__)

################################################################
# Base
################################################################
class BaseApi(abc.ABC):
    def __init__(
        self,
        apiKey: str = None,
        apiSecret: str = None,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret

        self._remains = None

    async def _request(self, method, route, **kwargs):
        # get request args
        args = self._gen_request_args(method, route, **kwargs)

        async with aiohttp.ClientSession() as client:
            async with client.request(method=method, **args) as resp:
                if resp.status not in [200, 201]:
                    _text = await resp.text()
                    raise ReferenceError(f"status code: {resp.status}, message: {_text}")
                _handlers = [
                    self._limit_handler(resp.headers),
                    self._default_handler(resp),
                ]
                results = await asyncio.gather(*_handlers)
                return results[-1]

    @staticmethod
    def _join_url(*args):
        url = []
        for arg in args:
            url += [arg.strip("/")]
        return "/".join(url)

    @staticmethod
    def _get_route(x):
        return "/" + x.split("://", 1)[-1].split("/", 1)[-1]

    @abc.abstractstaticmethod
    async def _limit_handler(resp):
        pass

    @abc.abstractmethod
    def _gen_request_args(self, method, route, **kwargs):
        pass
