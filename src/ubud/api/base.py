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
        redis_client: redis.Redis = None,
        redis_topic: str = "ubud",
        redis_expire_sec: int = 120,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec

        self._private_ready = all([c is not None for c in [apiKey, apiSecret]])
        self._remains = None

    async def _request(self, method, route, **kwargs):
        # get request args
        args = self._gen_request_args(route, **kwargs)

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

    @abc.abstractstaticmethod
    async def _limit_handler(resp):
        pass

    @abc.abstractmethod
    def _gen_request_args(self, route, **kwargs):
        pass
