import abc
from typing import List, Callable
import time
import aiohttp
import asyncio

################################################################
# Base
################################################################
class BaseApi(abc.ABC):
    def __init__(
        self,
        apiKey: str = None,
        apiSecret: str = None,
        handlers: List[Callable] = [],
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.handlers = handlers if isinstance(handlers, list) else [handlers]

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
                    *[handler(resp) for handler in self.handlers],
                    self._default_handler(resp),
                ]
                results = asyncio.gather(*_handlers)

                return results[-1]

    @abc.abstractmethod
    async def request(self, route, **kwargs):
        pass

    @abc.abstractstaticmethod
    async def _limit_handler(resp):
        pass

    @abc.abstractmethod
    def _gen_request_args(self, route, **kwargs):
        pass

    @abc.abstractmethod
    def _gen_header(self, route, **kwargs):
        pass

    @abc.abstractstaticmethod
    def _gen_api_sign(route, nonce, apiKey, apiSecret, **kwargs):
        pass

    @abc.abstractmethod
    def _gen_api_nonce():
        pass
