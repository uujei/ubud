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
        handlers: List[Callable] = None,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self._private_ready = all([c is not None for c in [apiKey, apiSecret]])
        self._remains = None

        handlers = handlers if handlers is not None else []
        handlers = handlers if isinstance(handlers, list) else [handlers]
        self.handlers = handlers + [self._default_handler]

    async def _request(self, method, route, **kwargs):
        # get request args
        args = self._gen_request_args(route, **kwargs)

        async with aiohttp.ClientSession() as client:
            async with client.request(method=method, **args) as resp:
                assert resp.status == 200, f"[REQUESTS] STATUS CODE {resp.status}"
                _ = self._limit_handler(resp.headers)

                for handler in self.handlers:
                    r = await handler(resp)
                    if r is not None:
                        return r

    @abc.abstractstaticmethod
    async def _default_handler(resp):
        pass

    @abc.abstractstaticmethod
    def _limit_handler(resp):
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
