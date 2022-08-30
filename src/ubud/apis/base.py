import abc
import asyncio
import logging
from typing import Callable, List

import aiohttp
import redis.asyncio as redis

from ..const import KST

logger = logging.getLogger(__name__)


async def log_handler(response):
    response = await response.json()
    logger.info(response)
    print(response)


################################################################
# Base
################################################################
class BaseApi(abc.ABC):
    baseUrl: str
    endpoints: dict
    payload_type: str  # json or data

    def __init__(
        self,
        apiKey: str = None,
        apiSecret: str = None,
        stream_handlers: list = [log_handler],
    ):
        # props
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.stream_handlers = stream_handlers if isinstance(stream_handlers, list) else [stream_handlers]

        # ratelimit
        self._remains = None

    async def __call__(self, path, **kwargs):
        model = self.endpoints[path.strip("/")](**kwargs)
        data = await self.request(**model.dict(exclude_none=True))
        return data

    async def run(self, path, interval=0.5, **kwargs):
        model = self.endpoints[path.strip("/")](**kwargs)
        while True:
            try:
                await self.periodic_request(
                    **model.dict(exclude_none=True),
                    handlers=self.stream_handlers,
                    interval=interval,
                )
            except KeyboardInterrupt as ex:
                raise ex
            except Exception as ex:
                logger.error(ex)

    async def request(
        self,
        method: str,
        prefix: str,
        path: str = None,
        interval: float = None,
        **kwargs,
    ):
        if interval:
            handlers = self.stream_handlers
        else:
            handlers = [self.ratelimit_handler, self.return_handler]
        async with aiohttp.ClientSession() as session:
            while True:
                args = self.generate_request_args(method, prefix, path, **kwargs)
                response = await self._request(session, method=method, handlers=handlers, **args)
                if interval is None:
                    return response
                await asyncio.sleep(interval)

    async def _request(self, session, method, handlers, **args):
        async with session.request(method=method, **args) as resp:
            if resp.status not in [200, 201]:
                _text = await resp.text()
                logger.warning(f"API Request Failed - status {resp.status}, {_text}")
            if handlers:
                results = await asyncio.gather(*[handler(resp) for handler in handlers])
                return results[-1]

    def generate_request_args(self, method, prefix, path, **kwargs):
        url = self._join_url(self.baseUrl, prefix, path)
        endpoint = self._get_endpoint(url)

        args = {
            "url": url,
            "headers": self.generate_headers(method=method, endpoint=endpoint, **kwargs),
        }
        if method.upper() == "GET":
            args.update({"params": kwargs}),
            return args
        args.update({self.payload_type: kwargs}),
        return args

    @staticmethod
    async def ratelimit_handler(resp):
        return None

    @staticmethod
    async def return_handler(resp):
        return await resp.json()

    @staticmethod
    def validate(resp: dict):
        return

    @staticmethod
    def _join_url(*args):
        return "/".join([arg.strip("/") for arg in args if arg])

    @staticmethod
    def _get_endpoint(x):
        return "/" + x.split("://", 1)[-1].split("/", 1)[-1]
