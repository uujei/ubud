import abc
import asyncio
import logging
from typing import Callable, List

import aiohttp
import time
import redis.asyncio as redis

from ..const import KST

logger = logging.getLogger(__name__)


async def log_handler(response):
    response = await response.json()
    logger.info(response)
    print(response)


class UbudApiResponseException(Exception):
    critical_status_codes = [400, 404]

    def __init__(self, status_code, body):
        # parse body
        if isinstance(body, str):
            error_code = "unknown"
            message = body
        else:
            error_code = body.get("status")
            message = body.get("message")

        # props
        self.status_code = status_code
        self.error_code = error_code
        self.message = message

        # we'll stop the loop if critical
        self.is_critical = self.status_code in self.critical_status_codes

    def __str__(self):
        return f"HTTP status [{self.status_code}] (critical={self.is_critical}), server sent error [{self.error_code}] {self.message}"


################################################################
# Base
################################################################
class BaseApi(abc.ABC):
    baseUrl: str
    endpoints: dict
    payload_type: str  # json or data
    ResponseException: Exception = UbudApiResponseException

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

    async def request(
        self,
        method: str,
        prefix: str,
        path: str = None,
        interval: float = None,
        ratelimit: int = 10,
        **kwargs,
    ):
        # drop none value parameters
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # set handlers
        if interval is not None:
            handlers = [self.ratelimit_handler, *self.stream_handlers]
            # set shortest interval aviod ratelimit if interval is zero
            if interval == 0:
                interval = 1.0 / ratelimit
        else:
            handlers = [self.ratelimit_handler, self.return_handler]

        # outer loop for re-connection, inner loop for periodic response
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    while True:
                        t0 = time.time()
                        args = self.generate_request_args(method, prefix, path, **kwargs)
                        response = await self._request(session, method=method, handlers=handlers, **args)
                        if interval is None:
                            return response
                        latency = time.time() - t0
                        await asyncio.sleep(interval - latency)
            except self.ResponseException as ex:
                if ex.is_critical:
                    raise ex
                logger.warning(ex)
            except Exception as ex:
                raise ex

    async def _request(self, session, method, handlers, **args):
        async with session.request(method=method, **args) as resp:
            if not (200 <= resp.status <= 299):
                body = await resp.json()
                raise self.ResponseException(status_code=resp.status, body=body)
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
    def generate_headers(self, method, endpoint, **kwargs):
        return None

    @staticmethod
    async def ratelimit_handler(resp):
        return None

    @staticmethod
    async def return_handler(resp):
        return await resp.json()

    @staticmethod
    def _join_url(*args):
        return "/".join([arg.strip("/") for arg in args if arg])

    @staticmethod
    def _get_endpoint(x):
        return "/" + x.split("://", 1)[-1].split("/", 1)[-1]
