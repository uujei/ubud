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


class UbudApiResponseException(Exception):
    critical_status_codes = [400, 404]

    def __init__(self, status_code, error_code=None, message=None):
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
        **kwargs,
    ):
        if interval:
            handlers = self.stream_handlers
        else:
            handlers = [self.ratelimit_handler, self.return_handler]
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    while True:
                        args = self.generate_request_args(method, prefix, path, **kwargs)
                        response = await self._request(session, method=method, handlers=handlers, **args)
                        if interval is None:
                            return response
                        await asyncio.sleep(interval)
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
                if isinstance(body, str):
                    error_code = "unknown"
                    message = body
                else:
                    error_code = body.get("status")
                    message = body.get("message")

                raise self.ResponseException(status_code=resp.status, error_code=error_code, message=message)
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
            args.update({"params": {k: v for k, v in kwargs.items() if v is not None}}),
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
