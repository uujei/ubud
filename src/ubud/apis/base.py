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
        stream_handler: Callable = log_handler,
    ):
        # props
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.stream_handler = stream_handler

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
        parser: Callable = None,
        interval: float = None,
        ratelimit: int = 10,
        **kwargs,
    ):
        # correct args
        method = method.upper()
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        # set interval and handlers
        # set shortest allowed interval if interval is zero
        if interval == 0:
            interval = 1.0 / ratelimit
        handler = self.stream_handler if interval else None

        # outer loop for re-connection, inner loop for periodic response
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    while True:
                        t0 = time.time()
                        args = self.generate_request_args(method, prefix, path, **kwargs)
                        response = await self._request(session, method=method, parser=parser, handler=handler, **args)
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

    async def _request(self, session, method, parser, handler=None, **args):
        async with session.request(method=method, **args) as resp:
            # handle ratelimit
            self.ratelimit_handler(resp.headers)
            # check reponse
            body = await resp.json()
            if not (200 <= resp.status <= 299):
                raise self.ResponseException(status_code=resp.status, body=body)
            # validator
            self.validator(body)
            # parser
            if parser:
                body = parser(body, **args)
            # handle
            if handler:
                handler(body)
            return body

    def generate_request_args(self, method, prefix, path, **kwargs):
        url = self._join_url(self.baseUrl, prefix, path)
        path_url = self._get_path_url(url)

        args = {
            "url": url,
            "headers": self.generate_headers(method=method, path_url=path_url, **kwargs),
        }
        if method.upper() == "GET":
            args.update({"params": kwargs}),
            return args
        args.update({self.payload_type: kwargs}),
        return args

    @staticmethod
    def generate_headers(self, method, path_url, **kwargs):
        return NotImplementedError()

    @staticmethod
    async def validator(body):
        raise NotImplementedError()

    @staticmethod
    async def ratelimit_handler(headers):
        return NotImplementedError()

    @staticmethod
    def _join_url(*args):
        return "/".join([arg.strip("/") for arg in args if arg])

    @staticmethod
    def _get_path_url(x):
        return "/" + x.split("://", 1)[-1].split("/", 1)[-1]
