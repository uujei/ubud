import asyncio
import base64
import hashlib
import hmac
import logging
import time
from typing import Callable, List
from urllib.parse import urlencode, urljoin

from pydantic import BaseModel, Extra

from .base import BaseApi
from .bithumb_api import BITHUMB_API

logger = logging.getLogger(__name__)

################################################################
# BithumbApi
################################################################
class BithumbApi(BaseApi):

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"

    async def request(self, path: str, **kwargs):
        model = BITHUMB_API[path.strip("/")](**kwargs)
        data = await self._request(**model.dict(exclude_none=True))
        return data

    def _gen_request_args(self, method, route, **kwargs):
        url = self._join_url(self.baseUrl, route)
        route = self._get_route(url)

        # Bithumb 특이사항, params에 endpoint를 다시 보냄...
        kwargs.update({"endpoint": route})
        args = {
            "url": urljoin(self.baseUrl, route),
            "headers": self._gen_headers(method=method, route=route, **kwargs),
        }
        if method.upper() == "GET":
            args.update({"params": kwargs}),
            return args
        args.update({"data": kwargs}),
        return args

    def _gen_headers(self, method, route, **kwargs):
        # no required headers for public endpoints
        if route.strip("/").startswith("public"):
            return

        # for non-public endpoints
        ts = str(int(time.time() * 1000))
        return {
            "Api-Key": self.apiKey,
            "Api-Sign": self._gen_api_sign(route=route, ts=ts, apiSecret=self.apiSecret, **kwargs),
            "Api-Nonce": ts,
        }

    @staticmethod
    def _gen_api_sign(route, ts, apiSecret, **kwargs):
        q = chr(0).join([route, urlencode(kwargs), ts])
        h = hmac.new(apiSecret.encode("utf-8"), q.encode("utf-8"), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode("utf-8")).decode()

    @staticmethod
    async def _default_handler(resp):
        body = await resp.json()
        # parse and valid provider's status code
        if body["status"] != "0000":
            raise ReferenceError(f"[ERROR] STATUS CODE {body['status']} - {body['message']}")
        return body["data"]

    @staticmethod
    async def _limit_handler(headers):
        # get rate limit info.
        try:
            rate_limit = {
                "per_sec_remaining": int(headers["X-RateLimit-Remaining"]),
                "per_sec_replenish": int(headers["X-RateLimit-Replenish-Rate"]),
                "per_min_remaining": None,
                "per_min_replenish": None,
            }
            logger.debug(f"[HTTP] Bithumb Rate Limit: {rate_limit}")
        except Exception as ex:
            logger.warning(ex)

        return rate_limit
