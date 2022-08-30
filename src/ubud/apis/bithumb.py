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
from .bithumb_endpoints import ENDPOINTS

logger = logging.getLogger(__name__)


################################
# Rate Limit Parser
################################
# TODO

################################################################
# BithumbApi
################################################################
class BithumbApi(BaseApi):

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"
    endpoints = ENDPOINTS
    payload_type = "data"

    def generate_headers(self, method, endpoint, **kwargs):
        # no required headers for public endpoints
        if endpoint.strip("/").startswith("public"):
            return

        # for non-public endpoints
        ts = str(int(time.time() * 1000))
        return {
            "Api-Key": self.apiKey,
            "Api-Sign": self.generate_api_sign(
                endpoint=endpoint,
                ts=ts,
                apiSecret=self.apiSecret,
                **kwargs,
            ),
            "Api-Nonce": ts,
        }

    @staticmethod
    def generate_api_sign(endpoint, ts, apiSecret, **kwargs):
        q = chr(0).join([endpoint, urlencode(kwargs), ts])
        h = hmac.new(apiSecret.encode("utf-8"), q.encode("utf-8"), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode("utf-8")).decode()

    @staticmethod
    async def return_handler(resp):
        body = await resp.json()
        # parse and valid provider's status code
        if body["status"] != "0000":
            # 5600: 거래 진행중인 내역이 존재하지 않습니다.
            if body["status"] == "5600":
                return []
            raise ReferenceError(f"[ERROR] STATUS CODE {body['status']} - {body}")
        return body["data"]

    @staticmethod
    async def ratelimit_handler(response):
        # get rate limit info.
        try:
            rate_limit = {
                "per_sec_remaining": int(response.headers["X-RateLimit-Remaining"]),
                "per_sec_replenish": int(response.headers["X-RateLimit-Replenish-Rate"]),
                "per_min_remaining": None,
                "per_min_replenish": None,
            }
            logger.debug(f"[API] Bithumb Rate Limit: {rate_limit}")
            return rate_limit
        except Exception as ex:
            logger.warning(f"[API] Bithumb Update rate_limit FALIED - {ex}, headers: {response.headers}")
