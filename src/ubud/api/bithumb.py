import base64
import hashlib
import hmac
from typing import List, Callable
import time
from urllib.parse import urlencode, urljoin
import logging
from .base import BaseApi

logger = logging.getLogger(__name__)

PATH_PARAMS_PUBLIC = ["order_currency", "payment_currency"]
DELIM_PATH_PARAMS = "_"
REQUEST_LIMIT_PREFIX = "X-RateLimit-"

################################################################
# Handlers
################################################################
def log_request_limit(resp):
    _msg = {k: v for k, v in resp.headers.items() if k.startswith(REQUEST_LIMIT_PREFIX)}
    logger.info(_msg)
    print(_msg)


################################################################
# BithumbApi
################################################################
class BithumbApi(BaseApi):

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"
    apiVersion = "unknown"

    def _gen_request_args(self, route, **kwargs):
        # for public endpoints
        if route.strip("/").startswith("public"):
            path = DELIM_PATH_PARAMS.join([kwargs.pop(p) for p in PATH_PARAMS_PUBLIC if p in kwargs.keys()])
            url = urljoin(self.baseUrl, route, path)
            queries = []
            if "limit" in kwargs.keys():
                queries += f"counts={kwargs['limit']}"
            if "chart_interval" in kwargs.keys():
                queries += kwargs["chart_interval"]
            if len(queries) > 0:
                queries = "&".join(queries)
                url = f"{url}?{queries}"
            return {
                "url": url,
                "headers": None,
                "data": None,
            }

        # for non public endpoints
        kwargs.update({"endpoint": route})
        headers = self._gen_header(route=route, **kwargs)
        return {
            "url": urljoin(self.baseUrl, route),
            "headers": headers,
            "data": kwargs,
        }

    def _gen_header(self, route, **kwargs):
        # no required headers for public endpoints
        if route.strip("/").startswith("public"):
            return

        # for non-public endpoints
        api_key = self.apiKey
        api_secret = self.apiSecret
        nonce = self._gen_api_nonce()
        return {
            "Api-Key": api_key,
            "Api-Sign": self._gen_api_sign(
                route=route,
                nonce=nonce,
                apiKey=api_key,
                apiSecret=api_secret,
                **kwargs,
            ),
            "Api-Nonce": nonce,
        }

    @staticmethod
    def _gen_api_sign(route, nonce, apiKey, apiSecret, **kwargs):
        q = chr(0).join([route, urlencode(kwargs), nonce])
        h = hmac.new(apiSecret.encode("utf-8"), q.encode("utf-8"), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode("utf-8")).decode()

    @staticmethod
    def _gen_api_nonce():
        return str(int(time.time() * 1000))

    @staticmethod
    async def _default_handler(resp):
        body = await resp.json()
        # parse and valid provider's status code
        assert body["status"] == "0000", f"[API] STATUS CODE {body['status']}"
        return body["data"]

    @staticmethod
    def _limit_handler(headers):
        # get rate limit info.
        rate_limit = {
            "per_sec_remaining": int(headers["X-RateLimit-Remaining"]),
            "per_sec_replenish": int(headers["X-RateLimit-Replenish-Rate"]),
            "per_min_remaining": None,
            "per_min_replenish": None,
        }

        return rate_limit
