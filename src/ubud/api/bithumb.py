import base64
import hashlib
import hmac
from typing import List, Callable
import time
from urllib.parse import urlencode, urljoin
import requests
import logging

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
class BithumbApi:

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"

    def __init__(
        self,
        apiKey: str = None,
        apiSecret: str = None,
        apiVersion: str = "unknown",
        handlers: List[Callable] = None,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.apiVersion = apiVersion
        self._private_ready = all([c is not None for c in [apiKey, apiSecret]])

        handlers = handlers if handlers is not None else []
        handlers = handlers if isinstance(handlers, list) else [handlers]
        self.handlers = handlers + [self._default_handler]

    def _gen_header(self, route, **kwargs):
        # no required headers for public endpoints
        if route.strip("/").startswith("public"):
            return

        # for non-public endpoints
        api_key = self.apiKey.encode("utf-8")
        api_secret = self.apiSecret.encode("utf-8")
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

    def _request(self, method, route, **kwargs):
        # get request args
        args = self._gen_request_args(route, **kwargs)

        # request and get response
        resp = requests.request(method=method, **args)

        # check status
        resp.raise_for_status()

        # handler
        for handler in self.handlers:
            r = handler(resp)
            if r is not None:
                return r

    @staticmethod
    def _gen_api_nonce():
        return str(int(time.time() * 1000))

    @staticmethod
    def _gen_api_sign(route, nonce, apiKey, apiSecret, **kwargs):
        q = chr(0).join([route, urlencode(kwargs), nonce])
        h = hmac.new(apiSecret, q.encode("utf-8"), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode("utf-8"))

    @staticmethod
    def _default_handler(resp):
        # parse and valid provider's status code
        body = resp.json()
        if body["status"] != "0000":
            raise ReferenceError(body)

        # get rate limit info.
        rate_limit = {
            "per_sec_remaining": int(resp.headers["X-RateLimit-Remaining"]),
            "per_sec_replenish": int(resp.headers["X-RateLimit-Replenish-Rate"]),
            "per_min_remaining": None,
            "per_min_replenish": None,
        }

        return {"data": body["data"], "limit": rate_limit}
