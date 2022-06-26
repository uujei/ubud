import parse
from typing import List, Callable
import hashlib
import logging
import time
import uuid
from urllib.parse import urlencode, urljoin

import jwt
import requests

logger = logging.getLogger(__name__)

REMAINING_REQ_FORM = parse.compile("group={group}; min={per_min_remaining}; sec={per_sec_remaining}")
REPLENISH = {
    "order": {"per_sec": 8, "per_min": 200},
    "default": {"per_sec": 30, "per_min": 900},
    "market": {"per_sec": 10, "per_min": 600},
}

################################################################
# Hanlders
################################################################
def log_request_limit(resp):
    _remaining_req = resp.headers["Remaining-Req"]
    _msg = REMAINING_REQ_FORM.parse(_remaining_req).named
    logger.info(_msg)
    print(_msg)


################################################################
# UpbitApi
################################################################
class UpbitApi:

    # Upbit URL
    baseUrl = "https://api.upbit.com"

    def __init__(
        self,
        apiKey: str = None,
        apiSecret: str = None,
        apiVersion: str = "v1",
        handlers: List[Callable] = None,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.apiVersion = apiVersion

        handlers = handlers if handlers is not None else []
        handlers = handlers if isinstance(handlers, list) else [handlers]
        self.handlers = handlers + [self._default_handler]

    def _gen_header(self, route, **kwargs):
        api_key = self.apiKey
        api_secret = self.apiSecret
        nonce = self._gen_api_nonce()
        return {
            "Authorization": self._gen_api_sign(
                route=route,
                nonce=nonce,
                apiKey=api_key,
                apiSecret=api_secret,
                **kwargs,
            )
        }

    def _gen_request_args(self, route, **kwargs):
        if not route.strip("/").startswith(self.apiVersion):
            route = "/".join([self.apiVersion, route.strip("/")])
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
        return str(uuid.uuid4())

    @staticmethod
    def _gen_api_sign(route, nonce, apiKey, apiSecret, **kwargs):
        payload = {"access_key": apiKey, "nonce": nonce}
        if kwargs:
            m = hashlib.sha512()
            m.update(urlencode(kwargs).encode())
            query_hash = m.hexdigest()
            payload.update({"query_hash": query_hash, "query_hash_alg": "SHA512"})
        return f"Bearer {jwt.encode(payload, apiSecret)}"

    @staticmethod
    def _default_handler(resp):
        # parse
        data = resp.json()

        # get rate limit info.
        _rate_limit = REMAINING_REQ_FORM.parse(resp.headers["Remaining-Req"]).named
        _group = _rate_limit["group"]
        rate_limit = {
            "per_sec_remaining": int(_rate_limit["per_sec_remaining"]),
            "per_sec_replenish": REPLENISH[_group]["per_sec"],
            "per_min_remaining": int(_rate_limit["per_min_remaining"]),
            "per_min_replenish": REPLENISH[_group]["per_min"],
        }

        return {"data": data, "limit": rate_limit}
