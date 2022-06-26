import parse
import base64
from typing import List, Callable
import hashlib
import hmac
import logging
import time
import uuid
from urllib.parse import urlencode, urljoin

import jwt
import requests

logger = logging.getLogger(__name__)

REMAINING_REQ_FORM = parse.compile("group={group}; min={min}; sec={sec}")

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
        handlers: list = None,
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
    def _default_handler(resp, handlers=None):
        return resp.json()
