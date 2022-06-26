import base64
import hashlib
import hmac
import time
from urllib.parse import urlencode, urljoin
import requests
import logging

logger = logging.getLogger(__name__)

PATH_PARAMS_PUBLIC = ["order_currency", "payment_currency"]


class BithumbApi:
    @staticmethod
    def _gen_api_nonce():
        return str(int(time.time() * 1000))

    @staticmethod
    def _gen_api_sign(route, nonce, apiSecret, **kwargs):
        q = chr(0).join([route, urlencode(kwargs), nonce])
        h = hmac.new(apiSecret, q.encode("utf-8"), hashlib.sha512)
        return base64.b64encode(h.hexdigest().encode("utf-8"))

    @staticmethod
    def _validate_and_parse(resp):
        resp.raise_for_status()
        body = resp.json()
        if body["status"] != "0000":
            raise ReferenceError(body)
        return body["data"]


class BithumbPublic(BithumbApi):
    def __init__(self):
        self.baseUrl = "https://api.bithumb.com/public"

    def get(self, route, **kwargs):
        DELIM = "_"
        path = DELIM.join([kwargs.pop(p) for p in PATH_PARAMS_PUBLIC if p in kwargs.keys()])
        url = urljoin(self.baseUrl, route, path)
        query = ""
        if "limit" in kwargs.keys():
            query += f"counte={kwargs['limit']}"
        if "chart_interval" in kwargs.keys():
            query += kwargs["chart_interval"]
        if query != "":
            url = "?".join([url, query])

        resp = requests.get(url=url)
        return self._validate_and_parse(resp)


class BithumbPrivate(BithumbApi):
    def __init__(self, apiKey, apiSecret):
        self.baseUrl = "https://api.bithumb.com"
        self.apiKey = apiKey.encode("utf-8")
        self.apiSecret = apiSecret.encode("utf-8")

    def post(self, route, **kwargs):
        url = urljoin(self.baseUrl, route)
        kwargs.update({"endpoint": route})
        nonce = self._gen_api_nonce()
        headers = {
            "Api-Key": self.apiKey,
            "Api-Sign": self._gen_api_sign(route, nonce, self.apiSecret, **kwargs),
            "Api-Nonce": nonce,
        }

        resp = requests.post(url=url, headers=headers, data=kwargs)
        return self._validate_and_parse(resp)
