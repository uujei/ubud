import hashlib
import logging
import time
import uuid
from typing import Callable, List
from urllib.parse import urlencode, urljoin

import jwt
import parse
from pydantic import BaseModel

from .base import BaseApi
from .upbit_api import UPBIT_API

logger = logging.getLogger(__name__)


################################
# Rate Limit Parser
################################
REMAINING_REQ_FORM = parse.compile("group={group}; min={per_min_remaining}; sec={per_sec_remaining}")
REPLENISH = {
    "order": {"per_sec": 8, "per_min": 200},
    "default": {"per_sec": 30, "per_min": 900},
    "market": {"per_sec": 10, "per_min": 600},
    "status-wallet": {"per_sec": 10, "per_min": 600},  # 정확하지 않음
}

################################################################
# UpbitApi
################################################################
class UpbitApi(BaseApi):

    # Upbit URL
    baseUrl = "https://api.upbit.com/v1"

    async def request(self, path: str, **kwargs):
        model = UPBIT_API[path.strip("/")](**kwargs)
        data = await self._request(**model.dict(exclude_none=True))
        return data

    def _gen_request_args(self, method, route, **kwargs):
        url = self._join_url(self.baseUrl, route)
        route = self._get_route(url)

        args = {
            "url": url,
            "headers": self._gen_headers(method=method, route=route, **kwargs),
        }
        if method.upper() == "GET":
            args.update({"params": kwargs}),
            return args
        args.update({"data": kwargs}),
        return args

    def _gen_headers(self, method, route, **kwargs):
        api_key = self.apiKey
        api_secret = self.apiSecret
        nonce = str(uuid.uuid4())
        return {
            "Authorization": self._gen_api_sign(
                route=route,
                nonce=nonce,
                apiKey=api_key,
                apiSecret=api_secret,
                **kwargs,
            )
        }

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
    async def _default_handler(resp):
        return await resp.json()

    @staticmethod
    async def _limit_handler(headers):
        # get rate limit info.
        _rate_limit = REMAINING_REQ_FORM.parse(headers["Remaining-Req"]).named
        _group = _rate_limit["group"]
        try:
            rate_limit = {
                "per_sec_remaining": int(_rate_limit["per_sec_remaining"]),
                "per_sec_replenish": REPLENISH[_group]["per_sec"],
                "per_min_remaining": int(_rate_limit["per_min_remaining"]),
                "per_min_replenish": REPLENISH[_group]["per_min"],
            }
            logger.debug(f"[HTTP] Upbit Rate Limit: {rate_limit}")
            return rate_limit
        except KeyError as e:
            logging.warn(f"NEW LIMIT HEADER GROUP FOUND {_group}")
