import hashlib
import hmac
import json
import logging
import time
from typing import Callable, List
from urllib.parse import urlencode, urljoin

import jwt
import parse
from pydantic import BaseModel

from .base import BaseApi
from .ftx_api import FTX_API

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
class FtxApi(BaseApi):

    # Upbit URL
    baseUrl = "https://ftx.com/api"

    async def request(self, path: str, **kwargs):
        model = FTX_API[path.strip("/")](**kwargs)
        data = await self._request(**model.dict(exclude_none=True))
        return data

    def _gen_request_args(self, method, route, **kwargs):
        url = self._join_url(self.baseUrl, route)
        route = self._get_route(url)

        # FTX 특이사항, POST payload가 data가 아닌 json이어야 함...
        args = {
            "url": url,
            "headers": self._gen_headers(method=method, route=route, **kwargs),
        }
        if method.upper() == "GET":
            args.update({"params": kwargs}),
            return args
        args.update({"json": kwargs}),
        return args

    def _gen_headers(self, method, route, **kwargs):
        ts = int(time.time() * 1e3)
        return {
            "FTX-KEY": self.apiKey,
            "FTX-SIGN": self._gen_api_sign(method=method, route=route, apiSecret=self.apiSecret, ts=ts, **kwargs),
            "FTX-TS": str(ts),
        }

    @staticmethod
    def _gen_api_sign(method, route, ts, apiSecret, **kwargs):
        method = method.upper()
        signature_payload = f"{ts}{method}/{route.strip('/')}"
        if method == "POST":
            signature_payload += json.dumps(kwargs)
        signature = hmac.new(apiSecret.encode(), signature_payload.encode(), "sha256").hexdigest()

        return signature

    @staticmethod
    async def _default_handler(resp):
        body = await resp.json()
        if not body["success"]:
            raise ReferenceError(f"[ERROR] STATUS CODE {body['status']} - {body['message']}")
        return body["result"]

    @staticmethod
    async def _limit_handler(headers):
        """
        [NOTE] 아직 작성되지 않음.
        """
        try:
            rate_limit = {
                "per_sec_remaining": 5,
                "per_sec_replenish": 5,
                "per_min_remaining": 5,
                "per_min_replenish": 5,
            }
            logger.debug(f"[HTTP] FTX Rate Limit: {headers}")
        except KeyError as e:
            logging.warn(e)

        return rate_limit
