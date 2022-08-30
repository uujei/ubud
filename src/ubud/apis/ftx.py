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
from .ftx_endpoints import ENDPOINTS

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
    endpoints = ENDPOINTS
    payload_type = "json"

    def generate_headers(self, method, endpoint, **kwargs):
        ts = int(time.time() * 1e3)
        return {
            "FTX-KEY": self.apiKey,
            "FTX-SIGN": self.generate_api_sign(
                method=method,
                endpoint=endpoint,
                apiSecret=self.apiSecret,
                ts=ts,
                **kwargs,
            ),
            "FTX-TS": str(ts),
        }

    @staticmethod
    def generate_api_sign(method, endpoint, ts, apiSecret, **kwargs):
        method = method.upper()
        signature_payload = f"{ts}{method}/{endpoint.strip('/')}"
        if method == "GET" and kwargs:
            signature_payload = "?".join([signature_payload, urlencode(kwargs)])
        if method == "POST":
            signature_payload += json.dumps(kwargs)
        signature = hmac.new(apiSecret.encode(), signature_payload.encode(), "sha256").hexdigest()

        return signature

    @staticmethod
    async def return_handler(resp):
        body = await resp.json()
        if not body["success"]:
            raise ReferenceError(f"[ERROR] STATUS CODE {body['status']} - {body['message']}")
        return body["result"]

    @staticmethod
    async def ratelimit_handler(response):
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
            logger.debug(f"[HTTP] FTX Rate Limit: {response.headers}")
        except KeyError as e:
            logging.warn(e)

        return rate_limit
