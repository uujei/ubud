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
# Exceptions
################################
class FtxException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail

    def __str__(self):
        return f"status_code: {self.status_code}, detail: {self.detail}"


class FtxResponseException(Exception):
    critical_status_codes = [400, 401, 404]

    def __init__(self, status_code, body):
        # parse body
        if isinstance(body, str):
            error_code = "unknown"
            message = body
        else:
            error_code = "ftx-has-no-error-code"
            message = body.get("error", "unknown")

        # props
        self.status_code = status_code
        self.error_code = error_code
        self.message = message

        # we'll stop the loop if critical
        self.is_critical = self.status_code in self.critical_status_codes

    def __str__(self):
        return f"HTTP status [{self.status_code}] (critical={self.is_critical}), server sent error [{self.error_code}] {self.message}"


# helper
def get_market_name(order_currency: str, payment_currency: str):
    EXCHANGES = ["USD"]
    if payment_currency in EXCHANGES:
        return f"{order_currency}/{payment_currency}"
    else:
        return f"{order_currency}-{payment_currency}"


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
# FtxApi
################################################################
class _FtxApi(BaseApi):
    baseUrl = "https://ftx.com/api"
    endpoints = ENDPOINTS
    payload_type = "json"
    ResponseException = FtxResponseException

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
        if method in ["GET", "DELETE"] and kwargs:
            signature_payload = "?".join([signature_payload, urlencode(kwargs)])
        if method == "POST":
            signature_payload += json.dumps(kwargs)
        signature = hmac.new(apiSecret.encode(), signature_payload.encode(), "sha256").hexdigest()

        return signature

    @staticmethod
    async def return_handler(resp):
        body = await resp.json()
        if not body["success"]:
            raise FtxException(status_code=body["status"], detail=body)
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


################################################################
# FtxPublicApi
################################################################
class FtxPublicApi:

    # get_markets
    async def get_markets(self, interval: float = None):
        return await self.request(
            method="GET",
            prefix="/markets",
            interval=interval,
        )

    async def get_market(self, order_currency: str, payment_currency: str = "USD", interval: float = None):
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="GET",
            prefix="/markets",
            path=market_name,
            interval=interval,
        )

    async def get_ticker(
        self,
        order_currency: str,
        payment_currency: str = "USD",
        resolution: int = 86400,
        start_time: int = None,
        end_time: int = None,
        interval: float = None,
    ) -> list:
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="GET",
            prefix="/markets",
            path=f"{market_name}/candles",
            resolution=resolution,
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )

    async def get_orderbook(
        self,
        order_currency: str,
        payment_currency: str = "USD",
        depth: str = 15,
        interval: float = None,
    ) -> list:
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="GET",
            prefix="/markets",
            path=f"{market_name}/orderbook",
            depth=depth,
            interval=interval,
        )

    # get_trades
    async def get_trades(
        self,
        order_currency: str,
        payment_currency: str = "USD",
        start_time: int = None,
        end_time: int = None,
        interval: float = None,
    ) -> list:
        """
        get_trades

        [NOTE]
         - transaction은 Websocket 사용을 권장
         - order_currency에 ALL 사용할 수 없음. (RateLimit 소진 이슈)
         - FTX는 count가 아닌 start_time, end_time을 받음

        Args:
            order_currency (str, optional): 주문통화. Defaults to "ALL".
            payment_currency (str, optional): 결제통화. Defaults to "KRW".
            count (int, optional): 체결 개수. Defaults to 30.
            interval (float, optional): Defaults to None.

        Returns:
            list
        """
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="GET",
            prefix="/markets",
            path=f"{market_name}/trades",
            start_time=start_time,
            end_time=end_time,
            interval=interval,
        )


################################################################
# FtxPrivateApi
################################################################
class FtxPrivateApi:
    ################################
    # PRIVATE API (INFO)
    ################################
    # get_account
    async def get_account(self):
        return await self.request(
            method="GET",
            prefix="/account",
        )

    # get_balance: /wallet/balances
    async def get_balance(self, interval: float = None) -> list:
        # ratelimit:default
        RATELIMIT = 15

        return await self.request(
            method="GET",
            prefix="/wallet/balances",
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_orders: "get open orders"
    async def get_orders(
        self,
        order_currency: str = None,
        payment_currency: str = "USD",
        interval: float = None,
    ):
        if order_currency and payment_currency:
            market = get_market_name(
                order_currency=order_currency,
                payment_currency=payment_currency,
            )
        else:
            market = None
        return await self.request(
            method="GET",
            prefix="/orders",
            market=market,
            interval=interval,
        )

    # get_orders: "get open orders"
    async def get_order(
        self,
        order_id: int,
        *,
        order_currency: str = None,
        payment_currency: str = None,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/orders",
            path=str(order_id),
            interval=interval,
        )

    ################################
    # PRIVATE API (TRADE)
    #  - ask, cancel_ask
    #  - bid, cancel_bid
    #  - sell
    #  - buy
    #  - withdraw
    # [NOTE]
    #  - 실수 방지 위해 interval 제외! (self.request에서는 None으로 명시!)
    ################################
    # ask
    async def ask(
        self,
        *,
        order_currency: str,
        payment_currency: str = "USD",
        price: float = None,
        units: float = None,
    ):
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="POST",
            prefix="/orders",
            market=market_name,
            side="sell",
            type="limit",
            price=price,
            size=units,
        )

    # bid
    async def bid(
        self,
        *,
        order_currency: str,
        payment_currency: str = "USD",
        price: float = None,
        units: float = None,
    ):
        market_name = get_market_name(order_currency=order_currency, payment_currency=payment_currency)
        return await self.request(
            method="POST",
            prefix="/orders",
            market=market_name,
            side="buy",
            type="limit",
            price=price,
            size=units,
        )

    # cancel_ask
    async def cancel_ask(
        self,
        *,
        order_id: int,
        order_currency: str = None,
        payment_currency: str = None,
    ) -> dict:
        """
        Args:
            order_id (str): 주문번호.
            order_currency (str): 무시할 것. For API Consistency Only.
            payment_currency (str, optional): 무시할 것. For API Consistency Only.

        Returns:
            dict
        """
        return await self.request(
            method="DELETE",
            prefix="/orders",
            path=str(order_id),
            interval=None,
        )

    # cancel_bid
    # [NOTE] upbit는 order_type 무관하게 cancel 가능하지만 API Consistency 위해 사용
    async def cancel_bid(
        self,
        *,
        order_id: int,
        order_currency: str = None,
        payment_currency: str = None,
    ) -> dict:
        """
        Args:
            order_id (str): 주문번호.
            order_currency (str): 무시할 것. For API Consistency Only.
            payment_currency (str, optional): 무시할 것. For API Consistency Only.

        Returns:
            dict
        """
        return await self.request(
            method="DELETE",
            prefix="/orders",
            path=str(order_id),
            interval=None,
        )


################################################################
# FtxApi
################################################################
class FtxApi(_FtxApi, FtxPublicApi, FtxPrivateApi):
    pass
