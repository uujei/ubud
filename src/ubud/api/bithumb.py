import asyncio
import base64
import hashlib
import hmac
import logging
import time
from typing import Callable, List
from urllib.parse import urlencode, urljoin

from pydantic import BaseModel

from .base import BaseApi

logger = logging.getLogger(__name__)

PATH_PARAMS_PUBLIC = ["order_currency", "payment_currency"]
DELIM_PATH_PARAMS = "_"


################################
# Models (Validators)
################################
class InfoAccount(BaseModel):
    method: str = "post"
    route: str = "/info/account"
    order_currency: str
    payment_currency: str = None


class InfoBalance(BaseModel):
    method: str = "post"
    route: str = "/info/balance"
    order_currency: str
    payment_currency: str = None


class InfoWalletAddress(BaseModel):
    method: str = "post"
    route: str = "/info/wallet_address"
    currency: str = "BTC"


class InfoTicker(BaseModel):
    method: str = "post"
    route: str = "/info/ticker"
    order_currency: str
    payment_currency: str = None


class InfoOrders(BaseModel):
    method: str = "post"
    route: str = "/info/orders"
    order_currency: str
    order_id: str = None
    payment_currency: str = None
    type: str = None
    count: int = 100
    after: str = None


class InfoOrderDetail(BaseModel):
    method: str = "post"
    order_currency: str
    order_id: str
    payment_currency: str = None


class InfoUserTransactions(BaseModel):
    method: str = "post"
    route: str = "/info/user_transactions"
    order_currency: str
    payment_currency: str
    offset: int = 0
    count: int = 20
    searchGb: str = None


class TradePlace(BaseModel):
    method: str = "post"
    route: str = "/trade/place"
    order_currency: str
    payment_currency: str
    units: float
    price: int
    type: str


class TradeMarketBuy(BaseModel):
    method: str = "post"
    route: str = "/trade/market_buy"
    order_currency: str
    payment_currency: str
    units: float


class TradeMarketSell(BaseModel):
    method: str = "post"
    route: str = "/trade/market_sell"
    order_currency: str
    payment_currency: str
    units: float


class TradeStopLimit(BaseModel):
    method: str = "post"
    route: str = "/trade/stop_limit"
    order_currency: str
    payment_currency: str
    units: float
    watch_price: float
    price: float
    type: str


class TradeCancel(BaseModel):
    method: str = "post"
    route: str = "/trade/cancel"
    order_currency: str
    order_id: str
    payment_currency: str
    type: str


class TradeBtcWithdrawal(BaseModel):
    method: str = "post"
    route: str = "/trade/btc_withdrawal"
    units: float
    address: str
    currency: str
    exchange_name: str
    cust_type_cd: str
    ko_name: str
    en_name: str


BITHUMB_API = {
    "info/account": InfoAccount,
    "info/balance": InfoBalance,
    "info/wallet_address": InfoWalletAddress,
    "info/ticker": InfoTicker,
    "info/orders": InfoOrders,
    "info/order_detail": InfoOrderDetail,
    "info/user_transactions": InfoUserTransactions,
    "trade/place": TradePlace,
    "trade/market_buy": TradeMarketBuy,
    "trade/market_sell": TradeMarketSell,
    "trade/stop_limit": TradeStopLimit,
    "trade/cancel": TradeCancel,
    "trade/btc_withdrawal": TradeBtcWithdrawal,
}


################################################################
# BithumbApi
################################################################
class BithumbApi(BaseApi):

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"
    apiVersion = "unknown"

    async def request(self, path: str, **kwargs):
        model = BITHUMB_API[path.strip("/")](**kwargs)
        data = await self._request(**model.dict(exclude_none=True))
        return data

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
        if body["status"] != "0000":
            raise ReferenceError(f"[ERROR] STATUS CODE {body['status']} - {body['message']}")
        return body["data"]

    @staticmethod
    async def _limit_handler(headers):
        # get rate limit info.
        rate_limit = {
            "per_sec_remaining": int(headers["X-RateLimit-Remaining"]),
            "per_sec_replenish": int(headers["X-RateLimit-Replenish-Rate"]),
            "per_min_remaining": None,
            "per_min_replenish": None,
        }

        return rate_limit
