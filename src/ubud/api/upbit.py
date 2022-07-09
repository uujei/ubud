import hashlib
import logging
import time
import uuid
from typing import Callable, List
from urllib.parse import urlencode, urljoin
from pydantic import BaseModel
import jwt
import parse

from .base import BaseApi

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

################################
# Models (Validators)
################################
# accounts
class Accounts(BaseModel):
    method: str = "get"
    route: str = "/accounts"


# orders/chance
class OrdersChance(BaseModel):
    method: str = "get"
    route: str = "/orders/chance"
    market: str


# order (get)
class OrderGet(BaseModel):
    method: str = "get"
    route: str = "/order"
    uuid: str = None
    identifier: str = None


# order (post)
class OrderPost(BaseModel):
    method: str = "post"
    route: str = "/order"
    market: str
    side: str
    ord_type: str


# order (delete)
class OrderDelete(BaseModel):
    method: str = "delete"
    route: str = "/order"
    uuid: str = None
    identifier: str = None


# orders
class Orders(BaseModel):
    method: str = "get"
    route: str = "/orders"
    market: str
    uuids: str
    state: str = None
    states: list = None
    page: int = 1
    limit: int = 1
    order_by: str = "desc"


# withdraws
class Withdraws(BaseModel):
    method: str = "get"
    route: str = "/withdraws"
    currency: str = None
    state: str = None
    uuids: list = None
    txids: list = None
    page: int = 1
    order_by: str = "desc"


# withdraw
class Withdraw(BaseModel):
    method: str = "get"
    route: str = "/withdraw"
    uuid: str = None
    txid: str = None
    currency: str = None


# withdraws/chance
class WithdrawsChance(BaseModel):
    method: str = "get"
    route: str = "/withdraws/chance"


# withdraw/coin
class WithdrawsCoin(BaseModel):
    method: str = "post"
    route: str = "/withdraws/coin"
    currency: str
    amount: float
    address: str
    secondary_address: str = None
    transaction_type: str = "default"


# withdraw/krw
class WithdrawsKrw(BaseModel):
    method: str = "post"
    route: str = "/withdraws/krw"
    amount: float


# deposits
class Deposits(BaseModel):
    method: str = "get"
    route: str = "/deposit"
    currency: str = None
    state: str = None
    uuids: list = None
    txids: list = None
    page: int = 1
    order_by: str = "desc"


# deposits/krw
class DepositsKrw(BaseModel):
    method: str = "post"
    route: str = "/deposits/krw"
    amount: float


# deposits/generate_coin_address
class DepositsGenerateCoinAddress(BaseModel):
    method: str = "post"
    route: str = "/deposits/generate_coin_address"
    currency: str


# deposits/coin_address
class DepositsCoinAddress(BaseModel):
    method: str = "get"
    route: str = "/deposits/coin_address"
    currency: str = None
    deposit_address: str = None
    secondary_address: str = None


# deposits/coin_addresses
class DepositsCoinAddresses(BaseModel):
    method: str = "get"
    route: str = "/deposits/coin_addresses"
    currency: str = None
    deposit_address: str = None
    secondary_address: str = None


# status/wallet
class StatusWallet(BaseModel):
    method: str = "get"
    route: str = "/status/wallet"


# api_keys
class ApiKeys(BaseModel):
    method: str = "get"
    route: str = "/api_keys"


UPBIT_API = {
    "accounts": Accounts,
    "orders/chance": OrdersChance,
    "order.get": OrderGet,
    "order.post": OrderPost,
    "order.delete": OrderDelete,
    "orders": Orders,
    "withdraws": Withdraws,
    "withdraw": Withdraw,
    "withdraws/chance": WithdrawsChance,
    "withdraws/coin": WithdrawsCoin,
    "withdraws/krw": WithdrawsKrw,
    "deposits": Deposits,
    "deposits/krw": DepositsKrw,
    "deposits/generate_coin_address": DepositsGenerateCoinAddress,
    "deposits/coin_address": DepositsCoinAddress,
    "deposits/coin_addresses": DepositsCoinAddresses,
    "status/wallet": StatusWallet,
    "api_keys": ApiKeys,
}

################################################################
# UpbitApi
################################################################
class UpbitApi(BaseApi):

    # Upbit URL
    baseUrl = "https://api.upbit.com"
    apiVersion = "v1"

    async def request(self, path: str, **kwargs):
        model = UPBIT_API[path.strip("/")](**kwargs)
        data = await self._request(**model.dict(exclude_none=True))
        return data

    def _gen_request_args(self, route, **kwargs):
        if not route.strip("/").startswith(self.apiVersion):
            route = "/".join([self.apiVersion, route.strip("/")])
        headers = self._gen_header(route=route, **kwargs)
        return {
            "url": urljoin(self.baseUrl, route),
            "headers": headers,
            "data": kwargs,
        }

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
    def _gen_api_nonce():
        return str(uuid.uuid4())

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
        except KeyError as e:
            logging.warn(f"NEW LIMIT HEADER GROUP FOUND {_group}")

        return rate_limit
