import asyncio
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
from .upbit_endpoints import ENDPOINTS

logger = logging.getLogger(__name__)

################################
# Exceptions
################################
class UpbitException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail

    def __str__(self):
        return f"status_code: {self.status_code}, detail: {self.detail}"


class UpbitResponseException(Exception):
    critical_status_codes = [400, 401, 404]

    def __init__(self, status_code, body):
        # parse body
        if isinstance(body, str):
            error_code = "unknown"
            message = body
        else:
            error_code = body.get("error", {}).get("name", "")
            message = body.get("error", {}).get("message", "")

        # props
        self.status_code = status_code
        self.error_code = error_code
        self.message = message

        # we'll stop the loop if critical
        self.is_critical = self.status_code in self.critical_status_codes

    def __str__(self):
        return f"HTTP status [{self.status_code}] (critical={self.is_critical}), server sent error [{self.error_code}] {self.message}"


################################
# Rate Limit Parser
################################
REMAINING_REQ_FORM = parse.compile("group={group}; min={per_min_remaining}; sec={per_sec_remaining}")
REPLENISH = {
    "order": {"per_sec": 8, "per_min": 200},
    "default": {"per_sec": 30, "per_min": 900},
    "orderbook": {"per_sec": 10, "per_min": 600},
    "market": {"per_sec": 10, "per_min": 600},
    "crix-trades": {"per_sec": 10, "per_min": 600},
    "ticker": {"per_sec": 10, "per_min": 600},
    "status-wallet": {"per_sec": 10, "per_min": 600},  # 정확하지 않음
}

################################################################
# _UpbitApi
################################################################
class _UpbitApi(BaseApi):

    # Upbit URL
    baseUrl = "https://api.upbit.com/v1"
    endpoints = ENDPOINTS
    payload_type = "data"
    ResponseException = UpbitResponseException

    def generate_headers(self, method, endpoint, **kwargs):
        api_key = self.apiKey
        api_secret = self.apiSecret
        nonce = str(uuid.uuid4())
        return {
            "Authorization": self.generate_api_sign(
                endpoint=endpoint,
                nonce=nonce,
                apiKey=api_key,
                apiSecret=api_secret,
                **kwargs,
            )
        }

    @staticmethod
    def generate_api_sign(endpoint, nonce, apiKey, apiSecret, **kwargs):
        payload = {"access_key": apiKey, "nonce": nonce}
        if kwargs:
            m = hashlib.sha512()
            m.update(urlencode(kwargs).encode())
            query_hash = m.hexdigest()
            payload.update({"query_hash": query_hash, "query_hash_alg": "SHA512"})
        return f"Bearer {jwt.encode(payload, apiSecret)}"

    @staticmethod
    async def return_handler(resp):
        body = await resp.json()
        return body

    @staticmethod
    async def ratelimit_handler(response):
        # get rate limit info.
        _rate_limit = REMAINING_REQ_FORM.parse(response.headers["Remaining-Req"]).named
        print(response.headers["Remaining-Req"])
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
        except KeyError as ex:
            logging.warn(f"새 RateLimit 그룹 발견! {_group} - Remaining-Req: {response.headers['Remaining-Req']}")


################################################################
# BithumbPublicApi
################################################################
class UpbitPublicApi:
    markets: list = None

    async def _all_markets_for_given_payment_currency(self, payment_currency: str = None):
        if self.markets is None:
            self.markets = [m["market"] for m in await self.get_markets()]
        if payment_currency:
            return ",".join([m for m in self.markets if m.startswith(f"{payment_currency}-")])
        return self.markets

    # get_markets: /market/all
    async def get_markets(
        self,
        isDetails: bool = False,
        interval: float = None,
    ):
        RATELIMIT = 10
        return await self.request(
            method="GET",
            prefix="/market/all",
            isDetails=str(isDetails),
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_markets: /market/all (isDetails="true")
    async def get_assets_status(
        self,
        interval: float = None,
    ) -> list:
        RATELIMIT = 10
        return await self.request(
            method="GET",
            prefix="/market/all",
            isDetails="true",
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_ticker: /ticker
    async def get_ticker(
        self,
        order_currency: str = "ALL",
        payment_currency: str = "KRW",
        interval: float = None,
    ) -> list:
        # ratelimit:ticker
        RATELIMIT = 10
        if order_currency == "ALL":
            markets = await self._all_markets_for_given_payment_currency(payment_currency=payment_currency)
        else:
            markets = f"{payment_currency}-{order_currency}"

        return await self.request(
            method="GET",
            prefix="/ticker",
            markets=markets,
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_orderbook: /orderbook
    async def get_orderbook(
        self,
        order_currency: str = "ALL",
        payment_currency: str = "KRW",
        interval: float = None,
    ) -> list:
        # ratelimit:ticker
        RATELIMIT = 10
        if order_currency == "ALL":
            markets = await self._all_markets_for_given_payment_currency(payment_currency=payment_currency)
        else:
            markets = f"{payment_currency}-{order_currency}"

        return await self.request(
            method="GET",
            prefix="/orderbook",
            markets=markets,
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_transaction
    async def get_transaction(
        self,
        order_currency: str,
        payment_currency: str = "KRW",
        count: int = 30,
        interval: float = None,
    ) -> list:
        """
        get_transaction

        [NOTE]
         - transaction은 Websocket 사용을 권장
         - order_currency에 ALL 사용할 수 없음. (RateLimit 소진 이슈)
         - BithumbApi와의 consistency를 위해 parameter에서 to, cursor, daysAgo 제외

        Args:
            order_currency (str, optional): 주문통화. Defaults to "ALL".
            payment_currency (str, optional): 결제통화. Defaults to "KRW".
            count (int, optional): 체결 개수. Defaults to 30.
            interval (float, optional): Defaults to None.

        Returns:
            list
        """
        # ratelimit:crix-trades
        RATELIMIT = 10
        market = f"{payment_currency}-{order_currency}"

        return await self.request(
            method="GET",
            prefix="/trades/ticks",
            market=market,
            count=count,
            interval=interval,
            ratelimit=RATELIMIT,
        )


################################################################
# UpbitPrivateApi
################################################################
class UpbitPrivateApi:
    ################################
    # PRIVATE API (INFO)
    ################################
    # get_balance: /accounts
    async def get_balance(self, interval: float = None) -> list:
        # ratelimit:default
        RATELIMIT = 15

        return await self.request(
            method="GET",
            prefix="/accounts",
            interval=interval,
            ratelimit=RATELIMIT,
        )

    async def get_orders(
        self,
        order_id: str = None,
        *,
        order_currency: str = None,
        payment_currency: str = "KRW",
        state: str = "wait",
        count: int = 100,
        interval: float = None,
    ):
        # ratelimit:default
        RATELIMIT = 15

        if order_currency and payment_currency:
            market = f"{payment_currency}-{order_currency}"
        else:
            market = None
        return await self.request(
            method="GET",
            prefix="/orders",
            order_id=order_id,
            market=market,
            state=state,
            limit=count,
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_order: /info/order_detail
    async def get_order(
        self,
        order_id: str,
        *,
        order_currency: str = None,
        payment_currency: str = None,
        interval: float = None,
    ):
        # ratelimit:default
        RATELIMIT = 15

        return await self.request(
            method="GET",
            prefix="/order",
            uuid=order_id,
            interval=interval,
            ratelimit=RATELIMIT,
        )

    # get_user_transaction: /orders
    async def get_transaction_history(
        self,
        *,
        order_currency: str = None,
        payment_currency: str = "KRW",
        count: int = 100,
        interval: float = None,
    ):
        # ratelimit:default
        RATELIMIT = 15

        if order_currency and payment_currency:
            market = f"{payment_currency}-{order_currency}"
        else:
            market = None
        return await self.request(
            method="GET",
            prefix="/orders",
            market=market,
            state="done",
            limit=count,
            interval=interval,
            ratelimit=RATELIMIT,
        )

    async def get_deposits(
        self,
        *,
        currency: str = None,
        state: str = None,
        uuid: str = None,
        txid: str = None,
        count: int = 100,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/deposits",
            currency=currency,
            state=state,
            uuids=uuid,
            txids=txid,
            limit=count,
            interval=interval,
        )

    async def get_deposit(
        self,
        *,
        uuid: str,
        txid: str = None,
        currency: str = None,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/deposit",
            uuid=uuid,
            txid=txid,
            currency=currency,
            interval=interval,
        )

    async def get_withdraws(
        self,
        *,
        currency: str = None,
        state: str = None,
        uuid: str = None,
        txid: str = None,
        count: int = 100,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/withdraws",
            currency=currency,
            state=state,
            uuids=uuid,
            txids=txid,
            limit=count,
            interval=interval,
        )

    async def get_withdraw(
        self,
        *,
        uuid: str,
        txid: str = None,
        currency: str = None,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/withdraw",
            uuid=uuid,
            txid=txid,
            currency=currency,
            interval=interval,
        )

    async def get_withdraw_chance(
        self,
        *,
        currency: str = None,
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/withdraws/chance",
            currency=currency,
            interval=interval,
        )

    async def get_wallet_addresses(self):
        return await self.request(
            method="GET",
            prefix="/deposits/coin_addresses",
        )

    async def get_wallet_address(self, currency: str):
        return await self.request(
            method="GET",
            prefix="/deposits/coin_address",
            currency=currency,
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
        payment_currency: str = "KRW",
        units: float = None,
        price: float = None,
    ):
        market = f"{payment_currency}-{order_currency}"
        return await self.request(
            method="POST",
            prefix="/orders",
            market=market,
            volume=units,
            price=price,
            side="ask",
            ord_type="limit",
        )

    # bid
    async def bid(
        self,
        *,
        order_currency: str,
        payment_currency: str = "KRW",
        units: float = None,
        price: float = None,
    ):
        market = f"{payment_currency}-{order_currency}"
        return await self.request(
            method="POST",
            prefix="/orders",
            market=market,
            volume=units,
            price=price,
            side="bid",
            ord_type="limit",
        )

    # cancel_ask
    async def cancel_ask(
        self,
        *,
        order_id: str,
        order_currency: str = None,
        payment_currency: str = None,
    ) -> dict:
        """
        Args:
            order_id (str): 주문번호 [uuid].
            order_currency (str): 무시할 것. For API Consistency Only.
            payment_currency (str, optional): 무시할 것. For API Consistency Only.

        Returns:
            dict
        """
        return await self.request(
            method="DELETE",
            prefix="/order",
            uuid=order_id,
            interval=None,
        )

    # cancel_bid
    # [NOTE] upbit는 order_type 무관하게 cancel 가능하지만 API Consistency 위해 사용
    async def cancel_bid(
        self,
        *,
        order_id: str,
        order_currency: str = None,
        payment_currency: str = None,
    ) -> dict:
        """
        Args:
            order_id (str): 주문번호 [uuid].
            order_currency (str): 무시할 것. For API Consistency Only.
            payment_currency (str, optional): 무시할 것. For API Consistency Only.

        Returns:
            dict
        """
        return await self.request(
            method="DELETE",
            prefix="/order",
            uuid=order_id,
            interval=None,
        )

    # buy
    # [NOTE]
    #  - upbit는 현재가와 수량으로 시장가 구매 안 됨. (주문총액으로만 시장가 구매 가능)
    #  - 여기서는 현재가 조회한 뒤 해당 현재가로 지정가 주문 보내는 방식을 사용.
    #  - 따라서 바로 체결되지 않는 경우 발생할 수 있음.
    async def buy(
        self,
        *,
        order_currency: str,
        payment_currency: str = "KRW",
        units: float = None,
    ):
        TIMEOUT_SEC = 10.0
        CHECK_INTERVAL = 0.5
        market = f"{payment_currency}-{order_currency}"

        # 마지막 체결가 조회
        last_transaction = await self.request(
            method="GET",
            prefix="/trades/ticks",
            market=market,
            count=1,
            interval=None,
        )
        market_price = last_transaction[0]["trade_price"]

        # 마지막 체결가를 지정가로 주문
        order = await self.request(
            method="POST",
            prefix="/orders",
            market=market,
            side="bid",
            price=market_price,
            volume=units,
            ord_type="limit",
        )
        order_id = order["uuid"]

        # 체결 시까지 대기
        t0 = time.time()
        while time.time() - t0 < TIMEOUT_SEC:
            _orders = await self.get_orders(
                order_id=order_id,
                order_currency=order_currency,
                payment_currency=payment_currency,
            )
            if len(_orders) == 0:
                return order
            await asyncio.sleep(CHECK_INTERVAL)

        # 미체결 시 주문 취소
        canceled_order = await self.cancel_bid(order_id=order_id)
        return

    async def sell(
        self,
        *,
        order_currency: str,
        payment_currency: str = "KRW",
        units: float = None,
    ):
        market = f"{payment_currency}-{order_currency}"
        return await self.request(
            method="POST",
            prefix="/orders",
            market=market,
            side="ask",
            volume=units,
            ord_type="market",
        )

    ################################
    # PRIVATE API (DEPOSIT/WITHDRWAW)
    ################################
    async def deposit_krw(self, units: float):
        return await self.request(
            method="POST",
            prefix="/deposits/krw",
            amount=str(units),
        )

    async def withdraw_krw(self, units: float):
        return await self.request(
            method="POST",
            prefix="/withdraws/krw",
            amount=units,
        )

    async def withraw_coin(
        self,
        currency: str,
        units: float,
        address: str,
        secondary_address: str = None,
    ):
        return await self.request(
            method="POST",
            prefix="/withdraws/coin",
            currency=currency,
            amount=units,
            address=address,
            secondary_address=secondary_address,
            transaction_type="default",
        )

    async def generate_wallet_address(self, currency: str):
        TIMEOUT = 10
        CHECK_INTERVAL = 1.0 / 15

        resp = await self.request(
            method="POST",
            prefix="/deposits/generate_coin_address",
            currency=currency,
        )
        if not resp["success"]:
            return resp

        t0 = time.time()
        while resp.get("currency") is None:
            await asyncio.sleep(CHECK_INTERVAL)
            if time.time() - t0 > TIMEOUT:
                return resp

            resp = await self.request(
                method="POST",
                prefix="/deposits/generate_coin_address",
                currency=currency,
            )

        return resp


class UpbitApi(_UpbitApi, UpbitPublicApi, UpbitPrivateApi):
    pass
