import asyncio
import base64
import hashlib
import hmac
import logging
import time
from typing import Callable, List
from urllib.parse import urlencode, urljoin

from pydantic import BaseModel, Extra

from .base import BaseApi
from .bithumb_endpoints import ENDPOINTS

logger = logging.getLogger(__name__)

################################
# Exceptions
################################
class BithumbException(Exception):
    def __init__(self, status_code, detail):
        self.status_code = status_code
        self.detail = detail
        
    def __str__(self):
        return f"status_code: {self.status_code}, detail: {self.detail}"
    
class BithumbResponseException(Exception):
    critical_status_codes = [400, 401, 404]

    def __init__(self, status_code, body):
        # parse body
        if isinstance(body, str):
            error_code = "unknown"
            message = body
        else:
            error_code = body.get("status")
            message = body.get("message")

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
# TODO

################################################################
# _BithumbApi
################################################################
class _BithumbApi(BaseApi):

    # BITHUMB API URL
    baseUrl = "https://api.bithumb.com"
    endpoints = ENDPOINTS
    payload_type = "data"
    ResponseException = BithumbResponseException

    def generate_headers(self, method, path_url, **kwargs):
        if self.apiKey is None or self.apiSecret is None:
            return {}
        ts = str(int(time.time() * 1000))
        q = chr(0).join([path_url, urlencode(kwargs), ts])
        h = hmac.new(self.apiSecret.encode("utf-8"), q.encode("utf-8"), hashlib.sha512)
        return {
            "Api-Key": self.apiKey,
            "Api-Sign": base64.b64encode(h.hexdigest().encode("utf-8")).decode(),
            "Api-Nonce": ts,
        }
        
    @staticmethod
    def validator(body):
        # [NOTE]
        #  - 5600: 거래 진행중인 내역이 존재하지 않습니다.
        if body["status"] not in ["0000", "5600"]:
            raise BithumbException(status_code=body["status"], detail=body)

    @staticmethod
    def ratelimit_handler(headers):
        # get rate limit info.
        try:
            rate_limit = {
                "per_sec_remaining": int(headers["X-RateLimit-Remaining"]),
                "per_sec_replenish": int(headers["X-RateLimit-Replenish-Rate"]),
                "per_min_remaining": None,
                "per_min_replenish": None,
            }
            logger.debug(f"[API] Bithumb Rate Limit: {rate_limit}")
            return rate_limit
        except Exception as ex:
            logger.warning(f"[API] Bithumb Update rate_limit FALIED - {ex}, headers: {headers}")


################################################################
# BithumbPublicApi
################################################################
class BithumbPublicApi:
    
    # get_assets_status
    async def get_assets_status(
        self,
        oc: str = "ALL",
        interval: float = None,
    ) -> dict:
        return await self.request(
            method="GET",
            prefix="/public/assetsstatus",
            path=f"{oc}",
            interval=interval,
        )
        
    # get_ticker: /public/ticker
    async def get_ticker(
        self,
        oc: str = "ALL",
        pc: str = "KRW",
        interval: float = None,
    ):
        return await self.request(
            method="GET",
            prefix="/public/ticker",
            path=f"{oc}_{pc}",
            interval=interval,
        )

    # get_orderbook: /public/orderbook
    async def get_orderbook(
        self,
        oc: str = "ALL",
        pc: str = "KRW",
        depth: int = 15,
        interval: float = None,
    ) -> dict:
        """
        get_orderbook
        
        Args:
            oc (str, optional): Defaults to "ALL".
            pc (str, optional): Defaults to "KRW".
            depth (int, optional): [count]. Defaults to 30.
            interval (float, optional): Defaults to None.

        Returns:
            dict:
        """
        if oc == "ALL":
            logger.info(f"get_orderbook: max depth is 5 for ALL_{pc}")
            depth = max(depth, 5)
        return await self.request(
            method="GET",
            prefix="/public/orderbook",
            path=f"{oc}_{pc}",
            count=depth,
            interval=interval,
        )

    async def get_trades(
        self,
        oc: str = "ALL",
        pc: str = "KRW",
        count: int = 30,
        interval: float = None,
    ) -> dict:
        """
        get_trades

        [NOTE]
         - transaction은 Websocket 사용을 권장

        Args:
            oc (str, optional): 주문통화. Defaults to "ALL".
            pc (str, optional): 결제통화. Defaults to "KRW".
            count (int, optional): 체결 개수. Defaults to 30.
            interval (float, optional): Defaults to None.

        Returns:
            list
        """
        return await self.request(
            method="GET",
            prefix="/public/transaction_history",
            path=f"{oc}_{pc}",
            count=count,
            interval=interval,
        )
        

################################################################
# BithumbPrivateApi
################################################################
class BithumbPrivateApi:
    ################################
    # PRIVATE API (INFO)
    ################################        
    # get_account: /info/account
    async def get_account(
        self,
        oc: str,
        pc: str = "KRW",
    ):
        return await self.request(
            method="POST",
            prefix="/info/account",
            oc=oc,
            pc=pc,
        )
        
    # get_balance: /info/balance
    async def get_balance(
        self,
        currency: str = "ALL",
        interval: float = None,
    ):
        return await self.request(
            method="POST",
            prefix="/info/balance",
            currency=currency,
            interval=interval,
        )
        
    # get_wallet_address: /info/wallet_address
    async def get_wallet_address(
        self,
        currency: str = "BTC",
        interval: float = None,
    ):
        return await self.request(
            method="POST",
            prefix="/info/wallet_address",
            currency=currency,
            interval=interval,
        )
        
    # get_user_ticker: /info/ticker
    async def get_user_ticker(
        self,
        oc: str,
        pc: str = "KRW",
        interval: float = None,
    ):
        return await self.request(
            method="POST",
            prefix="/info/ticker",
            oc=oc,
            pc=pc,
            interval=interval,
        )
    
    # get_orders: /info/orders
    async def get_orders(
        self,
        *,
        oc: str,
        pc: str = "KRW",
        count: int = 100,
        after: int = None,
        interval: float = None,
    ) -> dict:
        """
        get_info_orders _summary_

        [NOTE]
         - Bithumb 이슈로 order_id, type 삭제. (제대로 작동하지 않음)
         - 상세 주문내역은 다른 get_info_order_detail 사용.

        Args:
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".
            count (int, optional): Defaults to 100.
            after (int, optional): Defaults to None.
            interval (float, optional): Defaults to None.

        Returns:
            dict
        """

        return await self.request(
            method="POST",
            prefix="/info/orders",
            oc=oc,
            pc=pc,
            count=count,
            after=after,            
            interval=interval,
        )
        
    # get_order: /info/order_detail
    async def get_order(
        self,
        order_id: str,
        *,
        oc: str,
        pc: str = "KRW",
        interval: float = None,
    ):
        return await self.request(
            method="POST",
            prefix="/info/order_detail",
            order_id=order_id,
            oc=oc,
            pc=pc,
            interval=interval,
        )
        
    # get_transactions: /info/user_transactions
    async def get_transaction_history(
        self,
        *,
        oc: str,
        pc: str = "KRW",
        offset: int = 0,
        count: int = 20,
        searchGb: int = 0,
        interval: float = None,
    ):
        return await self.request(
            method="POST",
            prefix="/info/user_transactions",
            oc=oc,
            pc=pc,
            offset=offset,
            count=count,
            searchGb=searchGb,
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
        oc: str,
        pc: str = "KRW",
        price: int = None,
        units: float = None,
    ) -> dict:
        """
        [NOTE]
         - 시장가보다 높게 매수 주문 넣을 경우 시장가로 거래되는 것으로 보임.
        Args:
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".
            units (float, optional): 주문수량 (최대 50억원). Defaults to None.
            price (int, optional): 거래가. Defaults to None.
            type (str, optional): 매수 bid, 매도 ask. Defaults to None.

        Returns:
            dict
        """
        return await self.request(
            method="POST",
            prefix="/trade/place",
            oc=oc,
            pc=pc,
            price=price,
            units=units,
            type="ask",
            interval=None,
        )
        
    # bid
    async def bid(
        self,
        *,
        oc: str,
        pc: str = "KRW",
        price: int = None,
        units: float = None,
    ) -> dict:
        """
        Args:
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".
            units (float, optional): 주문수량 (최대 50억원). Defaults to None.
            price (int, optional): 거래가. Defaults to None.

        Returns:
            dict
        """
        return await self.request(
            method="POST",
            prefix="/trade/place",
            oc=oc,
            pc=pc,
            units=units,
            price=price,
            type="bid",
            interval=None,
        )
        
    async def cancel_ask(
        self,
        *,
        order_id: str,
        oc: str,
        pc: str = "KRW",
    ) -> dict:
        """
        [NOTE]
         - 시장가보다 높게 매수 주문 넣을 경우 시장가로 거래되는 것으로 보임.
        Args:
            order_id (str): 주문번호.
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".

        Returns:
            dict
        """
        return await self.request(
            type="ask",
            method="POST",
            prefix="/trade/cancel",
            order_id=order_id,
            oc=oc,
            pc=pc,
            interval=None,
        )
        
    async def cancel_bid(
        self,
        *,
        order_id: str,
        oc: str,
        pc: str = "KRW",
    ) -> dict:
        """
        [NOTE]
         - 시장가보다 높게 매수 주문 넣을 경우 시장가로 거래되는 것으로 보임.
        Args:
            order_id (str): 주문번호.
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".

        Returns:
            dict
        """
        return await self.request(
            type="bid",
            method="POST",
            prefix="/trade/cancel",
            order_id=order_id,
            oc=oc,
            pc=pc,
            interval=None,
        )
        
    async def buy(
        self,
        *,
        oc: str,
        pc: str = "KRW",
        units: float = None,
    ) -> dict:
        """
        Args:
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".
            units (float, optional): 주문수량 (최대 50억원). Defaults to None.

        Returns:
            dict
        """
        return await self.request(
            method="POST",
            prefix="/trade/market_buy",
            oc=oc,
            pc=pc,
            units=units,
            interval=None,
        )
        
    async def sell(
        self,
        *,
        oc: str,
        pc: str = "KRW",
        units: float = None,
    ) -> dict:
        """
        Args:
            oc (str): 주문통화.
            pc (str, optional): 결제통화. Defaults to "KRW".
            units (float, optional): 주문수량 (최대 50억원). Defaults to None.

        Returns:
            dict
        """
        return await self.request(
            method="POST",
            prefix="/trade/market_sell",
            oc=oc,
            pc=pc,
            units=units,
            interval=None,
        )
    
    ################################
    # PRIVATE API (DEPOSIT/WITHDRAW)
    ################################
    async def withdraw_coin(
        self,
        *,
        currency: str,
        units: float,
        address: str,
        exchange_name: str,
        ko_name: str,
        en_name: str,
        cust_type_cd: str = "01",   # 01 개인, 02 법인
        destination: int = None,
    ) -> dict:
        """
        withdraw 코인 출금하기 (개인)

        Args:
            currency (str): 가상자산 영문코드.
            units (float): 출금 수량.
            address (str): 출금 주소.
            exchange_name (str): 출금 거래소 이름.
            ko_name (str): 개인수취정보 국문명.
            en_name (str): 개인수최정보 영문명.
            cust_type_cd (str): 01 개인, 02 법인, Defaults to "01".
            destination (int, optional): 2차 주소를 갖는 코인의 추가 정보. Defaults to None.

        Returns:
            dict: _description_
        """
        return await self.request(
            method="POST",
            prefix="trade/btc_withdrawal",
            currency=currency,
            units=units,
            address=address,
            exchange_name=exchange_name,
            ko_name=ko_name,
            en_name=en_name,
            cust_type_cd=cust_type_cd,
            destination=destination,
            interval=None,
        )
    

class BithumbApi(_BithumbApi, BithumbPublicApi, BithumbPrivateApi):
    pass
    