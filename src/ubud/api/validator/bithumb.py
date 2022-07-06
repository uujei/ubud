import base64
import hashlib
import hmac
from typing import List, Callable
import time
from urllib.parse import urlencode, urljoin
import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)


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


MODEL = {
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
