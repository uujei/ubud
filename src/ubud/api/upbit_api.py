import logging

from pydantic import BaseModel, Extra

logger = logging.getLogger(__name__)


################################
# Models (Validators)
################################
# accounts
class Accounts(BaseModel):
    method: str = "get"
    route: str = "/accounts"

    class Config:
        extra = Extra.forbid


# orders/chance
class OrdersChance(BaseModel):
    method: str = "get"
    route: str = "/orders/chance"
    market: str

    class Config:
        extra = Extra.forbid


# order (get)
class OrderGet(BaseModel):
    method: str = "get"
    route: str = "/order"
    uuid: str = None
    identifier: str = None

    class Config:
        extra = Extra.forbid


# order (post)
class OrdersPost(BaseModel):
    method: str = "post"
    route: str = "/orders"
    market: str
    side: str
    ord_type: str

    class Config:
        extra = Extra.forbid


# order (delete)
class OrderDelete(BaseModel):
    method: str = "delete"
    route: str = "/order"
    uuid: str = None
    identifier: str = None

    class Config:
        extra = Extra.forbid


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

    class Config:
        extra = Extra.forbid


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

    class Config:
        extra = Extra.forbid


# withdraw
class Withdraw(BaseModel):
    method: str = "get"
    route: str = "/withdraw"
    uuid: str = None
    txid: str = None
    currency: str = None

    class Config:
        extra = Extra.forbid


# withdraws/chance
class WithdrawsChance(BaseModel):
    method: str = "get"
    route: str = "/withdraws/chance"

    class Config:
        extra = Extra.forbid


# withdraw/coin
class WithdrawsCoin(BaseModel):
    method: str = "post"
    route: str = "/withdraws/coin"
    currency: str
    amount: float
    address: str
    secondary_address: str = None
    transaction_type: str = "default"

    class Config:
        extra = Extra.forbid


# withdraw/krw
class WithdrawsKrw(BaseModel):
    method: str = "post"
    route: str = "/withdraws/krw"
    amount: float

    class Config:
        extra = Extra.forbid


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

    class Config:
        extra = Extra.forbid


# deposits/krw
class DepositsKrw(BaseModel):
    method: str = "post"
    route: str = "/deposits/krw"
    amount: float

    class Config:
        extra = Extra.forbid


# deposits/generate_coin_address
class DepositsGenerateCoinAddress(BaseModel):
    method: str = "post"
    route: str = "/deposits/generate_coin_address"
    currency: str

    class Config:
        extra = Extra.forbid


# deposits/coin_address
class DepositsCoinAddress(BaseModel):
    method: str = "get"
    route: str = "/deposits/coin_address"
    currency: str = None
    deposit_address: str = None
    secondary_address: str = None

    class Config:
        extra = Extra.forbid


# deposits/coin_addresses
class DepositsCoinAddresses(BaseModel):
    method: str = "get"
    route: str = "/deposits/coin_addresses"
    currency: str = None
    deposit_address: str = None
    secondary_address: str = None

    class Config:
        extra = Extra.forbid


# status/wallet
class StatusWallet(BaseModel):
    method: str = "get"
    route: str = "/status/wallet"

    class Config:
        extra = Extra.forbid


# api_keys
class ApiKeys(BaseModel):
    method: str = "get"
    route: str = "/api_keys"

    class Config:
        extra = Extra.forbid


UPBIT_API = {
    "accounts": Accounts,
    "orders/chance": OrdersChance,
    "orders": Orders,
    "orders.post": OrdersPost,
    "order.get": OrderGet,
    "order.delete": OrderDelete,
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
