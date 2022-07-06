import logging
from pydantic import BaseModel


logger = logging.getLogger(__name__)

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


MODEL = {
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
