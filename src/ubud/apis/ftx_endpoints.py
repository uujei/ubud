import logging
from pydantic import BaseModel, Extra


logger = logging.getLogger(__name__)


################################
# Models (Validators)
################################
# markets
class GetMarkets(BaseModel):
    method: str = "get"
    prefix: str = "/markets"


# account
class GetAccount(BaseModel):
    method: str = "get"
    prefix: str = "/wallet/balances"

    class Config:
        extra = Extra.forbid


# balances
class GetBalances(BaseModel):
    method: str = "get"
    prefix: str = "/wallet/balances"

    class Config:
        extra = Extra.forbid


# all balances
class GetAllBalances(BaseModel):
    method: str = "get"
    prefix: str = "/wallet/all_balances"

    class Config:
        extra = Extra.forbid


# positions
class GetPositions(BaseModel):
    method: str = "get"
    prefix: str = "/positions"
    showAvgPrice: str = "false"

    class Config:
        extra = Extra.forbid


ENDPOINTS = {
    "markets": GetMarkets,
    "account": GetAccount,
    "wallet/balances": GetBalances,
    "wallet/all_balances": GetAllBalances,
    "positions": GetPositions,
}
