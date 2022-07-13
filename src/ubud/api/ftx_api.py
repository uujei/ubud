import logging
from pydantic import BaseModel, Extra


logger = logging.getLogger(__name__)


################################
# Models (Validators)
################################
class GetMarkets(BaseModel):
    method: str = "get"
    route: str = "/markets"


class Positions(BaseModel):
    method: str = "get"
    route: str = "/positions"


# account
class GetBalances(BaseModel):
    method: str = "get"
    route: str = "/wallet/balances"

    class Config:
        extra = Extra.forbid


class GetAllBalances(BaseModel):
    method: str = "get"
    route: str = "/wallet/all_balances"

    class Config:
        extra = Extra.forbid


FTX_API = {
    "wallet/balances": GetBalances,
    "wallet/all_balances": GetAllBalances,
}
