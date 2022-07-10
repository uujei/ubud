import logging
from pydantic import BaseModel, Extra


logger = logging.getLogger(__name__)


################################
# Models (Validators)
################################
# accounts
class GetBalances(BaseModel):
    method: str = "get"
    route: str = "/wallet/balances"

    class Config:
        extra = Extra.forbid


FTX_API = {
    "wallet/balances": GetBalances,
}
