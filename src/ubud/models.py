from pydantic import BaseModel, Extra

# Stream Data
class Message(BaseModel):
    key: str
    value: dict


# [TOBE] not using yet
class TradeModel(BaseModel):
    datetime: str
    price: float
    quantity: float
    amount: float

    class Config:
        extra = "ignore"
