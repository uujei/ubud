from pydantic import BaseModel


class OrderbookRecord(BaseModel):
    ts: int
    oc: str
    pc: str
    side: str
    rank: int
    price: float
    quantity: float
