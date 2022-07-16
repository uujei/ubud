import asyncio
from datetime import datetime
import json
import logging
from urllib.parse import urlencode, urljoin
import redis.asyncio as redis
import time
import aiohttp
from pydantic import BaseModel

from ..const import KST

logger = logging.getLogger(__name__)


################################################################
# Model (사용하지 않음, 참고용)
################################################################
class ForexModel(BaseModel):
    code: str  # 'FRX.KRWUSD',
    currencyCode: str  # 'USD',
    currencyName: str  # '달러',
    country: str  # '미국',
    name: str  # '미국 (KRW/USD)',
    date: str  # '2022-07-08',
    time: str  # '20:01:00',
    recurrenceCount: int  # 554,
    basePrice: float  # 1301.5,
    openingPrice: float  # 1302.7,
    highPrice: float  # 1304.5,
    lowPrice: float  # 1295.3,
    change: str  # 'RISE',
    changePrice: float  # 1.0,
    cashBuyingPrice: float  # 1324.27,
    cashSellingPrice: float  # 1278.73,
    ttBuyingPrice: float  # 1288.8,
    ttSellingPrice: float  # 1314.2,
    tcBuyingPrice: str  # None,
    fcSellingPrice: str  # None,
    exchangeCommission: float  # 3.6743,
    usDollarRate: float  # 1.0,
    high52wPrice: float  # 1311.5,
    high52wDate: str  # '2022-07-05',
    low52wPrice: float  # 1140.5,
    low52wDate: str  # '2021-08-06',
    currencyUnit: int  # 1,
    provider: str  # '하나은행',
    timestamp: int  # 1657278061493,
    id: int  # 79,
    modifiedAt: str  # '2022-07-08T11:01:02.000+0000',
    createdAt: str  # '2016-10-21T06:13:34.000+0000',
    changeRate: float  # 0.000768935,
    signedChangePrice: float  # 1.0,
    signedChangeRate: float  # 0.000768935}


################################################################
# Api
################################################################
class ForexApi:

    # Dunamu URL
    baseUrl = "https://quotation-api-cdn.dunamu.com"
    apiVersion = "v1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }

    def __init__(
        self,
        codes: str = "FRX.KRWUSD",
        interval: float = 10.0,
        redis_client: redis.Redis = None,
        redis_topic: str = None,
        redis_expire_sec: int = 10,
        redis_xadd_maxlen: bool = 100,
        redis_xadd_approximate: bool = False,
    ):
        self.codes = codes
        self.interval = interval
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec
        self.redis_xadd_maxlen = redis_xadd_maxlen
        self.redis_xadd_approximate = redis_xadd_approximate

        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_name = f"{self.redis_topic}-stream/forex/{self.codes}"
        self._redis_field_key = f"{self.redis_topic}/forex/{self.codes}"
        self._path = "/forex/recent"

    async def request(self):
        url = f"{self.baseUrl}/{self.apiVersion}/{self._path.strip('/')}"
        query = f"codes={self.codes}"
        url = "?".join([url, query])
        async with aiohttp.ClientSession() as client:
            async with client.request(method="get", url=url, headers=self.headers) as resp:
                if resp.status not in [200, 201]:
                    _text = await resp.text()
                    raise ReferenceError(f"status code: {resp.status}, message: {_text}")
                resp = await resp.json()
                return resp


