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
        redis_client: redis.Redis = None,
        redis_topic: str = None,
        redis_xadd_maxlen: bool = 10,
        redis_xadd_approximate: bool = False,
    ):
        self.codes = codes
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = 120
        self.redis_xadd_maxlen = redis_xadd_maxlen
        self.redis_xadd_approximate = redis_xadd_approximate

        self._redis_stream_name = f"{redis_topic}-stream/forex"
        self._path = "/forex/recent"

    async def request(self, codes: str = "FRX.KRWUSD"):
        url = f"{self.baseUrl}/{self.apiVersion}/{self._path.strip('/')}"
        query = f"codes={codes}"
        url = "?".join([url, query])
        async with aiohttp.ClientSession() as client:
            async with client.request(method="get", url=url, headers=self.headers) as resp:
                if resp.status not in [200, 201]:
                    _text = await resp.text()
                    raise ReferenceError(f"status code: {resp.status}, message: {_text}")
                resp = await resp.json()
                return resp

    async def run(self, codes: str = "FRX.KRWUSD", interval: int = 30):
        _subtopic = f"/forex/{codes}"
        name = f"{self.redis_topic}-stream" + _subtopic
        field_key = f"{self.redis_topic}" + _subtopic
        while True:
            records = await self.request(codes=codes)
            logger.debug(f"[FOREX] get records: {records}")
            for r in records:
                try:
                    msg = {
                        "name": name,
                        "fields": {"name": field_key, "value": self._parser(r)},
                    }
                    await self.redis_client.xadd(
                        **msg,
                        maxlen=self.redis_xadd_maxlen,
                        approximate=self.redis_xadd_approximate,
                    )
                    logger.debug(f"[FOREX] Redis XADD {msg}")
                except Exception as ex:
                    logger.warning(ex)
                if interval is not None:
                    await asyncio.sleep(interval)

    @staticmethod
    def _parser(record):
        return json.dumps(
            {
                "datetime": datetime.fromtimestamp(record["timestamp"] / 1e3)
                .astimezone(KST)
                .isoformat(timespec="milliseconds"),
                **{k: v for k, v in record.items() if k.endswith("Price")},
            }
        )


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.DEBUG)

    redis_client = redis.Redis()
    api = ForexApi(redis_client=redis_client, redis_topic="ubud")

    if len(sys.argv) > 1:
        interval = int(sys.argv[1])
    else:
        interval = None
    asyncio.run(api.run(interval=interval))
