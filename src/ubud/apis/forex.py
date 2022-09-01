import asyncio
import json
import logging
import time
from datetime import datetime
from urllib.parse import urlencode, urljoin

import aiohttp
import redis.asyncio as redis
from pydantic import BaseModel

from ..const import KST
from .base import BaseApi

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
# ForexApi
################################################################
class ForexApi(BaseApi):

    # Dunamu URL
    baseUrl = "https://quotation-api-cdn.dunamu.com/v1"
    endpoints = dict()
    payload_type = "data"

    async def get_krwusd(self, interval=None):
        return await self.request(
            method="get",
            prefix="/forex/recent",
            interval=interval,
            codes="FRX.KRWUSD",
        )

    def generate_headers(self, method, path_url, **kwargs):
        return {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
        }
