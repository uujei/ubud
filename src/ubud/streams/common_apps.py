import asyncio
import logging
import os

import redis.asyncio as redis

from ..apps.common.premium import PremiumApp
from ..apps.common.usd_to_krw import Usd2KrwApp
from ..redis import RedisStreamHandler
from ..redis.handler import RedisStreamHandler
from ..utils.app import parse_redis_addr, split_delim
from ..websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

logger = logging.getLogger(__name__)


async def stream_common_apps(
    redis_addr: str = "localhost:6379",
    redis_topic: str = "ubud",
    redis_xadd_maxlen: int = 100,
):

    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    # create handler
    handler = RedisStreamHandler(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_xadd_maxlen=redis_xadd_maxlen,
    )

    # coroutines
    coros = []

    # add 원화 환산 앱
    forex_app = Usd2KrwApp(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_streams=["*/forex/*", "*/USD/*/[0-1]"],
        redis_stream_handler=handler,
    )
    coros += [forex_app.run()]

    # add 프리미엄 계산 앱
    premium_app = PremiumApp(
        redis_client=redis_client,
        redis_streams=["*/quotation/*/KRW*/*/[0-1]"],
        redis_stream_handler=handler,
    )
    coros += [premium_app.run()]

    # RUN
    await asyncio.gather(*coros)
