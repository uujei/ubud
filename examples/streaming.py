import asyncio
import logging

import redis.asyncio as redis
from ubud.api.forex import ForexApi
from ubud.redis.upserter import Upserter as RedisUpserter
from ubud.websocket.bithumb import BithumbWebsocket
from ubud.websocket.upbit import UpbitWebsocket

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
    ],
    format="%(asctime)s:%(levelname)s:%(message)s",
)
logger = logging.getLogger(__name__)


################################################################
# ARGUMENTS
################################################################
TOPIC = "ubud"

MARKETS = ["upbit", "bithumb"]
QUOTES = ["orderbook", "trade"]
SYMBOLS = ["BTC", "ETH", "WAVES"]
CURRENCY = "KRW"


################################################################
# BACKEND SETTINGS
################################################################
WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
}

REDIS_ADDRESS = "localhost"
REDIS_PORT = 6379


################################################################
# EXECUTE
################################################################
logger.info(f"[UBUD] markets: {MARKETS}")
logger.info(f"[UBUD] quotes: {QUOTES}")
logger.info(f"[UBUD] symbols: {SYMBOLS}")


async def tasks():
    # websockets
    recipes = [{"market": m, "args": {"quote": q, "symbols": SYMBOLS}} for m in MARKETS for q in QUOTES]
    handler = RedisUpserter(url=REDIS_ADDRESS, port=REDIS_PORT, root_topic=TOPIC)
    coroutines = [WEBSOCKET[opt["market"]](**opt["args"], currency=CURRENCY, handler=handler).run() for opt in recipes]

    # forex
    redis_client = redis.Redis()
    coroutines += [ForexApi(redis_client=redis_client, redis_topic=TOPIC).run(sleep=30)]

    await asyncio.gather(*coroutines)


asyncio.run(tasks())
