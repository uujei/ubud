import asyncio
import logging
import os

import dotenv
import redis.asyncio as redis
from ubud.api.forex import ForexApi
from ubud.api.updater import BithumbBalanceUpdater, FtxBalanceUpdater, UpbitBalanceUpdater
from ubud.redis import Collector, Streamer
from ubud.websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"

WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
    "ftx": FtxWebsocket,
}

BALANCE_UPDATER = {
    "upbit": UpbitBalanceUpdater,
    "bithumb": BithumbBalanceUpdater,
    "ftx": FtxBalanceUpdater,
}

# set log level
logging.basicConfig(
    level=logging.debug,
    format=DEFAULT_LOG_FORMAT,
)

################################################################
# Load Conf
################################################################
CONF = {
    "topic": "ubud",
    "env_file": ".env",
    "redis": {
        "host": "localhost",
        "port": 6379,
        "decode_responses": True,
        "exprire_sec": 60,
        "maxlen": 100,
    },
    "credential": {
        "upbit": {
            "apiKey": "UPBIT_API_KEY",
            "apiSecret": "UPBIT_API_SECRET",
        },
        "bithumb": {
            "apiKey": "BITHUMB_API_KEY",
            "apiSecret": "BITHUMB_API_SECRET",
        },
        "ftx": {
            "apiKey": "FTX_API_KEY",
            "apiSecret": "FTX_API_SECRET",
        },
    },
    "websocket": {
        "upbit": {
            "channel": ["trade", "orderbook"],
            "symbol": ["BTC", "ETH", "WAVES"],
            "currency": ["KRW"],
        },
        "bithumb": {
            "channel": ["trade", "orderbook"],
            "symbol": ["BTC", "ETH", "WAVES"],
            "currency": ["KRW"],
        },
        "ftx": {
            "channel": ["trade", "orderbook"],
            "symbol": ["BTC", "ETH", "WAVES"],
            "currency": ["USD", "-PERP"],
        },
    },
    "http": {
        "forex": {"request": None},
    },
}

# topic
topic = CONF["topic"]

# load dotenv
dotenv.load_dotenv(CONF["env_file"])

# credential
credential = CONF["credential"]
CREDS = dict()
for market, cred in credential.items():
    CREDS.update({market: dict()})
    for k, v in cred.items():
        CREDS[market].update({k: os.getenv(v)})

# redis
redis_conf = CONF["redis"]
redis_client_conf = {k: v for k, v in redis_conf.items() if k in ("host", "port", "decode_responses")}
redis_expire_sec = redis_conf.get("expire_sec")
redis_maxlen = redis_conf.get("maxlen")

websocket_conf = CONF["websocket"]

################################################################
# TASK SETTINGS
################################################################
async def tasks():
    # set redis client
    redis_client = redis.Redis(**redis_client_conf)
    coroutines = []

    # add collector task (redis stream to redis db)
    collector = Collector(redis_client=redis_client, redis_topic=topic, redis_expire_sec=redis_expire_sec)
    coroutines += [collector.run()]

    # add websocket streamer tasks
    streamer = Streamer(redis_client=redis_client, redis_topic=topic, redis_xadd_maxlen=redis_maxlen)
    for market, args in websocket_conf.items():
        for channel in args["channel"]:
            coroutines += [
                WEBSOCKET[market](
                    channel=channel,
                    symbols=args["symbol"],
                    currencies=args["currency"],
                    apiKey=CREDS[market]["apiKey"],
                    apiSecret=CREDS[market]["apiSecret"],
                    handler=streamer,
                ).run()
            ]

    # add http streamer task - 1. ForexApi
    coroutines += [ForexApi(redis_client=redis_client, redis_topic=topic, redis_xadd_maxlen=redis_maxlen).run()]

    # add balance updater
    for market, _ in websocket_conf.items():
        coroutines += [
            BALANCE_UPDATER[market](
                apiKey=CREDS[market]["apiKey"],
                apiSecret=CREDS[market]["apiSecret"],
                symbols=args["symbol"],
                interval=0.6,
                redis_client=redis_client,
                redis_topic="ubud",
            ).run()
        ]

    # run
    await asyncio.gather(*coroutines)


################################################################
# START TASKS
################################################################
logger.info("[UBUD] Start Websocket Stream")
asyncio.run(tasks())
