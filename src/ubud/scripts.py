import asyncio
import logging
import os

import click
import dotenv
import yaml
from click_loglevel import LogLevel

import redis.asyncio as redis

from .api.forex import ForexApi
from .api.unified import BithumbBalanceUpdater, FtxBalanceUpdater, UpbitBalanceUpdater
from .redis import Collector, Streamer
from .websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

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


################################################################
# GROUP UBUD
################################################################
@click.group()
def ubud():
    pass


################################################################
# STREAM WEBSOCKET
################################################################
@ubud.command()
@click.option("-c", "--conf", default="conf.yml", type=click.Path(exists=True))
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_stream(conf, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    ################################################################
    # Load Conf
    ################################################################
    with open(conf, "r") as f:
        conf = yaml.load(f, Loader=yaml.Loader)

    # topic
    topic = conf["topic"]

    # load dotenv
    dotenv.load_dotenv(conf["env_file"])

    # credential
    credential = conf["credential"]
    CREDS = dict()
    for market, cred in credential.items():
        CREDS.update({market: dict()})
        for k, v in cred.items():
            CREDS[market].update({k: os.getenv(v)})

    # redis
    redis_conf = conf["redis"]
    redis_client_conf = {k: v for k, v in redis_conf.items() if k in ("host", "port", "decode_responses")}
    redis_expire_sec = redis_conf.get("expire_sec")
    redis_maxlen = redis_conf.get("maxlen")

    websocket_conf = conf["websocket"]

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
