import asyncio
import logging
import os
import time

import click
import dotenv
import uvloop
import yaml
from click_loglevel import LogLevel

import redis.asyncio as redis

from .api.unified import BithumbBalanceUpdater, ForexUpdater, FtxBalanceUpdater, UpbitBalanceUpdater
from .redis import Collector, Streamer
from .websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"

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

    # load dotenv
    dotenv.load_dotenv(conf["env_file"])

    # topic
    topic = conf["topic"]

    # credential
    credential = conf["credential"]
    CREDS = dict()
    for market, cred in credential.items():
        CREDS.update({market: dict()})
        for k, v in cred.items():
            CREDS[market].update({k: os.getenv(v)})

    # redis
    redis_conf = conf["redis"]
    redis_opts = conf["redis_opts"]

    # symbols
    symbols = conf["symbols"]

    # websocket
    websocket_conf = conf["websocket"]

    # balance
    balance_conf = conf["balance"]

    # forex
    forex_conf = conf["forex"]

    ################################################################
    # TASK SETTINGS
    ################################################################
    async def tasks():

        # clean exist keys
        redis_client = redis.Redis(**redis_conf)
        _keys = await redis_client.keys(f"{topic}/*")
        _stream_keys = await redis_client.keys(f"{topic}-stream/*")
        _ = await asyncio.gather(*[redis_client.delete(k) for k in [*_keys, *_stream_keys]])

        # set redis client
        coroutines = []

        # add collector task (redis stream to redis db)
        collector = Collector(redis_client=redis_client, redis_topic=topic, redis_expire_sec=redis_opts["expire_sec"])
        coroutines += [collector.run()]

        # add http streamer task - 1. ForexApi
        coroutines += [
            ForexUpdater(
                **forex_conf,
                redis_client=redis_client,
                redis_topic=topic,
                redis_xadd_maxlen=redis_opts["xadd_maxlen"],
            ).run()
        ]

        # add websocket streamer tasks
        streamer = Streamer(
            redis_client=redis_client,
            redis_topic=topic,
            redis_xadd_maxlen=redis_opts["xadd_maxlen"],
        )
        for market, args in websocket_conf.items():
            for channel in args["channel"]:
                coroutines += [
                    WEBSOCKET[market](
                        channel=channel,
                        symbols=symbols,
                        currencies=args["currency"],
                        apiKey=CREDS[market]["apiKey"],
                        apiSecret=CREDS[market]["apiSecret"],
                        handler=streamer,
                    ).run()
                ]

        # add balance updater
        for market, args in balance_conf.items():
            args = {} if args is None else args
            coroutines += [
                BALANCE_UPDATER[market](
                    apiKey=CREDS[market]["apiKey"],
                    apiSecret=CREDS[market]["apiSecret"],
                    symbols=symbols,
                    **args,
                    redis_client=redis_client,
                    redis_topic=topic,
                ).run()
            ]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            print(f"start {start}, end {end}, ({end - start}s)")

    ################################################################
    # START TASKS
    ################################################################
    logger.info("[UBUD] Start Websocket Stream")

    # uvloop will be used for loop engine
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())
