import logging

import click
from click_loglevel import LogLevel
from clutter.aws import get_secrets

import redis.asyncio as redis

from .redis.collector import Collector
from .redis.streamer import Streamer
from .api.forex import ForexApi
from .websocket.bithumb import BithumbWebsocket
from .websocket.upbit import UpbitWebsocket

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"

WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
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
@click.option("-m", "--markets", default="upbit,bithumb")
@click.option("-q", "--quotes", default="trade,orderbook")
@click.option("-s", "--symbols", default=["BTC", "WAVES"], multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-t", "--topic", default="ubud")
@click.option("--host", default="localhost")
@click.option("--port", default=6379)
@click.option("--expire-sec", default=600)
@click.option("--buffer-len", default=100)
@click.option("--client-id")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def start_stream(
    markets, quotes, symbols, currency, topic, host, port, expire_sec, buffer_len, client_id, log_level, trace
):
    import asyncio

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    ################################################################
    # ARGS
    ################################################################
    # correct markets, quotes, symbols
    markets = [m.lower() for m in markets.split(",")]
    quotes = [q.lower() for q in quotes.split(",")]
    symbols = [s.upper() for ss in symbols for s in ss.split(",")]
    recipes = [{"market": m, "args": {"quote": q, "symbols": symbols}} for m in markets for q in quotes]

    # logging
    logger.info(f"[UBUD] markets: {markets}")
    logger.info(f"[UBUD] quotes: {quotes}")
    logger.info(f"[UBUD] symbols: {symbols}")

    ################################################################
    # TASK SETTINGS
    ################################################################
    async def tasks():
        # set redis client
        redis_client = redis.Redis(host=host, port=port, decode_responses=True)
        coroutines = []

        # add collector task (redis stream to redis db)
        collector = Collector(redis_client=redis_client, redis_topic=topic, redis_expire_sec=expire_sec)
        coroutines += [collector.run()]

        # add websocket streamer tasks
        streamer = Streamer(redis_client=redis_client, redis_topic=topic, redis_xadd_maxlen=buffer_len)
        coroutines += [
            WEBSOCKET[opt["market"]](**opt["args"], currency=currency, handler=streamer).run() for opt in recipes
        ]

        # add http streamer task - 1. ForexApi
        coroutines += [
            ForexApi(redis_client=redis_client, redis_topic=topic, redis_xadd_maxlen=buffer_len).run(interval=30)
        ]

        # run
        await asyncio.gather(*coroutines)

    ################################################################
    # START TASKS
    ################################################################
    logger.info("[UBUD] Start Websocket Stream")
    asyncio.run(tasks())
