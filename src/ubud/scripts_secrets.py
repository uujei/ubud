import asyncio
import logging
import time

import click
import uvloop
from click_loglevel import LogLevel
from clutter.aws import get_secrets

import redis.asyncio as redis

from .api.unified import BithumbBalanceUpdater, ForexUpdater, FtxBalanceUpdater, UpbitBalanceUpdater
from .connector import InfluxDBConnector
from .redis import Collector, Streamer
from .websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"

DEFAULT_SYMBOLS = "BTC,WAVES,KNC,AXS"

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

API_KEY = {
    "upbit": {"apiKey": "ubk", "apiSecret": "ubs"},
    "bithumb": {"apiKey": "btk", "apiSecret": "bts"},
    "ftx": {"apiKey": "ftk", "apiSecret": "fts"},
}

# HELPERS
async def _clean_namespace(redis_client, topic):
    # clean exist keys
    _keys = await redis_client.keys(f"{topic}/*")
    _stream_keys = await redis_client.keys(f"{topic}-stream/*")
    await asyncio.gather(*[redis_client.delete(k) for k in [*_keys, *_stream_keys]])


################################################################
# GROUP UBUD
################################################################
@click.group()
def ubud_secrets():
    pass


################################################################
# STREAM WEBSOCKET
################################################################
@ubud_secrets.command()
@click.option("-m", "--market", required=True, type=str)
@click.option("-c", "--channels", default="trade,orderbook", type=str)
@click.option("-s", "--symbols", default=DEFAULT_SYMBOLS, type=str)
@click.option("--currencies", default="KRW", type=str)
@click.option("--topic", default="ubud", type=str)
@click.option("--secrets", default="theone", type=str)
@click.option("--redis-addr", default="localhost:6379", type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_websocket(market, channels, symbols, currencies, topic, secrets, redis_addr, redis_xadd_maxlen, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # correct input
    channels = [x.strip() for x in channels.split(",")]
    symbols = [x.strip() for x in symbols.split(",")]
    currencies = [x.strip() for x in currencies.split(",")]

    secrets = get_secrets(secrets)
    redis_conf = {
        "host": redis_addr.split(":")[0],
        "port": redis_addr.split(":")[-1],
        "decode_responses": True,
    }

    # TASK #
    async def tasks():
        # clean exist keys
        redis_client = redis.Redis(**redis_conf)

        # set redis client
        coroutines = []

        # add websocket streamer tasks
        streamer = Streamer(
            redis_client=redis_client,
            redis_topic=topic,
            redis_xadd_maxlen=redis_xadd_maxlen,
        )

        for channel in channels:
            coroutines += [
                WEBSOCKET[market](
                    channel=channel,
                    symbols=symbols,
                    currencies=currencies,
                    apiKey=secrets[API_KEY[market]["apiKey"]],
                    apiSecret=secrets[API_KEY[market]["apiSecret"]],
                    handler=streamer,
                ).run()
            ]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            logger.error(f"[WEBSOCKET] EXIT {market}/{channel} - start {start}, end {end}, ({end - start}s)")

    # START TASKS
    logger.info(f"[UBUD] Start Websocket Stream - {market}, {channels}, {symbols}, {currencies}")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# STREAM BALANCE UPDATER
################################################################
@ubud_secrets.command()
@click.option("-m", "--market", required=True, type=str)
@click.option("-s", "--symbols", default=DEFAULT_SYMBOLS, type=str)
@click.option("-i", "--interval", default=0.6, type=float)
@click.option("--topic", default="ubud", type=str)
@click.option("--redis-addr", default="localhost:6379", type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--secrets", default="theone", type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_balance_updater(market, symbols, topic, interval, redis_addr, redis_xadd_maxlen, secrets, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # correct input
    symbols = [x.strip() for x in symbols.split(",")]

    # load conf
    secrets = get_secrets(secrets)
    redis_conf = {
        "host": redis_addr.split(":")[0],
        "port": redis_addr.split(":")[-1],
        "decode_responses": True,
    }

    # TASKS #
    async def tasks():
        # set redis client
        redis_client = redis.Redis(**redis_conf)
        coroutines = []

        conf = {
            "apiKey": secrets[API_KEY[market]["apiKey"]],
            "apiSecret": secrets[API_KEY[market]["apiSecret"]],
            "symbols": symbols,
            "interval": interval,
            "redis_xadd_maxlen": redis_xadd_maxlen,
            "redis_client": redis_client,
            "redis_topic": topic,
        }

        logger.info(f"[UBUD] Banace Updater Conf: {conf}")

        # add balance updater
        coroutines += [BALANCE_UPDATER[market](**conf).run()]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            logger.error(f"[WEBSOCKET] EXIT {market} - start {start}, end {end}, ({end - start}s)")

    # START TASKS
    logger.info(f"[UBUD] Start Balance Updater - {market}, {symbols}")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# STREAM FOREX UPDATER
################################################################
@ubud_secrets.command()
@click.option("-i", "--interval", default=5, type=float)
@click.option("--codes", default=["FRX.KRWUSD"], type=list)
@click.option("--topic", default="ubud", type=str)
@click.option("--redis-addr", default="localhost:6379", type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--secrets", default="theone", type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_forex_updater(interval, codes, topic, redis_addr, redis_xadd_maxlen, secrets, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    secrets = get_secrets(secrets)
    redis_conf = {
        "host": redis_addr.split(":")[0],
        "port": redis_addr.split(":")[-1],
        "decode_responses": True,
    }

    # TASKS #
    async def tasks():
        # clean exist keys
        redis_client = redis.Redis(**redis_conf)
        _keys = await redis_client.keys(f"{topic}/*")
        _stream_keys = await redis_client.keys(f"{topic}-stream/*")
        _ = await asyncio.gather(*[redis_client.delete(k) for k in [*_keys, *_stream_keys]])

        # set redis client
        coroutines = []

        # add http streamer task - 1. ForexApi
        for code in codes:
            coroutines += [
                ForexUpdater(
                    codes=code,
                    interval=interval,
                    redis_client=redis_client,
                    redis_topic=topic,
                    redis_xadd_maxlen=redis_xadd_maxlen,
                ).run()
            ]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            logger.error(f"[WEBSOCKET] EXIT - start {start}, end {end}, ({end - start}s)")

    # START TASKS
    logger.info("[UBUD] Start Forex Updater")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# START COLLECTOR
################################################################
@ubud_secrets.command()
@click.option("--topic", default="ubud", type=str)
@click.option("--secrets", default="theone", type=str)
@click.option("--redis-addr", default="localhost:6379", type=str)
@click.option("--redis-expire-sec", default=900, type=int)
@click.option("--redis-xread-count", default=20, type=int)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_collector(topic, secrets, redis_addr, redis_expire_sec, redis_xread_count, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    secrets = get_secrets(secrets)
    redis_conf = {
        "host": redis_addr.split(":")[0],
        "port": redis_addr.split(":")[-1],
        "decode_responses": True,
    }

    async def tasks():
        redis_client = redis.Redis(**redis_conf)
        await _clean_namespace(redis_client=redis_client, topic=topic)

        collector = Collector(
            redis_client=redis_client,
            redis_topic=topic,
            redis_expire_sec=redis_expire_sec,
            redis_xread_count=redis_xread_count,
        )
        await collector.run()

    # Sart Tasks
    logger.info("[UBUD] Start Collector")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# START INFLUXDB SINK
################################################################
@ubud_secrets.command()
@click.option("--topic", default="ubud", type=str)
@click.option("--redis", default="localhost:6379", type=str)
@click.option("--secrets", default="theone", type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_influxdb_sink(topic, redis, secrets, log_level):
    from clutter.aws import get_secrets

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    secrets = get_secrets(secrets)
    redis_conf = {
        "host": redis.split(":")[0],
        "port": redis.split(":")[-1],
        "decode_responses": True,
    }
    influxdb_conf = {
        "influxdb_url": secrets["iu"],
        "influxdb_token": secrets["it"],
        "influxdb_org": secrets["io"],
    }

    redis_client = redis.Redis(**redis_conf)
    connector = InfluxDBConnector(redis_client=redis_client, redis_topic=topic, **influxdb_conf)

    # Sart Tasks
    logger.info("[UBUD] Start InfluxDB Sink")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(connector.run())
