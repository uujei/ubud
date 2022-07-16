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
from .connector import InfluxDBConnector
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

# HELPERS
def _load_configs(conf):
    with open(conf, "r") as f:
        conf = yaml.load(f, Loader=yaml.Loader)
    return conf


def _load_credential(credential: dict):
    ret = dict()
    for market, conf in credential.items():
        ret.update({market: dict()})
        for k, v in conf.items():
            ret[market].update({k: os.getenv(v)})
    return ret


async def _clean_namespace(redis_client, topic):
    # clean exist keys
    _keys = await redis_client.keys(f"{topic}/*")
    _stream_keys = await redis_client.keys(f"{topic}-stream/*")
    await asyncio.gather(*[redis_client.delete(k) for k in [*_keys, *_stream_keys]])


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
def start_websocket(conf, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    conf = _load_configs(conf)
    dotenv.load_dotenv(conf["env_file"])
    topic = conf["topic"]
    credential = _load_credential(conf["credential"])
    redis_conf = conf["redis"]
    redis_opts = conf["redis_opts"]
    symbols = conf["symbols"]
    websocket_conf = conf["websocket"]

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
            redis_xadd_maxlen=redis_opts["xadd_maxlen"],
        )
        for market, args in websocket_conf.items():
            for channel in args["channel"]:
                coroutines += [
                    WEBSOCKET[market](
                        channel=channel,
                        symbols=symbols,
                        currencies=args["currency"],
                        apiKey=credential[market]["apiKey"],
                        apiSecret=credential[market]["apiSecret"],
                        handler=streamer,
                    ).run()
                ]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            print(f"start {start}, end {end}, ({end - start}s)")

    # START TASKS
    logger.info("[UBUD] Start Websocket Stream")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# STREAM BALANCE UPDATER
################################################################
@ubud.command()
@click.option("-c", "--conf", default="conf.yml", type=click.Path(exists=True))
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_balance_updater(conf, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    conf = _load_configs(conf)
    dotenv.load_dotenv(conf["env_file"])
    topic = conf["topic"]
    credential = _load_credential(conf["credential"])
    redis_conf = conf["redis"]
    symbols = conf["symbols"]
    balance_conf = conf["balance"]

    # TASKS #
    async def tasks():
        # clean exist keys
        redis_client = redis.Redis(**redis_conf)

        # set redis client
        coroutines = []

        # add balance updater
        for market, args in balance_conf.items():
            args = {} if args is None else args
            coroutines += [
                BALANCE_UPDATER[market](
                    apiKey=credential[market]["apiKey"],
                    apiSecret=credential[market]["apiSecret"],
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

    # START TASKS
    logger.info("[UBUD] Start Banace Updater")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# STREAM FOREX UPDATER
################################################################
@ubud.command()
@click.option("-c", "--conf", default="conf.yml", type=click.Path(exists=True))
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_forex_updater(conf, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    conf = _load_configs(conf)
    dotenv.load_dotenv(conf["env_file"])
    topic = conf["topic"]
    redis_conf = conf["redis"]
    redis_opts = conf["redis_opts"]
    forex_conf = conf["forex"]

    # TASKS #
    async def tasks():
        # clean exist keys
        redis_client = redis.Redis(**redis_conf)

        # set redis client
        coroutines = []

        # add http streamer task - 1. ForexApi
        coroutines += [
            ForexUpdater(
                **forex_conf,
                redis_client=redis_client,
                redis_topic=topic,
                redis_xadd_maxlen=redis_opts["xadd_maxlen"],
            ).run()
        ]

        # run
        start = time.time()
        try:
            await asyncio.gather(*coroutines)
        except Exception as ex:
            end = time.time()
            print(f"start {start}, end {end}, ({end - start}s)")

    # START TASKS
    logger.info("[UBUD] Start Forex Updater")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# START INFLUXDB SINK
################################################################
@ubud.command()
@click.option("-t", "--topic", default="ubud")
@click.option("-s", "--secret", default="theone")
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_influxdb_sink(topic, secret, log_level):
    from clutter.aws import get_secrets

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    conf = get_secrets(secret)
    influxdb_conf = {
        "influxdb_url": conf["iu"],
        "influxdb_token": conf["it"],
        "influxdb_org": conf["io"],
    }

    redis_client = redis.Redis(decode_responses=True)
    connector = InfluxDBConnector(redis_client=redis_client, redis_topic=topic, **influxdb_conf)

    # Sart Tasks
    logger.info("[UBUD] Start InfluxDB Sink")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(connector.run())


################################################################
# START COLLECTOR
################################################################
@ubud.command()
@click.option("-c", "--conf", default="conf.yml", type=click.Path(exists=True))
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_collector(conf, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # load conf
    conf = _load_configs(conf)
    topic = conf["topic"]
    redis_conf = conf["redis"]
    redis_opts = conf["redis_opts"]

    async def tasks():
        redis_client = redis.Redis(**redis_conf)
        await _clean_namespace(redis_client=redis_client, topic=topic)

        collector = Collector(
            redis_client=redis_client,
            redis_topic=topic,
            redis_expire_sec=redis_opts["expire_sec"],
            redis_xread_count=100,
        )
        await collector.run()

    # Sart Tasks
    logger.info("[UBUD] Start Collector")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# STREAM ALL
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

    # load conf
    conf = _load_configs(conf)
    dotenv.load_dotenv(conf["env_file"])
    topic = conf["topic"]
    credential = _load_credential(conf["credential"])
    redis_conf = conf["redis"]
    redis_opts = conf["redis_opts"]
    symbols = conf["symbols"]
    websocket_conf = conf["websocket"]
    balance_conf = conf["balance"]
    forex_conf = conf["forex"]

    # TASKS #
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
                        apiKey=credential[market]["apiKey"],
                        apiSecret=credential[market]["apiSecret"],
                        handler=streamer,
                    ).run()
                ]

        # add balance updater
        for market, args in balance_conf.items():
            args = {} if args is None else args
            coroutines += [
                BALANCE_UPDATER[market](
                    apiKey=credential[market]["apiKey"],
                    apiSecret=credential[market]["apiSecret"],
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

    # START TASKS
    logger.info("[UBUD] Start Websocket Stream")
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())
