import asyncio
import logging
import os

import click
import uvloop
from click_loglevel import LogLevel
from clutter.aws import get_secrets
from dotenv import load_dotenv

import redis

from .stream import stream_balance_api, stream_forex_api, stream_websocket, connect_influxdb
from .utils.app import repr_conf

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(name)s:%(levelname)s:%(message)s"

DEFAULT_MARKET = "upbit,bithumb,ftx"
DEFAULT_CHANNELS = "trade,orderbook"
DEFAULT_SYMBOLS = "BTC,WAVES,KNC,AXS"
DEFAULT_CURRENCIES = "KRW,USD,-PERP"
AVAILABLE_CURRENCIES = {
    "upbit": "KRW",
    "bithumb": "KRW",
    "ftx": "USD,-PERP",
}

DEFAULT_REDIS_ADDR = "localhost:6379"
DEFAULT_REDIS_TOPIC = "ubud"

# HELPERS
def _clean_namespace(redis_client, topic):
    # clean exist keys
    _keys = redis_client.keys(f"{topic}/*")
    _stream_keys = redis_client.keys(f"{topic}-stream/*")
    _ = [redis_client.delete(k) for k in [*_keys, *_stream_keys]]


def load_secret(secret_key):
    if secret_key is None:
        load_dotenv()
        return {
            "upbit": {"apiKey": os.getenv("UPBIT_API_KEY"), "apiSecret": os.getenv("UPBIT_API_SECRET")},
            "bithumb": {"apiKey": os.getenv("BITHUMB_API_KEY"), "apiSecret": os.getenv("BITHUMB_API_SECRET")},
            "ftx": {"apiKey": os.getenv("FTX_API_KEY"), "apiSecret": os.getenv("FTX_API_SECRET")},
            "influxdb": {
                "influxdb_url": os.getenv("INFLUXDB_URL"),
                "influxdb_org": os.getenv("INFLUXDB_ORG"),
                "influxdb_token": os.getenv("INFLUXDB_TOKEN"),
            },
        }
    _secret = get_secrets(secret_key)
    return {
        "upbit": {"apiKey": _secret["ubk"], "apiSecret": _secret["ubs"]},
        "bithumb": {"apiKey": _secret["btk"], "apiSecret": _secret["bts"]},
        "ftx": {"apiKey": _secret["ftk"], "apiSecret": _secret["fts"]},
        "influxdb": {
            "influxdb_url": _secret["iu"],
            "influxdb_org": _secret["io"],
            "influxdb_token": _secret["it"],
        },
    }


def filter_available_currencies(market, currencies):
    _currencies = []
    for c in currencies.split(","):
        if c.strip() in AVAILABLE_CURRENCIES[market]:
            _currencies += [c]
    return ",".join(_currencies)


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
@click.option("-m", "--market", default=DEFAULT_MARKET, type=str)
@click.option("-c", "--channels", default=DEFAULT_CHANNELS, type=str)
@click.option("-s", "--symbols", default=DEFAULT_SYMBOLS, type=str)
@click.option("--currencies", default=DEFAULT_CURRENCIES, type=str)
@click.option("--redis-addr", default=DEFAULT_REDIS_ADDR, type=str)
@click.option("--redis-topic", default=DEFAULT_REDIS_TOPIC, type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--secret-key", default=None, type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_websocket_stream(
    market, channels, symbols, currencies, redis_topic, secret_key, redis_addr, redis_xadd_maxlen, log_level
):
    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # secret
    secret = load_secret(secret_key)

    # correct input
    market = [x.strip() for x in market.split(",")]

    async def tasks():
        coroutines = []
        for m in market:
            _currencies = filter_available_currencies(m, currencies)
            conf = {
                "market": m,
                "channels": channels,
                "symbols": symbols,
                "currencies": _currencies,
                "redis_addr": redis_addr,
                "redis_topic": redis_topic,
                "redis_xadd_maxlen": redis_xadd_maxlen,
                "secret": secret,
            }
            logger.info(f"[UBUD] Start Websocket Stream - {repr_conf(conf)}")
            coroutines += [stream_websocket(**conf)]
        try:
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            logger.error(["ASYNCIO CANCELLED!! Wait 0.5s Before Exit"])
            await asyncio.sleep(0.5)
            raise

    # START TASKS
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# START BALANCE STREAM
################################################################
@ubud.command()
@click.option("-m", "--market", default=DEFAULT_MARKET, type=str)
@click.option("-s", "--symbols", default=DEFAULT_SYMBOLS, type=str)
@click.option("-i", "--interval", default=0.6, type=float)
@click.option("--redis-addr", default=DEFAULT_REDIS_ADDR, type=str)
@click.option("--redis-topic", default=DEFAULT_REDIS_TOPIC, type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--secret_key", default=None, type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_balance_stream(market, symbols, interval, redis_topic, redis_addr, redis_xadd_maxlen, secret_key, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # secret
    secret = load_secret(secret_key)

    # correct input
    market = [x.strip() for x in market.split(",")]

    # TASKS #
    async def tasks():
        coroutines = []
        for m in market:
            conf = {
                "market": m,
                "symbols": symbols,
                "interval": interval,
                "redis_addr": redis_addr,
                "redis_topic": redis_topic,
                "redis_xadd_maxlen": redis_xadd_maxlen,
                "secret": secret,
            }
            logger.info(f"[UBUD] Start Balance API Stream - {repr_conf(conf)}")
            coroutines += [stream_balance_api(**conf)]
        try:
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            logger.error(["ASYNCIO CANCELLED!! Wait 0.5s Before Exit"])
            await asyncio.sleep(0.5)
            raise

    # START TASKS
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(tasks())


################################################################
# START FOREX STREAM
################################################################
@ubud.command()
@click.option("-i", "--interval", default=5, type=float)
@click.option("--codes", default="FRX.KRWUSD", type=str)
@click.option("--redis-addr", default=DEFAULT_REDIS_ADDR, type=str)
@click.option("--redis-topic", default=DEFAULT_REDIS_TOPIC, type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_forex_stream(interval, codes, redis_addr, redis_topic, redis_xadd_maxlen, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # TASKS #
    conf = {
        "codes": codes,
        "interval": interval,
        "redis_addr": redis_addr,
        "redis_topic": redis_topic,
        "redis_xadd_maxlen": redis_xadd_maxlen,
    }
    logger.info(f"[UBUD] Start Forex API Stream - {repr_conf(conf)}")

    # START TASKS
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(stream_forex_api(**conf))


################################################################
# START ALL STREAMS
################################################################
@ubud.command()
@click.option("-m", "--market", default=DEFAULT_MARKET, type=str)
@click.option("-c", "--channels", default=DEFAULT_CHANNELS, type=str)
@click.option("-s", "--symbols", default=DEFAULT_SYMBOLS, type=str)
@click.option("--currencies", default=DEFAULT_CURRENCIES, type=str)
@click.option("--codes", default="FRX.KRWUSD", type=str)
@click.option("--interval", default=0.5, type=float)
@click.option("--redis-addr", default=DEFAULT_REDIS_ADDR, type=str)
@click.option("--redis-topic", default=DEFAULT_REDIS_TOPIC, type=str)
@click.option("--redis-xadd-maxlen", default=100, type=int)
@click.option("--secret-key", default=None, type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_stream(
    market,
    channels,
    symbols,
    currencies,
    codes,
    interval,
    redis_topic,
    secret_key,
    redis_addr,
    redis_xadd_maxlen,
    log_level,
):
    import ray

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # secret
    secret = load_secret(secret_key)

    # correct input
    market = [x.strip() for x in market.split(",")]

    @ray.remote
    class WebsocketActor:
        def __init__(self, log_level):
            logging.basicConfig(level=log_level)

        async def run(self):
            coroutines = []
            for m in market:
                _currencies = filter_available_currencies(m, currencies)
                conf = {
                    "market": m,
                    "channels": channels,
                    "symbols": symbols,
                    "currencies": _currencies,
                    "redis_addr": redis_addr,
                    "redis_topic": redis_topic,
                    "redis_xadd_maxlen": redis_xadd_maxlen,
                    "secret": secret,
                }
                logger.info(f"[UBUD] Start Websocket Stream - {repr_conf(conf)}")
                coroutines += [stream_websocket(**conf)]
            await asyncio.gather(*coroutines)

    # TASKS #
    @ray.remote
    class ApiActor:
        def __init__(self, log_level):
            logging.basicConfig(level=log_level)

        async def run(self):
            coroutines = []

            # balance_api stream
            for m in market:
                conf = {
                    "market": m,
                    "symbols": symbols,
                    "interval": interval,
                    "redis_addr": redis_addr,
                    "redis_topic": redis_topic,
                    "redis_xadd_maxlen": redis_xadd_maxlen,
                    "secret": secret,
                }
                logger.info(f"[UBUD] Start Balance API Stream - {repr_conf(conf)}")
                coroutines += [stream_balance_api(**conf)]

            # forex_api stream
            conf = {
                "codes": codes,
                "interval": interval,
                "redis_addr": redis_addr,
                "redis_topic": redis_topic,
                "redis_xadd_maxlen": redis_xadd_maxlen,
            }
            logger.info(f"[UBUD] Start Forex API Stream - {repr_conf(conf)}")
            coroutines += [stream_forex_api(**conf)]

            await asyncio.gather(*coroutines)

    # clean redis before start
    with redis.Redis(decode_responses=True) as r:
        _clean_namespace(redis_client=r, topic=redis_topic)

    # Redirecting Ray Log to Host and init ray
    os.environ["RAY_LOG_TO_STDOUT"] = "1"
    ray.init()

    # create instances
    websocket_actor = WebsocketActor.remote(log_level=log_level)
    api_actor = ApiActor.remote(log_level=log_level)

    # START TASKS
    ray.get([websocket_actor.run.remote(), api_actor.run.remote()])


################################################################
# START CONNECT INFLUXDB
################################################################
@ubud.command()
@click.option("--redis-addr", default=DEFAULT_REDIS_ADDR, type=str)
@click.option("--redis-topic", default=DEFAULT_REDIS_TOPIC, type=str)
@click.option("--secret-key", default="theone", type=str)
@click.option("--log-level", default=logging.WARNING, type=LogLevel())
def start_influxdb_sink(redis_addr, redis_topic, secret_key, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # secret
    secret = load_secret(secret_key)

    # influxdb
    influxdb_conf = secret["influxdb"]

    # TASKS #
    conf = {
        **influxdb_conf,
        "redis_addr": redis_addr,
        "redis_topic": redis_topic,
        "redis_categories": ["quotation"],
        "redis_xread_count": 300,
        "redis_smember_interval": 5.0,
    }
    logger.info(f"[UBUD] Start Forex API Stream - {repr_conf(conf)}")

    # START TASKS
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.run(connect_influxdb(**conf))
