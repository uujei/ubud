import logging
import asyncio
import redis.asyncio as redis

from ..utils.app import parse_redis_addr, split_delim
from ..api.updater import BithumbBalanceUpdater, ForexUpdater, FtxBalanceUpdater, UpbitBalanceUpdater
from ..redis import RedisStreamHandler

logger = logging.getLogger(__name__)

BALANCE_UPDATER = {
    "upbit": UpbitBalanceUpdater,
    "bithumb": BithumbBalanceUpdater,
    "ftx": FtxBalanceUpdater,
}

################################################################
# STREAM BALANCE API (http api -> redis stream)
################################################################
async def stream_balance_api(
    market: str,
    symbols: str,
    interval: float = 0.6,
    redis_addr="localhost:6379",
    redis_topic: str = "ubud",
    redis_xadd_maxlen=100,
    secret: dict = None,
):
    """
    Examples
    --------
    secret = {
        "upbit: {"apiKey": <upbit api key>, "apiSecret": <upbit api secret>},
        "bithumb: {"apiKey": <bithumb api key>, "apiSecret": <bithumb api secret>},
        "ftx": {"apiKey": <ftx api key>, "apiSecret": <ftx api secret>},
    }
    """

    # correct input
    symbols = split_delim(symbols)

    # redis_conf
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    # handler
    handler = RedisStreamHandler(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_xadd_maxlen=redis_xadd_maxlen,
    )

    # default_conf
    conf = {
        "apiKey": secret[market]["apiKey"],
        "apiSecret": secret[market]["apiSecret"],
        "symbols": symbols,
        "interval": interval,
        "handler": handler,
    }

    # coroutine
    updater = BALANCE_UPDATER[market](**conf)
    await updater.run()


################################################################
# [TODO]
# STREAM POSITION API
################################################################
async def stream_position_api(
    market: str,
    symbols: str,
    interval: float = 0.6,
    redis_addr="localhost:6379",
    redis_topic: str = "ubud",
    redis_xadd_maxlen=100,
    secret: dict = None,
):
    """
    Examples
    --------
    secret = {
        "upbit: {"apiKey": <upbit api key>, "apiSecret": <upbit api secret>},
        "bithumb: {"apiKey": <bithumb api key>, "apiSecret": <bithumb api secret>},
        "ftx": {"apiKey": <ftx api key>, "apiSecret": <ftx api secret>},
    }
    """

    # correct input
    symbols = split_delim(symbols)

    # redis_conf
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    # handler
    handler = RedisStreamHandler(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_xadd_maxlen=redis_xadd_maxlen,
    )

    # default_conf
    conf = {
        "apiKey": secret[market]["apiKey"],
        "apiSecret": secret[market]["apiSecret"],
        "symbols": symbols,
        "interval": interval,
        "handler": handler,
    }

    # coroutine
    updater = BALANCE_UPDATER[market](**conf)
    await updater.run()


################################################################
# STREAM FOREX API (http api -> redis stream)
################################################################
async def stream_forex_api(
    codes: str = "FRX.KRWUSD",
    interval: float = 5.0,
    redis_addr: str = "localhost:6379",
    redis_topic: str = "ubud",
    redis_xadd_maxlen: int = 100,
):

    # redis_conf
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    # handler
    handler = RedisStreamHandler(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_xadd_maxlen=redis_xadd_maxlen,
    )

    # conf
    conf = {
        "codes": codes,
        "interval": interval,
        "handler": handler,
    }

    # coroutine
    updater = ForexUpdater(**conf)
    await updater.run()


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import sys
    from clutter.aws import get_secrets

    logging.basicConfig(level=logging.DEBUG)

    _secret = get_secrets("theone")
    secret = {
        "upbit": {"apiKey": _secret["ubk"], "apiSecret": _secret["ubs"]},
        "bithumb": {"apiKey": _secret["btk"], "apiSecret": _secret["bts"]},
        "ftx": {"apiKey": _secret["ftk"], "apiSecret": _secret["fts"]},
    }

    # DEBUG EXAMPLE
    # python -m src.ubud.upbit trade,orderbook BTC,WAVES
    if len(sys.argv) > 1:
        market = sys.argv[1].split(",")
    else:
        market = ["upbit"]
    if len(sys.argv) > 2:
        symbols = sys.argv[2]
    else:
        symbols = "BTC,WAVES"

    async def tasks():
        coroutines = []

        # forex updater
        coroutines += [stream_forex_api()]

        # balance updater
        for m in market:
            conf = {
                "market": m,
                "symbols": symbols,
                "secret": secret,
            }
            logger.info(f"[API_SOURCE] Start with Conf: {conf}")
            coroutines += [stream_balance_api(**conf)]

        try:
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            logger.error(["ASYNCIO CANCELLED!! Wait 0.5s Before Exit"])
            await asyncio.sleep(0.5)
            raise

    asyncio.run(tasks())
