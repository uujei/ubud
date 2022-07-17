import os
import asyncio
import logging

import redis.asyncio as redis

from ..utils.app import parse_redis_addr, split_delim
from ..redis import RedisStreamHandler
from ..websocket import BithumbWebsocket, FtxWebsocket, UpbitWebsocket

logger = logging.getLogger(__name__)


WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
    "ftx": FtxWebsocket,
}


# HELPERS
async def _clean_namespace(redis_client, topic):
    # clean exist keys
    _keys = await redis_client.keys(f"{topic}/*")
    _stream_keys = await redis_client.keys(f"{topic}-stream/*")
    await asyncio.gather(*[redis_client.delete(k) for k in [*_keys, *_stream_keys]])


################################################################
# STREAM WEBSOCKET (websocket -> redis stream)
################################################################
async def stream_websocket(
    market: str,
    channels: str,
    symbols: str,
    currencies: str,
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
    channels, symbols, currencies = split_delim(channels, symbols, currencies)

    # redis_client
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    # set redis client
    coroutines = []

    # add websocket streamer tasks
    streamer = RedisStreamHandler(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_xadd_maxlen=redis_xadd_maxlen,
    )

    for channel in channels:
        conf = {
            "channel": channel,
            "symbols": symbols,
            "currencies": currencies,
            "apiKey": secret[market]["apiKey"],
            "apiSecret": secret[market]["apiSecret"],
            "handler": streamer,
        }
        coroutines += [WEBSOCKET[market](**conf).run()]

    await asyncio.gather(*coroutines)


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
        channels = sys.argv[2]
    else:
        channels = "orderbook,trade"
    if len(sys.argv) > 3:
        symbols = sys.argv[3]
    else:
        symbols = "BTC,WAVES"

    async def tasks():
        coroutines = []
        for m in market:
            currencies = "KRW" if m not in ["ftx"] else "USD,-PERP"
            conf = {
                "market": m,
                "channels": channels,
                "symbols": symbols,
                "currencies": currencies,
                "secret": secret,
            }
            logger.info(f"[WEBSOCKET_SOURCE] Start with Conf: {conf}")
            coroutines += [stream_websocket(**conf)]

        try:
            await asyncio.gather(*coroutines)
        except asyncio.CancelledError:
            logger.error(["ASYNCIO CANCELLED!! Wait 0.5s Before Exit"])
            await asyncio.sleep(0.5)
            raise

    asyncio.run(tasks())
