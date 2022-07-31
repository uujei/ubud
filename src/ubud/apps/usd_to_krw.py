import logging
from fnmatch import fnmatch

import parse

from ..const import DATETIME, QUOTATION_KEY_RULE, MARKET, ORDERTYPE, CURRENCY, RANK
from ..models import Message
from ..utils.business import get_order_unit
from .base import App

logger = logging.getLogger(__name__)

################################################################
# USD to KRW
################################################################
class Usd2KrwApp(App):
    # settings
    FOREX_PRICE = "basePrice"

    # forex store
    forex = dict()

    # logic
    async def on_stream(self, stream=None, offset=None, record=None):
        logger.debug(
            "[APP {app}] stream: {stream}, offset {offset}, record: {record}".format(
                app=self._me, stream=stream, offset=offset, record=record
            )
        )
        if "FRX.KRWUSD" in stream:
            self.forex.update(record)
            return

        usd = float(record["price"])
        if krw_per_usd := self.forex.get(self.FOREX_PRICE):
            try:
                krw = get_order_unit(usd * float(krw_per_usd))
                _stream = stream.split("/", 1)[-1].replace("/USD/", "/KRW.usd/")
                msg = Message(
                    key=_stream,
                    value={
                        DATETIME: record[DATETIME],
                        "price": krw,
                    },
                )

                # logging
                logger.debug(
                    "[APP {app}] From {src_stream}, {price}, {usd} -> To {dst_stream}, {price}, {krw}".format(
                        app=self._me, src_stream=stream, dst_stream=_stream, price="price", usd=usd, krw=krw
                    )
                )
            except Exception as ex:
                logger.warning("[APP {app}] Transform Failed {ex}".format(app=self._me, ex=ex))

            try:
                # xadd using handler
                if self.redis_stream_handler is not None:
                    await self.redis_stream_handler.xadd(msg)
                logger.info("[APP {app}] XADD {msg}".format(app=self._me, msg=msg))
            except Exception as ex:
                logger.warning("[APP {app}] XADD Failed {ex}".format(app=self._me, ex=ex))


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import asyncio
    import redis.asyncio as redis
    from ..redis.handler import RedisStreamHandler

    logging.basicConfig(level=logging.DEBUG)

    REDIS_SOCK = "/var/run/redis/redis-server.sock"

    redis_client = redis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    handler = RedisStreamHandler(redis_client=redis_client)

    app = Usd2KrwApp(
        redis_client=redis_client,
        redis_streams=["*/forex/*", "*/USD/*/[0-1]"],
        redis_stream_handler=handler,
        debug_sec=3,
    )

    asyncio.run(app.run())
