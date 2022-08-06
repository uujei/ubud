import logging
from fnmatch import fnmatch

import parse

from ..const import (
    CHANNEL,
    CURRENCY,
    DATETIME,
    MARKET,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    QUOTATION_KEY_RULE,
    RANK,
    SYMBOL,
    CATEGORY,
)
from ..models import Message
from ..utils.app import key_parser, key_maker
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

        if krw_per_usd := self.forex.get(self.FOREX_PRICE):
            # generate message
            try:
                # parse stream name and generate key
                parsed = key_parser(stream)
                parsed.update({CURRENCY: "KRW.USD"})
                # generate msg
                msg = Message(
                    key=key_maker(**parsed),
                    value={
                        DATETIME: record[DATETIME],
                        PRICE: get_order_unit(float(record[PRICE]) * float(krw_per_usd)),
                        QUANTITY: record[QUANTITY],
                    },
                )
                # logging
                logger.debug("[APP {app}] From {record} -> To {msg}".format(app=self._me, record=record, msg=msg))
            except Exception as ex:
                logger.warning("[APP {app}] Transform Failed {ex}".format(app=self._me, ex=ex))
                return

            # publish message
            try:
                # xadd using handler
                if self.redis_stream_handler is not None:
                    await self.redis_stream_handler(msg)
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
