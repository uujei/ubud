import json
import logging
import traceback
import time
from typing import Callable
import asyncio

import redis.asyncio as redis

from ..const import (
    CURRENCY,
    MARKET,
    MQ_SUBTOPICS,
    ORDERBOOK,
    ORDERTYPE,
    CHANNEL,
    SYMBOL,
    TICKER,
    TRADE,
    TS_MARKET,
    TS_MQ_RECV,
    TS_MQ_SEND,
    TS_WS_RECV,
    TS_WS_SEND,
    ts_to_strdt,
)

logger = logging.getLogger(__name__)


################################################################
# MQTT Default Callbacks
################################################################
class Streamer:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_xadd_maxlen: int = 10,
        redis_xadd_approximate: bool = False,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_xadd_maxlen = redis_xadd_maxlen
        self.redis_xadd_approximate = redis_xadd_approximate

        self._redis_stream_name = f"{redis_topic}-stream"
        self._redis_stream_keys_key = f"{self._redis_stream_name}/keys"
        self._redis_stream_keys = set()

    async def __call__(self, messages):
        try:
            await asyncio.gather(*[self.xadd(msg) for msg in messages])
        except Exception as ex:
            logger.warn(ex)
            traceback.print_exc()

    async def xadd(self, msg):
        try:
            msg = await self.parser(msg)
            logger.info(f"[STREAMER] XADD {msg['name']} {msg['fields']} MAXLEN {self.redis_xadd_maxlen}")
            await self.redis_client.xadd(**msg, maxlen=self.redis_xadd_maxlen, approximate=self.redis_xadd_approximate)
        except Exception as ex:
            logger.warning("[STREAMER] XADD Failed - {ex}")

    async def parser(self, msg: dict):
        _subtopic = "/".join([msg.pop(_TOPIC) for _TOPIC in MQ_SUBTOPICS])
        name = f"{self.redis_topic}-stream/" + _subtopic
        field_key = f"{self.redis_topic}/" + _subtopic
        field_value = json.dumps({**{k: v for k, v in msg.items()}, TS_MQ_SEND: time.time()})

        # register stream key
        if name not in self._redis_stream_keys:
            await self.redis_client.sadd(self._redis_stream_keys_key, name)
            self._redis_stream_keys = await self.redis_client.smembers(self._redis_stream_keys_key)

        return {
            "name": name,
            "fields": {"name": field_key, "value": field_value},
        }
