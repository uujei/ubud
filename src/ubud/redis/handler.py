import asyncio
import json
import logging
import time
import traceback
from typing import Callable

import redis.asyncio as redis

logger = logging.getLogger(__name__)


################################################################
# RedisStreamHandler2
################################################################
class RedisStreamHandler:
    """
    messages = [{"key": <key: str>, "value": <value: dict>}, ...]
    """

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

        # store
        self._streams_key = f"{self.redis_topic}/keys"
        self._streams = set()

    async def __call__(self, messages):
        try:
            await asyncio.gather(*[self.xadd(msg) for msg in messages])
        except Exception as ex:
            logger.warn(f"[STREAMER] {ex}")
            traceback.print_exc()

    async def xadd(self, msg):
        try:
            key = "/".join([self.redis_topic, msg.key])
            logger.info(f"[STREAMER] XADD {msg} MAXLEN {self.redis_xadd_maxlen}")
            await self.redis_client.xadd(
                name=key,
                fields=msg.value,
                maxlen=self.redis_xadd_maxlen,
                approximate=self.redis_xadd_approximate,
            )

            # register stream key
            if key not in self._streams:
                await self.redis_client.sadd(self._streams_key, key)
                self._streams = await self.redis_client.smembers(self._streams_key)

        except Exception as ex:
            logger.warning(f"[STREAMER] XADD Failed - {ex}, msg: {msg}")
            traceback.print_exc()
