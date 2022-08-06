import asyncio
import json
import logging
import time
import traceback
from typing import Callable, List

import redis.asyncio as redis

from ..models import Message

logger = logging.getLogger(__name__)


################################################################
# RedisStreamHandler2
################################################################
class RedisStreamHandler:
    """
    messages = [Message(key=key, value=value), ...]
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

    async def __call__(self, messages: List[Message]):
        if not isinstance(messages, list):
            messages = [messages]
        try:
            pipe = self.redis_client.pipeline()
            _messages = await asyncio.gather(*[self.parse(msg) for msg in messages])
            for msg in _messages:
                if msg:
                    pipe.xadd(**msg)
            await pipe.execute()
            if len(_messages) > 0:
                logger.info(
                    f"[STREAMER] XADD {len(messages)} Messages w/ MAXLEN {self.redis_xadd_maxlen}, Sample: {msg}"
                )
        except Exception as ex:
            logger.warn(f"[STREAMER] {ex}")
            traceback.print_exc()

    async def parse(self, msg: Message):
        try:
            key = "/".join([self.redis_topic, msg.key])
            # register stream key
            if key not in self._streams:
                await self.redis_client.sadd(self._streams_key, key)
                self._streams = await self.redis_client.smembers(self._streams_key)
            return {
                "name": key,
                "fields": msg.value,
                "maxlen": self.redis_xadd_maxlen,
                "approximate": self.redis_xadd_approximate,
            }
        except Exception as ex:
            logger.warning(f"[STREAMER] XADD Failed - {ex}, msg: {msg}")
            traceback.print_exc()
