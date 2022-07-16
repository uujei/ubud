import asyncio
import json
import logging
import time
import traceback
from typing import Callable

import redis.asyncio as redis

logger = logging.getLogger(__name__)


################################################################
# RedisSetHandler
################################################################
class RedisSetHandler:
    """
    messages = [{"name": <name: str>, "value": <value: dict>}, ...]
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_expire_sec: int = 600,
    ):
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec

        # db management
        self._redis_keys = set()
        self._redis_keys_key = f"{self.redis_topic}/keys"

    async def __call__(self, messages):
        try:
            await asyncio.gather(
                self.update_keys(messages),
                *[
                    self.redis_client.set(
                        name="/".join([self.redis_topic, msg["name"]]),
                        value=json.dumps(msg["value"]),
                        ex=self.redis_expire_sec,
                    )
                    for msg in messages
                ],
            )
            logger.info(
                f"[COLLECTOR] SET {len(messages)} Messages, EXPIRE {self.redis_expire_sec} - Sample: {messages[0]}"
            )
        except Exception as ex:
            logger.warning(f"[COLLECTOR] Redis SET Failed - {ex}")

    async def update_keys(self, messages):
        # update key only for last data ~ because stream name and key are paired
        for msg in messages:
            key = msg["name"]
            if key not in self._redis_keys:
                logger.info(f"[COLLECTOR] New Key '{key}' Found, SADD {self._redis_keys_key}, {key}")
                await self.redis_client.sadd(self._redis_keys_key, key)
                self._redis_keys = await self.redis_client.smembers(self._redis_keys_key)


################################################################
# RedisStreamHandler
################################################################
class RedisStreamHandler:
    """
    messages = [{"name": <name: str>, "value": <value: dict>}, ...]
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
        self._redis_stream_name = f"{redis_topic}-stream"
        self._redis_stream_keys_key = f"{self._redis_stream_name}/keys"
        self._redis_stream_keys = set()

    async def __call__(self, messages):
        try:
            await asyncio.gather(*[self.xadd(msg) for msg in messages])
        except Exception as ex:
            logger.warn(f"[STREAMER] {ex}")
            traceback.print_exc()

    async def xadd(self, msg):
        try:
            stream_name = "/".join([self._redis_stream_name, msg["name"]])
            name = "/".join([self.redis_topic, msg["name"]])
            value = json.dumps(msg["value"])

            logger.info(f"[STREAMER] XADD {stream_name} {msg} MAXLEN {self.redis_xadd_maxlen}")
            await self.redis_client.xadd(
                name=stream_name,
                fields={"name": name, "value": value},
                maxlen=self.redis_xadd_maxlen,
                approximate=self.redis_xadd_approximate,
            )

            # register stream key
            if stream_name not in self._redis_stream_keys:
                await self.redis_client.sadd(self._redis_stream_keys_key, stream_name)
                self._redis_stream_keys = await self.redis_client.smembers(self._redis_stream_keys_key)

        except Exception as ex:
            logger.warning(f"[STREAMER] XADD Failed - {ex}, msg: {msg}")
            traceback.print_exc()
