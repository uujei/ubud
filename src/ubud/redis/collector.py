import asyncio
import json
import logging
import traceback
from time import time
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv5, MQTTv311

import redis.asyncio as redis

from ..const import (
    CURRENCY,
    MARKET,
    MQ_SUBTOPICS,
    ORDERBOOK,
    ORDERTYPE,
    QUOTE,
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
class Collector:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_expire_sec: int = 600,
        redis_xread_offset: str = "0",
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec

        # correct offset
        if redis_xread_offset in ["earliest", "smallest"]:
            redis_xread_offset = "0"
        if redis_xread_offset in ["latest", "largest"]:
            redis_xread_offset = "$"
        self.redis_xread_offset = redis_xread_offset

        # stream management
        self._redis_stream_name = f"{self.redis_topic}-stream"
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_offset = None

        # db management
        self._redis_keys = set()
        self._redis_keys_key = f"{self.redis_topic}/keys"

    async def run(self):
        # read keys
        stream_names = await self.redis_client.smembers(self._redis_stream_names_key)

        # set offset
        if self._redis_stream_offset is None:
            self._redis_stream_offset = {stream_name: self.redis_xread_offset for stream_name in stream_names}

        # collect
        try:
            while True:
                await asyncio.gather(*[self.collect(key) for key in stream_names])
        except Exception as ex:
            logger.error(ex)
        finally:
            # wait and close
            await asyncio.sleep(1)
            await self.redis_client.close()

    async def collect(self, stream_name):
        # register new stream_name
        if stream_name not in self._redis_stream_offset.keys():
            self._redis_stream_offset.update({stream_name, self.redis_xread_offset})
        # do job
        streams = await self.redis_client.xread({stream_name: self._redis_stream_offset[stream_name]}, count=1)
        for _, stream in streams:
            for idx, data in stream:
                try:
                    # set data
                    logger.info(f"[COLLECTOR] SET {data['name']}, {data['value']}, EXPIRE {self.redis_expire_sec}")
                    await self.redis_client.set(**data, ex=self.redis_expire_sec)

                    # update stream offset
                    logger.info(f"[COLLECTOR] Update Stream Offset {stream_name}, {idx}")
                    self._redis_stream_offset.update({stream_name: idx})

                    # update key
                    key = data["name"]
                    if key not in self._redis_keys:
                        logger.info(f"[COLLECTOR] New Key '{key}' Found, SADD {self._redis_keys_key}, {key}")
                        await self.redis_client.sadd(self._redis_keys_key, key)
                        self._redis_keys = await self.redis_client.smembers(self._redis_keys_key)
                except Exception as ex:
                    logger.warning(ex)

    # async def __call__(self, messages):
    #     try:
    #         outputs = await asyncio.gather(*[parser(self.root_topic, msg) for msg in messages])
    #         await asyncio.gather(*[self.client.set(**o, ex=self.expire_sec) for o in outputs])
    #         await self.update_keys([o["name"] for o in outputs])
    #         logger.debug(f"[REDIS] Update {outputs}")
    #     except Exception as ex:
    #         logger.warn(f"[REDIS] Update Failed - {ex}")
    #         traceback.print_exc()

    # async def update_keys(self, keys):
    #     if self.keys is None:
    #         self.keys = await self.client.keys(f"{self.root_topic}/*")
    #         await self.client.sadd(self.keys_key, *self.keys)
    #         return
    #     unregistered = [k for k in keys if k not in self.keys]
    #     if len(unregistered) == 0:
    #         return
    #     await self.client.sadd(self.keys_key, *unregistered)


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)
    redis_client = redis.Redis(decode_responses=True)
    asyncio.run(Collector(redis_client=redis_client, redis_topic="ubud").run())
