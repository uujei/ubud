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
# Message Parser
################################################################
async def parser(root_topic: str, msg: dict):
    return {
        "name": f"{root_topic}/" + "/".join([msg.pop(_TOPIC) for _TOPIC in MQ_SUBTOPICS]),
        "value": json.dumps({**{k: v for k, v in msg.items()}, TS_MQ_SEND: time()}),
    }


################################################################
# MQTT Default Callbacks
################################################################
class Upserter:
    def __init__(
        self,
        url: str = "localhost",
        port: int = 6379,
        root_topic: str = "ubud",
        expire_sec: int = 600,
        client_id: str = None,
        parser: Callable = parser,
        _decode_responses: bool = True,
    ):
        # properties
        self.url = url
        self.port = port
        self.root_topic = root_topic
        self.expire_sec = expire_sec
        self.client_id = client_id
        self.parser = parser
        self._decode_responses = _decode_responses

        # client
        self.client = redis.Redis(host=self.url, port=self.port, decode_responses=self._decode_responses)

        # key store
        self.keys = None
        self.keys_key = f"{self.root_topic}/keys"

    async def __call__(self, messages):
        try:
            outputs = await asyncio.gather(*[parser(self.root_topic, msg) for msg in messages])
            await asyncio.gather(*[self.client.set(**o, ex=self.expire_sec) for o in outputs])
            await self.update_keys([o["name"] for o in outputs])
            logger.debug(f"[REDIS] Update {outputs}")
        except Exception as ex:
            logger.warn(f"[REDIS] Update Failed - {ex}")
            traceback.print_exc()

    async def update_keys(self, keys):
        if self.keys is None:
            self.keys = await self.client.keys(f"{self.root_topic}/*")
            await self.client.sadd(self.keys_key, *self.keys)
            return
        unregistered = [k for k in keys if k not in self.keys]
        if len(unregistered) == 0:
            return
        await self.client.sadd(self.keys_key, *unregistered)
