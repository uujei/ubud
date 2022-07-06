import json
import logging
import traceback
from time import time
from typing import Callable

import redis
import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv5, MQTTv311

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
# Message Parser
################################################################
def parser(root_topic: str, msg: dict):
    return {
        "channel": f"{root_topic}/" + "/".join([msg.pop(_TOPIC) for _TOPIC in MQ_SUBTOPICS]),
        "message": json.dumps({**{k: v for k, v in msg.items()}, TS_MQ_SEND: time()}),
    }


################################################################
# MQTT Default Callbacks
################################################################
class Publisher:
    def __init__(
        self,
        url: str = "localhost",
        port: int = 6379,
        root_topic: str = "ubud",
        client_id: str = None,
        parser: Callable = parser,
        _decode_responses: bool = True,
    ):
        # properties
        self.url = url
        self.port = port
        self.root_topic = root_topic
        self.client_id = client_id
        self.parser = parser
        self._decode_responses = _decode_responses

        # client
        self.client = redis.Redis(host=self.url, port=self.port, decode_responses=self._decode_responses)

    def __call__(self, msg):
        try:
            outputs = parser(self.root_topic, msg)
            logger.debug(f"[REDIS] Publish {outputs}")
            self.client.publish(**outputs)
        except Exception as ex:
            logger.warn(f"[REDIS] Publish Failed - {ex}")
            traceback.print_exc()
