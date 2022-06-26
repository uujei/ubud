import json
import logging
import traceback
from time import time
from typing import Callable

import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv5, MQTTv311

from ..const import (
    CURRENCY,
    MARKET,
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
# SETTINGS
################################################################
# MQTT TOPICS
MQTT_TOPICS = [QUOTE, MARKET, SYMBOL, CURRENCY, ORDERTYPE]

# default on_connect
def on_connect(client, userdata, flag, rc):
    if rc != 0:
        logger.error(f"Bad Connection Returned with CODE {rc}")
        raise
    logger.info(f"[MQTT] Connected, STATUS_CODE={rc}")


# default on_disconnect
def on_disconnect(client, userdata, flag, rc=0):
    logger.info(f"[MQTT] Disconnected, STATUS_CODE={rc}")


# default on_publish
def on_publish(client, userdata, mid):
    logger.info(f"[MQTT] M.ID {mid} is Published")


################################################################
# Message Parser
################################################################
def parser(root_topic: str, msg: dict):
    return {
        "topic": f"{root_topic}/" + "/".join([msg.pop(_TOPIC) for _TOPIC in MQTT_TOPICS]),
        "payload": json.dumps({**{k: v for k, v in msg.items()}, TS_MQ_SEND: time()}),
    }


################################################################
# MQTT Default Callbacks
################################################################
class Publisher:
    def __init__(
        self,
        url: str = "localhost",
        port: int = 1883,
        root_topic: str = "ubud",
        qos: int = 0,
        retain: bool = False,
        client_id: str = None,
        keepalive: int = 60,
        protocol: int = MQTTv5,
        transport: int = "tcp",
        reconnect_on_failure: bool = True,
    ):
        # properties
        self.root_topic = root_topic
        self.qos = qos
        self.retain = retain

        # client
        self.client = mqtt.Client(
            client_id=client_id,
            protocol=protocol,
            transport=transport,
            reconnect_on_failure=reconnect_on_failure,
        )
        self.client.on_connect = on_connect
        self.client.on_publish = on_publish
        self.client.on_disconnect = on_disconnect
        self.client.connect(
            host=url,
            port=port,
            keepalive=keepalive,
        )

    def __call__(self, msg):
        try:
            outputs = parser(self.root_topic, msg)
            logger.debug(f"[MQTT] Publish {outputs}")
            self.client.publish(**outputs, qos=self.qos, retain=self.retain)
        except Exception as ex:
            logger.warn(f"[MQTT] Publish Failed - {ex}")
            traceback.print_exc()
