import paho.mqtt.client as mqtt
from paho.mqtt.client import MQTTv311, MQTTv5
import logging
from typing import Callable
import json
from time import time
from ..const import MARKET, SYMBOL, CURRENCY, QUOTE, ORDERTYPE, TICKER, TRADE, ORDERBOOK, ts_to_strdt

logger = logging.getLogger(__name__)


################################################################
# SETTINGS
################################################################
# MQTT TOPICS
MQTT_TOPICS = [MARKET, SYMBOL, CURRENCY, QUOTE, ORDERTYPE]


def on_connect(client, userdata, flag, rc):
    if rc != 0:
        logger.error(f"Bad Connection Returned with CODE {rc}")
        raise
    logger.info(f"[MQTT] Connected, STATUS_CODE={rc}")


def on_disconnect(client, userdata, flag, rc=0):
    logger.info(f"[MQTT] Disconnected, STATUS_CODE={rc}")


def on_publish(client, userdata, mid):
    logger.info(f"[MQTT] M.ID {mid} is Published")


################################################################
# Message Parser
################################################################
def parser(topic, msg: dict):
    return {
        "topic": f"{topic}/" + "/".join([msg[_TOPIC] for _TOPIC in MQTT_TOPICS]),
        "payload": json.dumps({"ts_recv": time(), **{k: v for k, v in msg.items() if k not in MQTT_TOPICS}}),
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
        keepalive: int = 60,
        client_id: str = None,
        protocol: int = MQTTv5,
        transport: int = "tcp",
        reconnect_on_failure: bool = True,
    ):
        # properties
        self.url = url
        self.port = port
        self.root_topic = root_topic
        self.qos = qos
        self.retain = retain
        self.keepalive = keepalive
        self.client_id = client_id
        self.protocol = protocol
        self.transport = transport
        self.reconnect_on_failure = reconnect_on_failure

        # client
        self.client = mqtt.Client(client_id=self.client_id, reconnect_on_failure=self.reconnect_on_failure)
        self.client.on_connect = on_connect
        self.client.on_publish = on_publish
        self.client.on_disconnect = on_disconnect
        self.client.connect(host=self.url, port=self.port, keepalive=self.keepalive)

    def __call__(self, msg):
        try:
            parsed = parser(self.root_topic, msg)
            logger.debug(f"[MQTT] Publish {parsed}")
            self.client.publish(**parsed)
        except Exception as ex:
            logger.warn(f"[MQTT] Publish Failed: {parsed}, {ex}")
