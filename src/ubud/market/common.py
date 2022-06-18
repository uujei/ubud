import json
import logging
import time
from abc import ABC
from datetime import datetime

import paho.mqtt.client as mqtt
import pendulum

KST = pendulum.timezone("Asia/Seoul")
logger = logging.getLogger(__name__)


################################################################
# STATICS
################################################################
# PARAMS
TICKER = "ticker"
TRADE = "trade"
ORDERBOOK = "orderbook"

# COMMON
DATETIME = "datetime"
MARKET = "market"
TYPE = "type"
SYMBOL = "symbol"
CURRENCY = "currency"
ORDERTYPE = "orderType"
ASK = "ask"
BID = "bid"
PRICE = "price"
QUANTITY = "quantity"
AMOUNT = "amount"

# TRADE ONLY
TRADE_DATETIME = "trade_dt"
TRADE_SID = "trade_sid"

# ORDERBOOK ONLY
TOTAL_QUANTITY = "total_qty"
BOOK_COUNT = "book_count"

# MQTT TOPICS
MQTT_TOPICS = [MARKET, SYMBOL, CURRENCY, TYPE, ORDERTYPE]


################################################################
# Helpers
################################################################
# timestamp to string datetime (w/ ISO format)
def ts_to_strdt(ts, _float=False):
    DTFMT = "%Y-%m-%dT%H:%M:%S%z"
    if _float:
        DTFMT = DTFMT.replace("%z", ".%f%z")
    return datetime.fromtimestamp(ts).astimezone(KST).strftime(DTFMT)


# mqtt common callback / on_connect
def on_connect(client, userdata, flag, rc):
    if rc != 0:
        logger.error(f"Bad Connection Returned with CODE {rc}")
        raise
    logger.info(f"[MQTT] Connected, STATUS_CODE={rc}")


# mqtt common callback / on_disconnect
def on_disconnect(client, userdata, flag, rc=0):
    logger.info(f"[MQTT] Disconnected, STATUS_CODE={rc}")


# mqtt common callback / on_publish
def on_publish(client, userdata, mid):
    logger.info(f"[MQTT] M.ID {mid} is Published")


################################################################
# Translate Message
################################################################
def parse_default(topic, msg: dict):
    if topic is None:
        topic = "notopic"
    return {"topic": topic, "payload": json.dumps(msg)}


def parse_mqtt_default(topic, msg: dict):
    return {
        "topic": f"{topic}/" + "/".join([msg[_TOPIC] for _TOPIC in MQTT_TOPICS]),
        "payload": json.dumps({"ts_recv": time.time(), **{k: v for k, v in msg.items() if k not in MQTT_TOPICS}}),
    }


PARSER = {
    "mqtt": {
        TICKER: parse_default,
        TRADE: parse_mqtt_default,
        ORDERBOOK: parse_mqtt_default,
    },
}

################################################################
# Universial Connector (Websocket to Broker)
################################################################
class AbsConnector(ABC):
    def __init__(
        self,
        quote: str,
        broker: str = None,
        topic: str = None,
        client_id: str = None,
    ):
        if client_id is None:
            client_id = f"nonanme-{topic}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        self.type = quote
        self.broker = broker if broker is None else broker.lower()
        self.topic = topic if topic is not None else "notopic"
        self.client_id = client_id
        self.socket = self.get_socket(self.broker, client_id=self.client_id)
        self.parser = None if self.broker is None else PARSER[self.broker][quote]

        self.HANDLER = {
            TICKER: self._call_ticker,
            TRADE: self._call_trade,
            ORDERBOOK: self._call_orderbook,
        }

    def __call__(self, ws, msg):
        self.HANDLER[self.type](ws, msg)

    def publish(self, messages):
        if self.broker == "mqtt":
            for m in messages:
                # parsing
                try:
                    _p = self.parser(self.topic, m)
                except Exception as ex:
                    logger.warn(f"[MQTT] ParseError - {messages}")
                # logging
                logger.debug(f"[MQTT] Message: {_p}")
                # publishing
                try:
                    self.socket.publish(**_p)
                except Exception as ex:
                    logger.warn(f"[MQTT] ParseError - {messages}")

    # HANDLERS
    def _call_orderbook(self, ws, msg):
        logger.info(f"[ORDERBOOK] {json.loads(msg)}")

    def _call_trade(self, ws, msg):
        logger.info(f"[TRADE] {json.loads(msg)}")

    def _call_ticker(self, ws, msg):
        logger.info(f"[TICKER] {json.loads(msg)}")

    @staticmethod
    def get_socket(broker, client_id=None):
        if broker is None:
            return
        assert broker in ["mqtt"], f"[ERROR] Unknown broker {broker}!"
        # MQTT
        if broker == "mqtt":
            socket = mqtt.Client(client_id=client_id)
            socket.on_connect = on_connect
            socket.on_disconnect = on_disconnect
            socket.on_publish = on_publish
            socket.connect(host="localhost")
            return socket


################################################################
# Universial Consumer (Subscribe Topic)
################################################################
class AbsConsumer(ABC):
    def __init__(
        self,
        broker: str,
        topic: str,
        client_id: str = None,
    ):
        if client_id is None:
            client_id = f"nonanme-{topic}-{datetime.now().strftime('%Y%m%d%H%M%S')}"

        self.type = quote
        self.broker = broker if broker is None else broker.lower()
        self.topic = topic if topic is not None else "notopic"
        self.client_id = client_id
        self.socket = self.get_socket(self.broker, client_id=self.client_id)
        self.parser = None if self.broker is None else PARSER[self.broker][quote]

        self.HANDLER = {
            TICKER: self._call_ticker,
            TRADE: self._call_trade,
            ORDERBOOK: self._call_orderbook,
        }

    def __call__(self, ws, msg):
        self.HANDLER[self.type](ws, msg)

    def publish(self, messages):
        if self.broker == "mqtt":
            for m in messages:
                # parsing
                try:
                    _p = self.parser(self.topic, m)
                except Exception as ex:
                    logger.warn(f"[MQTT] ParseError - {messages}")
                # logging
                logger.debug(f"[MQTT] Message: {_p}")
                # publishing
                try:
                    self.socket.publish(**_p)
                except Exception as ex:
                    logger.warn(f"[MQTT] ParseError - {messages}")

    # HANDLERS
    def _call_orderbook(self, ws, msg):
        logger.info(f"[ORDERBOOK] {json.loads(msg)}")

    def _call_trade(self, ws, msg):
        logger.info(f"[TRADE] {json.loads(msg)}")

    def _call_ticker(self, ws, msg):
        logger.info(f"[TICKER] {json.loads(msg)}")

    @staticmethod
    def get_socket(broker, client_id=None):
        if broker is None:
            return
        assert broker in ["mqtt"], f"[ERROR] Unknown broker {broker}!"
        # MQTT
        if broker == "mqtt":
            socket = mqtt.Client(client_id=client_id)
            socket.on_connect = on_connect
            socket.on_disconnect = on_disconnect
            socket.on_publish = on_publish
            socket.connect(host="localhost")
            return socket
