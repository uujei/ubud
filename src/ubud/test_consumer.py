import paho.mqtt.client as mqtt
import logging
from time import time
import json
from datetime import datetime

logger = logging.getLogger(__name__)

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
def on_message(client, userdata, msg):
    logger.info("[MQTT] Received")
    topic = msg.topic
    payload = json.loads(msg.payload)
    logger.info(f"[MQTT]  - Topic: {topic}")
    logger.info(f"[MQTT]  - Payload: {payload}")
    if "trade" in topic:
        l12 = payload["ts_recv"] - datetime.strptime(payload["trade_dt"], "%Y-%m-%dT%H:%M:%S%z").timestamp()
        logger.info(f"[MQTT]  - Latency L12: {l12 * 1000} ms (Contract - Webscoket Recv)")
        l23 = time() - payload["ts_recv"]
        logger.info(f"[MQTT]  - Latency L23: {l23 * 1000} ms (Websocket Recv - MQTT Sub)")
        l13 = time() - datetime.strptime(payload["trade_dt"], "%Y-%m-%dT%H:%M:%S%z").timestamp()
        logger.info(f"[MQTT]  - Latency L13: {l13*1000} ms (Contract - MQTT Sub)")


#
def start_consume(broker, topic):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    client.subscribe(topic)
    client.loop_forever()
