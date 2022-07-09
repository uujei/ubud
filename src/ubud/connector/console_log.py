import paho.mqtt.client as mqtt
import logging
from time import time
import json
from datetime import datetime
from ..const import TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV

logger = logging.getLogger(__name__)

################################################################
# Helpers
################################################################
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

    ts_mq_recv = time()
    ts_mq_send = payload.get(TS_MQ_SEND)
    ts_ws_recv = payload.get(TS_WS_RECV)
    ts_ws_send = payload.get(TS_WS_SEND)
    ts_market = payload.get(TS_MARKET)

    LATENCY = {
        ("l15", "Market to Worker", ts_market, ts_mq_recv),
        ("l25", "WS SEND to Worker", ts_ws_send, ts_mq_recv),
        ("l35", "WS RECV to Worker", ts_ws_recv, ts_mq_recv),
        ("l45", "MQTT SEND to Worker", ts_mq_send, ts_mq_recv),
    }

    for _name, desc, time_start, time_end in LATENCY:
        if time_start is None:
            continue
        warn = ""
        if isinstance(time_start, int):
            time_start = float(time_start)
            warn = " - WARN! Float Minus Inteager"
        logger.info(f"[MQTT]  - Latency {_name}: {(time_end - time_start)*1000} ms ({desc}){warn}")


################################################################
# MAIN FUNCTION
################################################################
def start_mqtt_to_console(broker, topic):
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    client.subscribe(topic)
    client.loop_forever()
