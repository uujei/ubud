import paho.mqtt.client as mqtt
import logging
from time import time
import json
import parse
from clutter.aws import get_secrets
from ..const import TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV, QUOTE
from ..mqtt.publisher import MQTT_TOPICS
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS, SYNCHRONOUS

snap1 = []

logger = logging.getLogger(__name__)

_compile = "/".join(["{topic}/{api_category}", *[f"{{{t}}}" for t in MQTT_TOPICS]])
print(_compile)
Parser = parse.compile(_compile)
BUCKET = "api_category"
MEASUREMENT = QUOTE
TAGS = ["topic", "symbol", "market", "currency", "orderType"]
DATETIME_PRIORITY = [TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV]


################################################################
# Transfrom
################################################################
class Handler:
    def __init__(self, url, token, org, bucket):
        # set sink_client
        self.client = InfluxDBClient(url=url, token=token, org=org, verify_ssl=False)
        self.writer = self.client.write_api(write_options=ASYNCHRONOUS)
        self.bucket = bucket

    def __call__(self, client, userdata, msg, props=None):
        topic = msg.topic
        payload = json.loads(msg.payload)
        logger.info(f"[InfluxDB] MQTT Message Received: topic: {topic}, payload: {payload}")

        # get time
        ts_mq_recv = time()
        for name in DATETIME_PRIORITY:
            if name in payload.keys():
                influxdb_ts = int(payload[name] * 1e9)
                break
        payload.update({TS_MQ_RECV: ts_mq_recv})

        # parse and write
        # (remove string datetime fields)
        # common
        parsed = Parser.parse(topic).named

        for i, (k, v) in enumerate(payload.items()):
            if isinstance(v, str):
                continue
            p = Point(parsed[MEASUREMENT])
            p.tag("_time_src", name)
            for _TAG in TAGS:
                p.tag(_TAG, parsed[_TAG])
            p.field(k, v)

            # # [NOTE] time을 줄 경우 overwrite 발생 ~ time 해상도가 "분"
            # try:
            #     self.writer.write_points(self.bucket, record=p)
            # except Exception as ex:
            #     logger.warn(f"[InfluxDB] Write Exception {ex}")

            logger.debug(f"[InfluxDB] Point: {p.to_line_protocol()}")
            del p


# mqtt common callback / on_connect
def on_connect(client, userdata, flag, rc, props=None):
    if rc != 0:
        logger.error(f"Bad Connection Returned with CODE {rc}")
        raise
    logger.info(f"[MQTT] Connected, STATUS_CODE={rc}")


# mqtt common callback / on_disconnect
def on_disconnect(client, userdata, flag, rc=0):
    logger.info(f"[MQTT] Disconnected, STATUS_CODE={rc}")


#
def start_mqtt_consumer(
    topic: str,
    url: str = "localhost",
    port: int = 1883,
    handler=None,
):
    # mqtt
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_connect
    client.on_message = handler

    # connect and subscribe
    client.connect(url, port, 60)
    client.subscribe(topic)

    # start
    try:
        client.loop_forever()
    except Exception as ex:
        logger.error(ex)
        raise


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    logger.setLevel(logging.DEBUG)
    log_handler = logging.StreamHandler()
    logger.addHandler(log_handler)

    # influxdb
    secret = get_secrets("theone")
    influxdb_conf = {"url": secret["iu"], "token": secret["it"], "org": secret["io"]}
    handler = Handler(**influxdb_conf, bucket="dev")

    start_mqtt_consumer(topic="ubud", handler=handler)
