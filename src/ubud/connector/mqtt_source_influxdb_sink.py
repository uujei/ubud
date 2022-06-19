import paho.mqtt.client as mqtt
import logging
from time import time
import json
import parse
from clutter.aws import get_secrets
from ..const import TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV, QUOTE
from ..publisher.mqtt_publisher import MQTT_TOPICS
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import ASYNCHRONOUS

logger = logging.getLogger(__name__)

_compile = "/".join(["{topic}/{api_category}", *[f"{{{t}}}" for t in MQTT_TOPICS]])
Parser = parse.compile(_compile)
BUCKET = "api_category"
MEASUREMENT = QUOTE
TAGS = ["topic", "market", "currency", "orderType"]
DATETIME_PRIORITY = [TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV]


################################################################
# Transfrom
################################################################
class Handler:
    def __init__(self, sink_client=None, sink_bucket=None):
        # set sink_client
        self.client = sink_client

        self.bucket = sink_bucket

    def __call__(self, client, userdata, msg):
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

        points = []
        for i, (k, v) in enumerate(payload.items()):
            if isinstance(v, str):
                continue
            p = Point(parsed[MEASUREMENT])
            p.tag("_time_src", name)
            for _TAG in TAGS:
                p.tag(_TAG, parsed[_TAG])
            p.field(k, v)
            # p.time(influxdb_ts)
            points += [p]
            logger.debug(f"[InfluxDB] Point: {p.to_line_protocol()}")
            del p

        # batch write
        if self.client is not None:
            with self.client.write_api(write_options=ASYNCHRONOUS) as writer:
                writer.write(self.bucket, self.client.org, points)


# mqtt common callback / on_connect
def on_connect(client, userdata, flag, rc):
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
    sink_client=None,
    sink_bucket=None,
):

    # mqtt
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_disconnect = on_connect
    client.on_message = Handler(
        sink_client=sink_client,
        sink_bucket=sink_bucket,
    )
    client.connect(url, port, 60)
    client.subscribe(topic)

    # start
    client.loop_forever()


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
    sink_client = InfluxDBClient(**influxdb_conf)

    start_mqtt_consumer(topic="ubud", sink_client=sink_client, sink_bucket="dev")
