import asyncio
import json
import logging
import traceback
from datetime import datetime
from fnmatch import fnmatch
from time import time

import influxdb_client.client.util.date_utils as date_utils
import parse
import redis.asyncio as redis
from clutter.aws import get_secrets
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper

from ..const import (
    AMOUNT,
    API_CATEGORY,
    CHANNEL,
    CURRENCY,
    DATETIME,
    MARKET,
    MQ_SUBTOPICS,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    RANK,
    SYMBOL,
    TS_MARKET,
    TS_MQ_RECV,
    TS_MQ_SEND,
    TS_WS_RECV,
    TS_WS_SEND,
    UTC,
)

# for InfluxDB Nanoseconds Precision
date_utils.date_helper = PandasDateTimeHelper()

logger = logging.getLogger(__name__)

KEY_RULE = parse.compile(f"{{topic}}/{{{API_CATEGORY}}}/{{subtopics}}")
MESSAGE_RULE = parse.compile("/".join(["{topic}", *[f"{{{t}}}" for t in MQ_SUBTOPICS]]))
BUCKET = "topic"
MEASUREMENT = API_CATEGORY

RECIPES = {
    "quotation": {
        "tags": [MARKET, CHANNEL, SYMBOL, CURRENCY, ORDERTYPE, RANK],
        "fields": [PRICE, QUANTITY, AMOUNT],
    }
}


TIMESTAMPS = [TS_MARKET, TS_WS_SEND, TS_WS_RECV, TS_MQ_SEND, TS_MQ_RECV]


logger = logging.getLogger(__name__)

################################################################
# MQTT Default Callbacks
################################################################
class InfluxDBWriteHandler:
    def __init__(self, bucket: str, url: str, org: str, token: str):
        # influxdb
        self.bucket = bucket
        self.conf = {"url": url, "org": org, "token": token}

        # influxdb opts
        self._buffer_size = 1000
        self._flush_interval = 0.1

        # buffer management
        self._last_flush = time.time()
        self._point_buffer = []

    async def handle(self, msg):
        logger.debug(f"[INFLUXDB] Prepare Writing {msg['name']}, {msg['value']} into InfluxDB")
        _key = KEY_RULE.parse(msg["name"]).named
        if _key[API_CATEGORY] == "quotation":
            p = await self.quotation_parser(msg)
            self._point_buffer += [p]

    async def __call__(self, messages):
        try:
            # set data
            await asyncio.gather(*[self.handle(msg) for msg in messages])

            # write data
            now = time.time()
            if (now - self._last_flush) > self._flush_interval or len(self._point_buffer) > self._buffer_size:
                await self.batch_writer()
                self._last_flush = now

        except Exception as ex:
            logger.warning(f"[CONNECTOR] Write InfluxDB FAILED - {ex}")
            traceback.print_exc()

    @staticmethod
    async def quotation_parser(msg):
        # k-v
        key = MESSAGE_RULE.parse(msg["name"]).named
        value = json.loads(msg["value"])

        # create point
        p = {
            "measurement": key[MEASUREMENT],
            "tags": {k: v for k, v in key.items() if k in RECIPES["quotation"]["tags"]},
            "fields": {k: float(v) for k, v in value.items() if k in RECIPES["quotation"]["fields"]},
            "time": value[DATETIME],
        }
        logger.debug(f"[CONNECTOR] Quotation Record Parsed: {p}")
        return p

    async def batch_writer(self):
        try:
            async with InfluxDBClientAsync(**self.conf) as client:
                write_api = client.write_api()
                ack = await write_api.write(self.redis_topic, self.org, self._point_buffer)
                if ack:
                    logger.info(
                        f"[CONNECTOR] Write Ack {ack} - Write {len(self._point_buffer):4d} Points into Bucket {self.redis_topic}, Sample {self._point_buffer[0]}"
                    )
                    self._point_buffer = []
                else:
                    raise
        except Exception as ex:
            logger.warning(f"[CONNECTOR] Write InfluxDB FAILED - {ex}")
            traceback.print_exc()


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # influxdb
    secret = get_secrets("theone")
    influxdb_conf = {"influxdb_url": secret["iu"], "influxdb_token": secret["it"], "influxdb_org": secret["io"]}
