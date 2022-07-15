import asyncio
import json
import logging
import traceback
from datetime import datetime
from fnmatch import fnmatch
from time import time

import parse
import redis.asyncio as redis
from clutter.aws import get_secrets
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ..const import (
    AMOUNT,
    API_CATEGORY,
    CHANNEL,
    CURRENCY,
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

snap1 = []

logger = logging.getLogger(__name__)

KEY_PARSER = parse.compile(f"{{topic}}/{{{API_CATEGORY}}}/{{subtopics}}")
MESSAGE_PARSER = parse.compile("/".join(["{topic}", *[f"{{{t}}}" for t in MQ_SUBTOPICS]]))
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
class InfluxDBConnector:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        influxdb_url: str = "http://localhost:8086",
        influxdb_org: str = "myorgt",
        influxdb_token: str = "mytoken",
        redis_xread_offset: str = "0",
        redis_xread_count: int = 100,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic

        # influxdb (sink)
        self.influxdb_conf = {
            "url": influxdb_url,
            "org": influxdb_org,
            "token": influxdb_token,
        }
        self.influxdb_org = influxdb_org

        # redis stream offset and coount
        if redis_xread_offset in ["earliest", "smallest"]:
            redis_xread_offset = "0"
        elif redis_xread_offset in ["latest", "largest"]:
            redis_xread_offset = str(int(time.time() * 1e3))
        self.redis_xread_offset = redis_xread_offset
        self.redis_xread_count = redis_xread_count

        # stream management
        self._redis_stream_name = f"{self.redis_topic}-stream"
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_offset = dict()

        # db management
        self._redis_keys = set()
        self._redis_keys_key = f"{self.redis_topic}/keys"

    async def run(self):
        # collect
        try:
            while True:
                stream_names = await self.redis_client.smembers(self._redis_stream_names_key)

                try:
                    Points = filter(
                        lambda x: x is not None,
                        await asyncio.gather(*[self.connect(stream_name) for stream_name in stream_names]),
                    )

                except Exception as ex:
                    logger.warning(f"[CONNECTOR] Gather Points FAILED - {ex}")

                try:
                    async with InfluxDBClientAsync(**self.influxdb_conf) as client:
                        write_api = client.write_api()
                        records = [p for points in Points for p in points]
                        ack = await write_api.write(self.redis_topic, self.influxdb_org, records)
                        logger.info(
                            f"[CONNECTOR] Write Ack {ack} - Write {len(records):4d} Points into Bucket {self.redis_topic}, Sample {records[0]}"
                        )

                except Exception as ex:
                    logger.warning(f"[CONNECTOR] Write InfluxDB FAILED - {ex}")
                    traceback.print_exc()

                await asyncio.sleep(0.001)
        except Exception as ex:
            logger.error(ex)
        finally:
            # wait and close
            await asyncio.sleep(1)
            await self.redis_client.close()

    async def connect(self, stream_name):
        # register new stream_name
        if stream_name not in self._redis_stream_offset.keys():
            self._redis_stream_offset.update({stream_name: self.redis_xread_offset})
        # do job
        streams = await self.redis_client.xread(
            {stream_name: self._redis_stream_offset[stream_name]}, count=self.redis_xread_count
        )
        if len(streams) > 0:
            points = []
            for _, stream in streams:
                for idx, data in stream:
                    try:
                        # set data
                        logger.debug(f"[CONNECTOR] Prepare Writing {data['name']}, {data['value']} into InfluxDB")

                        # parse
                        try:
                            k = KEY_PARSER.parse(data["name"]).named
                            if k[API_CATEGORY] in ["quotation"]:
                                p = self.quotation_parser(idx, data)
                                points += [p]
                        except Exception as ex:
                            logger.warning(f"[CONNECTOR] Parse Error - {ex}")

                    except Exception as ex:
                        logger.warning(ex)

                # update stream offset (only for last idx)
                logger.debug(f"[COLLECTOR] Update Stream Offset {stream_name}, {idx}")
                self._redis_stream_offset.update({stream_name: idx})

                return points

    @staticmethod
    def quotation_parser(idx, data):
        tags = RECIPES["quotation"]["tags"]
        fields = RECIPES["quotation"]["fields"]

        r = MESSAGE_PARSER.parse(data["name"]).named
        p = {
            "measurement": r[MEASUREMENT],
            "tags": {k: v for k, v in r.items() if k in tags},
            "fields": {k: float(v) for k, v in json.loads(data["value"]).items() if k in fields},
            "time": datetime.fromtimestamp(float(idx.replace("-", ".")) / 1e3, tz=UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%f%z"
            ),
        }
        logger.debug(f"[CONNECTOR] Quotation Record Parsed: {p}")
        return p


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # influxdb
    secret = get_secrets("theone")
    influxdb_conf = {"influxdb_url": secret["iu"], "influxdb_token": secret["it"], "influxdb_org": secret["io"]}

    redis_client = redis.Redis(decode_responses=True)
    connector = InfluxDBConnector(redis_client=redis_client, **influxdb_conf)

    asyncio.run(connector.run())
