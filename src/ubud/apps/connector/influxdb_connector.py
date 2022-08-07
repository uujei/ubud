import asyncio
import logging
import time
import traceback
from typing import Callable

import redis.asyncio as redis
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ...const import (
    AMOUNT,
    CATEGORY,
    CHANNEL,
    CODES,
    CURRENCY,
    DATETIME,
    MARKET,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    RANK,
    SYMBOL,
)
from ...utils.app import key_maker, key_parser
from ..base import App

logger = logging.getLogger(__name__)

#
PARSER = {
    "quotation": {
        "tags": [MARKET, CHANNEL, SYMBOL, CURRENCY, ORDERTYPE, RANK],
        "fields": [PRICE, QUANTITY, AMOUNT],
    },
    "forex": {
        "tags": [CODES],
        "fields": ["basePrice", "highPrice", "lowPrice", "cashBuyingPrice", "cashSellingPrice"],
    },
    "premium": {
        "tags": [MARKET, CHANNEL, SYMBOL, CURRENCY, ORDERTYPE, RANK],
        "fields": ["factor", "premium"],
    },
}

################################################################
# InfluxDB Connector
################################################################
class InfluxDBConnector(App):
    """
    redis_streams = ["*/quotation/*", "*/forex/*", "*/premium/*"]
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_streams: list = [f"*/{cat}/*" for cat in PARSER],
        redis_xread_block: int = 100,
        redis_stream_handler: Callable = None,
        redis_stream_update_interval: int = 5,
        influxdb_url: str = "http://localhost:8086",
        influxdb_org: str = "myorgt",
        influxdb_token: str = "mytoken",
        influxdb_flush_sec: float = 1,
        influxdb_flush_size: float = 5000,
    ):
        super().__init__(
            redis_client=redis_client,
            redis_topic=redis_topic,
            redis_streams=redis_streams,
            redis_xread_block=redis_xread_block,
            redis_stream_handler=redis_stream_handler,
            redis_stream_update_interval=redis_stream_update_interval,
        )

        # influxdb opts
        self.influxdb_flush_sec = influxdb_flush_sec
        self.influxdb_flush_size = influxdb_flush_size

        # influxdb (sink)
        self.influxdb_conf = {
            "url": influxdb_url,
            "org": influxdb_org,
            "token": influxdb_token,
        }
        self.influxdb_org = influxdb_org

        # stream management
        self._streams_key = f"{self.redis_topic}/keys"
        self._offsets = dict()

        # InfluxDB Queue
        self._elapsed = time.time()
        self._queue = list()

    async def on_stream(self, stream, offset, record):
        logger.debug(
            "[APP {app}] stream: {stream}, offset {offset}, record: {record}".format(
                app=self._me, stream=stream, offset=offset, record=record
            )
        )
        point = self.parser(key=stream, value=record)
        self._queue.append(point)

        n = len(self._queue)
        now = time.time()
        elapsed = now - self._elapsed
        logger.debug(
            "[APP {app}] queue: {n}, elapsed: {elapsed}".format(app=self._me, n=len(self._queue), elapsed=elapsed)
        )
        if elapsed > self.influxdb_flush_sec or n > self.influxdb_flush_size:
            points, self._queue = self._queue[:n], self._queue[n:]
            asyncio.create_task(self.write_points(points))
            self._elapsed = time.time()

    async def write_points(self, points):
        try:
            async with InfluxDBClientAsync(**self.influxdb_conf, enable_gzip=True) as client:
                write_api = client.write_api()
                ack = await write_api.write(self.redis_topic, self.influxdb_org, points)
                if not ack:
                    raise asyncio.TimeoutError(f"Ack Timeout '{ack}' - {len(points)} points")
            logger.info(
                "[INFLUXDB] Write {0:4d} Points into Bucket {1}, Sample {2}".format(
                    len(points), self.redis_topic, points[0]
                )
            )
        except Exception as ex:
            logger.warning(f"[INFLUXDB] Write Failed - {ex}")
            traceback.print_exc()

    def parser(self, key, value):

        parsed = key_parser(key)
        category = parsed[CATEGORY]

        p = {
            "measurement": category,
            "tags": {k: v for k, v in parsed.items() if k in PARSER[category]["tags"]},
            "fields": {k: float(v) for k, v in value.items() if k in PARSER[category]["fields"]},
            "time": value[DATETIME],
        }
        logger.debug("[APP {app}] Point : {p}".format(app=self._me, p=p))
        return p


################################################################
# Debug
################################################################
if __name__ == "__main__":
    from clutter.aws import get_secrets

    logging.basicConfig(level=logging.INFO)

    _secret = get_secrets("theone")
    influxdb_conf = {
        "influxdb_url": _secret["iu"],
        "influxdb_org": _secret["io"],
        "influxdb_token": _secret["it"],
    }

    redis_client = redis.Redis(decode_responses=True)
    connector = InfluxDBConnector(
        redis_client=redis_client,
        redis_topic="ubud",
        redis_streams=[f"*/{cat}/*" for cat in PARSER],
        **influxdb_conf,
    )

    asyncio.run(connector.run())
