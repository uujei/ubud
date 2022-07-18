import asyncio
import json
import logging
import time
import traceback
from typing import Callable

import parse
import redis.asyncio as redis
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

from ..const import (
    AMOUNT,
    API_CATEGORY,
    CHANNEL,
    CURRENCY,
    DATETIME,
    EXCHANGE_KEY_RULE,
    FOREX_KEY_RULE,
    MARKET,
    MQ_SUBTOPICS,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    QUOTATION_KEY_RULE,
    RANK,
    SYMBOL,
    TS_MARKET,
    TS_MQ_RECV,
    TS_MQ_SEND,
    TS_WS_RECV,
    TS_WS_SEND,
    UTC,
)
from ..utils.app import parse_redis_addr

logger = logging.getLogger(__name__)


_quotation_subtopics = "/".join([f"{{{t}}}" for t in QUOTATION_KEY_RULE])
QUOTATION_KEY_PARSER = parse.compile("/".join(["{topic}", _quotation_subtopics]))
QUOTATION_TAGS = [MARKET, CHANNEL, SYMBOL, CURRENCY, ORDERTYPE, RANK]
QUOTATION_FIELDS = [PRICE, QUANTITY, AMOUNT]

_exchange_subtopics = "/".join([f"{{{t}}}" for t in EXCHANGE_KEY_RULE])
EXCHANGE_KEY_PARSER = parse.compile("/".join(["{topic}", _exchange_subtopics]))
EXCHANGE_TAGS = [MARKET, SYMBOL]
EXCHANGE_FIELDS = ["total", "locked", "free"]

_forex_subtopics = "/".join([f"{{{t}}}" for t in FOREX_KEY_RULE])
FOREX_KEY_PARSER = parse.compile("/".join(["{topic}", _forex_subtopics]))


################################################################
# InfluxDB Connector
################################################################
class InfluxdDBConnector:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_categories: list = ["quotation"],
        redis_xread_block: int = 100,
        redis_smember_interval: float = 5.0,
        influxdb_url: str = "http://localhost:8086",
        influxdb_org: str = "myorgt",
        influxdb_token: str = "mytoken",
        influxdb_interval: float = 0.1,
        influxdb_flush_sec: float = 1,
        influxdb_flush_size: float = 5000,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_categories = [f"{self.redis_topic}-stream/{cat}" for cat in redis_categories]
        self.redis_xread_block = redis_xread_block
        self.redis_smember_interval = redis_smember_interval
        self.influxdb_interval = influxdb_interval
        self.influxdb_flush_sec = influxdb_flush_sec
        self.influxdb_flush_size = influxdb_flush_size

        # influxdb (sink)
        self.influxdb_conf = {
            "url": influxdb_url,
            "org": influxdb_org,
            "token": influxdb_token,
        }
        self.influxdb_org = influxdb_org

        # parser
        self.parser = {
            "quotation": self.parse_quotation,
        }

        # stream management
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_streams = dict()

        # [NOTE] refresh
        # stream list update가 리소스에 부담되어 주기 설정, 매번 update할 때보다 CPU 사용률 20~30% 가량 감소
        self._last_smemeber = time.time() - self.redis_smember_interval

        # InfluxDB Queue
        self._last_write = time.time()
        self._queue = list()

    async def run(self):
        await asyncio.gather(self.updater(), self.collector(), self.writer())

    async def updater(self):
        while True:
            try:
                _last_smember = time.time()
                if _last_smember - self._last_smemeber < self.redis_smember_interval:
                    return
                # 지정한 category에 속하는 stream만 구독
                _streams = await self.redis_client.smembers(self._redis_stream_names_key)
                self._redis_streams = [s for s in _streams if any([s.startswith(c) for c in self.redis_categories])]
                # reset
                self._last_smemeber = _last_smember
            except Exception as ex:
                logger.warning("[INFLUXDB] Updater Failed - {0}".format(ex))

    async def collector(self):
        while True:
            try:
                if len(self._redis_streams) == 0:
                    await asyncio.sleep(0.1)
                streams = await self.redis_client.xread(
                    {k: "$" for k in self._redis_streams}, block=self.redis_xread_block
                )
                for _, stream in streams:
                    for _, data in stream:
                        try:
                            # set data
                            logger.debug(f"[INFLUXDB] Prepare Writing {data['name']}, {data['value']} into InfluxDB")
                            key, value = data["name"], data["value"]
                            measurement, ch = key.split("/", 3)[1:3]
                            if measurement == "exchange":
                                measurement = ch
                            parser = self.parser.get(measurement)
                            if parser is not None:
                                p = await self.parse_quotation(measurement, key, value)
                                self._queue += [p]
                        except Exception as ex:
                            logger.warning("[INFLUXDB] Parse Error {0}".format(ex))
                            traceback.print_exc()
            except Exception as ex:
                logger.warning("[INFLUXDB] Collector Failed - {0}".format(ex))

    async def writer(self):
        while True:
            _n = len(self._queue)
            _now = time.time()
            if _n > self.influxdb_flush_size or (_now - self._last_write) > self.influxdb_flush_sec:
                if _n == 0:
                    continue
                try:
                    async with InfluxDBClientAsync(**self.influxdb_conf) as client:
                        write_api = client.write_api()
                        ack = await write_api.write(self.redis_topic, self.influxdb_org, self._queue[:_n])
                        if not ack:
                            raise
                        logger.info(
                            "[INFLUXDB] Write {0:4d} Points into Bucket {1}, Sample {2}".format(
                                len(self._queue), self.redis_topic, self._queue[-1]
                            )
                        )
                    self._queue = self._queue[_n:]
                except Exception as ex:
                    logger.warning(f"[INFLUXDB] Write Failed - {ex}")
            # wait
            await asyncio.sleep(self.influxdb_interval)

    @staticmethod
    async def parse_quotation(measurement, key, value):
        key = QUOTATION_KEY_PARSER.parse(key).named
        value = json.loads(value)
        p = {
            "measurement": measurement,
            "tags": {k: v for k, v in key.items() if k in QUOTATION_TAGS},
            "fields": {k: float(v) for k, v in value.items() if k in QUOTATION_FIELDS},
            "time": value[DATETIME],
        }
        logger.debug("[INFLUXDB] Quotation Record Parsed: {0}".format(p))
        return p

    @staticmethod
    def _get_offset():
        return str(int(time.time() * 1e3)) + "-0"


################################################################
# InfluxDB Connector
################################################################
async def connect_influxdb(
    influxdb_url: str,
    influxdb_org: str,
    influxdb_token: str,
    redis_addr: str = "localhost:6379",
    redis_topic: str = "ubud",
    redis_categories: list = ["quotation"],
    redis_xread_count: int = 300,
    redis_smember_interval: float = 5.0,
):

    # redis_client
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    connector = InfluxdDBConnector(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_categories=redis_categories,
        redis_xread_count=redis_xread_count,
        redis_smember_interval=redis_smember_interval,
        influxdb_url=influxdb_url,
        influxdb_org=influxdb_org,
        influxdb_token=influxdb_token,
    )

    await connector.run()


################################################################
# Debug
################################################################
if __name__ == "__main__":
    import sys
    from clutter.aws import get_secrets

    logging.basicConfig(level=logging.INFO)

    _secret = get_secrets("theone")
    influxdb_conf = {
        "influxdb_url": _secret["iu"],
        "influxdb_org": _secret["io"],
        "influxdb_token": _secret["it"],
    }

    redis_client = redis.Redis(decode_responses=True)
    connector = InfluxdDBConnector(
        redis_client=redis_client,
        redis_topic="ubud",
        **influxdb_conf,
    )

    asyncio.run(connector.run())
