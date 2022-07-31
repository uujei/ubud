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
    PREMIUM_KEY_RULE,
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
FOREX_TAGS = ["channel", "codes"]
FOREX_FIELDS = ["basePrice", "highPrice", "lowPrice", "cashBuyingPrice", "cashSellingPrice"]

_premium_subtopics = "/".join([f"{{{t}}}" for t in PREMIUM_KEY_RULE])
PREMIUM_KEY_PARSER = parse.compile("/".join(["{topic}", _premium_subtopics]))
PREMIUM_TAGS = [MARKET, CHANNEL, SYMBOL, CURRENCY, ORDERTYPE, RANK]
PREMIUM_FIELDS = ["factor", "premium"]


################################################################
# InfluxDB Connector
################################################################
class InfluxdDBConnector:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_categories: list = ["quotation", "premium", "forex"],
        redis_xread_interval: float = 0.1,
        redis_xread_count: int = None,
        redis_smember_interval: float = 5.0,
        influxdb_url: str = "http://localhost:8086",
        influxdb_org: str = "myorgt",
        influxdb_token: str = "mytoken",
        influxdb_write_interval: float = 0.1,
        influxdb_flush_sec: float = 1,
        influxdb_flush_size: float = 5000,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_categories = [f"{self.redis_topic}/{cat}" for cat in redis_categories]

        # stream opts
        self.redis_xread_interval = redis_xread_interval
        self.redis_xread_count = redis_xread_count
        self.redis_smember_interval = redis_smember_interval

        # influxdb opts
        self.influxdb_write_interval = influxdb_write_interval
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
        self.PARSER = {
            "quotation": self.parse_quotation,
            "premium": self.parse_premium,
            "forex": self.parse_forex,
        }

        # stream management
        self._streams_key = f"{self.redis_topic}/keys"
        self._offsets = dict()

        # [NOTE] refresh
        # stream list update가 리소스에 부담되어 주기 설정, 매번 update할 때보다 CPU 사용률 20~30% 가량 감소
        self._last_smember = time.time() - self.redis_smember_interval

        # InfluxDB Queue
        self._last_write = time.time()
        self._queue = list()

    async def run(self):
        await asyncio.gather(self.updater(), self.collector(), self.writer())

    async def updater(self):
        while True:
            try:
                # 지정한 category에 속하는 stream만 구독
                streams = await self.redis_client.smembers(self._streams_key)
                self._update_new_streams(streams)
            except Exception as ex:
                logger.warning("[INFLUXDB] Updater Failed - {0}".format(ex))
            await asyncio.sleep(self.redis_smember_interval)

    async def collector(self):
        while True:
            try:
                if self._offsets:
                    streams = await self.redis_client.xread({k: v for k, v in self._offsets.items()})
                    for key, messages in streams:
                        source = key.split("/", 2)[1]
                        if source not in self.PARSER.keys():
                            continue
                        parser = self.PARSER[source]
                        for _offset, value in messages:
                            try:
                                p = parser(source=source, key=key, value=value)
                                if p is not None:
                                    self._queue.append(p)
                            except Exception as ex:
                                logger.warning("[INFLUXDB] Parse Error {0}".format(ex))
                                traceback.print_exc()
                        self._offsets.update({key: _offset})
                await asyncio.sleep(self.redis_xread_interval)
            except Exception as ex:
                logger.warning("[INFLUXDB] Collector Failed - {0}".format(ex))

    async def writer(self):
        t0 = time.time()
        while True:
            now = time.time()
            elapsed = now - t0
            n = len(self._queue)
            if n > 0:
                if (elapsed > self.influxdb_flush_sec) or (n > self.influxdb_flush_size):
                    try:
                        async with InfluxDBClientAsync(**self.influxdb_conf) as client:
                            write_api = client.write_api()
                            ack = await write_api.write(self.redis_topic, self.influxdb_org, self._queue[:n])
                            if not ack:
                                raise
                            logger.info(
                                "[INFLUXDB] Write {0:4d} Points into Bucket {1}, Sample {2}".format(
                                    n, self.redis_topic, self._queue[n - 1]
                                )
                            )
                        self._queue = self._queue[n:]
                        t0 = now
                    except Exception as ex:
                        logger.warning(f"[INFLUXDB] Write Failed - {ex}")
            # wait
            await asyncio.sleep(self.influxdb_write_interval)

    def _update_new_streams(self, streams):
        streams = [s for s in streams if any([s.startswith(c) for c in self.redis_categories])]
        streams = [s for s in streams if s not in self._offsets.keys()]
        _offset = self._get_offset()
        _ = [self._offsets.update({s: _offset for s in streams})]

    @staticmethod
    def parse_quotation(source, key, value):
        key = QUOTATION_KEY_PARSER.parse(key).named
        p = {
            "measurement": source,
            "tags": {k: v for k, v in key.items() if k in QUOTATION_TAGS},
            "fields": {k: float(v) for k, v in value.items() if k in QUOTATION_FIELDS},
            "time": value[DATETIME],
        }
        logger.debug("[INFLUXDB] Quotation Record Parsed: {0}".format(p))
        return p

    @staticmethod
    def parse_premium(source, key, value):
        key = PREMIUM_KEY_PARSER.parse(key).named
        p = {
            "measurement": source,
            "tags": {k: v for k, v in key.items() if k in PREMIUM_TAGS},
            "fields": {k: float(v) for k, v in value.items() if k in PREMIUM_FIELDS},
            "time": value[DATETIME],
        }
        logger.debug("[INFLUXDB] Premium Record Parsed: {0}".format(p))
        return p

    @staticmethod
    def parse_forex(source, key, value):
        key = FOREX_KEY_PARSER.parse(key).named
        p = {
            "measurement": source,
            "tags": {k: v for k, v in key.items() if k in FOREX_TAGS},
            "fields": {k: float(v) for k, v in value.items() if k in FOREX_FIELDS},
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
    redis_addr: str = "localhost:6379",
    redis_topic: str = "ubud",
    redis_categories: list = ["quotation", "premium", "forex"],
    redis_xread_interval: float = 0.1,
    redis_xread_count: int = None,
    redis_smember_interval: float = 5.0,
    influxdb_url: str = "http://myinfluxdb",
    influxdb_org: str = "myorg",
    influxdb_token: str = "mytoken",
    influxdb_write_interval: float = 0.1,
    influxdb_flush_sec: float = 1,
    influxdb_flush_size: float = 5000,
):

    # redis_client
    redis_conf = parse_redis_addr(redis_addr)
    redis_client = redis.Redis(**redis_conf)

    connector = InfluxdDBConnector(
        redis_client=redis_client,
        redis_topic=redis_topic,
        redis_categories=redis_categories,
        redis_xread_interval=redis_xread_interval,
        redis_xread_count=redis_xread_count,
        redis_smember_interval=redis_smember_interval,
        influxdb_url=influxdb_url,
        influxdb_org=influxdb_org,
        influxdb_token=influxdb_token,
        influxdb_write_interval=influxdb_write_interval,
        influxdb_flush_sec=influxdb_flush_sec,
        influxdb_flush_size=influxdb_flush_size,
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
