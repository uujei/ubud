import asyncio
import logging

import redis.asyncio as redis
import uvloop

from ..apps.connector.influxdb_connector import InfluxDBConnector
from ..utils.app import repr_conf

logger = logging.getLogger(__name__)


async def stream_influxdb_sink(redis_topic, redis_streams, redis_conf, redis_opts, influxdb_conf, influxdb_opts):

    # redis_client
    redis_client = redis.Redis(**redis_conf)

    # TASKS #
    conf = {
        "redis_client": redis_client,
        "redis_topic": redis_topic,
        "redis_streams": redis_streams,
        **redis_opts,
        **influxdb_conf,
        **influxdb_opts,
    }
    logger.info(f"[UBUD] Start InfluxDB Sink Stream - {repr_conf(conf)}")

    #
    coros = []
    coros += [InfluxDBConnector(**conf).run()]

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)
    await asyncio.gather(*coros)
