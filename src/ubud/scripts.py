import logging

import click
from click_loglevel import LogLevel
from clutter.aws import get_secrets
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from .connector.mqtt_source_console_log import start_mqtt_to_console as _start_mqtt_to_console
from .connector.mqtt_source_influxdb_sink import Handler
from .connector.mqtt_source_influxdb_sink import start_mqtt_consumer as _start_mqtt_consumer
from .mqtt.publisher import Publisher as MqttPublisher
from .redis.publisher import Publisher as RedisPublisher
from .redis.upserter import Upserter as RedisUpserter
from .websocket.bithumb import BithumbWebsocket
from .websocket.upbit import UpbitWebsocket

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

DEFAULT_LOG_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"

WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
}

PUBLISHER = {
    "mqtt": MqttPublisher,
    "redis": RedisUpserter,
    "redis-pub": RedisPublisher,
}

# Helper
def get_broker_conf(broker):
    if broker is None:
        return broker, None

    if "://" not in broker:
        return broker, {}

    broker, host = broker.split("://")
    if ":" not in host:
        return broker, {"url": host}

    host, port = host.split(":")
    return broker, {"url": host, "port": port}


################################################################
# GROUP UBUD
################################################################
@click.group()
def ubud():
    pass


################################################################
# STREAM WEBSOCKET
################################################################
@ubud.command()
@click.option("-m", "--markets", required=True)
@click.option("-q", "--quotes", required=True)
@click.option("-s", "--symbols", required=True, multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud")
@click.option("--client-id")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def stream_websocket(markets, quotes, symbols, currency, broker, topic, client_id, log_level, trace):
    import asyncio

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # correct markets, quotes, symbols
    markets = [m.lower() for m in markets.split(",")]
    logger.info(f"[UBUD] markets: {markets}")
    quotes = [q.lower() for q in quotes.split(",")]
    logger.info(f"[UBUD] quotes: {quotes}")
    symbols = [s.upper() for ss in symbols for s in ss.split(",")]
    logger.info(f"[UBUD] symbols: {symbols}")

    # set publisher
    broker, broker_conf = get_broker_conf(broker)
    logger.info(f"[UBUD] Start Websocket Stream - Broker {broker} ({broker_conf})")
    if broker is not None:
        handler = PUBLISHER[broker](**broker_conf, root_topic=topic, client_id=client_id)
    else:
        handler = None

    # gather coroutines
    async def gatherer():
        coroutines = []
        for market in markets:
            for quote in quotes:
                logger.info(f"[UBUD] {market}, {quote}, {symbols}")
                coroutines += [
                    WEBSOCKET[market](quote=quote, symbols=symbols, currency=currency, handler=handler).run()
                ]
        await asyncio.gather(*coroutines)

    asyncio.run(gatherer())


################################################################
# QUOTATION STREAM
################################################################
@ubud.command()
@click.option("-m", "--market", required=True)
@click.option("-q", "--quote", required=True)
@click.option("-s", "--symbols", required=True, multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud/quotation")
@click.option("--client-id")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def stream_websocket_single(market, quote, symbols, currency, broker, topic, client_id, log_level, trace):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # correct symbols
    symbols = [s.strip().upper() for ss in symbols for s in ss.split(",")]

    # set publisher
    broker, broker_conf = get_broker_conf(broker)
    logger.info(f"[UBUD] Start Websocket Stream - Broker {broker} ({broker_conf})")
    if broker is not None:
        handler = PUBLISHER[broker](**broker_conf, root_topic=topic, client_id=client_id)
    else:
        handler = None

    # set streamer
    market = market.lower()
    streamer = WEBSOCKET[market](
        quote=quote,
        symbols=symbols,
        currency=currency,
        handler=handler,
    )

    # start streaming
    streamer.start()


################################################################
# CONSOLE LOG CONNECTOR
################################################################
@ubud.command()
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
def start_mqtt_to_console(broker, topic, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # start stream
    _start_mqtt_to_console(broker=broker, topic=topic)


################################################################
# INFLUXDB CONSUMER
################################################################
@ubud.command()
@click.option("-u", "--src-url", default="localhost")
@click.option("-p", "--src-port", default=1883)
@click.option("-t", "--src-topic", default="ubud/#")
@click.option("-b", "--sink-bucket", default="dev")
@click.option("-s", "--sink-secret", default="theone")
@click.option("--sink-url", default=None)
@click.option("--log-level", default=logging.INFO, type=LogLevel())
def start_mqtt_to_influxdb(src_topic, src_url, src_port, sink_bucket, sink_secret, sink_url, log_level):
    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # influxdb
    if sink_secret is not None:
        secret = get_secrets(sink_secret)
        influxdb_conf = {"url": secret["iu"], "token": secret["it"], "org": secret["io"]}
        if sink_url is not None:
            influxdb_conf.update({"url": sink_url})
        handler = Handler(**influxdb_conf, bucket=sink_bucket)
    else:
        handler = None

    logger.info(src_url)
    logger.info(src_topic)

    _start_mqtt_consumer(
        topic=src_topic,
        url=src_url,
        port=src_port,
        handler=handler,
    )
