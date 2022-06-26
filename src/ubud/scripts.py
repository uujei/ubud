import click

from .websocket.bithumb import BithumbWebsocket
from .websocket.upbit import UpbitWebsocket
from .mqtt.publisher import Publisher as MqttPublisher
from .connector.mqtt_source_console_log import start_mqtt_to_console as _start_mqtt_to_console
from .connector.mqtt_source_influxdb_sink import Handler, start_mqtt_consumer as _start_mqtt_consumer
import logging
from click_loglevel import LogLevel
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from clutter.aws import get_secrets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler()
logger.addHandler(log_handler)

DEFAULT_LOG_FORMAT = "%(asctime)s:%(levelname)s:%(message)s"

WEBSOCKET = {
    "upbit": UpbitWebsocket,
    "bithumb": BithumbWebsocket,
}

PUBLISHER = {
    "mqtt": MqttPublisher,
}

# Helper
def get_broker_conf(broker):
    if broker is None:
        return broker, None
    broker = broker.lower()
    if broker.startswith("mqtt"):
        if broker == "mqtt":
            return "mqtt", {}
        _broker = broker.replace("mqtt://", "").split(":")
        if len(_broker) == 1:
            return "mqtt", {"url": _broker[0]}
        if len(_broker) == 2:
            return "mqtt", {"url": _broker[0], "port": int(_broker[1])}
    raise ReferenceError(f"Broker {broker} is Not Available!")


################################################################
# GROUP UBUD
################################################################
@click.group()
def ubud():
    pass


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
def start_websocket_stream(market, quote, symbols, currency, broker, topic, client_id, log_level, trace):

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
# MULTI STREAM
################################################################
@ubud.command()
@click.option("-m", "--markets", required=True)
@click.option("-q", "--quotes", required=True)
@click.option("-s", "--symbols", required=True, multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud/quotation")
@click.option("--client-id")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def start_websocket_stream_multi(markets, quotes, symbols, currency, broker, topic, client_id, log_level, trace):
    import asyncio

    # set log level
    logging.basicConfig(
        level=log_level,
        format=DEFAULT_LOG_FORMAT,
    )

    # correct markets, quotes, symbols
    markets = [m.lower() for m in markets.split(",")]
    quotes = [q.lower() for q in quotes.split(",")]
    symbols = [s.upper() for ss in symbols for s in ss.split(",")]

    logger.info(markets)
    logger.info(quotes)
    logger.info(symbols)
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
