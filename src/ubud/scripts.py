import click
from .streamer import upbit, bithumb
from .streamer import publisher_mqtt
from .connector.mqtt_source_console_log import start_mqtt_to_console as _start_mqtt_to_console
from .connector.mqtt_source_influxdb_sink import start_mqtt_consumer as _start_mqtt_consumer
import logging
from click_loglevel import LogLevel
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from clutter.aws import get_secrets

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler()
logger.addHandler(log_handler)

STREAMER = {
    "upbit": upbit.Streamer,
    "bithumb": bithumb.Streamer,
}

PUBLISHER = {
    "mqtt": publisher_mqtt.Publisher,
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
def start_quotation_stream(market, quote, symbols, currency, broker, topic, client_id, log_level, trace):

    # set log level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    # correct symbols
    symbols = [s for ss in symbols for s in ss.split(",")]

    # set publisher
    broker, broker_conf = get_broker_conf(broker)
    if broker is not None:
        handler = PUBLISHER[broker](**broker_conf, root_topic=topic, client_id=client_id)
    else:
        handler = None

    # set streamer
    market = market.lower()
    symbols = symbols if isinstance(symbols, (tuple, list)) else [s.strip() for s in symbols.split(",")]
    streamer = STREAMER[market](
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
        format="%(asctime)s:%(levelname)s:%(message)s",
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
@click.option("--log-level", default=logging.INFO, type=LogLevel())
def start_mqtt_to_influxdb(src_topic, src_url, src_port, sink_bucket, sink_secret, log_level):
    # set log level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    # influxdb
    if sink_secret is not None:
        secret = get_secrets(sink_secret)
        influxdb_conf = {"url": secret["iu"], "token": secret["it"], "org": secret["io"]}
        sink_client = InfluxDBClient(**influxdb_conf)
    else:
        sink_client = None

    logger.info(src_url)
    logger.info(src_topic)
    _start_mqtt_consumer(
        topic=src_topic,
        url=src_url,
        port=src_port,
        sink_client=sink_client,
        sink_bucket=sink_bucket,
    )
