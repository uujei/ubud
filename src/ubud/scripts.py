import click
from .streamer import upbit, bithumb
from .publisher import mqtt_publisher
from .test_consumer import start_consume as _start_consume

import logging
from click_loglevel import LogLevel

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler()
logger.addHandler(log_handler)

STREAMER = {
    "upbit": upbit.Streamer,
    "bithumb": bithumb.Streamer,
}

PUBLISHER = {
    "mqtt": mqtt_publisher.Publisher,
}

# Helper
def get_broker_conf(broker):
    if broker is None:
        return broker, None
    broker = broker.lower()
    if broker.startswith("mqtt"):
        if broker == "mqtt":
            return broker, {}
        _broker = broker.replace("mqtt://", "").split(":")
        if len(_broker) == 1:
            return broker, {"url": _broker[0]}
        if len(_broker) == 2:
            return broker, {"url": _broker[0], "port": _broker[1]}
    raise ReferenceError(f"Broker {broker} is Not Available!")


@click.group()
def ubud():
    pass


@ubud.command()
@click.option("-m", "--market", required=True)
@click.option("-q", "--quote", required=True)
@click.option("-s", "--symbols", required=True, multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud")
@click.option("--client-id")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def start_stream(market, quote, symbols, currency, broker, topic, client_id, log_level, trace):

    # set log level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

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


@ubud.command()
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
def start_consume(broker, topic, log_level):

    # set log level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    # start stream
    _start_consume(broker=broker, topic=topic)
