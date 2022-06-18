import click
from .receiver import start_stream as _start_stream
import logging
from click_loglevel import LogLevel

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
log_handler = logging.StreamHandler()
logger.addHandler(log_handler)


@click.group()
def ubud():
    pass


@ubud.command()
@click.option("-m", "--market")
@click.option("-q", "--quote")
@click.option("-s", "--symbols", multiple=True)
@click.option("-c", "--currency", default="KRW")
@click.option("-b", "--broker", default=None)
@click.option("-t", "--topic", default="ubud")
@click.option("--log-level", default=logging.INFO, type=LogLevel())
@click.option("--trace", is_flag=True)
def start_stream(market, quote, symbols, currency, broker, topic, log_level, trace):

    # set log level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(message)s",
    )

    # start stream
    _start_stream(
        market=market,
        quote=quote,
        symbols=symbols,
        currency=currency,
        broker=broker,
        topic=topic,
        trace=trace,
    )
