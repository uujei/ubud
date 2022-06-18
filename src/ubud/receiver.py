import json
import logging
import socket
import time
from functools import partial
import rel
import websocket
import zmq

from . import market as MARKET
from .market.common import ts_to_strdt

logger = logging.getLogger(__name__)


################################################################
# Handlers
################################################################
# Upbit param names (default) to bithumb param names


# after receive 1st message
def on_open(ws, register=None):
    if register is not None:
        try:
            register()
        except Exception as ex:
            raise ReferenceError(ex)


# default message handler
def on_message(ws, msg, handler=None):
    if handler is not None:
        handler(ws, msg)
        return 1
    _received_dt = ts_to_strdt(time(), _float=True)
    logger.info(f"[WEBSOCKET] {_received_dt}")
    logger.debug(f"[WEBSOCKET] Message: {json.loads(msg)}")


# error handler
def on_error(ws, err):
    logger.error(f"[ERROR] {err}")


# on close
def on_close(ws, close_status_code, close_msg):
    logger.error(f"[CLOSED] Websocket closed with code {close_status_code} and message {close_msg}!")


################################################################
# MAIN
################################################################
# [MAIN] receive messages from
def start_stream(
    market: str,
    quote: str,
    symbols: list,
    currency: str,
    broker=None,
    topic="ubud",
    client_id=None,
    trace=False,
):
    # load module
    module = getattr(MARKET, market)

    # correct symbols
    symbols = symbols if isinstance(symbols, (list, tuple)) else [symbols]
    if market in ["upbit"]:
        symbols = [f"{currency}-{symbol}".upper() if "-" not in symbol else symbol.upper() for symbol in symbols]
        register_conf = {
            "quote": quote,
            "symbols": symbols,
            "isOnlyRealtime": True,
            "ticket": f"{quote}-{','.join(sorted(symbols))}".lower(),
        }
    if market in ["bithumb"]:
        symbols = [f"{symbol}_{currency}".upper() if "_" not in symbol else symbol.upper() for symbol in symbols]
        register_conf = {
            "quote": quote,
            "symbols": symbols,
        }

    # get handler
    if client_id is None:
        client_id = f"{socket.gethostname()}-{market}-{quote}-{','.join(symbols)}"
    handler = module.Connector(quote=quote, client_id=client_id, broker=broker, topic=topic)

    # logging
    logger.info(
        f"[UBUD] Start! MARKET: {market}, TYPE: {quote}, SYMBOLS: {', '.join(sorted(symbols))}, BROKER: {broker}, TOPIC: {topic}"
    )
    logger.info(f"[UBUD] Client ID: {client_id}")

    # register
    conf = {
        "url": module.URL,
        "on_open": partial(module.register, **register_conf),
        "on_message": partial(on_message, handler=handler),
        "on_error": on_error,
        "on_close": on_close,
    }

    if trace:
        websocket.enableTrace(True)
    ws = websocket.WebSocketApp(**conf)
    ws.run_forever()
