import logging
from fnmatch import fnmatch

import parse

from ..const import DATETIME, QUOTATION_KEY_RULE, MARKET, ORDERTYPE, CURRENCY, RANK
from ..models import Message
from ..utils.business import get_order_unit
from .base import App

logger = logging.getLogger(__name__)

################################################################
# USD to KRW
################################################################
class Usd2KrwApp(App):
    # settings
    FOREX_PRICE = ["basePrice", "cashBuyingPrice", "cashSellingPrice"]

    # forex store
    forex = dict()

    # logic
    async def on_stream(self, stream=None, offset=None, record=None):
        if "FRX.KRWUSD" in stream:
            self.forex.update(record)
            return

        usd = float(record["price"])
        for forex_price in self.FOREX_PRICE:
            if krw_per_usd := self.forex.get(forex_price):
                try:
                    krw = get_order_unit(usd * float(krw_per_usd))
                    _stream = stream.split("/", 1)[-1].replace("/USD/", f"/KRW.usd.{forex_price}/")
                    msg = Message(
                        key=_stream,
                        value={
                            DATETIME: record[DATETIME],
                            "price": krw,
                        },
                    )

                    # logging
                    logger.debug(
                        "[APP {app}] From {src_stream}, {price}, {usd} -> To {dst_stream}, {price}, {krw}".format(
                            app=self._me, src_stream=stream, dst_stream=_stream, price="price", usd=usd, krw=krw
                        )
                    )
                except Exception as ex:
                    logger.warning("[APP {app}] Transform Failed {ex}".format(app=self._me, ex=ex))

                try:
                    # xadd using handler
                    if self.redis_stream_handler is not None:
                        await self.redis_stream_handler.xadd(msg)
                    logger.info("[APP {app}] XADD {msg}".format(app=self._me, msg=msg))
                except Exception as ex:
                    logger.warning("[APP {app}] XADD Failed {ex}".format(app=self._me, ex=ex))


################################################################
# USD to KRW
################################################################
class GetPremiumApp(App):
    """
    Examples
    --------
    >>> app = GetPremiumApp(
            redis_client=redis_client,
            redis_streams=["*/KRW*/*/[1]"],
            redis_stream_handler=handler,
        )
    >>> asyncio.run(app.run())
    """

    # api_category
    api_category = "premium"

    # store store
    store = dict()

    # parser
    rule = "/".join(["{topic}", *[f"{{{x}}}" for x in QUOTATION_KEY_RULE]])
    parser = parse.compile(rule)

    def select_counters(self, parsed_stream):
        _stream = parsed_stream.copy()
        counter_orderType = "bid" if _stream[ORDERTYPE] == "ask" else "ask"
        _stream.update(
            {
                ORDERTYPE: counter_orderType,
                CURRENCY: _stream[CURRENCY].split(".")[0] + "*",
            }
        )
        anti_pattern = self.rule.format(**_stream)
        _stream.update({MARKET: "*"})
        pattern = self.rule.format(**_stream)
        return {k: v for k, v in self.store.items() if fnmatch(k, pattern) and not fnmatch(k, anti_pattern)}

    # logic
    async def on_stream(self, stream=None, offset=None, record=None):
        # update store
        self.store.update({stream: record})

        # parse stream
        parsed_stream = self.parser.parse(stream).named

        # forex
        forex_base = parsed_stream[CURRENCY].split(".")[-1]
        if forex_base not in ["KRW", "basePrice"]:
            return

        # message holder
        messages = []

        # when ask comes
        if parsed_stream[ORDERTYPE] == "ask":
            bids = self.select_counters(parsed_stream)
            for bid_key, bid_value in bids.items():
                try:
                    parsed_bid_key = self.parser.parse(bid_key)
                    factor = float(bid_value["price"]) / float(record["price"])
                    msg = Message(
                        key="/".join(
                            [
                                self.api_category,
                                parsed_stream["channel"],
                                "-".join([parsed_bid_key[MARKET], parsed_stream[MARKET]]),
                                parsed_stream["symbol"],
                                parsed_stream[CURRENCY].split(".", 1)[0],
                                "-".join([parsed_bid_key[ORDERTYPE], parsed_stream[ORDERTYPE]]),
                                "-".join([parsed_bid_key[RANK], parsed_stream[RANK]]),
                            ]
                        ),
                        value={
                            "datetime": record["datetime"],
                            "factor": factor,
                            "premium": factor - 1.0,
                        },
                    )
                    logger.debug("[App {app}] {msg}".format(app=self._me, msg=msg))
                except Exception as ex:
                    logger.warning("[APP {app}] Transform Failed {ex}".format(app=self._me, ex=ex))

                try:
                    # xadd using handler
                    if self.redis_stream_handler is not None:
                        await self.redis_stream_handler.xadd(msg)
                    logger.info("[APP {app}] XADD {msg}".format(app=self._me, msg=msg))
                except Exception as ex:
                    logger.warning("[APP {app}] XADD Failed {ex}".format(app=self._me, ex=ex))
            return

        # when bid comes
        if parsed_stream[ORDERTYPE] == "bid":
            asks = self.select_counters(parsed_stream)
            for ask_key, ask_value in asks.items():
                try:
                    parsed_ask_key = self.parser.parse(ask_key)
                    factor = float(record["price"]) / float(ask_value["price"])
                    msg = Message(
                        key="/".join(
                            [
                                self.api_category,
                                parsed_stream["channel"],
                                "-".join([parsed_stream[MARKET], parsed_ask_key[MARKET]]),
                                parsed_stream["symbol"],
                                parsed_stream[CURRENCY].split(".", 1)[0],
                                "-".join([parsed_stream[ORDERTYPE], parsed_ask_key[ORDERTYPE]]),
                                "-".join([parsed_stream[RANK], parsed_ask_key[RANK]]),
                            ]
                        ),
                        value={
                            "datetime": record["datetime"],
                            "factor": factor,
                            "premium": factor - 1.0,
                        },
                    )
                    logger.debug("[App {app}] {msg}".format(app=self._me, msg=msg))
                except Exception as ex:
                    logger.warning("[APP {app}] Transform Failed {ex}".format(app=self._me, ex=ex))

                try:
                    # xadd using handler
                    if self.redis_stream_handler is not None:
                        await self.redis_stream_handler.xadd(msg)
                    logger.info("[APP {app}] XADD {msg}".format(app=self._me, msg=msg))
                except Exception as ex:
                    logger.warning("[APP {app}] XADD Failed {ex}".format(app=self._me, ex=ex))

            return


if __name__ == "__main__":
    import asyncio
    import sys
    import redis.asyncio as redis
    from ..redis.handler import RedisStreamHandler

    logging.basicConfig(level=logging.DEBUG)

    REDIS_SOCK = "/var/run/redis/redis-server.sock"

    redis_client = redis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    handler = RedisStreamHandler(redis_client=redis_client)

    if len(sys.argv) > 1:
        test_app = sys.argv[1].lower()
    else:
        test_app = "usd2krwapp"

    if test_app == "usd2krwapp":
        app = Usd2KrwApp(
            redis_client=redis_client,
            redis_streams=["*/forex/*", "*/USD/*/[0-1]"],
            redis_stream_handler=handler,
            debug_sec=3,
        )

    if test_app == "getpremiumapp":
        app = GetPremiumApp(
            redis_client=redis_client,
            redis_streams=["*/KRW*/*/[1]"],
            redis_stream_handler=handler,
            debug_sec=3,
        )

    asyncio.run(app.run())
