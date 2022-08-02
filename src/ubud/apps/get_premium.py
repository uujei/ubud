import logging
from fnmatch import fnmatch

import parse

from ..const import DATETIME, QUOTATION_KEY_RULE, MARKET, ORDERTYPE, CURRENCY, RANK, SYMBOL
from ..models import Message
from ..utils.business import get_order_unit
from ..utils.app import key_parser
from .base import App

logger = logging.getLogger(__name__)

################################################################
# Get Premium
################################################################
class GetPremiumApp(App):
    """
    Examples
    --------
    >>> app = GetPremiumApp(
            redis_client=redis_client,
            redis_streams=["*/trade/*/KRW*/*"],
            redis_stream_handler=handler,
        )
    >>> asyncio.run(app.run())
    """

    # category
    category = "premium"

    # store store
    store = dict()
    markets = set()

    # logic
    async def on_stream(self, stream=None, offset=None, record=None):
        """
        [NOTE] 불필요하게 많은 계산 줄이기 위해 입력된 market을 분모로 사용
         - Bithumb 레코드 들어오면 Upbit/Bithumb, FTX/Bithumb, ... 프리미엄 계산
         - Upbit 들어오면 Bithumb/Upbit, FTX/Upbit, ... 프리미엄 계산
        """
        # parse key
        parsed = key_parser(stream)

        if parsed[MARKET] not in self.markets:
            self.markets.union(parsed[MARKET])

        key = "/".join([parsed[MARKET], parsed[SYMBOL]])
        self.store.update({key: record})

        # update store
        self.store.update({stream: record})

        # parse stream
        parsed = key_parser(stream)

        # message holder
        messages = []

        #
        self.store()

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
                                self.category,
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
                                self.category,
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
    import redis.asyncio as redis
    from ..redis.handler import RedisStreamHandler

    logging.basicConfig(level=logging.DEBUG)

    REDIS_SOCK = "/var/run/redis/redis-server.sock"

    redis_client = redis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    handler = RedisStreamHandler(redis_client=redis_client)

    app = GetPremiumApp(
        redis_client=redis_client,
        redis_streams=["*/KRW*/*/[1]"],
        redis_stream_handler=handler,
        debug_sec=3,
    )

    asyncio.run(app.run())
