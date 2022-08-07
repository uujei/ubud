import logging
from datetime import datetime

from ...const import ASK, BID, CATEGORY, CHANNEL, CURRENCY, DATETIME, MARKET, ORDERTYPE, SYMBOL
from ...models import Message
from ...utils.app import key_maker, key_parser
from ...utils.business import get_order_unit
from ..base import App

logger = logging.getLogger(__name__)

################################################################
# Get Premium
################################################################
class PremiumApp(App):
    """
    Examples
    --------
    >>> app = GetPremiumApp(
            redis_client=redis_client,
            redis_streams=["*/quotation/*/*/KRW*/*/[0-1]"],
            redis_stream_handler=handler,
        )
    >>> asyncio.run(app.run())
    """

    # settings
    category = "premium"
    timedelta_limit_sec = 60.0

    # store store
    store = dict()

    # logic
    async def on_stream(self, stream=None, offset=None, record=None):
        """
        [NOTE] 불필요하게 많은 계산 줄이기 위해 입력된 market을 분모로 사용
         - Bithumb 레코드 들어오면 Upbit/Bithumb, FTX/Bithumb, ... 프리미엄 계산
         - Upbit 들어오면 Bithumb/Upbit, FTX/Upbit, ... 프리미엄 계산
        """
        # parse stream name and generate key
        parsed = key_parser(stream)
        channel = parsed[CHANNEL]
        symbol = parsed[SYMBOL]
        market = parsed[MARKET]
        orderType = parsed[ORDERTYPE]

        # update meta and store
        if symbol not in self.store:
            self.store[symbol] = dict()
        if channel not in self.store[symbol]:
            self.store[symbol][channel] = dict()
        if orderType not in self.store[symbol][channel]:
            self.store[symbol][channel][orderType] = dict()

        self.store[symbol][channel][orderType].update({market: record})

        # update premium
        messages = []
        for o, store in self.store[symbol][channel].items():
            if o == orderType:
                continue
            for m, r in store.items():
                # upbit.upbit is meaningless
                if m == market:
                    continue

                # check timedelta
                dt_record = datetime.fromisoformat(record[DATETIME])
                dt_stored = datetime.fromisoformat(r[DATETIME])
                delta = dt_record.timestamp() - dt_stored.timestamp()
                if delta > self.timedelta_limit_sec:
                    logger.info(
                        f"[APP {self._me}] DROP! (Stored Is Too Far - {delta:.0f} sec) - {market}/{channel}/{symbol} {dt_record} & {dt_stored}"
                    )
                    continue

                # conservative premium ~ BIT price / ASK price
                if orderType == ASK:
                    _market = ".".join([m, market])
                    factor = float(r["price"]) / float(record["price"])
                else:
                    _market = ".".join([market, m])
                    factor = float(record["price"]) / float(r["price"])

                # update message
                messages += [
                    Message(
                        key=key_maker(
                            **{
                                CATEGORY: self.category,
                                CHANNEL: channel,
                                MARKET: _market,
                                SYMBOL: symbol,
                                CURRENCY: "KRW",
                            }
                        ),
                        value={
                            DATETIME: record[DATETIME],
                            "timedelta": delta,
                            "premium": factor - 1,
                            "factor": factor,
                        },
                    )
                ]

        # publish message
        try:
            # xadd using handler
            if self.redis_stream_handler is not None:
                await self.redis_stream_handler(messages)
        except Exception as ex:
            logger.warning(
                "[APP {app}] XADD Failed {ex} - messages: {messages}".format(app=self._me, ex=ex, messages=messages)
            )


if __name__ == "__main__":
    import asyncio
    import redis.asyncio as redis
    from ..redis.handler import RedisStreamHandler

    logging.basicConfig(level=logging.DEBUG)

    redis_client = redis.Redis(decode_responses=True)
    handler = RedisStreamHandler(redis_client=redis_client)

    app = PremiumApp(
        redis_client=redis_client,
        redis_streams=["*/quotation/*/KRW*/*/[0-1]"],
        redis_stream_handler=handler,
        debug_sec=3,
    )

    asyncio.run(app.run(debug=True))
