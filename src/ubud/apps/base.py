import redis.asyncio as redis
from ..redis.db import Database
import asyncio
import time
import logging
from typing import Union, List

logger = logging.getLogger(__name__)


class BaseApp:
    """
    [NOTE] 가장 기본적인 Application
    주어진 redis_streams에 새로운 event가 들어올 때마다 on_stream 함수를 실행
    적절한 redis_streams이 주어지지 않는다면 작업이 늦게 수행되던지 너무 많은 리소스를 소모
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_streams: Union[str, list] = "*",
        redis_xread_block: int = 100,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_streams = self._ensure_list(redis_streams)
        self.redis_xread_block = redis_xread_block

        # settings
        self.db = Database(redis_client=self.redis_client, redis_topic=self.redis_topic)

        # offset management
        self._redis_streams = None
        self._offset = str(int(time.time() * 1e3))

        # whoami
        self.me = self.__class__.__name__

    async def check(self):
        streams = await asyncio.gather(*[self.db.streams(s) for s in self.redis_streams])
        _pad = max([len(s) for s in self.redis_streams])
        logger.info("[APP {app}] Check Your Streams. It's Important!".format(app=self.me))
        for s, _s in zip(self.redis_streams, streams):
            logger.info(
                "[APP {app}] {pattern:>{pad}}: {results}".format(
                    app=self.me, pattern=s, pad=_pad, results=", ".join(_s)
                )
            )
        self._redis_streams = [_s for s in streams for _s in s]

        return self._redis_streams

    async def run(self, debug=False, repeat=10):
        try:
            while True:
                # catch streams
                try:
                    streams = await self.redis_client.xread(
                        {s: "$" for s in self.redis_streams}, block=self.redis_xread_block
                    )
                except Exception as ex:
                    logger.warning("[APP {0}] Fail XREAD - {1}".format(self.me, ex))
                    continue

                # continue if no streams
                if len(streams) == 0:
                    continue

                # trigger on_stream
                try:
                    await self.on_stream(db=self.db, offset=self._offset)
                except Exception as ex:
                    logger.warning("[APP {0}] Fail Execute Task - {1}".format(self.me, ex))

                # update offset "after" on_stream
                self._offset = streams[-1][1][-1][0]

        except Exception as ex:
            logger.warning("[APP {0}] STOP - {1}".format(self.me, ex))

    @staticmethod
    async def on_stream(db=None, offset=None):
        """
        Arguments
        ---------
        db: ubud.redis.Database
            ubud object for redis db
        """
        results = await db.balances()
        logger.info("[ON_STREAM] offset {0}, data {1}".format(offset, results))
        return

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return x
        if not isinstance(x, (list, tuple)):
            x = [x]
        return [__x.strip() for _x in x for __x in _x.split(",")]


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    redis_client = redis.Redis(decode_responses=True)
    app = BaseApp(
        redis_client=redis_client,
        redis_topic="ubud",
        redis_streams=["*/forex/*", "*quotation/trade/ftx/BTC/*"],
    )

    async def task():
        streams = await app.check()

    asyncio.run(task())
