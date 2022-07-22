import redis.asyncio as redis
from ..redis.db import Database
import asyncio
import time
import logging
from fnmatch import fnmatch
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
        redis_stream_update_interval: int = 5,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_streams = self._ensure_list(redis_streams)
        self.redis_xread_block = redis_xread_block
        self.redis_stream_update_interval = redis_stream_update_interval

        # settings
        self.db = Database(redis_client=self.redis_client, redis_topic=self.redis_topic)

        # offset management
        self._redis_streams_set = f"{self.reids_topic}-stream/keys"
        self._offsets = dict()

        # whoami
        self._me = self.__class__.__name__

    async def on_stream(self):
        """
        Arguments
        ---------
        db: ubud.redis.Database
            ubud object for redis db
        """
        results = await self.db.balances()
        logger.info("[ON_STREAM] offset {0}, data {1}".format(self._offsets, results))
        return

    async def run(self):
        try:
            while True:
                # catch streams
                try:
                    new_streams = self.update_stream()
                    if new_streams:
                        for s, _ in new_streams.items():
                            asyncio.create_task(self.consumer(s))
                except Exception as ex:
                    logger.warning("[APP {0}] Fail Update Stream - {1}".format(self._me, ex))
                    continue
                finally:
                    await asyncio.sleep(self.stream_update_interval)
        except Exception as ex:
            logger.warning("[APP {0}] STOP - {1}".format(self._me, ex))

    async def consumer(self, stream):
        while True:
            try:
                data = await self.redis_client.xread({stream: self._offsets[stream]}, block=self.redis_xread_block)
                if len(data) > 0:
                    await self.on_stream()
            except Exception as ex:
                logger.warning("[APP {0}] Fail XREAD - {1}".format(self._me, ex))
                continue

    async def update_stream(self):
        # get new streams
        streams = await self.redis_client.smembers(self._redis_streams_set)
        offsets = {
            s: "$"
            for s in streams
            if any([fnmatch(s, p) for p in self.redis_streams]) and s not in self._offsets.keys()
        }

        # register new streams
        self._offsets.update(offsets)

        return offsets

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
