import asyncio
import logging
import time
import traceback
from fnmatch import fnmatch
from ..redis.handler import RedisStreamHandler
from typing import Callable, List, Union

import redis.asyncio as redis

from ..redis.db import Database

logger = logging.getLogger(__name__)


class OnStream:
    def __init__(self, debug):
        self.store = dict()
        self.debug = debug

    async def __call__(self, stream: str = None, offset: str = None, record: dict = None):
        self.store.update({stream: record})
        if self.debug:
            logger.warning("[ON_STREAM] stream: {0}, offset: {1}, record: {2}".format(stream, offset, record))


class App:
    """
    [NOTE] 가장 기본적인 Application
    주어진 redis_streams에 새로운 event가 들어올 때마다 on_stream 함수를 실행
    적절한 redis_streams이 주어지지 않는다면 작업이 늦게 수행되던지 너무 많은 리소스를 소모

    Example
    -------
    >>> # on_stream 정의
    >>> def on_stream(stream, offset, record):
    >>>     logger.info("stream: {}".format(stream))
    >>>     logger.info("offset: {}".format(offset))
    >>>     logger.info("record: {}".format(record))
    >>> # app 생성 및 on_stream override
    >>> app = App(redis_client=redis_client)
    >>> app.on_stream = on_stream
    >>> # 실행 (debug=True 적용하면 3초만 실행 후 종료)
    >>> asyncio.run(app.run(debug=True))
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_streams: Union[str, list] = "*",
        redis_xread_block: int = 100,
        redis_stream_handler: RedisStreamHandler = None,
        redis_stream_update_interval: int = 5,
        debug_sec: int = 1,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_streams = self._ensure_list(redis_streams)
        self.redis_xread_block = redis_xread_block
        self.redis_stream_handler = redis_stream_handler
        self.redis_stream_update_interval = redis_stream_update_interval
        self.debug_sec = debug_sec

        # consumer management
        self._consumers = list()
        self._redis_streams_set = f"{self.redis_topic}/keys"
        self._offsets = dict()

        # whoami
        self._me = self.__class__.__name__

    async def on_stream(self, stream: str = None, offset: str = None, record: dict = None):
        logger.warning("[ON_STREAM] stream: {0}, offset: {1}, record: {2}".format(stream, offset, record))

    async def run(self, debug=False):
        # clean offsets
        self._offsets = dict()

        # debug stopwatch
        if debug:
            logger.info(
                "[APP {app}] Debug Mode On, App Will Be Stopped After {sec} Seconds".format(
                    app=self._me, sec=self.debug_sec
                )
            )
            asyncio.create_task(self._cancel_all(self.debug_sec))

        # run
        try:
            while True:
                # catch streams
                try:
                    new_streams = await self.update_stream()
                    if new_streams:
                        for s, _ in new_streams.items():
                            consumer = asyncio.create_task(self.consumer(s))
                            self._consumers.append(consumer)
                except Exception as ex:
                    logger.warning("[APP {0}] Fail Update Stream - {1}".format(self._me, ex))
                    traceback.print_exc()
                    continue
                finally:
                    await asyncio.sleep(self.redis_stream_update_interval)
        except asyncio.CancelledError:
            await asyncio.sleep(1)
            if debug:
                logger.error("[APP {0}] Debug Run Finished!".format(self._me))
            else:
                logger.error("[APP {0}] STOP!".format(self._me))

    async def consumer(self, stream):
        while True:
            try:
                streams = await self.redis_client.xread({stream: self._offsets[stream]}, block=self.redis_xread_block)
                for stream, records in streams:
                    for offset, record in records:
                        await self.on_stream(stream=stream, offset=offset, record=record)
                    self._offsets.update({stream: offset})
            except Exception as ex:
                logger.warning("[APP {0}] Fail XREAD - {1}".format(self._me, ex))
                continue

    async def update_stream(self):
        streams = await self.redis_client.smembers(self._redis_streams_set)
        offsets = {
            s: "$"
            for s in streams
            if any([fnmatch(s, p) for p in self.redis_streams]) and s not in self._offsets.keys()
        }
        self._offsets.update(offsets)
        logger.debug(
            "[APP {app}] {n} New Streams, Sample: {sample}".format(
                app=self._me,
                n=len(offsets),
                sample=list(offsets.items())[0] if len(offsets) > 0 else "None",
            )
        )
        return offsets

    async def _cancel_all(self, timeout):
        start = time.time()
        try:
            while True:
                if time.time() - start > timeout:
                    raise asyncio.CancelledError("timout!")
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            tasks = [t for t in asyncio.all_tasks() if t != asyncio.current_task()]
            _ = [t.cancel() for t in tasks]
            await asyncio.gather(*tasks)
            loop = asyncio.get_running_loop()
            loop.stop()

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
    app = App(
        redis_client=redis_client,
        redis_topic="ubud",
        redis_streams=["*/forex/*", "*quotation/trade/ftx/BTC/*"],
    )

    async def task():
        streams = await app.check()

    asyncio.run(task())
