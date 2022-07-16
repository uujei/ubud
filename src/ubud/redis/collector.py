import asyncio
import logging
import time
from typing import Callable
import redis.asyncio as redis

logger = logging.getLogger(__name__)


################################################################
# MQTT Default Callbacks
################################################################
class Collector:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        redis_categories: list = ["exchange", "forex", "quotation"],
        redis_expire_sec: int = 600,
        redis_xread_count: int = 300,
        redis_smember_interval: float = 5.0,
        handler: Callable = None,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_categories = [f"{self.redis_topic}-stream/{cat}" for cat in redis_categories]
        self.redis_expire_sec = redis_expire_sec
        self.redis_xread_count = redis_xread_count
        self.redis_smember_interval = redis_smember_interval

        # handler
        self.handler = handler

        # stream management
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_offset = dict()

        # [NOTE] refresh
        # stream list update가 리소스에 부담되어 주기 설정, 매번 update할 때보다 CPU 사용률 20~30% 가량 감소
        self._last_refresh = time.time() - redis_smember_interval

    async def run(self):
        # collect
        try:
            while True:
                await asyncio.gather(
                    self.update_stream_names(),
                    *[self.collect(name=s, offset=o) for s, o in self._redis_stream_offset.items()],
                )
                await asyncio.sleep(0.001)
        except Exception as ex:
            logger.error(f"[COLLECTOR] Stop Running - {ex}")
        finally:
            await asyncio.sleep(1)

    async def collect(self, name, offset):
        # do job
        streams = await self.redis_client.xread({name: offset}, count=self.redis_xread_count, block=1)
        if len(streams) > 0:
            messages = []
            for _, stream in streams:
                for _offset, msg in stream:
                    messages += [msg]
            if self.handler is not None:
                try:
                    self.handler(messages)
                except Exception as ex:
                    logger.warning(f"[COLLECTOR] Handler Failed - {ex}")
        else:
            _offset = self._get_offset()

        # update stream offset only for last idx
        logger.debug(f"[COLLECTOR] Update Stream Offset {name}, {_offset}")
        self._redis_stream_offset.update({name: _offset})

    async def update_stream_names(self):
        last_refresh = time.time()
        if last_refresh - self._last_refresh < self.redis_smember_interval:
            return
        self._last_refresh = last_refresh
        stream_names = await self.redis_client.smembers(self._redis_stream_names_key)

        # 지정한 category에 속하는 stream만 구독
        stream_names = [s for s in stream_names if any([s.startswith(c) for c in self.redis_categories])]
        for stream_name in stream_names:
            if stream_name not in self._redis_stream_offset.keys():
                offset = self._get_offset()
                logger.debug(f"[COLLECTOR] Register New Stream {stream_name} with Offset {offset}")
                self._redis_stream_offset.update({stream_name: offset})

    @staticmethod
    def _get_offset():
        return str(int(time.time() * 1e3)) + "-0"


################################################################
# Debug
################################################################
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    redis_client = redis.Redis(decode_responses=True)
    collector = Collector(redis_client=redis_client, redis_topic="ubud")
    asyncio.run(collector.run())
