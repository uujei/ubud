import asyncio
import logging
import time

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
        redis_expire_sec: int = 600,
        redis_xread_count: int = 20,
        redis_smember_interval: float = 5.0,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec
        self.redis_xread_count = redis_xread_count
        self.redis_smember_interval = redis_smember_interval

        # stream management
        self._redis_stream_name = f"{self.redis_topic}-stream"
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_offset = dict()

        # db management
        self._redis_keys = set()
        self._redis_keys_key = f"{self.redis_topic}/keys"

        # refresh
        # [NOTE]
        #   stream-key를 매번 조회하는 것이 리소스에 크게 부담되어 Refresh 주기 설정
        #   (매 loop마다 조회할 때 대비 CPU 사용률 20~30%p 가량 감소)
        self._last_refresh = time.time() - redis_smember_interval

    async def run(self):
        # collect
        try:
            while True:
                await asyncio.gather(
                    self.update_stream_names(),
                    *[self.collect(name=s, offset=o) for s, o in self._redis_stream_offset.items()],
                )
        except Exception as ex:
            logger.error(f"[COLLECTOR] Stop Running - {ex}")
        finally:
            await asyncio.sleep(1)

    async def update_stream_names(self):
        _last_refresh = time.time()
        if _last_refresh - self._last_refresh < self.redis_smember_interval:
            return
        self._last_refresh = _last_refresh
        _stream_names = await self.redis_client.smembers(self._redis_stream_names_key)
        for s in _stream_names:
            if s not in self._redis_stream_offset.keys():
                offset = self._get_offset()
                logger.debug(f"[COLLECTOR] Register New Stream {s} with Offset {offset}")
                self._redis_stream_offset.update({s: offset})

    async def collect(self, name, offset):
        # do job
        streams = await self.redis_client.xread({name: offset}, count=self.redis_xread_count, block=1)
        if len(streams) > 0:
            for _, stream in streams:
                for _offset, data in stream:
                    k, v = data["name"], data["value"]
                    try:
                        logger.info(f"[COLLECTOR] SET {k}, {v}, EXPIRE {self.redis_expire_sec}")
                        await self.redis_client.set(name=k, value=v, ex=self.redis_expire_sec)
                    except Exception as ex:
                        logger.warning(f"[COLLECTOR] Redis SET Failed - {ex}")
            # update key only for last data ~ because stream name and key are paired
            if k not in self._redis_keys:
                logger.info(f"[COLLECTOR] New Key '{k}' Found, SADD {self._redis_keys_key}, {k}")
                await self.redis_client.sadd(self._redis_keys_key, k)
                self._redis_keys = await self.redis_client.smembers(self._redis_keys_key)
        else:
            _offset = self._get_offset()

        # update stream offset only for last idx
        logger.debug(f"[COLLECTOR] Update Stream Offset {name}, {_offset}")
        self._redis_stream_offset.update({name: _offset})

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
