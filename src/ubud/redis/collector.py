import asyncio
import logging
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
        redis_xread_offset: str = "0",
        redis_xread_count: int = 10,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec
        self.redis_xread_count = redis_xread_count

        # correct offset
        if redis_xread_offset in ["earliest", "smallest"]:
            redis_xread_offset = "0"
        if redis_xread_offset in ["latest", "largest"]:
            redis_xread_offset = "$"
        self.redis_xread_offset = redis_xread_offset

        # stream management
        self._redis_stream_name = f"{self.redis_topic}-stream"
        self._redis_stream_names_key = f"{self.redis_topic}-stream/keys"
        self._redis_stream_offset = dict()

        # db management
        self._redis_keys = set()
        self._redis_keys_key = f"{self.redis_topic}/keys"

    async def run(self):
        # collect
        try:
            while True:
                stream_names = await self.redis_client.smembers(self._redis_stream_names_key)
                await asyncio.gather(*[self.collect(stream_name) for stream_name in stream_names])
                await asyncio.sleep(0.0001)
        except Exception as ex:
            logger.error(ex)
        finally:
            # wait and close
            await asyncio.sleep(1)
            await self.redis_client.close()

    async def collect(self, stream_name):
        assert isinstance(
            stream_name, str
        ), "[COLLECTOR] You Must Turn On 'decode_responses'! - client = Redis(..., decode_reponses=True)"
        # register new stream_name
        if stream_name not in self._redis_stream_offset.keys():
            self._redis_stream_offset.update({stream_name: self.redis_xread_offset})
        # do job
        streams = await self.redis_client.xread(
            {stream_name: self._redis_stream_offset[stream_name]}, count=self.redis_xread_count
        )
        for _, stream in streams:
            for idx, data in stream:
                try:
                    # set data
                    logger.info(f"[COLLECTOR] SET {data['name']}, {data['value']}, EXPIRE {self.redis_expire_sec}")
                    await self.redis_client.set(name=data["name"], value=data["value"], ex=self.redis_expire_sec)

                    # update stream offset
                    logger.debug(f"[COLLECTOR] Update Stream Offset {stream_name}, {idx}")
                    self._redis_stream_offset.update({stream_name: idx})

                    # update key
                    key = data["name"]
                    if key not in self._redis_keys:
                        logger.info(f"[COLLECTOR] New Key '{key}' Found, SADD {self._redis_keys_key}, {key}")
                        await self.redis_client.sadd(self._redis_keys_key, key)
                        self._redis_keys = await self.redis_client.smembers(self._redis_keys_key)
                except Exception as ex:
                    logger.warning(ex)


################################################################
# Debug
################################################################
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    redis_client = redis.Redis(decode_responses=True)
    collector = Collector(redis_client=redis_client, redis_topic="ubud")
    asyncio.run(collector.run())
