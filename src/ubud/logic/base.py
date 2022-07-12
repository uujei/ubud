import redis.asyncio as redis
from ..redis.db import Database


class BaseLogic:
    def __init__(
        self,
        redis_client: redis.Redis,
        stream_keys: list = None,
    ):
        self.redis_client = redis_client
        self.db = Database(redis_client=self.redis_client)
        self.stream_keys = stream_keys

    async def run(self):
        while True:
            stream = await self.redis_client.xread({k: "$" for k in self.stream_keys}, count=1, block=1)
            if len(stream) > 0:
                await self.trigger(db=self.db)

    @staticmethod
    async def trigger(db):
        return True
