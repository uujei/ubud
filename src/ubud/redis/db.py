import redis.asyncio as redis
from fnmatch import fnmatch
import json
import asyncio


class Database:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
    ):
        # props
        self.redis_client = redis_client
        self.redis_topic = redis_topic

        # client and keys
        self._redis_keys_key = f"{self.redis_topic}/keys"

        # trick async_init
        self._initialized = False

    async def get(self, keys: list, drop_ts=True):
        # parse keys
        registered_keys = {
            k for k in await self.redis_client.smembers(self._redis_keys_key) if not k.endswith("/keys")
        }
        keys = [k for key in self._ensure_list(keys) for k in registered_keys if fnmatch(k, key)]

        # get items
        items = await asyncio.gather(*[self._get(key, drop_ts=drop_ts) for key in keys])
        return {k: v for k, v in [kv for kv in items if kv is not None]}

    async def _get(self, key, drop_ts):
        value = await self.redis_client.get(key)
        if value is None:
            print(f"[DB] Empty Value for '{key}'!")
            return
        value = json.loads(value)
        if drop_ts:
            value = {k: v for k, v in value.items() if not k.startswith("_ts")}
        return (key, value)

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return x
        if isinstance(x, str):
            return [x]
        raise ReferenceError("_ensure_list: x should be a list, tuple, str or None!")
