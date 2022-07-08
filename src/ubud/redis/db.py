import redis.asyncio as redis
from fnmatch import fnmatch
import json


class Database:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        root_topic: str = "ubud",
    ):
        # props
        self.host = host
        self.port = port
        self.root_topic = root_topic

        # settings
        self._decode_responses = True

        # client and keys
        self.client = redis.Redis(host=self.host, port=self.port, decode_responses=self._decode_responses)
        self.keys = None

    async def get(self, keys: list, drop_ts=True):
        if self.keys is None:
            await self.update_keys()
        keys = self._ensure_list(keys)
        keys = [k for key in keys for k in self.keys if fnmatch(k, key) and k != f"{self.root_topic}/keys"]
        values = [json.loads(v) for v in await self.client.mget(keys)]
        # return {k: v for k, v in zip(keys, values)}
        return {k: {_k: _v for _k, _v in v.items() if not _k.startswith("_ts")} for k, v in zip(keys, values)}

    async def update_keys(self):
        self.keys = await self.client.smembers(f"{self.root_topic}/keys")

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return x
        if isinstance(x, str):
            return [x]
        raise ReferenceError("_ensure_list: x should be a list, tuple, str or None!")
