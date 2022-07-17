import redis.asyncio as redis
from ..redis.db import Database
import asyncio
import time


class BaseApp:
    def __init__(
        self,
        redis_client: redis.Redis,
        redis_events: list = None,
        redis_xread_block: int = 1,
    ):
        # properties
        self.redis_client = redis_client
        self.redis_events = self._ensure_list()
        self.redis_xread_block = redis_xread_block

        # settings
        self.db = Database(redis_client=self.redis_client)
        self.stream_offsets = {stream_key: None for stream_key in self.redis_stream_keys}

    async def run(self, debug=False, repeat=10):
        async def do():
            await asyncio.gather(
                *[self.stream(name=name, offset=offset) for name, offset in self.stream_offsets.items()]
            )

        if not debug:
            while True:
                await do()
        else:
            for i in range(repeat):
                await do()

    async def stream(self, name, offset):
        if offset is None:
            offset = self._get_offset()
        streams = await self.redis_client.xread({name: offset}, count=1, block=1)
        if len(streams) > 0:
            for _, stream in streams:
                for _offset, data in stream:
                    await self.on_stream(db=self.db, offset=_offset, data=data)
        else:
            _offset = self._get_offset()
        self.stream_offsets.update({name: _offset})

    @staticmethod
    async def on_stream(**kwargs):
        """
        Arguments
        ---------
        db: ubud.redis.Database
            ubud object for redis db
        offset: str
            <miliisecond timestamp>-<seq> of redis stream, ex. "1657962650216-1"
            필요 시 offset으로부터 timestamp을 얻을 수 있기 때문에 포함시킴
        data: dict
            stream data, {name: value}
        """
        return

    @staticmethod
    def _get_offset():
        return str(int(time.time() * 1e3)) + "-0"

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return x
        if isinstance(x, (list, tuple)):
            return x
        return [_.strip() for _ in x.split(",")]
