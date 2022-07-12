import redis.asyncio as redis
from fnmatch import fnmatch
import json
import logging
import asyncio

logger = logging.getLogger(__name__)


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
        self.redis_keys_key = f"{self.redis_topic}/keys"

        # trick async_init
        self._initialized = False

    async def get(self, key, drop_ts=True):
        value = await self._get(key, drop_ts=drop_ts)
        if value is None:
            return
        return value[-1]

    async def mget(self, patterns: list = None, drop_ts=True):
        # search keys matched
        keys = await self.keys(patterns=patterns)
        # get items
        kvs = await asyncio.gather(*[self._get(key, drop_ts=drop_ts) for key in keys])
        return {k: v for k, v in [kv for kv in kvs if kv is not None]}

    async def _get(self, key, drop_ts=True):
        value = await self.redis_client.get(key)
        if value is None:
            logger.warning(f"[DB] Empty Value for '{key}'!")
            return
        value = json.loads(value)
        if drop_ts:
            value = {k: v for k, v in value.items() if not k.startswith("_ts")}
        return (key, value)

    async def keys(self, patterns=None):
        registered_keys = await self.redis_client.smembers(self.redis_keys_key)
        if patterns is None:
            return sorted(registered_keys)
        return [k for key in self._ensure_list(patterns) for k in registered_keys if fnmatch(k, key)]

    async def balance(self, market="*", symbol="*"):
        _prefix = f"{self.redis_topic}/exchange/balance/"
        market, symbol = self._split(market, symbol)
        values = await self.mget([f"{_prefix}{m}/{s}" for m in market for s in symbol])
        return {k.replace(_prefix, ""): v for k, v in values.items()}

    async def orderbook(self, market="*", symbol="*", currency="*", orderType="*"):
        _prefix = f"{self.redis_topic}/quotation/orderbook/"
        market, symbol, currency, orderType = self._split(market, symbol, currency, orderType)
        values = await self.mget(
            [f"{_prefix}{m}/{s}/{c}/{o}" for m in market for s in symbol for c in currency for o in orderType]
        )
        return {k.replace(_prefix, ""): v for k, v in values.items()}

    async def trade(self, market="*", symbol="*", currency="*", orderType="*"):
        _prefix = f"{self.redis_topic}/quotation/trade/"
        market, symbol, currency, orderType = self._split(market, symbol, currency, orderType)
        values = await self.mget(
            [f"{_prefix}{m}/{s}/{c}/{o}" for m in market for s in symbol for c in currency for o in orderType]
        )
        return {k.replace(_prefix, ""): v for k, v in values.items()}

    async def forex(self, code="FRX.KRWUSD", prop="basePrice"):
        _prefix = f"{self.redis_topic}/forex/"
        code = self._split(code)
        values = await self.mget([f"{_prefix}{c}" for c in code])
        return {k.replace(_prefix, ""): v for k, v in values.items()}

    @staticmethod
    def _ensure_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple)):
            return x
        if isinstance(x, str):
            return [x]
        raise ReferenceError("_ensure_list: x should be a list, tuple, str or None!")

    @staticmethod
    def _split(*x):
        return tuple([__x.strip() for __x in _x.split(",")] for _x in x)
