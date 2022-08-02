import redis.asyncio as redis
from fnmatch import fnmatch
import json
import logging
import asyncio
from ..models import Message

logger = logging.getLogger(__name__)


class Database:
    """
    Redis Stream의 Stream(Topic)을 Database인 것처럼 접근할 수 있게 하는 Helper.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        redis_topic: str = "ubud",
        strip_prefix: bool = False,
        hide_metric: bool = True,
    ):
        # props
        self.redis_client = redis_client
        self.redis_topic = redis_topic

        # hide _*
        self.strip_prefix = strip_prefix
        self.hide_metric = hide_metric

        # client and keys
        self._stream_key = f"{self.redis_topic}/keys"
        self._streams = dict()

        # trick async_init
        self._initialized = False

    async def streams(self, patterns=None):
        streams = await self.redis_client.smembers(self._stream_key)
        if patterns is None:
            return sorted(streams)
        return [k for key in self._ensure_list(patterns) for k in streams if fnmatch(k, key)]

    async def xget(self, key):
        msg = await self._xget(key)
        if not msg:
            return {}
        return msg["value"]

    async def mxget(self, patterns: list = None):
        # search keys matched
        keys = await self.streams(patterns=patterns)
        # get items
        messages = await asyncio.gather(*[self._xget(key) for key in keys])
        return {msg["key"]: msg["value"] for msg in messages if msg is not None}

    async def _xget(self, key):
        messages = await self.redis_client.xrevrange(key, count=1)
        if len(messages) > 0:
            for offset, msg in messages:
                if self.hide_metric:
                    return {"key": key, "value": {k: v for k, v in msg.items() if not k.startswith("_")}}
                return {"key": key, "value": msg}

    # [BALANCE]
    async def balance(self, market, symbol):
        _path = "exchange/balance"
        key = "/".join([self.redis_topic, _path, market, symbol])
        return await self.xget(key)

    async def balances(self, market="*", symbol="*"):
        _path = "exchange/balance"
        market, symbol = self._split(market, symbol)
        values = await self.mxget(["/".join([self.redis_topic, _path, m, s]) for m in market for s in symbol])
        if not self.strip_prefix:
            return values
        return {self._strip_prefix(k, _path): v for k, v in values.items()}

    # [TRADE]
    async def trade(self, market, symbol, currency, orderType):
        _path = "quotation/trade"
        key = "/".join([self.redis_topic, _path, market, symbol, currency, orderType, "0"])
        return await self.xget(key)

    async def trades(self, market="*", symbol="*", currency="*", orderType="*"):
        _path = "quotation/trade"
        market, symbol, currency, orderType = self._split(market, symbol, currency, orderType)
        values = await self.mxget(
            [
                "/".join([self.redis_topic, _path, m, s, c, o, "0"])
                for m in market
                for s in symbol
                for c in currency
                for o in orderType
            ]
        )
        if not self.strip_prefix:
            return values
        return {self._strip_prefix(k, _path).replace("/0", ""): v for k, v in values.items()}

    # [ORDERBOOK]
    async def orderbook(self, market, symbol, currency, orderType, rank=1):
        _path = "quotation/orderbook"
        key = "/".join([self.redis_topic, _path, market, symbol, currency, orderType, str(rank)])
        return await self.xget(key)

    async def orderbooks(self, market="*", symbol="*", currency="*", orderType="*", rank="*", max_rank=None):
        _path = "quotation/orderbook"
        if max_rank is not None:
            rank = ",".join([str(i + 1) for i in range(max_rank)])
        market, symbol, currency, orderType, rank = self._split(market, symbol, currency, orderType, str(rank))
        values = await self.mxget(
            [
                "/".join([self.redis_topic, _path, m, s, c, o, r])
                for m in market
                for s in symbol
                for c in currency
                for o in orderType
                for r in rank
            ]
        )
        if not self.strip_prefix:
            return values
        return {self._strip_prefix(k, _path): v for k, v in values.items()}

    # [PREMIUM]
    async def premium(self, market, symbol, currency, orderType, rank="1-1"):
        _path = "premium/orderbook"
        key = "/".join([self.redis_topic, _path, market, symbol, currency, orderType, str(rank)])
        return await self.xget(key)

    async def premiums(self, market="*", symbol="*", currency="*", orderType="*", rank="*"):
        _path = "premium"
        market, symbol, currency, orderType, rank = self._split(market, symbol, currency, orderType, str(rank))
        values = await self.mxget(
            ["/".join([self.redis_topic, _path, m, s, c]) for m in market for s in symbol for c in currency]
        )
        if not self.strip_prefix:
            return values
        return {self._strip_prefix(k, _path): v for k, v in values.items()}

    # [FOREX]
    async def forex(self, codes="FRX.KRWUSD"):
        _path = f"forex/{codes}"
        key = "/".join([self.redis_topic, _path])
        return await self.xget(key)

    def _strip_prefix(self, x, _path):
        _prefix = "/".join([self.redis_topic, _path])
        return x.replace(_prefix, "").strip("/")

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
