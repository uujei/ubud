import asyncio
import json
import logging

import redis.asyncio as redis

from .bithumb import BithumbApi
from .ftx import FtxApi
from .upbit import UpbitApi

logger = logging.getLogger(__name__)


class BalanceUpdater:
    def __init__(
        self,
        apiKey: str,
        apiSecret: str,
        symbols: list = None,
        interval: float = 0.5,
        redis_client: redis.Redis = None,
        redis_topic: str = "ubud",
        redis_expire_sec: int = 120,
        redis_xadd_maxlen: bool = 10,
        redis_xadd_approximate: bool = False,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.symbols = symbols
        self.interval = interval
        self.redis_client = redis_client
        self.redis_topic = redis_topic
        self.redis_expire_sec = redis_expire_sec
        self.redis_xadd_maxlen = redis_xadd_maxlen
        self.redis_xadd_approximate = redis_xadd_approximate

        self.prefix = f"{redis_topic}/exchange/balance"
        self.stream_prefix = f"{redis_topic}-stream/exchange/balance"

        # key registry
        self._redis_stream_names_key = f"{redis_topic}-stream/keys"
        self._redis_stream_names = None

    async def run(self):
        # get stream names
        self._redis_stream_names = await self.redis_client.smembers(self._redis_stream_names_key)

        # register key
        try:
            while True:
                # get records
                records = await self.get()
                logger.debug(f"[BALANCE] HTTP Response: {records}")
                await asyncio.gather(*[self.xadd(k, v) for k, v in records.items()])
                if self.interval is not None:
                    await asyncio.sleep(self.interval)
        except Exception as ex:
            logger.error(ex)
            raise (ex)

    async def xadd(self, k, v):
        try:
            stream_name = f"{self.stream_prefix}/{k}"
            msg = {"name": stream_name, "fields": {"name": f"{self.prefix}/{k}", "value": v}}
            await self.redis_client.xadd(
                **msg,
                maxlen=self.redis_xadd_maxlen,
                approximate=self.redis_xadd_approximate,
            )
            logger.debug(f"[BALANCE] XADD {msg['name']} {msg['fields']}")
            if stream_name not in self._redis_stream_names:
                await self.redis_client.sadd(self._redis_stream_names_key, stream_name)
                self._redis_stream_names = await self.redis_client.smembers(self._redis_stream_names_key)
        except Exception as ex:
            logger.warning(ex)


class UpbitBalanceUpdater(BalanceUpdater, UpbitApi):
    MARKET = "upbit"
    ARGS = {
        "path": "/accounts",
    }

    async def get(self):
        balances = await self.request(**self.ARGS)
        results = dict()
        _ = [results.update(self._parser(b)) for b in balances]
        return results

    def _parser(self, bal):
        symbol = bal["currency"]
        if self.symbols is not None and symbol not in self.symbols:
            return {}
        free = float(bal["balance"])
        locked = float(bal["locked"])
        return {f"{self.MARKET}/{symbol}": json.dumps({"total": free + locked, "locked": locked, "free": free})}


class BithumbBalanceUpdater(BalanceUpdater, BithumbApi):
    MARKET = "bithumb"
    ARGS = {
        "path": "/info/balance",
        "currency": "ALL",
    }
    MAP = {
        "total": "total",
        "available": "free",
        "in_use": "locked",
    }

    async def get(self):
        balances = await self.request(**self.ARGS)
        if self.symbols is not None:
            if "KRW" not in self.symbols:
                self.symbols.append("KRW")
            balances = {k: v for k, v in balances.items() if any([k.endswith(f"_{s.lower()}") for s in self.symbols])}
        results = dict()
        for k, v in balances.items():
            name, field_key = self._parser(k)
            if field_key is not None:
                if name not in results.keys():
                    results[name] = dict()
                results[name].update({field_key: v})
        return {k: json.dumps(v) for k, v in results.items()}

    def _parser(self, key):
        cat, symbol = key.rsplit("_", 1)
        return f"{self.MARKET}/{symbol.upper()}", self.MAP.get(cat)


class FtxBalanceUpdater(BalanceUpdater, FtxApi):
    MARKET = "ftx"
    ARGS = {
        "path": "/wallet/balances",
    }

    async def get(self):
        balances = await self.request(**self.ARGS)
        results = dict()
        _ = [results.update(self._parser(b)) for b in balances]
        return results

    def _parser(self, bal):
        symbol = bal["coin"]
        if self.symbols is not None and symbol not in self.symbols:
            return {}
        total = float(bal["total"])
        free = float(bal["free"])
        return {f"{self.MARKET}/{symbol}": json.dumps({"total": total, "locked": total - free, "free": free})}


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import sys

    from clutter.aws import get_secrets

    secrets = get_secrets("theone")
    logging.basicConfig(level=logging.DEBUG)

    redis_client = redis.Redis(decode_responses=True)

    async def tasks():
        coros = []

        upbit_updater = UpbitBalanceUpdater(
            apiKey=secrets["ubk"],
            apiSecret=secrets["ubs"],
            interval=0.02,
            redis_client=redis_client,
            redis_topic="ubud",
        )
        coros += [upbit_updater.run()]

        bithumb_updater = BithumbBalanceUpdater(
            apiKey=secrets["btk"],
            apiSecret=secrets["bts"],
            symbols=["KRW", "BTC"],
            interval=0.05,
            redis_client=redis_client,
            redis_topic="ubud",
        )
        coros += [bithumb_updater.run()]

        ftx_updater = FtxBalanceUpdater(
            apiKey=secrets["ftk"],
            apiSecret=secrets["fts"],
            interval=0.01,
            redis_client=redis_client,
            redis_topic="ubud",
        )
        coros += [ftx_updater.run()]

        await asyncio.gather(*coros)

    asyncio.run(tasks())
