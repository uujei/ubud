import asyncio
import json
import logging
import traceback
from datetime import datetime

import redis.asyncio as redis

from ..const import KST
from .bithumb import BithumbApi
from .forex import ForexApi
from .ftx import FtxApi
from .upbit import UpbitApi

logger = logging.getLogger(__name__)


################################################################
# Forex Updater
################################################################
class ForexUpdater(ForexApi):
    async def run(self):
        # register key
        await self.redis_client.sadd(self._redis_stream_names_key, self._redis_stream_name)

        _n_retry = 0
        while True:
            try:
                while True:
                    # get record
                    records = await self.request()
                    logger.debug(f"[FOREX] get records: {records}")

                    # stream
                    await asyncio.gather(*[self.xadd(r) for r in records])

                    # sleep
                    if self.interval is not None:
                        await asyncio.sleep(self.interval)
            except Exception as ex:
                logger.warning(f"[FOREX] Connection Error - {ex}")
                _n_retry += 1
                if _n_retry > 10:
                    logger.error(f"[FOREX] Connection Error 10 Times! - {ex}")
                    raise ex
                await asyncio.sleep(1)

    async def xadd(self, record):
        try:
            msg = {
                "name": self._redis_stream_name,
                "fields": {"name": self._redis_field_key, "value": self._parser(record)},
            }
            await self.redis_client.xadd(
                **msg,
                maxlen=self.redis_xadd_maxlen,
                approximate=self.redis_xadd_approximate,
            )
            logger.debug(f"[FOREX] XADD {msg['name']} {msg['fields']}")
        except Exception as ex:
            logger.warning(ex)

    @staticmethod
    def _parser(record):
        return json.dumps(
            {
                "datetime": datetime.fromtimestamp(record["timestamp"] / 1e3)
                .astimezone(KST)
                .isoformat(timespec="milliseconds"),
                **{k: v for k, v in record.items() if k.endswith("Price")},
            }
        )


################################################################
# Balance Updater
################################################################
class BalanceUpdater:
    def __init__(
        self,
        apiKey: str,
        apiSecret: str,
        symbols: list = None,
        interval: float = 0.5,
        redis_client: redis.Redis = None,
        redis_topic: str = "ubud",
        redis_xadd_maxlen: bool = 10,
        redis_xadd_approximate: bool = False,
    ):
        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.symbols = symbols
        self.interval = interval
        self.redis_client = redis_client
        self.redis_topic = redis_topic
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

        _n_retry = 0
        while True:
            try:
                while True:
                    # get records
                    try:
                        records = await self.get()
                        logger.debug(f"[BALANCE] API Response: {records}")
                    except Exception as ex:
                        logger.warning(f"[BALANCE] API Request Failed - {ex}")
                        traceback.print_exc()
                        raise

                    # register key
                    try:
                        await asyncio.gather(*[self.xadd(k, v) for k, v in records.items()])
                    except Exception as ex:
                        logger.warning(f"[BALANCE] REDIS XADD Failed - {ex}")
                        traceback.print_exc()
                        raise

                    # reset retry and sleep
                    _n_retry = 0
                    await asyncio.sleep(self.interval)

            except Exception as ex:
                _n_retry += 1
                if _n_retry > 10:
                    logger.error(f"[BALANCE] FAILED 10 Times, Stop Taks! - {ex}")
                    raise
                await asyncio.sleep(1)

    async def xadd(self, k, v):
        try:
            # stream name
            stream_name = f"{self.stream_prefix}/{k}"

            # stamp datetime when streaming
            _datetime = datetime.now().astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S.%f%z")
            msg = {
                "name": stream_name,
                "fields": {"name": f"{self.prefix}/{k}", "value": json.dumps({"datetime": _datetime, **v})},
            }
            await self.redis_client.xadd(
                **msg,
                maxlen=self.redis_xadd_maxlen,
                approximate=self.redis_xadd_approximate,
            )
            logger.info(f"[BALANCE] XADD {msg['name']} {msg['fields']}")
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
        if self.symbols is not None and symbol not in ["KRW", *self.symbols]:
            return {}
        free = float(bal["balance"])
        locked = float(bal["locked"])
        return {f"{self.MARKET}/{symbol}": {"total": free + locked, "locked": locked, "free": free}}


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
            balances = {
                k: v for k, v in balances.items() if any([k.endswith(f"_{s.lower()}") for s in ["KRW", *self.symbols]])
            }
        results = dict()
        for k, v in balances.items():
            name, field_key = self._parser(k)
            if field_key is not None:
                if name not in results.keys():
                    results[name] = dict()
                results[name].update({field_key: float(v)})
        return {k: v for k, v in results.items()}

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
        return {
            f"{self.MARKET}/{bal['coin']}": {
                "total": float(bal["total"]),
                "locked": float(bal["total"]) - float(bal["free"]),
                "free": float(bal["free"]),
            }
            for bal in balances
        }


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import sys

    from clutter.aws import get_secrets

    secrets = get_secrets("theone")
    logging.basicConfig(level=logging.DEBUG)

    redis_client = redis.Redis(decode_responses=True)

    UPDATER = {
        "upbit": UpbitBalanceUpdater,
        "bithumb": BithumbBalanceUpdater,
        "ftx": FtxBalanceUpdater,
        "forex": ForexUpdater,
    }

    API_KEY = {
        "upbit": {"apiKey": "ubk", "apiSecret": "ubs"},
        "bithumb": {"apiKey": "btk", "apiSecret": "bts"},
        "ftx": {"apiKey": "ftk", "apiSecret": "fts"},
    }

    async def tasks():
        coros = []

        for target in sys.argv[1:]:
            conf = {
                "interval": 0.1,
                "redis_client": redis_client,
                "redis_topic": "ubud",
            }
            if target not in ["forex"]:
                conf.update(
                    {
                        "apiKey": secrets[API_KEY[target]["apiKey"]],
                        "apiSecret": secrets[API_KEY[target]["apiSecret"]],
                        "symbols": ["BTC", "WAVES"],
                    }
                )

            logger.info(conf)
            coros += [UPDATER[target](**conf).run()]

        await asyncio.gather(*coros)

    asyncio.run(tasks())
