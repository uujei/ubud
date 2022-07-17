import abc
import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Callable

import redis.asyncio as redis

from ..const import DATETIME, KST
from .bithumb import BithumbApi
from .forex import ForexApi
from .ftx import FtxApi
from .upbit import UpbitApi

logger = logging.getLogger(__name__)


################################################################
# Abstract
################################################################
# Abstract Updater
class Updater(abc.ABC):
    def __init__(
        self,
        interval: float = 0.5,
        handler: Callable = None,
    ):
        self.interval = interval
        self.handler = handler

    async def run(self):
        _n_retry = 0
        while True:
            try:
                while True:
                    # get records
                    try:
                        records = await self.request(**self.REQUEST_ARGS)
                        logger.debug(f"[UPDATER] API Response: {records}")
                    except Exception as ex:
                        logger.warning(f"[UPDATER] API Request Failed - {ex}")
                        traceback.print_exc()
                        raise

                    # handler
                    if self.handler is not None:
                        try:
                            # [NOTE] ubud의 handler들은 list를 입력으로 받음
                            messages = await self.parser(records)
                        except Exception as ex:
                            logger.error(f"[FOREX] Parse Error - {ex}")
                            traceback.print_exc()
                        try:
                            await self.handler(messages)
                        except Exception as ex:
                            logger.error(f"[FOREX] Handler Failed - {ex}")

                    # wait
                    if self.interval is not None:
                        await asyncio.sleep(self.interval)

                    _n_retry = 0
                    await asyncio.sleep(self.interval)

            except Exception as ex:
                _n_retry += 1
                if _n_retry > 10:
                    logger.error(f"[BALANCE] FAILED 10 Times, Stop Taks! - {ex}")
                    raise
                await asyncio.sleep(1)

    @abc.abstractmethod
    async def parser(self, record):
        return {
            "name": None,
            "value": None,
        }


################################################################
# Forex Updater
################################################################
# Only ForexUpdater
class ForexUpdater(Updater, ForexApi):
    REQUEST_ARGS = {}

    def __init__(
        self,
        codes: str = "FRX.KRWUSD",
        interval: float = 10.0,
        handler: Callable = None,
    ):
        # super
        Updater.__init__(self)
        ForexApi.__init__(self)

        # properties
        self.codes = codes
        self.interval = interval
        self.handler = handler

    async def parser(self, records):
        messages = []
        for r in records:
            msg = {
                "name": "/".join([self.route, self.codes]),
                "value": {
                    DATETIME: datetime.fromtimestamp(r["timestamp"] / 1e3)
                    .astimezone(KST)
                    .isoformat(timespec="milliseconds"),
                    **{k: v for k, v in r.items() if k.endswith("Price")},
                },
            }
            logger.debug(f"[UPDATER] FOREX Message Parsed: {msg}")
            messages += [msg]

        return messages


################################################################
# Forex Updater
################################################################
# Abstract BalanceUpdater
class BalanceUpdater(Updater):
    def __init__(
        self,
        apiKey: str,
        apiSecret: str,
        symbols: list,
        interval: float = 0.6,
        handler: Callable = None,
    ):
        Updater.__init__(self)
        UpbitApi.__init__(self)

        self.apiKey = apiKey
        self.apiSecret = apiSecret
        self.symbols = symbols
        self.interval = interval
        self.handler = handler


# Upbit
class UpbitBalanceUpdater(BalanceUpdater, UpbitApi):
    MARKET = "upbit"
    REQUEST_ARGS = {
        "path": "/accounts",
    }

    async def parser(self, records):
        messages = []
        for r in records:
            symbol = r["currency"]
            if self.symbols is not None and symbol not in ["KRW", *self.symbols]:
                continue
            free = float(r["balance"])
            locked = float(r["locked"])
            msg = {
                "name": f"exchange/balance/{self.MARKET}/{symbol}",
                "value": {
                    DATETIME: str(datetime.now().astimezone(KST).isoformat(timespec="microseconds")),
                    "total": free + locked,
                    "locked": locked,
                    "free": free,
                },
            }

            logger.debug(f"[UPDATER] Upbit Balance Message Parsed: {msg}")
            messages += [msg]

        return messages


# Bithumb
class BithumbBalanceUpdater(BalanceUpdater, BithumbApi):
    MARKET = "bithumb"
    REQUEST_ARGS = {
        "path": "/info/balance",
        "currency": "ALL",
    }
    MAP = {
        "total": "total",
        "available": "free",
        "in_use": "locked",
    }

    async def parser(self, records):
        holder = dict()
        for k, v in records.items():
            cat, symbol = k.rsplit("_", 1)
            if symbol.upper() not in ["KRW", *self.symbols] or cat not in self.MAP.keys():
                continue
            if symbol not in holder.keys():
                holder[symbol] = dict()
            holder[symbol].update({self.MAP[cat]: float(v)})

        messages = []
        for symbol, value in holder.items():
            value.update({DATETIME: str(datetime.now().astimezone(KST).isoformat(timespec="microseconds"))})
            msg = {
                "name": f"exchange/balance/{self.MARKET}/{symbol.upper()}",
                "value": value,
            }

            logger.debug(f"[UPDATER] Bithumb Balance Message Parsed: {msg}")
            messages += [msg]

        return messages


# FTX
class FtxBalanceUpdater(BalanceUpdater, FtxApi):
    MARKET = "ftx"
    REQUEST_ARGS = {
        "path": "/wallet/balances",
    }

    async def parser(self, records):
        messages = []
        for r in records:
            msg = {
                "name": f"exchange/balance/{self.MARKET}/{r['coin']}",
                "value": {
                    DATETIME: str(datetime.now().astimezone(KST).isoformat(timespec="microseconds")),
                    "total": float(r["total"]),
                    "locked": float(r["total"]) - float(r["free"]),
                    "free": float(r["free"]),
                },
            }

            logger.debug(f"[UPDATER] FTX Balance Message Parsed: {msg}")
            messages += [msg]

        return messages


################################################################
# DEBUG
################################################################
if __name__ == "__main__":
    import sys

    from clutter.aws import get_secrets

    from ..redis.handler import RedisSetHandler, RedisStreamHandler

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

        redis_client = redis.Redis(decode_responses=True)
        for target in sys.argv[1:]:
            conf = {
                "interval": 0.1,
                "handler": RedisSetHandler(redis_client=redis_client),
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
