import logging
from datetime import datetime
import time

logger = logging.getLogger(__name__)


# 김프 구하기
async def get_gimp(
    db,
    symbol,
    krw_market="upbit",
    usd_market="ftx",
    forex_code="FRX.KRWUSD",
    forex_price="cashBuyingPrice",
    dt_check=True,
    dt_limit_sec=60,
):
    usd_prices = await db.trades(market=usd_market, symbol=symbol, currency="USD")
    krw_prices = await db.trades(market=krw_market, symbol=symbol, currency="KRW")
    krw_per_usd = await db.forex(code=forex_code)

    if any([x is None for x in [usd_prices, krw_prices, krw_per_usd]]):
        return

    results = dict()
    for k, v in usd_prices.items():
        _krw_market_key = k.replace(usd_market, krw_market).replace("USD", "KRW")
        if _krw_market_key in krw_prices.keys():
            _gimp_key = f"premium/{krw_market}/{usd_market}/{symbol}/{_krw_market_key.rsplit('/',1)[-1]}"
            usd_market_krw = v["price"] * krw_per_usd[forex_price]
            krw_market_krw = krw_prices[_krw_market_key]["price"]
            if dt_check:
                _usd_market_dt = datetime.fromisoformat(v["datetime"])
                _krw_market_dt = datetime.fromisoformat(krw_prices[_krw_market_key]["datetime"])
                _timedelta = (_usd_market_dt - _krw_market_dt).total_seconds()
                if abs(_timedelta) > dt_limit_sec:
                    logger.warning(
                        f"[PREMIUM] Datetetime Too Far - {usd_market} minus {krw_market} is {_timedelta} sec"
                    )
                    continue
                if abs(time.time() - min(_usd_market_dt, _krw_market_dt).timestamp()) > dt_limit_sec:
                    continue
            results.update({_gimp_key: krw_market_krw / usd_market_krw})
    return results


# 업비트 프리미엄 / 빗썸 구하기
async def get_upbit_per_bithumb(
    db,
    symbol,
    dt_check=True,
    dt_limit_sec=60,
):
    upbit_prices = await db.trades(market="upbit", symbol=symbol, currency="KRW")
    bithumb_prices = await db.trades(market="bithumb", symbol=symbol, currency="KRW")

    if any([x is None for x in [upbit_prices, bithumb_prices]]):
        return

    results = dict()
    for _upbit_key, _upbit in upbit_prices.items():
        _bithumb_key = _upbit_key.replace("upbit", "bithumb")
        if _bithumb_key in bithumb_prices.keys():
            _bithumb = bithumb_prices[_bithumb_key]
            _ratio_key = f"premium/upbit/bithumb/{symbol}/{_bithumb_key.rsplit('/',1)[-1]}"
            upbit_pr = _upbit["price"]
            bithumb_pr = _bithumb["price"]
            if dt_check:
                _upbit_dt = datetime.fromisoformat(_bithumb["datetime"])
                _bithumb_dt = datetime.fromisoformat(bithumb_prices[_bithumb_key]["datetime"])
                _timedelta = (_upbit_dt - _bithumb_dt).total_seconds()
                if abs(_timedelta) > dt_limit_sec:
                    logger.warning(f"[PREMIUM] Datetetime Too Far - upbit minus bithumb is {_timedelta} sec")
                    continue
                if abs(time.time() - min(_upbit_dt, _bithumb_dt).timestamp()) > dt_limit_sec:
                    continue
            results.update({_ratio_key: upbit_pr / bithumb_pr})
    return results


if __name__ == "__main__":
    import asyncio
    import logging

    import redis.asyncio as redis
    from ubud.redis.db import Database

    logging.basicConfig(level=logging.DEBUG)

    r = redis.Redis(decode_responses=True)
    db = Database(redis_client=r)

    asyncio.run(get_gimp(db, "BTC"))
