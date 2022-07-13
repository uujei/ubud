import logging

logger = logging.getLogger(__name__)

# 김프 구하기
async def get_gimp(db, symbol, channel="orderbook", orderType="bid", redis_topic="ubud", korea="upbit", usa="ftx"):
    # Upbit의 {symbol} 가격
    x = await db.get(f"{redis_topic}/quotation/{channel}/{korea}/{symbol}/KRW/{orderType}")
    krw_per_coin_korea = x["price"]

    # FTX의 {symbol} 가격 (원화 환산)
    x = await db.get(f"{redis_topic}/forex/FRX.KRWUSD")
    krw_per_usd = x["basePrice"]

    x = await db.get(f"{redis_topic}/quotation/{channel}/{usa}/{symbol}/USD/{orderType}")
    usd_per_coin_usa = x["price"]
    krw_per_coin_usa = usd_per_coin_usa * krw_per_usd

    gimp = krw_per_coin_korea / krw_per_coin_usa
    logger.debug(f"{korea.upper():7s} ({orderType}): {krw_per_coin_korea:.1f} KRW/{symbol}")
    logger.debug(f"{usa.upper():7s} ({orderType}): {krw_per_coin_usa:.1f} KRW/{symbol}")
    logger.debug(f"GIMP!   ({orderType}): {gimp:.6f} {symbol}/{symbol}")

    # 김프
    return gimp


if __name__ == "__main__":
    import asyncio
    import logging
    import redis.asyncio as redis
    from ubud.redis.db import Database

    logging.basicConfig(level=logging.DEBUG)

    r = redis.Redis(decode_responses=True)
    db = Database(redis_client=r)

    asyncio.run(get_gimp(db, "BTC"))
