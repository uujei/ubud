import asyncio
import sys
import logging
import redis.asyncio as redis
from ubud.redis.db import Database

logger = logging.getLogger(__name__)

r = redis.Redis(decode_responses=True)
db = Database(redis_client=r)

# 김프 구하기
async def get_gimp(symbol, _print=True):
    # Upbit의 BTC 가격
    x = await db.get(f"ubud/quotation/orderbook/upbit/{symbol}/KRW/bid")
    krw_btc_upbit = x["price"]

    # FTX의 BTC 가격 (원화 환산)
    x = await db.get("ubud/forex/FRX.KRWUSD")
    krw_per_usd = x["basePrice"]
    x = await db.get(f"ubud/quotation/orderbook/ftx/{symbol}/USD/bid")
    usd_btc_ftx = x["price"]
    krw_btc_ftx = usd_btc_ftx * krw_per_usd

    # 김프
    gimp = krw_btc_upbit / krw_btc_ftx
    if _print:
        logger.info(f"UPBIT : {krw_btc_upbit:.1f} KRW/{symbol}")
        logger.info(f"FTX   : {krw_btc_ftx:.1f} KRW/{symbol}")
        logger.info(f"GIMP! : {gimp:.3f} {symbol}/{symbol}")

    return gimp


async def main(symbol, min_gimp):
    # [NOTE] REDIS STREAM을 Trigger로 사용하여 데이터 업데이트 있을 때만 작동!
    # 설정 값 넘기는 것 10번 보고 종료
    # 설정 값 안 넘으면 1,000회 중 최대 김프 반환
    logging.info(f"[START] Alarm if {symbol}'s gimp exceed {min_gimp}")

    i = 0
    j = 0
    max_gimp = 0
    while True:
        stream = await r.xread({f"ubud-stream/quotation/orderbook/ftx/{symbol}/USD/bid": "$"}, count=1, block=1)
        if len(stream) > 0:
            gimp = await get_gimp(symbol=symbol, _print=True)
            if gimp > min_gimp:
                j += 1
                logger.info("########################################")
                logger.info(f"#### SUPER GIMP {j:02d} TIMES! - {gimp:.4f} #####")
                logger.info("########################################")
                if j > 9:
                    break
            if gimp > max_gimp:
                max_gimp = gimp
            i += 1
            if j > 999:
                logger.info(f"NO SUPER GIMP TODAY! - max gimp is {max_gimp}")
                break
            await asyncio.sleep(0.001)


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    symbol = sys.argv[1].upper()
    min_gimp = float(sys.argv[2])

    asyncio.run(main(symbol, min_gimp))
