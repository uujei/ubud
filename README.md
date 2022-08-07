## Ubud



####  Update

2022.8.7	influxdb sink connector 리소스 이슈로 multi-processing으로 변경 (ray)

**2022.7.17	conf.yml 사용 안 함, KEY와 SECRET들만 환경변수 또는 .env로 저장**

2022.7.16	리소스 이슈로 구조 수정 (CPU 사용률 절반으로 줄임)

2022.7.14	conf.yml 형식 수정

2022.7.14	InfluxDB Sink 작성 및 테스트

2022.7.14	Orderbook 레코드에 Rank 포함 (Rank 포함 안 되어 있어서, 후순위 호가가 선순위를 계속 덮어쓰고 있었음)

2022.7.12	서버측 문제로 API 끊어질 경우 (수 시간에 한 번씩 발생) 다시 시도하도록 BalanceUpdater 수정

2022.7.12	Database에 trade, ordrebook, balance, forex 메소드 추가 (사용하기 쉽도록)

2022.7.11	Websocket 끊어지는 문제 해결 (ping_timeout 제거)

2022.7.11	속도 개선 (비동기 Loop를 asyncio native에서 uvloop으로 교체)



#### TO DO

주문 처리기 개발



#### Stream 구조

  **(참고) default topic은 ubud**

  **환율   {topic}/forex**

  **잔고   {topic}/balance/{market}/{symbol}**

​    . market: upbit, bithumb, ftx

​    . symbol: BTC, ETH, WAVES, ...

  **현재가   {topic}/quotation/{channel}/{market}/{symbol}/{currency}/{orderType}/{rank}**

​    . channel: orderbook, trade

​    . market: upbit, bithumb, ftx

​    . symbol: BTC, ETH, WAVES, ...

​    . currency: KRW, USD, PERP, KRW.USD   * KRW.USD는 USD를 KRW로 환산한 값

​    . orderType: ask, bid

​    . rank: 0, 1, 2, ...   * trade의 rank는 항상 0, orderbook은 1, 2, 3, ...

  **프리미엄   {topic}/premium/{channel}/{market}/{symbol}/{currency}**

​    . channel: orderbook, trade   * 현재 orderbook 1순위 및 trade로 premium 계산하고 있음

​    . market: upbit.bithumb, upbit,ftx, bithumb.upbit, bithumb.ftx, ...   * premium A.B = A 거래소의 bid 가격을 B 거래소의 ask 가격으로 나눈 것

​    . symbol: BTC, ETH, WAVES, ...

​    . currency: KRW   * 현재는 KRW 기준으로만



#### 설치 방법

```bash
# Redis 설치
$ sudo apt install redis

# PONG 출력되면 잘 설치된 것
$ redis-cli ping

# 이 Repository Clone 및 설치
$ git clone https://github.com/uujei/ubud.git
$ cd ubud
$ pip install -e .

# 실행하기
# 환경변수 지정 필요 (UPBIT_API_KEY, UPBIT_API_SECRET, BITHUMB_API_*, FTX_API_*)
# 실행 위치에 .env를 두고 실행해도 됨
$ ubud start-stream

# stream 가동 후 다른 터미널에서 필요한 작업 수행
# (참고) stream 잘 되는 지 확인하고 싶으면 --log-level INFO 또는 DEBUG로 가동
$ ubud start-stream --log-level INFO

# (참고) Background에서 가동하고 싶으면
$ nohup ubud start-stream &> ubud.log &
```



#### Database 사용 방법

```python
import redis.asyncio as redis
from ubud.redis.db import Database

# 스트림은 별도 프로세스로 가동 중이어야 함
# (참고) 터미널에서 "ubud start-stream" 실행한 상태

redis_client = redis.Redis(decode_responses=True)
db = Database(redis_client=redis_client)

# 모든 스트림 조회하기
await db.streams()

# 모든 Balance 조회하기
await db.balances()

# 모든 Orderbook 조회하기
await db.orderbooks()

# 모든 Trade 조회하기
await db.trades()

# 모든 Premiums 조회하기
await db.premiums()

# 환율 조회하기
await db.forex()

# 필터 활용 - balances, orderbooks, trades 모두 필터 지원
# 업비트와 FTX의 BTC에 대한 최신 Trade 조회하기
await db.trades(market="upbit,ftx", symbol="BTC")

# 업비트와 빗썸의 WAVES에 대한 최신 Orderbook을 3순위까지 조회하기
await db.orderbooks(market="upbit,bithumb", symbol="WAVES", max_rank=3)

```
