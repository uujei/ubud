## Ubud



####  Update

2022.7.11   Websocket 끊어지는 문제 해결 (ping_timeout 제거)

2022.7.11  속도 개선 (비동기 Loop를 asyncio native에서 uvloop으로 교체)



#### TO DO

Configuration (conf.yml) 좀 더 명확하게

주문 처리기 개발



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
# (참고 1.) conf.yml이 있는 폴더에서 실행해야 함, conf.yml 안에 받아올 데이터들 정의되어 있음.
# (참고 2.) UPBIT_API_KEY, UPBIT_API_SECRET, BITHUMB_API_*, FTX_API_*, ... 있는 .env 작성 필요.
$ ubud start-stream

# stream 가동 후 다른 터미널에서 필요한 작업 수행
# (참고) stream 잘 되는 지 확인하고 싶으면 --log-level INFO 또는 DEBUG로 가동
$ ubud start-stream --log-level INFO

# (참고) Background에서 가동하고 싶으면
$ nohup ubud start-stream &> ubud.log &
```



