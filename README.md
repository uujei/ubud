
$ sudo apt install redis

$ redis-cli ping
PONG 출력되면 잘 설치된 것

$ git clone https://github.com/uujei/ubud.git

$ cd ubud

$ pip install -e .

$ ubud start-stream
(참고) conf.yml이 있는 폴더에서 실행해야 함, conf.yml 안에 받아올 데이터들 정의되어 있음.

stream 가동 후 다른 터미널에서 필요한 작업 수행
stream 잘 되는 지 확인하고 싶으면 --log-level DEBUG로 가동
$ ubud start-stream --log-level BEBUG