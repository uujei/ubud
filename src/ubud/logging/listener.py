import zmq

import logging


logger = logging.getLogger(__name__)


class ZmqLogAggregator:
    def __init__(
        self,
        host: str = "*",
        port: int = None,
        handler=logging.StreamHandler(),
    ):
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        self.host = host

        context = zmq.Context()
        self.socket = context.socket(zmq.REP)

        if port is not None:
            self.port = port
            self.socket.bind(f"tcp://{host}:{self.port}")
        else:
            self.port = self.socket.bind_to_random_port(f"tcp://{host}")

        logger.info(f"[LOGA] Running server on port: {self.port}")

    def run(self):
        try:
            while True:
                msg = self.socket.recv()
                msg = msg.decode("utf-8").strip("\n")
                print(msg)
                self.socket.send(b"0")
        except KeyboardInterrupt:
            logger.error("[LOGA] Keyboard Interrupted!")
        finally:
            self.dealer.close()


if __name__ == "__main__":
    log_aggregator = ZmqLogAggregator()
    log_aggregator.run()
