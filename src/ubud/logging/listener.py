import zmq

import logging


logger = logging.getLogger(__name__)


class ZmqLogAggregator:
    def __init__(
        self,
        host: str = "*",
        handler=logging.StreamHandler(),
    ):
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        self.host = host

        context = zmq.Context()
        self.server = context.socket(zmq.REP)

        # rep
        self._server_port = self.server.bind_to_random_port(f"tcp://{host}")
        logger.info(f"[LOGA] Running server on port: {self._server_port}")

    def run(self):
        try:
            while True:
                msg = self.server.recv()
                msg = msg.decode("utf-8").strip("\n")
                print(msg)
                self.server.send(b"0")
        except KeyboardInterrupt:
            logger.error("[LOGA] Keyboard Interrupted!")
        finally:
            self.dealer.close()


if __name__ == "__main__":
    log_aggregator = ZmqLogAggregator()
    log_aggregator.run()
