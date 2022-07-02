import sys
import zmq
import logging
from logging import Handler

logger = logging.getLogger(__name__)

DEFAULT_FORMATTERS = {
    logging.DEBUG: logging.Formatter("%(levelname)s %(name)s:%(lineno)d - %(message)s"),
    logging.INFO: logging.Formatter("%(levelname)s %(name)s - %(message)s"),
    logging.WARN: logging.Formatter("%(levelname)s %(name)s:%(lineno)d - %(message)s"),
    logging.ERROR: logging.Formatter("%(levelname)s %(name)s:%(lineno)d - %(message)s"),
    logging.CRITICAL: logging.Formatter("%(levelname)s %(name)s:%(lineno)d - %(message)s"),
}


class ZmqHandler(Handler):
    def __init__(
        self,
        host: str = "localhost",
        port: int = 54321,
    ):
        super().__init__()

        self.host = host
        self.port = port

        self.formatters = DEFAULT_FORMATTERS

        context = zmq.Context()
        self.socket = context.socket(zmq.REQ)
        self.socket.connect(f"tcp://{self.host}:{self.port}")

    def format(self, record):
        """Format a record."""
        return self.formatters[record.levelno].format(record)

    def emit(self, record):
        msg = self.format(record).encode("utf-8")
        self.socket.send(msg)
        r = self.socket.recv()
        assert r == b"0", "Log Aggregator Disconnnected!"


if __name__ == "__main__":

    port = sys.argv[1]

    handler = ZmqHandler(port=port)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    logger.debug("I'M DEBUG")
    logger.info("I'M INFO")
    logger.warning("I'M WARNING")
    logger.error("I'M ERROR")
