import json
import logging
from time import time

from .common import (
    AbsConnector,
    ASK,
    BID,
    DATETIME,
    TRADE,
    TICKER,
    MARKET,
    ORDERBOOK,
    ORDERTYPE,
    PRICE,
    QUANTITY,
    TRADE_DATETIME,
    TRADE_SID,
    CURRENCY,
    SYMBOL,
    TYPE,
    ts_to_strdt,
)

logger = logging.getLogger(__name__)

THIS_MARKET = "UPBIT"
URL = "wss://api.upbit.com/websocket/v1"
TYPE_PARAMS = {
    TICKER: "ticker",
    TRADE: "trade",
    ORDERBOOK: "orderbook",
}

# after receive 1st message
def register(
    ws,
    quote: str,
    symbols: list,
    isOnlyRealtime: bool = True,
    ticket: str = "test",
):
    assert quote in TYPE_PARAMS.keys(), f"[ERROR] unknown type {quote}"
    params = json.dumps(
        [
            {"ticket": ticket},
            {
                "type": TYPE_PARAMS[quote],
                "codes": symbols if isinstance(symbols, list) else [symbols],
                "isOnlyRealtime": isOnlyRealtime,
            },
            {"format": "SIMPLE"},
        ]
    )
    ws.send(params)
    logger.info(f"[WEBSOCKET] Register Params: {params}")


# Handler Class
class Connector(AbsConnector):

    # TRADE
    def _call_trade(self, ws, msg):
        # message in
        _received_dt = ts_to_strdt(time(), _float=True)
        logger.info(f"[WEBSOCKET] Receive Message from TRADE {_received_dt}")

        # load body
        try:
            body = json.loads(msg)
            logger.debug(f"[WEBSOCKET] Body: {body}")
            messages = [
                {
                    DATETIME: ts_to_strdt(int(body["tms"]) / 1e3),
                    MARKET: THIS_MARKET,
                    TYPE: self.type,
                    **self._parse_symbol(body["cd"]),
                    TRADE_SID: body["sid"],
                    TRADE_DATETIME: ts_to_strdt(int(body["ttms"]) / 1e3),
                    ORDERTYPE: body["ab"].lower(),
                    PRICE: body["tp"],
                    QUANTITY: body["tv"],
                }
            ]
            if self.broker is not None:
                self.publish(messages)
        except Exception as ex:
            logger.warn(f"[Connector.__call__] {ex}")

    # ORDERBOOK
    def _call_orderbook(self, ws, msg):
        # message in
        _received_dt = ts_to_strdt(time(), _float=True)
        logger.info(f"[WEBSOCKET] Receive Message from ORDERBOOK {_received_dt}")

        # load body
        body = json.loads(msg)
        logger.debug(f"[WEBSOCKET] Body: {body}")

        # parse and pub
        if "obu" in body.keys():
            # base message
            msg = {
                DATETIME: ts_to_strdt(int(body["tms"]) / 1e3),
            }
            base_msg = {
                DATETIME: ts_to_strdt(int(body["tms"]) / 1e3),
                MARKET: THIS_MARKET,
                TYPE: self.type,
                **self._parse_symbol(body["cd"]),
            }
            # parse and pub
            try:
                for r in body["obu"]:
                    messages = [
                        {**base_msg, ORDERTYPE: ASK, PRICE: r["ap"], QUANTITY: r["as"]},
                        {**base_msg, ORDERTYPE: BID, PRICE: r["bp"], QUANTITY: r["bs"]},
                    ]
                    if self.broker is not None:
                        self.publish(messages)
                # Additional Info (total_ask_size, total_bid_size)
                base_msg[TYPE] = f"{base_msg[TYPE]}_total_qty"
                messages = [
                    {**base_msg, ORDERTYPE: ASK, QUANTITY: body["tas"]},
                    {**base_msg, ORDERTYPE: BID, QUANTITY: body["tbs"]},
                ]
                if self.broker is not None:
                    self.publish(messages)
            except Exception as ex:
                logger.warn(f"[Connector.__call__] {ex}")

    @staticmethod
    def _parse_symbol(symbol):
        DELIM = "-"
        if DELIM in symbol:
            cur, symbol = symbol.split(DELIM)
            return {SYMBOL: symbol, CURRENCY: cur}
        return {SYMBOL: symbol, CURRENCY: "unknown"}
