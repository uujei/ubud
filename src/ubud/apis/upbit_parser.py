from .models import OrderbookRecord


def orderbook_parser(body, **args):
    records = []
    for item in body:
        pc, oc = item["market"].split("-")
        ts = item["timestamp"]
        for i, unit in enumerate(item["orderbook_units"]):
            records += [
                OrderbookRecord(
                    ts=ts, oc=oc, pc=pc, side="ask", rank=i + 1, price=unit["ask_price"], quantity=unit["ask_size"]
                ),
                OrderbookRecord(
                    ts=ts, oc=oc, pc=pc, side="bid", rank=i + 1, price=unit["bid_price"], quantity=unit["bid_size"]
                ),
            ]
    return records
