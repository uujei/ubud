from .models import OrderbookRecord

# orderbook_parser
def orderbook_parser(body, **args):
    # model
    def _to_records(ts, oc, pc, data):
        return [
            OrderbookRecord(
                ts=ts, oc=oc, pc=pc, side=side[:3], rank=i + 1, price=record["price"], quantity=record["quantity"]
            )
            for side in ["bids", "asks"]
            for i, record in enumerate(data[side])
        ]

    data = body["data"]
    ts = data.pop("timestamp")
    pc = data.pop("payment_currency")

    if "order_currency" in data:
        oc = data.pop("order_currency")
        return _to_records(ts, oc, pc, data)

    records = []
    for _, _data in data.items():
        oc = _data.pop("order_currency")
        records += _to_records(ts, oc, pc, _data)

    return records
