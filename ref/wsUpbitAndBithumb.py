import asyncio
import websockets
import json
import pprint
import pybithumb
import pyupbit
import ray
import time
import datetime
from operator import itemgetter
import pandas as pd

con_key = ""
sec_key = ""
bithumb = pybithumb.Bithumb(con_key, sec_key)

access = ""
secret = ""
upbit = pyupbit.Upbit(access, secret)

coinList = ["WAVES"]  # 거래 대상 코인 목록
adrList = {"WAVES": {"address": "3PLVRmMMPKU3qFXgBqCFCpUMtL3aFcqUc6w", "memo": ""}}  # 빗썸에서 업비트로 코인 출금할 때 주소
coinOrderSize = {
    "WAVES": {
        "shortBal": 2000,
        "orderSize": 50,
        "minQty": 50,
        "minSellQty": 2,
        "tradeOK": True,
    },  # shortBal(short balance): 해외거래소 공매도 수량. 코인 보유 수량과 동일
    "KNC": {
        "shortBal": 5000,
        "orderSize": 20,
        "minQty": 25,
        "minSellQty": 5,
        "tradeOK": True,
    },  # orderSize: 빗썸에서 1회 매수 주문 시 수량
    "AXS": {
        "shortBal": 500,
        "orderSize": 2,
        "minQty": 25,
        "minSellQty": 1,
        "tradeOK": True,
    },  # minQty: 최소 보유 수량. 이 값 보다 보유 수량이 적으면 거래 중지
}  # minSellQty: 업비트에서 매도할 때 최소 수량. tradeOK: 보유수량을 기준으로 거래 가능 여부 판단

bitSymbolList = []  # symbol과 coin은 같은 의미로 사용함.
upSymboList = []
orderbook = {}
global comOB  # 업비트와 빗썸의 web socket 데이터를 받아 오더북 만듬
comOB = {}

reportA = []
reportB = []
print("start")

innerBitBal = {}
innerUpBal = {}

bitBalance = bithumb.get_balance("ALL")  # 빗썸에서 잔고 가져 오기
upBalance = upbit.get_balances()  # 업빗에서 잔고 가져 오기
# print(upBalance)
for coin in coinList:  # 받아온 잔고 데이터 정리
    balStr = "total_" + coin.lower()
    bal = round(float(bitBalance["data"][balStr]), 2)
    innerBitBal[coin] = bal
    ret = next((item for item in upBalance if item["currency"] == coin), None)
    innerUpBal[coin] = round(float(ret["balance"]), 2)
# print(bitCoinBalances, upCoinBalances)

for coin in coinList:
    comOB[coin] = {"upbitOB": "", "bithumbOB": ""}  # 웹소켓 데이터를 comOB에 정리하여 저장
    #  print(comOB)
    bitSymbolList.append(coin + "_KRW")
    upSymboList.append("KRW-" + coin)
    orderbook[coin] = {"ask": pybithumb.get_orderbook(coin)["asks"], "bid": pybithumb.get_orderbook(coin)["bids"]}

# 업비트 시세 조회 Web socket
async def upbit_ws_client():
    uri = "wss://api.upbit.com/websocket/v1"
    async with websockets.connect(uri) as websocket:
        subscribe_fmt = [
            {"ticket": "test"},
            {"type": "orderbook", "codes": upSymboList, "isOnlyRealtime": True},
            {"format": "SIMPLE"},
        ]
        subscribe_data = json.dumps(subscribe_fmt)
        await websocket.send(subscribe_data)

        while True:
            result = await websocket.recv()
            result = json.loads(result)
            upbitResult = {"coin": result["cd"].replace("KRW-", ""), "upbitOB": result["obu"][0]}
            symbol = result["cd"].replace("KRW-", "")
            # print("ccc")
            global upResult
            upResult = upbitResult
            global comOB
            comOB[symbol]["upbitOB"] = result["obu"][0]
            # print(upResult)
            # monitoring(upbitResult)


# 빗썸 시세 조회 Web socket
async def bithumb_ws_client():
    uri = "wss://pubwss.bithumb.com/pub/ws"
    async with websockets.connect(uri, ping_interval=None) as websocket:
        greeting = await websocket.recv()
        subscribe_fmt = {"type": "orderbookdepth", "symbols": bitSymbolList}
        subscribe_data = json.dumps(subscribe_fmt)
        await websocket.send(subscribe_data)
        while True:

            start = time.time()
            data = await websocket.recv()
            data = json.loads(data)

            if "content" in data:
                dataList = data["content"]["list"]
                # pprint.pprint(dataList)
                for data in dataList:
                    dataSymbol = data["symbol"].replace("_KRW", "")
                    if float(data["quantity"]) == 0.0:
                        # pprint.pprint(data)
                        idxA = next(
                            (
                                index
                                for (index, item) in enumerate(orderbook[dataSymbol]["ask"])
                                if item["price"] == float(data["price"])
                            ),
                            None,
                        )
                        idxB = next(
                            (
                                index
                                for (index, item) in enumerate(orderbook[dataSymbol]["bid"])
                                if item["price"] == float(data["price"])
                            ),
                            None,
                        )
                        if idxA is not None:
                            del orderbook[dataSymbol]["ask"][idxA]
                        if idxB is not None:
                            del orderbook[dataSymbol]["bid"][idxB]

                    else:
                        if data["orderType"] == "ask":
                            idx = next(
                                (
                                    index
                                    for (index, item) in enumerate(orderbook[dataSymbol]["ask"])
                                    if item["price"] == float(data["price"])
                                ),
                                None,
                            )
                            if idx is not None:
                                orderbook[dataSymbol]["ask"][idx]["quantity"] = float(data["quantity"])
                                # pprint.pprint(orderbook[dataSymbol]['ask'])
                            else:
                                orderbook[dataSymbol]["ask"].append(
                                    {"price": float(data["price"]), "quantity": float(data["quantity"])}
                                )

                            orderbook[dataSymbol]["ask"] = sorted(
                                orderbook[dataSymbol]["ask"], key=itemgetter("price"), reverse=False
                            )
                            # if(len(orderbook[dataSymbol]['ask'])>5):
                            #    del orderbook[dataSymbol]['ask'][5]
                            # pprint.pprint([orderbook[dataSymbol]['ask']])
                        if data["orderType"] == "bid":
                            idx = next(
                                (
                                    index
                                    for (index, item) in enumerate(orderbook[dataSymbol]["bid"])
                                    if item["price"] == float(data["price"])
                                ),
                                None,
                            )
                            if idx is not None:
                                orderbook[dataSymbol]["bid"][idx]["quantity"] = float(data["quantity"])
                                #  pprint.pprint(orderbook[dataSymbol]['bid'])
                            else:
                                orderbook[dataSymbol]["bid"].append(
                                    {"price": float(data["price"]), "quantity": float(data["quantity"])}
                                )

                            orderbook[dataSymbol]["bid"] = sorted(
                                orderbook[dataSymbol]["bid"], key=itemgetter("price"), reverse=True
                            )
                            # if(len(orderbook[dataSymbol]['bid'])>5):
                            #    del orderbook[dataSymbol]['bid'][5]
                            #  pprint.pprint([orderbook[dataSymbol]['bid']])
                    # print("ask ", orderbook[dataSymbol]['ask'][0])
                    # print("bid ", orderbook[dataSymbol]['bid'][0])
                    # print(list(orderbook.keys())[0])
                    # pprint.pprint(orderbook[dataSymbol])
                    askOB = orderbook[dataSymbol]["ask"][0]
                    bidOB = orderbook[dataSymbol]["bid"][0]
                    # pprint.pprint(orderbook[dataSymbol]['ask'])
                    bithumbResult = {
                        "coin": dataSymbol,
                        "bithumbOB": {
                            "ap": askOB["price"],
                            "bp": bidOB["price"],
                            "as": askOB["quantity"],
                            "bs": bidOB["quantity"],
                        },
                    }
                    global bitResult
                    bitResult = bithumbResult
                    global comOB
                    comOB[dataSymbol]["bithumbOB"] = {
                        "ap": askOB["price"],
                        "bp": bidOB["price"],
                        "as": askOB["quantity"],
                        "bs": bidOB["quantity"],
                    }

                    # monitoring(bithumbResult)


async def submitOrder():
    num = 0
    loopCount = 0
    orderCount = 0
    await asyncio.sleep(2)  # 웹소켓이 데이터 가져오는 동안 대기
    upCoinBalances = {}
    bitCoinBalances = {}
    preCoin = (
        "WAVES"  # 거래 대상 코인이 바뀌는 경우를 대비하여 PreCoin(이전 코인)과 Curcoin(현재 코인)을 분리함. 초기 값은 WAVES 또는 보유한 코인 중 특정 코인으로 동일하게 설정.
    )
    curCoin = "WAVES"
    orders = getOrders(curCoin)  # 빗썸에서 현재 코인의 주문 내역을 불러 옴.
    if orders is None:
        orders = []
    while True:
        loopCount = loopCount + 1
        global comOB
        print("while문 시작")
        await asyncio.sleep(0)
        bitBalance = bithumb.get_balance(
            "ALL"
        )  # 빗썸 잔고는 while 루프에서 매번 새로 불러 옴. 매수 주문에서 얼마가 체결 되었는지 알 수 있는 방법이 잔고 조회밖에 없기 때문. 체결 주문 조회 api가 있지만 잔고 조회로 처리하는게 더 깔끔함.
        if orderCount % 10 == 0:  #
            upBalance = upbit.get_balances()  # 업비트 잔고의 경우 매도 주문 10번 나갈 때 마다 한번씩 동기화를 위해서 불러 온다.
        coinQty = coinOrderSize[curCoin]["shortBal"]  # 공매도 수량(코인 보유 수량과 같아야 함)
        #  coinSum = round(upCoinBal[curCoin] + bitCoinBal[curCoin] - coinQty,1)
        for coin in coinList:  # 빗썸 잔고 조회 내역 정리
            await asyncio.sleep(0)
            balStr = "total_" + coin.lower()
            bal = round(float(bitBalance["data"][balStr]), 2)
            bitCoinBalances[coin] = bal

            if orderCount % 10 == 0:  # 매도 주문 10 번에 한번 씩 업비트 잔고 조회 한 내용을 정리함.
                ret = next((item for item in upBalance if item["currency"] == coin), None)
                upCoinBalances[coin] = round(float(ret["balance"]), 2)

            coinSum = round(
                bitCoinBalances[coin] + upCoinBalances[coin] - coinOrderSize[coin]["shortBal"], 1
            )  # 업비트 보유 수량과 빗썸 보유 수량을 더한 다음 공매도 수량을 뺌. 이 차이 만큼 매도 주문을 넣음
            minQty = coinOrderSize[coin]["minQty"]  # 코인 최소 보유 수량. 이 값보다 보유 수량이 적으면 거래를 중지 함.

            if (
                coinSum > coinOrderSize[coin]["minSellQty"] and upCoinBalances[coin] > minQty
            ):  # coinSum(매도 수량)이 미리 정의한 최소 거래 수량보다 크고 업비트 잔고가 최소 보유 수량보다 큰 경우
                upbitSymbol = "KRW-" + coin
                orderQty = coinSum
                upsellOrder = upbit.sell_market_order(upbitSymbol, orderQty)  # 시장가로 coinSum(매도 수량) 주문
                upCoinBalances[coin] = upCoinBalances[coin] - orderQty  # 시장가 주문이기 때문에 모두 체결되었다고 보고 잔고에서 주문수량을 뺌.
                orderCount = orderCount + 1  # 매도주문을 카운트하여 179행의 api 호출 횟수를 1/10로 줄임.

            # 업비트 보유 수량이 최소 수량보다 적거나 빗썸 보유 수량이 일정치(공매도 수량 - 최소 보유 수량)를 초과하거나 CoinSum(매도수량)의 값이 음수 값으로 공매두 수량의 절반을 초과하는 경우(수동으로 출금 시 발생)
            if (
                upCoinBalances[coin] < minQty
                or bitCoinBalances[coin] > coinOrderSize[coin]["shortBal"] - minQty
                or coinSum * (-1) > coinOrderSize[coin]["shortBal"] / 2
            ):
                coinOrderSize[coin]["tradeOK"] = False  # 거래 가능 여부 판단하는 변수
                if orders is not None:  # 해당 코인으로 주문 내역이 있는 경우 모두 취소
                    for order in orders:
                        if order["order_currency"] == coin:
                            cancelOrder(order)
                            orders.remove(order)  # 오더 목록에서 해당 주문 삭제

                print("업빗 수량 부족")
                print("업빗 bid price: ", comOB[preCoin]["upbitOB"]["bp"])
                orderCount = 0
            else:
                coinOrderSize[coin]["tradeOK"] = True

            if (
                bitCoinBalances[coin] > coinOrderSize[coin]["shortBal"] - minQty
            ):  # 빗썸 보유량이 (공매도 수량 - 최소 보유수량) 보다 큰 경우 빗썸에서 업비트로 코인 자동 출금.
                print("빗썸 출금 요청", coin, bitCoinBalances[coin])
                withdraw = bithumb.withdraw_coinNew(
                    bitCoinBalances[coin] - 1, adrList[coin]["address"], adrList[coin]["memo"], coin
                )
        print("cc")
        for symbol in coinList:  # 시세 차이에 따라 거래할 코인 종류를 바꿔주는 부분. 현재는 WAVES 하나만 거래하기 때문에 사용하지 않고 있음.
            await asyncio.sleep(0)
            upbitOB = comOB[symbol]["upbitOB"]  # 업비트 시세 데이터 오더북
            bithumbOB = comOB[symbol]["bithumbOB"]  # 빗썸 시세 데이터 오더북
            if upbitOB != "" and bithumbOB != "":
                baseGimp = round(comOB[preCoin]["upbitOB"]["bp"] / comOB[preCoin]["bithumbOB"]["ap"], 3)
                curGimp = round(upbitOB["bp"] / bithumbOB["ap"], 3)
                if (
                    curGimp >= 1.003 and curGimp > baseGimp and coinOrderSize[symbol]["tradeOK"]
                ):  # for문에서 선택된 김프가 기준 김프보다 높은 경우
                    curCoin = symbol  # 김프가 가장 높은 코인이 변경 된 경우 기존 주문 모두 취소
                    if curCoin != preCoin:
                        orders = getOrders(preCoin)
                        for order in orders:
                            print("coin diff.0 cancel order")
                            cancelOrder(order)
                            orders.remove(order)
                        preCoin = curCoin
        print("dd")
        orders = getOrders(curCoin)
        if orders is not None:  # 주문 내역이 있는 경우
            print(curCoin, "tradeOK ====", coinOrderSize[curCoin]["tradeOK"])
            for order in orders:
                orderPrice = float(order["price"])
                if order["order_currency"] != curCoin:
                    print("coin diff. cancel order")
                    cancelOrder(order)
                    orders.remove(order)  # 주문 내역 상의 코인이 현재 코인(curCoin)과 다른 경우 주문 취소
                if (
                    orderPrice / upbitOB["bp"] > 0.997 or (orderPrice) / upbitOB["bp"] < 0.996
                ):  # 기존 주문의 가격 범위가 일정치를 벗어 나는 경우 주문 취소
                    print(upbitOB["bp"], orderPrice)
                    print("price change cancel order")
                    cancelOrder(order)
                    orders.remove(order)
        print("orders ===", orders)
        if orders is None or orders == []:  # 주문이 없는 경우 설정한 퍼센트(0.9665)의 가격으로 주문 넣기
            print("new order")
            print(coinOrderSize[curCoin]["tradeOK"])
            orderPrice = int(round(0.9965 * upbitOB["bp"], -1))
            orderQty = min(round(upCoinBalances[curCoin] / 4, 0), coinOrderSize[curCoin]["orderSize"])
            print("orderQty", orderQty)
            print("orderPrice", orderPrice)
            if coinOrderSize[curCoin]["tradeOK"]:
                bitOrder = bithumb.buy_limit_order(curCoin, orderPrice, orderQty)
                buyOrder = {
                    "order_currency": curCoin,
                    "payment_currency": "KRW",
                    "order_id": bitOrder[2],
                    "type": "bid",
                    "price": orderPrice,
                }
                print(buyOrder)
                orders = []
                orders.append(buyOrder)
                print("order submitted")


async def getBalance():
    while True:
        await asyncio.sleep(0)
        global upCoinBalances
        global bitCoinBalances
        bitBalance = bithumb.get_balance("ALL")
        upBalance = upbit.get_balances()
        for coin in coinList:
            balStr = "total_" + coin.lower()
            bal = round(float(bitBalance["data"][balStr]), 2)
            bitCoinBalances[coin] = bal
            ret = next((item for item in upBalance if item["currency"] == coin), None)
            upCoinBalances[coin] = round(float(ret["balance"]), 2)
        # print(bitCoinBalances, upCoinBalances)


def getOrders(symbol):
    try:
        orders = bithumb.get_orders(symbol)  # get_orders는 pybihumb에 없는 함수. 별도로 추가 하였음.
        return orders
    except Exception as e:
        print("bithumb load open orders error", e)
        return "break"


def cancelOrder(order):
    try:
        order_desc = list(range(0, 4))
        order_desc[0] = order["type"]
        order_desc[1] = order["order_currency"]
        order_desc[2] = order["order_id"]
        order_desc[3] = order["payment_currency"]
        result = bithumb.cancel_order(order_desc)
    except Exception as e:
        print("bithumb calcel order failed", e)


def upSellOrder(upbitSymbol, orderSize):
    try:
        upOrder = upbit.sell_market_order(upbitSymbol, orderSize)  # 업비트에서 매도 주문 넣고
    except Exception as e:
        print("upbit sell limit order failed", e)


if __name__ == "__main__":
    tasks = [
        asyncio.ensure_future(bithumb_ws_client()),
        asyncio.ensure_future(upbit_ws_client()),
        asyncio.ensure_future(submitOrder()),
        # asyncio.ensure_future(getBalance())
    ]
    asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))
