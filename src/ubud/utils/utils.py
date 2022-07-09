# def order_unit_krw(krw):  # 코인 종류에 따른 주문 가격 int로 환산. 주문 넣을때 int로 정확한 값이 들어가야하기 때문.


def old(krw):
    #  if coin in listA:
    if krw < 10:
        return round(krw, 2)
    if krw >= 10 and krw < 100:
        return round(krw, 1)
    if krw >= 100 and krw < 1000:
        return round(krw, 0)
    if krw >= 1000 and krw < 10000:
        return round(krw * 2, -1) / 2
    # if coin in listB:
    if krw >= 10000 and krw < 100000:
        return round(krw, -1)
    if krw >= 100000 and krw < 1000000:
        return round(krw * 2, -2) / 2
    if krw >= 1000000 and krw < 10000000:
        return round(krw * 2, -2) / 3
    if krw >= 10000000:
        return round(krw, -3)
