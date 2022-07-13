import math


def get_order_unit(x):
    """
    [업비트] 코인 가격별 호가 표시 단위
    https://upbitcs.zendesk.com/hc/ko/articles/4403838454809-%EA%B1%B0%EB%9E%98-%EC%9D%B4%EC%9A%A9-%EC%95%88%EB%82%B4
    """
    if x < 1_000:
        return round(x, math.floor(3 - math.log(x, 10)))
    if x < 10_000:
        return round(x * 2, math.floor(3 - math.log(x, 10))) / 2
    if x < 100_000:
        return round(x, math.floor(4 - math.log(x, 10)))
    if x < 500_000:
        return round(x * 2, math.floor(4 - math.log(x, 10))) / 2
    if x < 1_000_000:
        return round(x, math.floor(4 - math.log(x, 10)))
    if x < 2_000_000:
        return round(x * 2, math.floor(4 - math.log(x, 10))) / 2
    return round(x, math.floor(4 - math.log(x, 10)))
