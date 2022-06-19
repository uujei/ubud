from pydantic import BaseModel

# 현재가 GET
ep = "https://api.bithumb.com/public/ticker/ALL_{payment_currency}"
ep = "https://api.bithumb.com/public/ticker/{order_currency}_{payment_currency}"

# 호가 GET
ep = "https://api.bithumb.com/public/orderbook/ALL_{payment_currency}"
ep = "https://api.bithumb.com/public/orderbook/{order_currency}_{payment_currency}"

# 최근 체결 내역 GET
ep = "https://api.bithumb.com/public/transaction_history/{order_currency}_{payment_currency}"

# 입출금지원현황 GET
ep = "https://api.bithumb.com/public/assetsstatus/ALL"
ep = "https://api.bithumb.com/public/assetsstatus/{order_currency}"

# 회원정보 POST - 회원정보, 코인거래수수료
# header로 정보 전송
ep = "https://api.bithumb.com/info/account"
header = [
    "Accept: application",
    "Api-Key: 사용자 Access Key",  # 사용자 API Key
    "Api-Nonce: 현재시각(ms)",  # 현재시간 millisecond timestamp
    "Api-Sign: 상세 가이드 참고",  # EP + Request Parameter + Api-Nonce + 사용자 Secret
    "Content-Type: application/json",
]
data = {
    "order_currency": "XLM",  # 주문통화 (필수)
    "payment_currency": "KRW",  # 결제통화 (KRW, BTC)
}


class AccountModel(BaseModel):
    status: str
    created: int
    account_id: str
    order_currency: str
    payment_currency: str
    trade_fee: str  # Number
    balance: str  # Number


# 보유자산
ep = "https://api.bithumb.com/info/balance"
header = [
    "Accept: application",
    "Api-Key: 사용자 Access Key",  # 사용자 API Key
    "Api-Nonce: 현재시각(ms)",  # 현재시간 millisecond timestamp
    "Api-Sign: 상세 가이드 참고",  # EP + Request Parameter + Api-Nonce + 사용자 Secret
    "Content-Type: application/json",
]
data = {
    "currency": "BTC",
}


class BalanceModel(BaseModel):
    status: str
    created: int
    total_CURRENCY: str  # CURRENCY 가변
    total_krw: str
    in_use_CURRENCY: str  # CURRENCY 가변
    in_use_krw: str
    avaialable_CURRENCY: str
    avaialable_krw: str
    xcoin_last_CURRENCY: str  # 마지막 체결된 거래금액
