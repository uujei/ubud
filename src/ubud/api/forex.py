import logging
from urllib.parse import urlencode, urljoin

import aiohttp

logger = logging.getLogger(__name__)


################################################################
# Base
################################################################
class ForexApi:

    # Dunamu URL
    baseUrl = "https://quotation-api-cdn.dunamu.com"
    apiVersion = "v1"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36"
    }

    def __init__(
        self,
        codes: str = "FRX.KRWUSD",
        handlers: list = [],
    ):
        self.codes = codes
        self.handlers = handlers if isinstance(handlers, list) else [handlers]

    async def _request(self, path: str = "/forex/recent", codes: str = "FRX.KRWUSD"):

        url = f"{self.baseUrl}/{self.apiVersion}/{path.strip('/')}"
        query = f"codes={codes}"
        url = "?".join([url, query])

        async with aiohttp.ClientSession() as client:
            async with client.request(method="get", url=url, headers=self.headers) as resp:
                if resp.status not in [200, 201]:
                    _text = await resp.text()
                    raise ReferenceError(f"status code: {resp.status}, message: {_text}")
                resp = await resp.json()
                if self.handlers is not None:
                    for handler in self.handlers:
                        await handler(resp)
                return resp
