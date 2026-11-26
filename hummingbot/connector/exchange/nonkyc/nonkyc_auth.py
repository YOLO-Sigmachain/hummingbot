import hashlib
import hmac
import json
import secrets
from typing import Any, Dict, Mapping, Optional
from urllib.parse import urlencode

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


def encode_query(params: Mapping[str, Any]) -> str:
    """Query string as sent and as signed. '/' stays as is, as aiohttp would leave it."""
    return urlencode([(key, str(value)) for key, value in params.items()], safe="/")


def compact_body(data: Any) -> str:
    """The POST body as sent and as signed: compact JSON, like NonKYC's own client."""
    if isinstance(data, (str, bytes)):
        data = json.loads(data)
    return json.dumps(data, separators=(",", ":"), ensure_ascii=False)


class NonkycAuth(AuthBase):
    """
    Signs requests as the spec says: HMAC-SHA256 with the secret over
    api key + full url (with query) + body (POST only) + nonce.
    The nonce is in unix milliseconds and never repeats or goes back.
    """

    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self._api_key = api_key
        self._secret_key = secret_key
        self.time_provider = time_provider
        self._last_nonce = 0

    def __repr__(self) -> str:
        # keep the key and secret out of any repr or log
        return f"{self.__class__.__name__}()"

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)

        if request.method == RESTMethod.GET:
            # Put the query in the url ourselves, so the signed url is the one sent
            if request.params:
                request.url = f"{request.url}?{encode_query(request.params)}"
                request.params = None
            payload = request.url
        else:
            body = compact_body(request.data) if request.data is not None else ""
            # Send exactly the bytes that are signed
            request.data = body if request.data is not None else None
            payload = f"{request.url}{body}"

        headers.update(self.header_for_authentication(payload))
        request.headers = headers
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # NonKYC authenticates the websocket with a login message, not per request
        return request

    def generate_ws_authentication_message(self, request_id: Optional[int] = None) -> Dict[str, Any]:
        nonce = secrets.token_hex(16)
        payload = {
            "method": "login",
            "params": {
                "algo": "HS256",
                "pKey": self._api_key,
                "nonce": nonce,
                "signature": self._generate_signature(nonce),
            },
        }
        if request_id is not None:
            payload["id"] = request_id
        return payload

    def next_nonce(self) -> int:
        now_ms = int(self.time_provider.time() * 1e3)
        nonce = max(self._last_nonce + 1, now_ms)
        self._last_nonce = nonce
        return nonce

    def header_for_authentication(self, data: str) -> Dict[str, str]:
        nonce = self.next_nonce()
        signature = self._generate_signature(f"{self._api_key}{data}{nonce}")
        return {"X-API-KEY": self._api_key,
                "X-API-NONCE": str(nonce),
                "X-API-SIGN": signature}

    def _generate_signature(self, data: str) -> str:
        return hmac.new(self._secret_key.encode("utf8"), data.encode("utf8"), hashlib.sha256).hexdigest()
