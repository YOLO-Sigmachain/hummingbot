import hashlib
import hmac
import json
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from yarl import URL

from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

API_KEY = "test-api-key"
SECRET = "test-secret-value"


def expected_signature(message: str) -> str:
    return hmac.new(SECRET.encode(), message.encode(), hashlib.sha256).hexdigest()


class NonkycAuthTests(IsolatedAsyncioWrapperTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.time_provider = MagicMock()
        self.time_provider.time.return_value = 1795727618.756
        self.auth = NonkycAuth(api_key=API_KEY, secret_key=SECRET, time_provider=self.time_provider)

    async def test_get_signs_the_exact_url_sent(self):
        # M3: the query is put in the url and signed as sent; '/' is not escaped
        request = RESTRequest(method=RESTMethod.GET,
                              url="https://api.nonkyc.io/api/v2/account/trades",
                              params={"symbol": "ERG/USDT", "since": 1795727558756, "limit": 500},
                              is_auth_required=True)
        signed = await self.auth.rest_authenticate(request)

        expected_url = ("https://api.nonkyc.io/api/v2/account/trades"
                        "?symbol=ERG/USDT&since=1795727558756&limit=500")
        self.assertEqual(expected_url, signed.url)
        self.assertIsNone(signed.params)
        nonce = signed.headers["X-API-NONCE"]
        self.assertEqual(expected_signature(f"{API_KEY}{expected_url}{nonce}"), signed.headers["X-API-SIGN"])
        self.assertEqual(API_KEY, signed.headers["X-API-KEY"])
        # aiohttp/yarl sends this url unchanged
        self.assertEqual(expected_url, str(URL(signed.url)))

    async def test_post_signs_the_exact_body_sent(self):
        # M2: the sent body is the signed body, compact, with spaces inside values kept
        data = {"symbol": "ERG/USDT", "note": "a b", "quantity": "10"}
        request = RESTRequest(method=RESTMethod.POST,
                              url="https://api.nonkyc.io/api/v2/createorder",
                              data=json.dumps(data),
                              is_auth_required=True)
        signed = await self.auth.rest_authenticate(request)

        self.assertEqual('{"symbol":"ERG/USDT","note":"a b","quantity":"10"}', signed.data)
        nonce = signed.headers["X-API-NONCE"]
        self.assertEqual(
            expected_signature(f"{API_KEY}https://api.nonkyc.io/api/v2/createorder{signed.data}{nonce}"),
            signed.headers["X-API-SIGN"])

    async def test_nonce_is_milliseconds_and_never_goes_back(self):
        # M1: 13-digit ms nonce; strictly increasing even when the clock steps back
        first = int(self.auth.header_for_authentication("x")["X-API-NONCE"])
        self.assertEqual(1795727618756, first)
        self.assertEqual(13, len(str(first)))
        second = int(self.auth.header_for_authentication("x")["X-API-NONCE"])
        self.time_provider.time.return_value = 1795727610.000
        third = int(self.auth.header_for_authentication("x")["X-API-NONCE"])
        self.assertLess(first, second)
        self.assertLess(second, third)

    def test_ws_login_uses_random_hex_nonce_and_signs_it(self):
        # L3: crypto nonce, not a printed Python list
        message = self.auth.generate_ws_authentication_message(request_id=7)
        nonce = message["params"]["nonce"]
        self.assertEqual("login", message["method"])
        self.assertEqual(7, message["id"])
        self.assertEqual(32, len(nonce))
        int(nonce, 16)
        self.assertEqual(expected_signature(nonce), message["params"]["signature"])
        self.assertNotEqual(nonce, self.auth.generate_ws_authentication_message()["params"]["nonce"])

    def test_repr_hides_secrets(self):
        self.assertNotIn(SECRET, repr(self.auth))
        self.assertNotIn(API_KEY, repr(self.auth))
