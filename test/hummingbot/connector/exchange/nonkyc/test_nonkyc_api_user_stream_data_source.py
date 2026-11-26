import asyncio
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS
from hummingbot.connector.exchange.nonkyc.nonkyc_api_user_stream_data_source import (
    NonkycAPIUserStreamDataSource,
    NonkycWSLoginError,
)
from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.core.web_assistant.connections.data_types import WSResponse


class FakeWS:
    """Websocket double: records what is sent, replays queued replies."""

    def __init__(self, replies):
        self.sent = []
        self.replies = list(replies)
        self.disconnected = False

    async def connect(self, **kwargs):
        pass

    async def send(self, request):
        self.sent.append(request.payload)

    async def receive(self):
        if not self.replies:
            await asyncio.sleep(3600)
        reply = self.replies.pop(0)
        if callable(reply):
            reply = reply(self.sent)
        return WSResponse(data=reply)

    async def disconnect(self):
        self.disconnected = True


class NonkycUserStreamTests(IsolatedAsyncioWrapperTestCase):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        time_provider = MagicMock()
        time_provider.time.return_value = 1795727618.756
        self.auth = NonkycAuth(api_key="key", secret_key="secret", time_provider=time_provider)
        self.api_factory = MagicMock()
        self.data_source = NonkycAPIUserStreamDataSource(
            auth=self.auth, trading_pairs=["ERG-USDT"], connector=MagicMock(), api_factory=self.api_factory)
        self.data_source._sleep = AsyncMock()

    def use_ws(self, ws: FakeWS):
        self.api_factory.get_ws_assistant = AsyncMock(return_value=ws)

    async def test_login_reply_is_awaited_before_subscribing(self):
        # H1: login result is read; reports are subscribed after it; no balance subscription
        def login_ok(sent):
            return {"jsonrpc": "2.0", "result": True, "id": sent[0]["id"]}
        ws = FakeWS([{"method": "ticker", "params": {}}, login_ok])
        self.use_ws(ws)
        connected = await self.data_source._connected_websocket_assistant()
        await self.data_source._subscribe_channels(connected)
        self.assertEqual(["login", CONSTANTS.WS_METHOD_SUBSCRIBE_REPORTS], [p["method"] for p in ws.sent])
        self.assertNotIn("subscribeBalances", [p["method"] for p in ws.sent])

    async def test_refused_login_raises_and_disconnects(self):
        def login_refused(sent):
            return {"jsonrpc": "2.0", "error": {"code": 1002, "message": "Authorisation failed"}, "id": sent[0]["id"]}
        ws = FakeWS([login_refused])
        self.use_ws(ws)
        with self.assertRaises(NonkycWSLoginError):
            await self.data_source._connected_websocket_assistant()
        self.assertTrue(ws.disconnected)
        self.assertEqual(["login"], [p["method"] for p in ws.sent])

    async def test_refused_login_backs_off_exponentially_up_to_a_cap(self):
        # ADV MIN-2: the base loop reconnected at once, one login per round trip, for ever
        def login_refused(sent):
            return {"jsonrpc": "2.0", "error": {"code": 1002, "message": "Authorisation failed"}, "id": sent[-1]["id"]}

        def login_ok(sent):
            return {"jsonrpc": "2.0", "result": True, "id": sent[-1]["id"]}
        for _ in range(8):
            self.use_ws(FakeWS([login_refused]))
            with self.assertRaises(NonkycWSLoginError):
                await self.data_source._connected_websocket_assistant()
        delays = [c.args[0] for c in self.data_source._sleep.await_args_list]
        self.assertEqual([5.0, 10.0, 20.0, 40.0, 80.0, 160.0, 300.0, 300.0], delays)

        # a good login resets the backoff
        self.use_ws(FakeWS([login_ok]))
        await self.data_source._connected_websocket_assistant()
        self.use_ws(FakeWS([login_refused]))
        with self.assertRaises(NonkycWSLoginError):
            await self.data_source._connected_websocket_assistant()
        self.assertEqual(5.0, self.data_source._sleep.await_args.args[0])

    async def test_unanswered_login_times_out(self):
        ws = FakeWS([])
        self.use_ws(ws)
        original = CONSTANTS.WS_LOGIN_TIMEOUT
        CONSTANTS.WS_LOGIN_TIMEOUT = 0.05
        try:
            with self.assertRaises(NonkycWSLoginError):
                await self.data_source._connected_websocket_assistant()
        finally:
            CONSTANTS.WS_LOGIN_TIMEOUT = original

    async def test_only_notifications_are_queued(self):
        queue = asyncio.Queue()
        await self.data_source._process_event_message({"result": True, "id": 3}, queue)
        await self.data_source._process_event_message(b"binary", queue)
        await self.data_source._process_event_message({"method": "report", "params": {}}, queue)
        self.assertEqual(1, queue.qsize())
