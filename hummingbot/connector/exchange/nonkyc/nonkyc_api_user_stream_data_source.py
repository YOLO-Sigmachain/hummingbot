import asyncio
import itertools
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS
from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange


class NonkycWSLoginError(ConnectionError):
    """The websocket login was refused or not answered. The base loop reconnects after a backoff."""


class NonkycAPIUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Logs in, waits for the login result, then subscribes to order reports.
    Balances come from REST only: no balance stream is documented.
    """

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: NonkycAuth,
                 trading_pairs: List[str],
                 connector: 'NonkycExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: NonkycAuth = auth
        self._domain = domain
        self._api_factory = api_factory
        self._connector = connector
        self._request_ids = itertools.count(1)
        # refused or unanswered logins in a row, for the backoff
        self._login_failures = 0

    def _login_backoff_seconds(self) -> float:
        delay = CONSTANTS.WS_LOGIN_BACKOFF_INITIAL_SECONDS * (2 ** max(self._login_failures - 1, 0))
        return min(delay, CONSTANTS.WS_LOGIN_BACKOFF_MAX_SECONDS)

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        try:
            await self._authenticate_ws_connection(ws)
        except Exception as error:
            await ws.disconnect()
            if isinstance(error, NonkycWSLoginError):
                # The base loop reconnects at once on a ConnectionError: wait first,
                # longer after each refusal, so a bad key does not hammer NonKYC
                self._login_failures += 1
                delay = self._login_backoff_seconds()
                self.logger().warning(f"{error}. Next login attempt in {delay:.0f} s.")
                await self._sleep(delay)
            raise
        self._login_failures = 0
        return ws

    async def _authenticate_ws_connection(self, ws: WSAssistant):
        request_id = next(self._request_ids)
        await ws.send(WSJSONRequest(payload=self._auth.generate_ws_authentication_message(request_id=request_id)))
        reply = await self._wait_for_reply(ws, request_id)
        if reply.get("result") is not True:
            error = reply.get("error")
            # the reply never carries the key; log only the exchange's error
            raise NonkycWSLoginError(f"NonKYC websocket login failed: {error if error is not None else 'no result'}")
        self.logger().info("Logged in to the NonKYC private websocket.")

    async def _wait_for_reply(self, ws: WSAssistant, request_id: int) -> Dict[str, Any]:
        async def wait():
            while True:
                response = await ws.receive()
                if response is None:
                    raise NonkycWSLoginError("NonKYC websocket closed before the login reply")
                data = response.data
                if isinstance(data, dict) and data.get("id") == request_id:
                    return data
        try:
            return await asyncio.wait_for(wait(), timeout=CONSTANTS.WS_LOGIN_TIMEOUT)
        except asyncio.TimeoutError:
            raise NonkycWSLoginError("No NonKYC websocket login reply in time")

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        await websocket_assistant.send(WSJSONRequest(payload={
            "method": CONSTANTS.WS_METHOD_SUBSCRIBE_REPORTS,
            "params": {},
            "id": next(self._request_ids),
        }))
        self.logger().info("Subscribed to user order reports")

    async def _process_event_message(self, event_message: Any, queue: asyncio.Queue):
        # Only notifications go to the exchange; replies and binary frames do not
        if isinstance(event_message, dict) and event_message.get("method"):
            queue.put_nowait(event_message)
        elif isinstance(event_message, dict) and event_message.get("error") is not None:
            self.logger().warning(f"NonKYC private websocket error: {event_message.get('error')}")
