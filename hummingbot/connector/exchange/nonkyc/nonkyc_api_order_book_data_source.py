import asyncio
import itertools
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS, nonkyc_web_utils as web_utils
from hummingbot.connector.exchange.nonkyc.nonkyc_order_book import NonkycOrderBook, book_update_id
from hummingbot.connector.exchange.nonkyc.nonkyc_utils import to_milliseconds
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange


class NonkycAPIOrderBookDataSource(OrderBookTrackerDataSource):
    """
    Websocket snapshots and diffs are ordered by NonKYC's websocket book sequence.
    The REST book 'sequence' is the trade counter, so a REST snapshot carries no
    book id and is replaced by the next websocket snapshot.

    Diffs are sequenced as they are read:
    - the next sequence, or the same one again, is applied;
    - a diff up to BOOK_REORDER_MAX_GAP ahead is held for up to
      BOOK_REORDER_WINDOW_SECONDS for the missing ones, then applied in order
      (NonKYC sometimes sends two adjacent diffs swapped);
    - a larger gap, a missing diff that does not come in time, a backward
      sequence, or a diff before any snapshot on a connection logs a warning and
      resubscribes the pair, which brings a fresh websocket snapshot. Resubscribes
      are at most one per RESYNC_MIN_INTERVAL per pair; one wanted sooner is queued;
    - a diff without a sequence is not applied, and keeps the connector not ready.
    A pair with no websocket book message for BOOK_STALE_SECONDS is resubscribed.
    """
    # at most one resubscribe per symbol in this many seconds
    RESYNC_MIN_INTERVAL = 30.0

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: 'NonkycExchange',
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        self._domain = domain
        self._api_factory = api_factory
        # books that replay only the diffs that follow their snapshot
        self._order_book_create_function = lambda: NonkycOrderBook()
        self._request_ids = itertools.count(1)
        self._last_sequence: Dict[str, int] = {}
        # symbol -> sequence -> raw diffs held for a missing earlier sequence
        self._pending_diffs: Dict[str, Dict[int, List[Dict[str, Any]]]] = {}
        self._pending_since: Dict[str, float] = {}
        self._release_timers: Dict[str, asyncio.TimerHandle] = {}
        # symbols waiting for a fresh snapshot after a resubscribe was asked for
        self._resync_pending: Set[str] = set()
        self._last_resync_time: Dict[str, float] = {}
        self._queued_resyncs: Dict[str, asyncio.Future] = {}
        # symbol -> time of the last websocket book message (or of the subscribe)
        self._last_book_message_time: Dict[str, float] = {}
        self._watchdog_task: Optional[asyncio.Future] = None
        # symbol -> time of the last diff that had no sequence
        self._unsequenced_diff_time: Dict[str, float] = {}

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    def books_sequenced(self) -> bool:
        """False while any pair has had a diff without a sequence in the last minute."""
        now = self._time()
        return all(now - seen >= CONSTANTS.UNSEQUENCED_DIFF_NOT_READY_SECONDS
                   for seen in self._unsequenced_diff_time.values())

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
            "limit": CONSTANTS.ORDER_BOOK_DEPTH,
        }
        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.ORDERBOOK_SNAPSHOT_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.ORDERBOOK_SNAPSHOT_PATH_URL,
            timeout=CONSTANTS.REQUEST_TIMEOUT,
        )
        if not isinstance(data, dict):
            raise IOError(f"Unexpected NonKYC order book reply for {trading_pair}")
        return data

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot = await self._request_order_book_snapshot(trading_pair)
        ts_ms = to_milliseconds(snapshot.get("timestamp"))
        timestamp = ts_ms * 1e-3 if ts_ms is not None else self._time()
        # REST 'sequence' is the trade counter: never a book id
        return NonkycOrderBook.rest_snapshot_message_from_exchange(
            snapshot, timestamp, metadata={"trading_pair": trading_pair})

    def _next_request_id(self) -> int:
        return next(self._request_ids)

    def _subscribe_payloads(self, symbol: str) -> List[Dict[str, Any]]:
        return [
            {"method": CONSTANTS.WS_METHOD_SUBSCRIBE_TRADES, "params": {"symbol": symbol},
             "id": self._next_request_id()},
            {"method": CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
             "params": {"symbol": symbol, "limit": CONSTANTS.ORDER_BOOK_DEPTH},
             "id": self._next_request_id()},
        ]

    async def _subscribe_channels(self, ws: WSAssistant):
        try:
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                for payload in self._subscribe_payloads(symbol):
                    await ws.send(WSJSONRequest(payload=payload))
                self._last_book_message_time[symbol] = self._time()
            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = safe_ensure_future(self._book_watchdog())

    def _reset_sequencing(self):
        for timer in self._release_timers.values():
            timer.cancel()
        for queued in self._queued_resyncs.values():
            queued.cancel()
        self._release_timers.clear()
        self._queued_resyncs.clear()
        self._last_sequence.clear()
        self._pending_diffs.clear()
        self._pending_since.clear()
        self._resync_pending.clear()
        self._last_resync_time.clear()
        self._last_book_message_time.clear()

    async def _connected_websocket_assistant(self) -> WSAssistant:
        # A new connection starts a new sequence: wait for its snapshots
        self._reset_sequencing()
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _on_order_stream_interruption(self, websocket_assistant: Optional[WSAssistant] = None):
        if self._watchdog_task is not None:
            self._watchdog_task.cancel()
            self._watchdog_task = None
        await super()._on_order_stream_interruption(websocket_assistant=websocket_assistant)

    async def _pair_for_message(self, raw_message: Dict[str, Any]) -> str:
        symbol = (raw_message.get("params") or {}).get("symbol")
        return await self._connector.trading_pair_associated_to_exchange_symbol(symbol=symbol)

    async def _parse_order_book_snapshot_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or {}
        trading_pair = await self._pair_for_message(raw_message)
        message_queue.put_nowait(NonkycOrderBook.snapshot_message_from_exchange(
            dict(params), self._time(), metadata={"trading_pair": trading_pair}))

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        params = raw_message.get("params") or {}
        trading_pair = await self._pair_for_message(raw_message)
        if book_update_id(params) is None:
            self.logger().error(f"NonKYC diff for {trading_pair} has no sequence; it is not applied.")
            return
        message_queue.put_nowait(NonkycOrderBook.diff_message_from_exchange(
            dict(params), self._time(), metadata={"trading_pair": trading_pair}))

    # === sequencing, in the websocket reader so messages are seen in the order sent ===

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            if data is None:  # the websocket was disconnected
                continue
            routed = self._route_message(data)
            if routed is None:
                await self._process_message_for_unknown_channel(
                    event_message=data, websocket_assistant=websocket_assistant)
                continue
            for channel, message in routed:
                self._message_queue[channel].put_nowait(message)

    def _route_message(self, event_message: Any) -> Optional[List[Tuple[str, Dict[str, Any]]]]:
        """(queue key, raw message) pairs to queue, in order, for one websocket message; None if unknown."""
        channel = self._channel_originating_message(event_message)
        if channel == "":
            return None
        params = event_message.get("params") or {}
        symbol = params.get("symbol")
        if channel == self._trade_messages_queue_key or symbol is None:
            return [(channel, event_message)]
        now = self._time()
        self._last_book_message_time[symbol] = now
        if channel == self._snapshot_messages_queue_key:
            released = self._on_snapshot(symbol, book_update_id(params))
            return [(channel, event_message)] + [(self._diff_messages_queue_key, d) for d in released]
        return [(self._diff_messages_queue_key, d) for d in self._sequence_diff(symbol, params, event_message, now)]

    def _on_snapshot(self, symbol: str, sequence: Optional[int]) -> List[Dict[str, Any]]:
        if sequence is None:
            return []
        self._last_sequence[symbol] = sequence
        self._resync_pending.discard(symbol)
        queued = self._queued_resyncs.pop(symbol, None)
        if queued is not None:
            queued.cancel()
        # held diffs the snapshot already contains are dropped; later ones may now follow
        pending = self._pending_diffs.get(symbol)
        if pending:
            for held in [s for s in pending if s <= sequence]:
                del pending[held]
            if not pending:
                self._clear_pending(symbol)
        return self._drain(symbol)

    def _sequence_diff(self, symbol: str, params: Dict[str, Any], message: Dict[str, Any],
                       now: float) -> List[Dict[str, Any]]:
        sequence = book_update_id(params)
        if sequence is None:
            previous = self._unsequenced_diff_time.get(symbol)
            self._unsequenced_diff_time[symbol] = now
            if previous is None or now - previous >= CONSTANTS.UNSEQUENCED_DIFF_NOT_READY_SECONDS:
                self.logger().error(f"NonKYC diff for {symbol} has no sequence; it is not applied and the "
                                    f"connector is not ready until diffs carry one again.")
            return []
        released = self._release_expired(symbol, now)
        last = self._last_sequence.get(symbol)
        if last is None:
            self.logger().warning(f"NonKYC order book diff for {symbol} before any snapshot on this "
                                  f"connection (sequence {sequence}). Resubscribing.")
            self._last_sequence[symbol] = sequence
            self._schedule_resync(symbol)
            return released + [message]
        if sequence == last:
            # same sequence again: levels are absolute sizes, so applying it is safe
            return released + [message]
        if sequence < last:
            if symbol not in self._resync_pending:
                self.logger().warning(f"NonKYC order book for {symbol} went back from {last} to {sequence}. "
                                      f"Dropping the diff and resubscribing.")
            self._schedule_resync(symbol)
            return released
        if sequence == last + 1:
            self._last_sequence[symbol] = sequence
            return released + [message] + self._drain(symbol)
        if sequence - last <= CONSTANTS.BOOK_REORDER_MAX_GAP:
            pending = self._pending_diffs.setdefault(symbol, {})
            if not pending:
                self._pending_since[symbol] = now
            pending.setdefault(sequence, []).append(message)
            self._arm_release_timer(symbol)
            return released
        self.logger().warning(f"NonKYC order book for {symbol} skipped from {last} to {sequence}. Resubscribing.")
        self._schedule_resync(symbol)
        flushed = self._flush_pending(symbol)
        self._last_sequence[symbol] = sequence
        return released + flushed + [message]

    def _drain(self, symbol: str) -> List[Dict[str, Any]]:
        """Held diffs that now follow the last applied sequence, in order."""
        pending = self._pending_diffs.get(symbol)
        out: List[Dict[str, Any]] = []
        if not pending:
            if symbol in self._pending_diffs or symbol in self._pending_since or symbol in self._release_timers:
                self._clear_pending(symbol)
            return out
        last = self._last_sequence[symbol]
        while last + 1 in pending:
            last += 1
            out.extend(pending.pop(last))
        self._last_sequence[symbol] = last
        if not pending:
            self._clear_pending(symbol)
        return out

    def _flush_pending(self, symbol: str) -> List[Dict[str, Any]]:
        """All held diffs in sequence order; the last applied sequence moves to the highest."""
        pending = self._pending_diffs.get(symbol)
        out: List[Dict[str, Any]] = []
        if pending:
            for sequence in sorted(pending):
                out.extend(pending[sequence])
            self._last_sequence[symbol] = max(self._last_sequence.get(symbol, 0), max(pending))
        self._clear_pending(symbol)
        return out

    def _clear_pending(self, symbol: str):
        self._pending_diffs.pop(symbol, None)
        self._pending_since.pop(symbol, None)
        timer = self._release_timers.pop(symbol, None)
        if timer is not None:
            timer.cancel()

    def _release_expired(self, symbol: str, now: float) -> List[Dict[str, Any]]:
        since = self._pending_since.get(symbol)
        if since is None or now - since < CONSTANTS.BOOK_REORDER_WINDOW_SECONDS:
            return []
        last = self._last_sequence.get(symbol)
        self.logger().warning(f"NonKYC order book for {symbol}: the diff after {last} did not arrive within "
                              f"{CONSTANTS.BOOK_REORDER_WINDOW_SECONDS} s. Applying the held diffs and resubscribing.")
        self._schedule_resync(symbol)
        return self._flush_pending(symbol)

    def _arm_release_timer(self, symbol: str):
        if symbol in self._release_timers:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._release_timers[symbol] = loop.call_later(
            CONSTANTS.BOOK_REORDER_WINDOW_SECONDS, self._on_release_timer, symbol)

    def _on_release_timer(self, symbol: str):
        self._release_timers.pop(symbol, None)
        for message in self._release_expired(symbol, self._time()):
            self._message_queue[self._diff_messages_queue_key].put_nowait(message)
        if symbol in self._pending_diffs:
            self._arm_release_timer(symbol)

    # === resync ===

    def _schedule_resync(self, symbol: str):
        safe_ensure_future(self._request_resync(symbol))

    async def _request_resync(self, symbol: str, force: bool = False):
        now = self._time()
        last_resync = self._last_resync_time.get(symbol)
        wait = 0.0 if last_resync is None else self.RESYNC_MIN_INTERVAL - (now - last_resync)
        if not force and wait > 0:
            # Too soon after the last one: queue it, never drop it. A snapshot that
            # arrives first makes it unneeded and cancels it.
            self._resync_pending.add(symbol)
            if symbol not in self._queued_resyncs:
                self._queued_resyncs[symbol] = safe_ensure_future(self._run_queued_resync(symbol, wait))
            return
        ws = self._ws_assistant
        if ws is None:
            return
        self._last_resync_time[symbol] = now
        self._resync_pending.add(symbol)
        try:
            await ws.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK, "params": {"symbol": symbol},
                "id": self._next_request_id()}))
            await ws.send(WSJSONRequest(payload={
                "method": CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
                "params": {"symbol": symbol, "limit": CONSTANTS.ORDER_BOOK_DEPTH},
                "id": self._next_request_id()}))
        except asyncio.CancelledError:
            raise
        except Exception:
            self._resync_pending.discard(symbol)
            self.logger().exception(f"Could not resubscribe the {symbol} order book.")

    async def _resync_later(self, symbol: str, wait: float):
        await self._sleep(wait)
        if symbol in self._last_book_message_time:
            await self._request_resync(symbol, force=True)

    async def _run_queued_resync(self, symbol: str, wait: float):
        await self._sleep(wait)
        self._queued_resyncs.pop(symbol, None)
        if symbol in self._resync_pending:
            await self._request_resync(symbol, force=True)

    async def _book_watchdog(self):
        while True:
            await self._sleep(CONSTANTS.BOOK_WATCHDOG_INTERVAL)
            try:
                self._check_stale_books()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in the NonKYC order book watchdog.")

    def _check_stale_books(self):
        if self._ws_assistant is None:
            return
        now = self._time()
        for symbol, last_seen in list(self._last_book_message_time.items()):
            if now - last_seen >= CONSTANTS.BOOK_STALE_SECONDS:
                self.logger().warning(f"No NonKYC order book message for {symbol} in {now - last_seen:.0f} s. "
                                      f"Resubscribing.")
                self._last_book_message_time[symbol] = now
                self._schedule_resync(symbol)

    async def _request_order_book_snapshots(self, output: asyncio.Queue):
        # The hourly refresh asks the websocket for fresh snapshots, so every book id
        # stays in the websocket's sequence. REST is used only with no websocket; a
        # REST snapshot carries no book id and replays no diff.
        if self._ws_assistant is None:
            await super()._request_order_book_snapshots(output)
            return
        for trading_pair in self._trading_pairs:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            await self._request_resync(symbol, force=True)

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._pair_for_message(raw_message)
        for trade in (raw_message.get("params") or {}).get("data") or []:
            if not isinstance(trade, dict):
                continue
            try:
                message_queue.put_nowait(NonkycOrderBook.trade_message_from_exchange(
                    dict(trade), {"trading_pair": trading_pair}))
            except (KeyError, ValueError):
                self.logger().warning(f"Skipping a malformed NonKYC public trade for {trading_pair}.")

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        if not isinstance(event_message, dict):
            return ""
        event_type = event_message.get("method")
        params = event_message.get("params")
        if event_type in (CONSTANTS.SNAPSHOT_EVENT_TYPE, CONSTANTS.DIFF_EVENT_TYPE) and not isinstance(params, dict):
            return ""
        if event_type == CONSTANTS.SNAPSHOT_EVENT_TYPE:
            return self._snapshot_messages_queue_key
        if event_type == CONSTANTS.DIFF_EVENT_TYPE:
            return self._diff_messages_queue_key
        if event_type == CONSTANTS.TRADE_EVENT_TYPE:
            return self._trade_messages_queue_key
        return ""

    async def _process_message_for_unknown_channel(self, event_message: Dict[str, Any], websocket_assistant: WSAssistant):
        if isinstance(event_message, dict) and event_message.get("error") is not None:
            self.logger().warning(f"NonKYC public websocket error: {event_message.get('error')}")

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot subscribe to {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            for payload in self._subscribe_payloads(symbol):
                await self._ws_assistant.send(WSJSONRequest(payload=payload))
            self._last_book_message_time[symbol] = self._time()
            self.add_trading_pair(trading_pair)
            # The tracker builds this pair's book from REST after subscribing, and drops a
            # websocket snapshot that comes before that book exists. A REST book takes no
            # diffs, so ask for a fresh websocket snapshot once the book is there.
            safe_ensure_future(self._resync_later(symbol, CONSTANTS.RESYNC_AFTER_ADD_SECONDS))
            self.logger().info(f"Subscribed to {trading_pair} order book and trade channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error subscribing to {trading_pair} channels")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if self._ws_assistant is None:
            self.logger().warning(f"Cannot unsubscribe from {trading_pair}: WebSocket not connected")
            return False
        try:
            symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            for method in (CONSTANTS.WS_METHOD_UNSUBSCRIBE_TRADES, CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK):
                await self._ws_assistant.send(WSJSONRequest(payload={
                    "method": method, "params": {"symbol": symbol}, "id": self._next_request_id()}))
            self.remove_trading_pair(trading_pair)
            self._last_sequence.pop(symbol, None)
            self._clear_pending(symbol)
            self._resync_pending.discard(symbol)
            self._last_book_message_time.pop(symbol, None)
            self._unsequenced_diff_time.pop(symbol, None)
            queued = self._queued_resyncs.pop(symbol, None)
            if queued is not None:
                queued.cancel()
            self.logger().info(f"Unsubscribed from {trading_pair} channels")
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception(f"Unexpected error unsubscribing from {trading_pair} channels")
            return False
