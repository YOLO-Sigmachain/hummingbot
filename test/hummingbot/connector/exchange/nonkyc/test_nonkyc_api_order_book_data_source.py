import asyncio
import json
import re
from decimal import Decimal
from test.hummingbot.connector.exchange.nonkyc import nonkyc_replay_messages as msgs
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock

from aioresponses import aioresponses
from bidict import bidict

from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS
from hummingbot.connector.exchange.nonkyc.nonkyc_api_order_book_data_source import NonkycAPIOrderBookDataSource
from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange
from hummingbot.connector.exchange.nonkyc.nonkyc_order_book import NonkycOrderBook
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessageType


class NonkycOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.connector = NonkycExchange(
            nonkyc_api_key="k", nonkyc_api_secret="s",
            trading_pairs=[msgs.TRADING_PAIR], trading_required=False)
        self.connector._set_trading_pair_symbol_map(bidict({msgs.SYMBOL: msgs.TRADING_PAIR}))
        self.connector._time_synchronizer.add_time_offset_ms_sample(0)
        self.data_source = NonkycAPIOrderBookDataSource(
            trading_pairs=[msgs.TRADING_PAIR], connector=self.connector,
            api_factory=self.connector._web_assistants_factory)
        self.ws = AsyncMock()
        self.data_source._ws_assistant = self.ws
        self.now = 1795727600.0
        self.data_source._time = lambda: self.now
        self.set_loggers([self.data_source.logger()])

    def tearDown(self) -> None:
        self.data_source.logger().removeHandler(self)
        getattr(self.data_source, "_reset_sequencing", lambda: None)()  # cancels timers and queued resyncs
        super().tearDown()

    @staticmethod
    async def settle():
        for _ in range(10):
            await asyncio.sleep(0)

    def sent_payloads(self):
        return [call.args[0].payload for call in self.ws.send.call_args_list]

    def subscribes(self):
        return [p for p in self.sent_payloads() if p["method"] == CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK]

    def route(self, method: str, sequence: int):
        """The (queue, sequence) pairs the reader queues for one websocket book message."""
        routed = self.data_source._route_message(msgs.ws_book_message(method, sequence))
        return [(key, int(message["params"]["sequence"])) for key, message in routed or []]

    def diffs(self, *sequences: int):
        """Feed diffs; return the sequences of the diffs queued, in queue order."""
        out = []
        for sequence in sequences:
            out.extend(seq for _, seq in self.route(CONSTANTS.DIFF_EVENT_TYPE, sequence))
        return out

    async def test_rest_snapshot_carries_no_book_id(self):
        # live finding A: the REST 'sequence' is the trade counter, so it is never a book id
        seen = {}
        with aioresponses() as mock_api:
            url = re.compile(r"^https://api\.nonkyc\.io/api/v2/market/orderbook")
            mock_api.get(url, body=json.dumps(msgs.ORDER_BOOK_SNAPSHOT),
                         callback=lambda u, **kw: seen.update(url=str(u)))
            snapshot = await self.data_source._order_book_snapshot(msgs.TRADING_PAIR)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, snapshot.type)
        self.assertEqual(CONSTANTS.UNSEQUENCED_SNAPSHOT_ID, snapshot.update_id)
        self.assertIn("symbol=ERG/USDT", seen["url"])
        self.assertIn(f"limit={CONSTANTS.ORDER_BOOK_DEPTH}", seen["url"])
        self.assertEqual(Decimal("0.188714"), Decimal(str(snapshot.bids[0].price)))

    def test_ws_snapshot_is_routed_as_snapshot(self):
        # M7: snapshotOrderbook was ignored before
        snapshot = msgs.ws_book_message(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        diff = msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, 101)
        self.assertEqual(self.data_source._snapshot_messages_queue_key,
                         self.data_source._channel_originating_message(snapshot))
        self.assertEqual(self.data_source._diff_messages_queue_key,
                         self.data_source._channel_originating_message(diff))
        self.assertEqual("", self.data_source._channel_originating_message(
            {"method": CONSTANTS.TRADE_SNAPSHOT_EVENT_TYPE, "params": {}}))
        self.assertEqual("", self.data_source._channel_originating_message({"result": True, "id": 1}))

    async def test_equal_diff_is_applied_and_backward_diff_resyncs(self):
        # ADV MAJ-2: same sequence was dropped silently; a backward one too, with no resync
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.assertEqual([101], self.diffs(101))
        self.assertEqual([101], self.diffs(101))
        self.assertEqual([], self.sent_payloads())
        self.assertEqual([], self.diffs(99))
        await self.settle()
        self.assertEqual([CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK, CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK],
                         [p["method"] for p in self.sent_payloads()])
        self.assertTrue(self.is_partially_logged("WARNING", "went back from 101 to 99"))
        # the new snapshot ends the resync
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 200)
        self.assertNotIn(msgs.SYMBOL, self.data_source._resync_pending)

    async def test_swapped_adjacent_diffs_are_applied_in_order(self):
        # live finding B: 810829 came before 810828; the late one used to be dropped
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 810825)
        self.assertEqual([810826, 810827], self.diffs(810826, 810827))
        self.assertEqual([], self.diffs(810829))
        self.assertEqual([810828, 810829], self.diffs(810828))
        self.assertEqual([810830], self.diffs(810830))
        await self.settle()
        self.assertEqual([], self.sent_payloads())
        self.assertFalse(any(r.levelname == "WARNING" for r in self.log_records))

    async def test_held_diff_is_released_after_the_window_and_resyncs(self):
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.assertEqual([101], self.diffs(101))
        self.assertEqual([], self.diffs(103))
        self.now += CONSTANTS.BOOK_REORDER_WINDOW_SECONDS
        self.data_source._on_release_timer(msgs.SYMBOL)
        queue = self.data_source._message_queue[self.data_source._diff_messages_queue_key]
        self.assertEqual(103, int(queue.get_nowait()["params"]["sequence"]))
        self.assertEqual([104], self.diffs(104))
        await self.settle()
        self.assertIn(CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK, [p["method"] for p in self.sent_payloads()])
        self.assertTrue(self.is_partially_logged("WARNING", "did not arrive within"))

    async def test_real_gap_resyncs_at_once(self):
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.assertEqual([105], self.diffs(105))
        await self.settle()
        self.assertEqual([CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK, CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK],
                         [p["method"] for p in self.sent_payloads()])
        self.assertTrue(self.is_partially_logged("WARNING", "skipped from 100 to 105"))

    async def test_resync_is_rate_limited_per_symbol(self):
        # a sequence that is not contiguous must not cause a resubscribe storm
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        for sequence in (110, 120, 130):
            self.diffs(sequence)
            self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, sequence + 1)
            await self.settle()
        self.assertEqual(1, len(self.subscribes()))

    async def test_resync_wanted_inside_the_cooldown_is_queued_not_dropped(self):
        # ADV MIN-1: a second gap within 30 s used to be dropped, and the book stayed wrong
        release = asyncio.Event()

        async def sleep(delay):
            await release.wait()
        self.data_source._sleep = sleep
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.diffs(110)
        await self.settle()
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 200)
        self.now += 5
        self.diffs(201, 210)
        await self.settle()
        self.assertEqual(1, len(self.subscribes()))
        self.assertIn(msgs.SYMBOL, self.data_source._resync_pending)
        release.set()
        await self.settle()
        self.assertEqual(2, len(self.subscribes()))

    async def test_snapshot_before_the_queued_resync_cancels_it(self):
        release = asyncio.Event()

        async def sleep(delay):
            await release.wait()
        self.data_source._sleep = sleep
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.diffs(110)
        await self.settle()
        self.now += 5
        self.diffs(120)
        await self.settle()
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 121)
        release.set()
        await self.settle()
        self.assertEqual(1, len(self.subscribes()))

    async def test_book_with_no_websocket_message_for_a_minute_is_resubscribed(self):
        ds = self.data_source
        ds._last_book_message_time = {msgs.SYMBOL: self.now, "ERG/BTC": self.now + 20}
        self.now += CONSTANTS.BOOK_STALE_SECONDS - 1
        ds._check_stale_books()
        await self.settle()
        self.assertEqual([], self.subscribes())
        self.now += 1
        ds._check_stale_books()
        await self.settle()
        self.assertEqual([msgs.SYMBOL], [p["params"]["symbol"] for p in self.subscribes()])
        # a book message resets the clock: 30 s later only ERG/BTC (silent for 70 s) is resubscribed
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        self.now += 30
        ds._check_stale_books()
        await self.settle()
        self.assertEqual([msgs.SYMBOL, "ERG/BTC"], [p["params"]["symbol"] for p in self.subscribes()])

    async def test_subscribing_starts_the_watchdog_and_an_interruption_stops_it(self):
        ds = self.data_source
        ds._sleep = AsyncMock(side_effect=asyncio.CancelledError)
        await ds._subscribe_channels(self.ws)
        await self.settle()
        self.assertIn(msgs.SYMBOL, ds._last_book_message_time)
        self.assertIsNotNone(ds._watchdog_task)
        task = ds._watchdog_task
        await ds._on_order_stream_interruption(self.ws)
        await self.settle()
        self.assertIsNone(ds._watchdog_task)
        self.assertTrue(task.done())

    async def test_pair_added_while_running_gets_a_fresh_websocket_snapshot(self):
        # the tracker drops the subscribe snapshot of a pair whose book is not built yet, and a
        # REST book takes no diffs, so the pair is resubscribed once the book is there
        self.data_source._sleep = AsyncMock()
        self.assertTrue(await self.data_source.subscribe_to_trading_pair(msgs.TRADING_PAIR))
        await self.settle()
        self.assertEqual([CONSTANTS.WS_METHOD_SUBSCRIBE_TRADES, CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK,
                          CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK, CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK],
                         [p["method"] for p in self.sent_payloads()])
        self.data_source._sleep.assert_awaited_once_with(CONSTANTS.RESYNC_AFTER_ADD_SECONDS)

    async def test_diff_without_sequence_makes_the_connector_not_ready(self):
        # MP-M4: such diffs were dropped with an error log only, and the book froze
        ds = self.connector.order_book_tracker.data_source
        ds._time = lambda: self.now
        self.assertTrue(self.connector.status_dict["order_book_sequenced"])
        routed = ds._route_message({"method": CONSTANTS.DIFF_EVENT_TYPE, "params": {
            "symbol": msgs.SYMBOL, "bids": [{"price": "0.18", "quantity": "1"}], "asks": []}})
        self.assertEqual([], routed)
        self.assertFalse(self.connector.status_dict["order_book_sequenced"])
        self.now += CONSTANTS.UNSEQUENCED_DIFF_NOT_READY_SECONDS
        self.assertTrue(self.connector.status_dict["order_book_sequenced"])

    async def test_hourly_refresh_resubscribes_instead_of_mixing_in_rest(self):
        # keeps every book id in the websocket's sequence
        output = asyncio.Queue()
        await self.data_source._request_order_book_snapshots(output)
        self.assertTrue(output.empty())
        self.assertEqual([CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK, CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK],
                         [p["method"] for p in self.sent_payloads()])

    async def test_reconnect_waits_for_a_new_snapshot(self):
        # M7: after a reconnect a diff with no snapshot first forces a resync
        ds = self.data_source
        self.route(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100)
        ws_factory_ws = AsyncMock()
        ds._api_factory.get_ws_assistant = AsyncMock(return_value=ws_factory_ws)
        await ds._connected_websocket_assistant()
        self.assertEqual({}, ds._last_sequence)
        self.diffs(300)
        await self.settle()
        self.assertIn(CONSTANTS.WS_METHOD_SUBSCRIBE_ORDERBOOK, [p["method"] for p in self.sent_payloads()])

    async def test_diff_message_uses_sequence(self):
        queue = asyncio.Queue()
        raw = msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, 101,
                                   bids=[{"price": "0.185065", "quantity": "0"}],
                                   asks=[{"price": "0.1890", "quantity": "5.5"}])
        await self.data_source._parse_order_book_diff_message(raw, queue)
        diff = queue.get_nowait()
        self.assertEqual(101, diff.update_id)
        self.assertEqual(OrderBookMessageType.DIFF, diff.type)
        self.assertEqual(Decimal("0"), Decimal(str(diff.bids[0].amount)))

    def test_stale_level_is_not_replayed_over_a_fresh_snapshot(self):
        # MP-H4 scenario: an old diff that re-adds a taken bid must not apply after the snapshot
        book = NonkycOrderBook()
        old_diff = NonkycOrderBook.diff_message_from_exchange(
            dict(msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, 99,
                                      bids=[{"price": "0.185065", "quantity": "71.9266"}])["params"]),
            0, {"trading_pair": msgs.TRADING_PAIR})
        new_diff = NonkycOrderBook.diff_message_from_exchange(
            dict(msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, 101,
                                      asks=[{"price": "0.1890", "quantity": "5"}])["params"]),
            0, {"trading_pair": msgs.TRADING_PAIR})
        snapshot = NonkycOrderBook.snapshot_message_from_exchange(
            {**msgs.ws_book_message(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100,
                                    bids=[{"price": "0.1880", "quantity": "1"}],
                                    asks=[{"price": "0.1888", "quantity": "2"}])["params"]},
            0, {"trading_pair": msgs.TRADING_PAIR})
        book.restore_from_snapshot_and_diffs(snapshot, [old_diff, new_diff])
        bid_prices = [Decimal(str(round(entry.price, 6))) for entry in book.bid_entries()]
        ask_prices = [Decimal(str(round(entry.price, 6))) for entry in book.ask_entries()]
        self.assertNotIn(Decimal("0.185065"), bid_prices)
        self.assertIn(Decimal("0.189"), ask_prices)
        self.assertEqual(101, book.last_diff_uid)

    @staticmethod
    def book_diff(sequence: int, bids=None, asks=None):
        return NonkycOrderBook.diff_message_from_exchange(
            dict(msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, sequence, bids=bids, asks=asks)["params"]),
            0, {"trading_pair": msgs.TRADING_PAIR})

    @staticmethod
    def bid_prices(book):
        return [Decimal(str(round(entry.price, 6))) for entry in book.bid_entries()]

    def test_reset_snapshot_does_not_replay_diffs_from_the_old_sequence(self):
        # ADV MAJ-2: after a reset to a low sequence, every saved diff (5340 > 3) was replayed
        stale = self.book_diff(5340, bids=[{"price": "0.185065", "quantity": "71.9266"}])
        follows = self.book_diff(4, bids=[{"price": "0.1870", "quantity": "2"}])
        after_gap = self.book_diff(6, bids=[{"price": "0.1860", "quantity": "3"}])
        snapshot = NonkycOrderBook.snapshot_message_from_exchange(
            {**msgs.ws_book_message(CONSTANTS.SNAPSHOT_EVENT_TYPE, 3,
                                    bids=[{"price": "0.1880", "quantity": "1"}])["params"]},
            0, {"trading_pair": msgs.TRADING_PAIR})
        book = NonkycOrderBook()
        book.restore_from_snapshot_and_diffs(snapshot, [stale, follows, after_gap])
        self.assertEqual([Decimal("0.188"), Decimal("0.187")], self.bid_prices(book))
        # the override matters: the stock restore on the same book brings the stale level back
        stock = NonkycOrderBook()
        OrderBook.restore_from_snapshot_and_diffs(stock, snapshot, [stale, follows, after_gap])
        self.assertIn(Decimal("0.185065"), self.bid_prices(stock))

    def test_rest_snapshot_replays_no_diff_and_waits_for_a_websocket_snapshot(self):
        # live finding A: REST ids (trade counter, ~5k) sat below every book id (~22k), so every
        # saved diff was replayed over the fresher REST book
        old = self.book_diff(22150, bids=[{"price": "0.185065", "quantity": "71.9266"}])
        rest = NonkycOrderBook.rest_snapshot_message_from_exchange(
            msgs.copy_of(msgs.ORDER_BOOK_SNAPSHOT), 0, {"trading_pair": msgs.TRADING_PAIR})
        book = NonkycOrderBook()
        book.restore_from_snapshot_and_diffs(rest, [old])
        self.assertEqual([Decimal("0.188714"), Decimal("0.188046")], self.bid_prices(book))
        # diffs are not layered on a REST snapshot ...
        book.apply_diffs(old.bids, old.asks, old.update_id)
        self.assertNotIn(Decimal("0.185065"), self.bid_prices(book))
        # ... the next websocket snapshot replaces it, and diffs follow it again
        ws_snapshot = NonkycOrderBook.snapshot_message_from_exchange(
            {**msgs.ws_book_message(CONSTANTS.SNAPSHOT_EVENT_TYPE, 22151,
                                    bids=[{"price": "0.1880", "quantity": "1"}])["params"]},
            0, {"trading_pair": msgs.TRADING_PAIR})
        book.restore_from_snapshot_and_diffs(ws_snapshot, [old, self.book_diff(22152, bids=[
            {"price": "0.1870", "quantity": "2"}])])
        self.assertEqual([Decimal("0.188"), Decimal("0.187")], self.bid_prices(book))
        self.assertEqual(22151, book.snapshot_uid)

    def test_diff_older_than_the_snapshot_is_ignored_while_tracking(self):
        # a diff still in the pipeline when a fresh snapshot lands must not undo it
        book = self.data_source.order_book_create_function()
        self.assertIsInstance(book, NonkycOrderBook)
        snapshot = NonkycOrderBook.snapshot_message_from_exchange(
            {**msgs.ws_book_message(CONSTANTS.SNAPSHOT_EVENT_TYPE, 100,
                                    bids=[{"price": "0.1880", "quantity": "1"}])["params"]},
            0, {"trading_pair": msgs.TRADING_PAIR})
        book.apply_snapshot(snapshot.bids, snapshot.asks, snapshot.update_id)
        old = NonkycOrderBook.diff_message_from_exchange(
            dict(msgs.ws_book_message(CONSTANTS.DIFF_EVENT_TYPE, 99,
                                      bids=[{"price": "0.1880", "quantity": "0"}])["params"]),
            0, {"trading_pair": msgs.TRADING_PAIR})
        book.apply_diffs(old.bids, old.asks, old.update_id)
        self.assertEqual([Decimal("0.188")], [Decimal(str(round(e.price, 6))) for e in book.bid_entries()])

    async def test_trade_batch_gives_one_message_per_trade(self):
        # L6: every trade of a batch is kept
        queue = asyncio.Queue()
        raw = {"method": CONSTANTS.TRADE_EVENT_TYPE, "params": {"symbol": msgs.SYMBOL, "data": [
            {"id": "t1", "price": "0.1888", "quantity": "1.5", "side": "buy", "timestampms": 1795727619232},
            {"id": "t2", "price": "0.1887", "quantity": "0.5", "side": "sell", "timestampms": 1795727619240},
        ]}}
        await self.data_source._parse_trade_message(raw, queue)
        self.assertEqual(2, queue.qsize())
        self.assertEqual("t1", queue.get_nowait().trade_id)
        self.assertEqual("t2", queue.get_nowait().trade_id)

    async def test_unsubscribe_sends_unsubscribe_messages(self):
        # L5: unsubscribe used to only drop the pair locally
        result = await self.data_source.unsubscribe_from_trading_pair(msgs.TRADING_PAIR)
        self.assertTrue(result)
        methods = [p["method"] for p in self.sent_payloads()]
        self.assertEqual([CONSTANTS.WS_METHOD_UNSUBSCRIBE_TRADES, CONSTANTS.WS_METHOD_UNSUBSCRIBE_ORDERBOOK], methods)
        self.assertNotIn(msgs.TRADING_PAIR, self.data_source._trading_pairs)
