"""
Keyless replay harness for the NonKYC connector.

Real auth, URL building and REST assistant code run; only aiohttp is replaced
by aioresponses, fed with the replies in nonkyc_replay_messages. Websocket
reports are pushed into the user stream queue the exchange listens to.
"""
import asyncio
import hashlib
import hmac
import json
import re
import time
from decimal import Decimal
from pathlib import Path
from test.hummingbot.connector.exchange.nonkyc import nonkyc_replay_messages as msgs
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

from aioresponses import aioresponses

import hummingbot.connector.exchange.nonkyc as nonkyc_package
from hummingbot.connector.client_order_tracker import ClientOrderTracker
from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS, nonkyc_web_utils as web_utils
from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, DeductedFromReturnsTradeFee, TokenAmount
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from hummingbot.logger import NETWORK

BASE = "https://api.nonkyc.io/api/v2"
API_KEY = "KEY-test-01"
SECRET = "SEC-test-01"
NOW = 1795727600.0
# getorder by NonKYC id once known, by client id before
BY_ID = f"{CONSTANTS.ORDER_INFO_PATH_URL}/{msgs.EXCHANGE_ORDER_ID}"
BY_CLIENT_ID = f"{CONSTANTS.ORDER_INFO_PATH_URL}/HBOT-B1"


def wall_clock_offset_ms() -> float:
    # TimeSynchronizer.time() is perf_counter plus this offset
    return (time.time() - time.perf_counter()) * 1e3


def path_re(path: str) -> re.Pattern:
    return re.compile(r"^" + re.escape(BASE + path) + r"(\?.*)?$")


class Replay:
    """aioresponses plus a log of what was sent."""

    def __init__(self, mock_api: aioresponses):
        self.mock_api = mock_api
        self.calls: List[Dict[str, Any]] = []

    def _record(self, method: str):
        def callback(url, **kwargs):
            self.calls.append({"method": method, "url": str(url), "data": kwargs.get("data"),
                               "headers": kwargs.get("headers") or {}})
        return callback

    def get(self, path: str, payload: Any = None, status: int = 200, body: Optional[str] = None,
            exception: Optional[BaseException] = None, repeat: bool = False):
        self.mock_api.get(path_re(path), status=status,
                          body=body if body is not None else json.dumps(payload),
                          exception=exception, repeat=repeat, callback=self._record("GET"))

    def post(self, path: str, payload: Any = None, status: int = 200, body: Optional[str] = None,
             exception: Optional[BaseException] = None):
        self.mock_api.post(path_re(path), status=status,
                           body=body if body is not None else json.dumps(payload),
                           exception=exception, callback=self._record("POST"))

    def sent(self, method: str, path: str) -> List[Dict[str, Any]]:
        return [c for c in self.calls if c["method"] == method and c["url"].startswith(BASE + path)]


class NonkycExchangeTests(IsolatedAsyncioWrapperTestCase):
    level = 0

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.log_records = []
        self.exchange = NonkycExchange(
            nonkyc_api_key=API_KEY, nonkyc_api_secret=SECRET,
            trading_pairs=[msgs.TRADING_PAIR], trading_required=True)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        ClientOrderTracker.logger().setLevel(1)
        ClientOrderTracker.logger().addHandler(self)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(wall_clock_offset_ms())
        self.exchange._set_current_timestamp(NOW)
        self.exchange._sleep = AsyncMock()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(msgs.copy_of(msgs.MARKETS))
        rules = await self.exchange._format_trading_rules(msgs.copy_of(msgs.MARKETS))
        self.exchange._trading_rules = {rule.trading_pair: rule for rule in rules}

        self.events: Dict[MarketEvent, EventLogger] = {}
        for event in (MarketEvent.OrderFilled, MarketEvent.BuyOrderCreated, MarketEvent.SellOrderCreated,
                      MarketEvent.OrderCancelled, MarketEvent.OrderFailure, MarketEvent.BuyOrderCompleted,
                      MarketEvent.SellOrderCompleted):
            self.events[event] = EventLogger()
            self.exchange.add_listener(event, self.events[event])

    def tearDown(self) -> None:
        self.exchange.logger().removeHandler(self)
        ClientOrderTracker.logger().removeHandler(self)
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def event_count(self, event: MarketEvent) -> int:
        return len(self.events[event].event_log)

    @staticmethod
    async def settle():
        for _ in range(10):
            await asyncio.sleep(0)

    async def place(self, replay: Replay, order_id: str = "HBOT-B1", amount: str = "10", price: str = "0.188",
                    trade_type: TradeType = TradeType.BUY, reply: Any = None, status: int = 200,
                    body: Optional[str] = None, exception: Optional[BaseException] = None,
                    order_type: OrderType = OrderType.LIMIT, trading_pair: str = msgs.TRADING_PAIR,
                    send: bool = True) -> InFlightOrder:
        if send:
            if reply is None and body is None and exception is None:
                reply = msgs.order_record(order_id, quantity=amount, price=price,
                                          side="buy" if trade_type is TradeType.BUY else "sell")
            replay.post(CONSTANTS.CREATE_ORDER_PATH_URL, payload=reply, status=status, body=body, exception=exception)
        await self.exchange._create_order(trade_type=trade_type, order_id=order_id, trading_pair=trading_pair,
                                          amount=Decimal(amount), order_type=order_type, price=Decimal(price))
        await self.settle()
        return self.exchange._order_tracker.all_orders.get(order_id) or self.exchange._order_tracker.lost_orders.get(order_id)

    async def poll(self):
        await self.exchange._update_order_status()
        await self.settle()

    async def push_ws(self, *messages: Dict[str, Any]):
        listener = asyncio.ensure_future(self.exchange._user_stream_event_listener())
        for message in messages:
            self.exchange._user_stream_tracker.user_stream.put_nowait(message)
        for _ in range(40):
            await asyncio.sleep(0)
            if self.exchange._user_stream_tracker.user_stream.empty():
                break
        await self.settle()
        listener.cancel()
        await self.settle()

    # --- order types and local checks ---

    def test_only_limit_orders_are_advertised(self):
        # MP-C1: MARKET is not offered, so amm_arb sends a LIMIT at its buffered price
        self.assertEqual([OrderType.LIMIT], self.exchange.supported_order_types())
        self.assertEqual(OrderType.LIMIT, self.exchange.get_taker_order_type())

    async def test_market_order_fails_without_a_request(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, order_type=OrderType.MARKET, send=False)
            sent = dict(mock_api.requests)
        self.assertTrue(order.is_failure)
        self.assertEqual({}, sent)

    async def test_create_order_body_and_signature_match_what_is_sent(self):
        # L1, MP-C1, M2: lower-case type, price always sent, strictValidate, signed bytes == sent bytes
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
        call = replay.sent("POST", CONSTANTS.CREATE_ORDER_PATH_URL)[0]
        body = json.loads(call["data"])
        self.assertEqual({"userProvidedId": "HBOT-B1", "symbol": "ERG/USDT", "side": "buy", "type": "limit",
                          "quantity": "10.0000", "price": "0.188000", "strictValidate": True}, body)
        self.assertNotIn(" ", call["data"])
        nonce = call["headers"]["X-API-NONCE"]
        expected = hmac.new(SECRET.encode(), f"{API_KEY}{BASE}/createorder{call['data']}{nonce}".encode(),
                            hashlib.sha256).hexdigest()
        self.assertEqual(expected, call["headers"]["X-API-SIGN"])
        self.assertEqual(13, len(nonce))
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(msgs.EXCHANGE_ORDER_ID, order.exchange_order_id)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCreated))

    async def test_order_below_min_notional_fails_locally(self):
        # M8 / MP-C2a: 5 ERG at 0.1888 is 0.944 USDT, under minQuote 1
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, amount="5", price="0.1888", send=False)
            sent = dict(mock_api.requests)
        self.assertTrue(order.is_failure)
        self.assertEqual({}, sent)

    async def test_paused_and_api_excluded_markets_fail_locally(self):
        # M8 / MP-C2e: pause flags are checked before any request
        cases = [("XPE-USDT", TradeType.BUY), ("ERG-BTC", TradeType.BUY), ("ERG-XMR", TradeType.SELL)]
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            for index, (pair, side) in enumerate(cases):
                order = await self.place(replay, order_id=f"HBOT-P{index}", trading_pair=pair, trade_type=side,
                                         amount="100", price="0.5", send=False)
                self.assertTrue(order.is_failure, pair)
            sent = dict(mock_api.requests)
        self.assertEqual({}, sent)
        self.assertEqual(3, self.event_count(MarketEvent.OrderFailure))

    async def test_sells_still_allowed_when_only_buys_are_paused(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, order_id="HBOT-S1", trading_pair="ERG-BTC", trade_type=TradeType.SELL,
                                     amount="100", price="0.000003")
        self.assertEqual(OrderState.OPEN, order.current_state)

    def test_trading_rules_read_from_the_market_list(self):
        # M8: minimum size and value read from getlist, exact decimals, inactive markets skipped
        erg = self.exchange.trading_rules["ERG-USDT"]
        self.assertEqual(Decimal("1"), erg.min_notional_size)
        self.assertEqual(Decimal("0.0001"), erg.min_order_size)
        self.assertEqual(Decimal("0.000001"), erg.min_price_increment)
        self.assertEqual(Decimal("0.0001"), erg.min_base_amount_increment)
        self.assertFalse(erg.supports_market_orders)
        ltc = self.exchange.trading_rules["LTC-BTC"]
        self.assertEqual(Decimal("0.008"), ltc.min_order_size)
        self.assertEqual(Decimal("0.00001"), ltc.min_notional_size)
        self.assertNotIn("OLD-USDT", self.exchange.trading_rules)
        self.assertEqual("ERG-USDT", __import__(
            "hummingbot.connector.exchange.nonkyc.nonkyc_utils", fromlist=["EXAMPLE_PAIR"]).EXAMPLE_PAIR)

    # --- the replay: place, partial fills over WS and REST, fill, cancel ---

    async def test_replay_place_partial_fills_ws_and_rest_then_cancel(self):
        # C1, H1, H2, MP-H1, MP-M1: one fill per trade id across WS and REST; cancel by exchange id
        t1 = msgs.trade_record("T1", "4", fee="0.000752")
        t2 = msgs.trade_record("T2", "2", price="0.1879", fee="0.000376", timestamp=1795727710000)
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)

            # WS report for T1: re-read over REST, book from the trade record
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            await self.push_ws(msgs.ws_report("HBOT-B1", executed="4", trade_id="T1", trade_quantity="4"))
            self.assertEqual(Decimal("4"), order.executed_amount_base)

            # REST poll sees T1 again: no second booking
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            await self.poll()
            self.assertEqual(Decimal("4"), order.executed_amount_base)
            self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)

            # REST poll finds T2 first, then the WS report for T2 arrives
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="6", status="Partly Filled"))
            await self.poll()
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="6", status="Partly Filled"))
            await self.push_ws(msgs.ws_report("HBOT-B1", executed="6", trade_id="T2", trade_quantity="2"))
            self.assertEqual(Decimal("6"), order.executed_amount_base)

            # cancel: by exchange id; fills checked before CANCELED
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="6", status="Cancelled", is_active=False))
            result = await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()

        self.assertEqual("HBOT-B1", result)
        cancel_call = replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL)[0]
        self.assertEqual({"id": msgs.EXCHANGE_ORDER_ID}, json.loads(cancel_call["data"]))
        fills = self.events[MarketEvent.OrderFilled].event_log
        self.assertEqual(["T1", "T2"], [f.exchange_trade_id for f in fills])
        self.assertEqual([Decimal("4"), Decimal("2")], [f.amount for f in fills])
        self.assertEqual([Decimal("0.1880"), Decimal("0.1879")], [f.price for f in fills])
        self.assertEqual([TokenAmount("USDT", Decimal("0.000752"))], fills[0].trade_fee.flat_fees)
        self.assertEqual(Decimal("6"), order.executed_amount_base)
        self.assertEqual(Decimal("4") * Decimal("0.1880") + Decimal("2") * Decimal("0.1879"),
                         order.executed_amount_quote)
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(1, self.event_count(MarketEvent.OrderCancelled))

    async def test_replay_full_fill_completes_from_trades(self):
        t1 = msgs.trade_record("T1", "4")
        t2 = msgs.trade_record("T2", "6", timestamp=1795727720000)
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(2, self.event_count(MarketEvent.OrderFilled))
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))
        self.assertEqual(Decimal("10"), order.executed_amount_base)

    async def test_cumulative_quantity_is_never_booked(self):
        # H1 / MP-M1: a report or record saying 6 executed books nothing until trades say so
        t1 = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="6", status="Partly Filled"))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            await self.push_ws(msgs.ws_report("HBOT-B1", executed="6", trade_id="T9", trade_quantity="6"))
        self.assertEqual(Decimal("4"), order.executed_amount_base)
        self.assertEqual(["T1"], list(order.order_fills.keys()))

    async def test_overfill_is_refused(self):
        # C1: never book past the order amount
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL,
                       [msgs.trade_record("T2", "7"), msgs.trade_record("T1", "4")])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="10", status="Partly Filled"))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            await self.poll()
        self.assertLessEqual(order.executed_amount_base, order.amount)
        self.assertEqual(1, len(order.order_fills))
        self.assertTrue(any("not booked" in r.getMessage() for r in self.log_records))

    async def test_per_order_trade_path_uses_the_trade_id_and_hb_pair(self):
        # C1 / MP-H1: the trade id is the trade's own id, never the order id; pair is ERG-USDT
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL,
                       [msgs.trade_record("T2", "2"), msgs.trade_record("T1", "4"),
                        msgs.trade_record("TX", "1", exchange_order_id="other")])
            updates = await self.exchange._all_trade_updates_for_order(order)
        self.assertEqual(["T2", "T1"], [u.trade_id for u in updates])
        self.assertEqual({msgs.TRADING_PAIR}, {u.trading_pair for u in updates})
        url = replay.sent("GET", CONSTANTS.USER_TRADES_PATH_URL)[0]["url"]
        self.assertIn("symbol=ERG/USDT", url)
        self.assertIn("limit=500", url)
        self.assertIn("since=", url)

    async def test_two_rest_paths_book_each_trade_once(self):
        # C1: the pair poller and the per-order path share trade ids, so nothing doubles
        t1 = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            await self.poll()
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            await self.exchange._update_orders_fills([order])
            await self.settle()
        self.assertEqual(Decimal("4"), order.executed_amount_base)
        self.assertEqual(1, self.event_count(MarketEvent.OrderFilled))

    # --- createorder with an unknown outcome ---

    async def test_timeout_then_found(self):
        # C2 / MP-H2: a timeout leaves the order pending; getorder by client id finds it
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, exception=asyncio.TimeoutError())
            self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
            self.assertIsNone(order.exchange_order_id)
            self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_CLIENT_ID, msgs.order_record("HBOT-B1"))
            await self.poll()
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(msgs.EXCHANGE_ORDER_ID, order.exchange_order_id)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCreated))
        self.assertEqual(1, len(replay.sent("POST", CONSTANTS.CREATE_ORDER_PATH_URL)))

    async def test_504_then_found_recovers_missed_fills(self):
        # C2 / MP-H2: a fill the pair poll could not match is fetched once the id is known
        t1 = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=504, body=msgs.GATEWAY_TIMEOUT_BODY)
            self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            replay.get(BY_CLIENT_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            await self.poll()
        self.assertEqual(msgs.EXCHANGE_ORDER_ID, order.exchange_order_id)
        self.assertEqual(Decimal("4"), order.executed_amount_base)
        self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)
        self.assertEqual(1, self.event_count(MarketEvent.OrderFilled))

    async def test_unknown_create_outcome_is_not_failed_inside_the_grace_period(self):
        # MP-H1 / ADV MAJ-1: 'not found' answers do not count for 300 s, and neither do cancel attempts
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=500, body=json.dumps({"error": {"code": 500}}))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [], repeat=True)
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [], repeat=True)
            replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY, repeat=True)
            for step in range(10):
                self.exchange._set_current_timestamp(NOW + 29 * step)
                await self.poll()
            for _ in range(4):
                await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
        self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))
        self.assertNotIn("HBOT-B1", self.exchange._order_tracker.lost_orders)
        self.assertEqual(0, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))
        self.assertEqual([], replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL))
        lookups = replay.sent("GET", CONSTANTS.ACCOUNT_ORDERS_PATH_URL)
        # M-2: per poll without status, then filled, then cancelled
        self.assertEqual(30, len(lookups))
        self.assertNotIn("status=", lookups[0]["url"])
        self.assertIn("status=filled", lookups[1]["url"])
        self.assertIn("status=cancelled", lookups[2]["url"])
        self.assertIn("symbol=ERG/USDT", lookups[0]["url"])
        self.assertIn(f"since={int(NOW * 1e3) - CONSTANTS.TRADES_OVERLAP_MS}", lookups[0]["url"])

    async def test_unknown_create_outcome_found_in_account_orders_even_when_filled(self):
        # ADV MAJ-1: getorder answers 20002 for a filled order; /account/orders still lists it
        t1 = msgs.trade_record("T1", "4")
        t2 = msgs.trade_record("T2", "6", timestamp=1795727720000)
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, exception=asyncio.TimeoutError())
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1], repeat=True)
            replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY)
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [
                msgs.account_order_record("HBOT-OTHER", exchange_order_id="someone-else"),
                msgs.account_order_record("HBOT-B1", executed="10", status="Filled", is_active=False)])
            await self.poll()
        self.assertEqual(msgs.EXCHANGE_ORDER_ID, order.exchange_order_id)
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(Decimal("10"), order.executed_amount_base)
        self.assertEqual(2, self.event_count(MarketEvent.OrderFilled))
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    async def test_unknown_create_outcome_fails_with_marker_after_grace_and_late_order_is_swept(self):
        # MP-H1 / ADV MAJ-1: unproven after 300 s -> FAILED with SUBMIT_OUTCOME_UNKNOWN; a late listing
        # is then cancelled and its fills booked
        late_trade = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=504, body=msgs.GATEWAY_TIMEOUT_BODY)
            for _ in range(2):
                replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
                replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY)
                replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [])
            self.exchange._set_current_timestamp(NOW + 10)
            await self.poll()
            self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))
            self.exchange._set_current_timestamp(NOW + CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS + 1)
            await self.poll()
            self.assertTrue(order.is_failure)
            self.assertNotIn("HBOT-B1", self.exchange._order_tracker.lost_orders)
            failure = self.events[MarketEvent.OrderFailure].event_log[0]
            self.assertIn(CONSTANTS.SUBMIT_OUTCOME_UNKNOWN, failure.error_message)
            self.assertIn(CONSTANTS.SUBMIT_OUTCOME_UNKNOWN, self.exchange.order_failure_reason("HBOT-B1"))
            self.assertIn("HBOT-B1", self.exchange._unknown_outcome_sweep)

            # NonKYC lists the order later, open and partly filled
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [
                msgs.account_order_record("HBOT-B1", executed="4", status="Partly Filled")])
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [late_trade])
            self.exchange._set_current_timestamp(
                NOW + CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS + 1 + CONSTANTS.UNKNOWN_OUTCOME_SWEEP_INTERVAL)
            await self.exchange._sweep_unknown_outcomes()
            await self.settle()
        cancels = replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL)
        self.assertEqual(1, len(cancels))
        self.assertEqual({"id": msgs.EXCHANGE_ORDER_ID}, json.loads(cancels[0]["data"]))
        fills = self.events[MarketEvent.OrderFilled].event_log
        self.assertEqual([("HBOT-B1", "T1", Decimal("4"))], [(f.order_id, f.exchange_trade_id, f.amount) for f in fills])
        self.assertEqual({}, self.exchange._unknown_outcome_sweep)
        self.assertEqual(1, self.event_count(MarketEvent.OrderFailure))

    async def test_unknown_create_outcome_found_when_only_status_filled_lists_it(self):
        # M-2: the spec gives /account/orders no default status; if the default lists active orders
        # only, an order that filled at once was never found and its fill never booked
        t1 = msgs.trade_record("T1", "10")
        filled_only = re.compile(r"^" + re.escape(BASE + CONSTANTS.ACCOUNT_ORDERS_PATH_URL) + r"\?.*status=filled.*$")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, exception=asyncio.TimeoutError())
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1], repeat=True)
            replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY)
            mock_api.get(filled_only, body=json.dumps([
                msgs.account_order_record("HBOT-B1", executed="10", status="Filled", is_active=False)]),
                repeat=True, callback=replay._record("GET"))
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [], repeat=True)
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))
        urls = [c["url"] for c in replay.sent("GET", CONSTANTS.ACCOUNT_ORDERS_PATH_URL)]
        # without status first, then filled, where it is found: cancelled is not asked
        self.assertEqual(2, len(urls))
        self.assertNotIn("status=", urls[0])
        self.assertIn("status=filled", urls[1])

    async def test_sweep_finds_a_late_order_listed_only_as_cancelled(self):
        # M-2: the sweep read /account/orders without status only
        self.exchange._unknown_outcome_sweep["HBOT-B1"] = (msgs.TRADING_PAIR, NOW)
        cancelled_only = re.compile(
            r"^" + re.escape(BASE + CONSTANTS.ACCOUNT_ORDERS_PATH_URL) + r"\?.*status=cancelled.*$")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            mock_api.get(cancelled_only, body=json.dumps([
                msgs.account_order_record("HBOT-B1", executed="4", status="Cancelled", is_active=False)]),
                callback=replay._record("GET"))
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [], repeat=True)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "4")])
            self.exchange._set_current_timestamp(NOW + CONSTANTS.UNKNOWN_OUTCOME_SWEEP_INTERVAL)
            await self.exchange._sweep_unknown_outcomes()
            await self.settle()
        urls = [c["url"] for c in replay.sent("GET", CONSTANTS.ACCOUNT_ORDERS_PATH_URL)]
        self.assertEqual(3, len(urls))
        self.assertEqual([], replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL))
        fills = self.events[MarketEvent.OrderFilled].event_log
        self.assertEqual([("HBOT-B1", "T1", Decimal("4"))], [(f.order_id, f.exchange_trade_id, f.amount) for f in fills])
        self.assertEqual({}, self.exchange._unknown_outcome_sweep)

    async def test_account_orders_page_cap_is_reported_to_the_operator(self):
        # L-3: a full /account/orders page was a plain log line
        page = [msgs.account_order_record(f"HBOT-X{i}", exchange_order_id=f"X{i}") for i in range(500)]
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, page)
            records = await self.exchange._account_orders(msgs.TRADING_PAIR, since_ms=0)
        self.assertEqual(500, len(records))
        capped = [r for r in self.log_records if "filled a whole page" in r.getMessage()]
        self.assertEqual([NETWORK], [r.levelno for r in capped])

    async def test_create_request_sent_once_the_request_goes_out(self):
        # L-1: amm_arb starts the leg timer here; createorder can first wait for the signed-request lock
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.post(CONSTANTS.CREATE_ORDER_PATH_URL, payload=msgs.order_record("HBOT-B1"))
            replay.post(CONSTANTS.CREATE_ORDER_PATH_URL, exception=asyncio.TimeoutError())
            await self.exchange._signed_request_lock.acquire()
            task = asyncio.ensure_future(self.exchange._create_order(
                trade_type=TradeType.BUY, order_id="HBOT-B1", trading_pair=msgs.TRADING_PAIR,
                amount=Decimal("10"), order_type=OrderType.LIMIT, price=Decimal("0.188")))
            await self.settle()
            self.assertEqual([], replay.sent("POST", CONSTANTS.CREATE_ORDER_PATH_URL))
            self.assertFalse(self.exchange.create_request_sent("HBOT-B1"))
            self.exchange._signed_request_lock.release()
            await task
            await self.settle()
            self.assertTrue(self.exchange.create_request_sent("HBOT-B1"))
            # an unknown outcome was still sent
            await self.place(replay, order_id="HBOT-B2", send=False)
        self.assertTrue(self.exchange.create_request_sent("HBOT-B2"))
        self.assertFalse(self.exchange.create_request_sent("HBOT-B3"))

    async def test_not_found_while_the_create_is_in_flight_is_not_counted(self):
        # MP-H1 [P4]: polls during the 10 s POST window used to count toward lost
        release = asyncio.Event()

        async def slow_place(**kwargs):
            await release.wait()
            return msgs.EXCHANGE_ORDER_ID, NOW
        self.exchange._place_order = slow_place
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [], repeat=True)
            replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY, repeat=True)
            task = asyncio.ensure_future(self.exchange._create_order(
                trade_type=TradeType.BUY, order_id="HBOT-B1", trading_pair=msgs.TRADING_PAIR,
                amount=Decimal("10"), order_type=OrderType.LIMIT, price=Decimal("0.188")))
            await self.settle()
            order = self.exchange.in_flight_orders["HBOT-B1"]
            for _ in range(5):
                await self.poll()
            self.assertEqual(0, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))
            release.set()
            await task
            await self.settle()
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    async def test_getorder_uses_the_nonkyc_id_once_known(self):
        # MP-M1 [P2]: the path was always the client id
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID, msgs.order_record("HBOT-B1"))
            await self.poll()
        self.assertEqual(1, len(replay.sent("GET", BY_ID)))
        self.assertEqual([], replay.sent("GET", BY_CLIENT_ID))

    async def test_filled_order_is_not_failed_when_its_trades_cannot_be_read(self):
        # MP-M1 [P3]: 20002 also means 'filled'; with the trades poll failing it used to go FAILED
        t1 = msgs.trade_record("T1", "4")
        t2 = msgs.trade_record("T2", "6", timestamp=1795727720000)
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            for _ in range(4):
                replay.get(CONSTANTS.USER_TRADES_PATH_URL, status=500, body="{}")
                replay.get(CONSTANTS.USER_TRADES_PATH_URL, status=500, body="{}")
                replay.get(BY_ID, status=400, body=msgs.NOT_FOUND_BODY)
                replay.get(BY_CLIENT_ID, status=400, body=msgs.NOT_FOUND_BODY)
                await self.poll()
            self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))
            self.assertNotIn("HBOT-B1", self.exchange._order_tracker.lost_orders)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(2, self.event_count(MarketEvent.OrderFilled))
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    async def test_lost_order_fills_are_read_before_its_cancel(self):
        # MP-M1: the lost loop cancelled first, and a 'not found' cancel dropped the order unread
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            for _ in range(4):
                replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
                replay.get(BY_ID, status=400, body=msgs.NOT_FOUND_BODY)
                replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
                await self.poll()
            self.assertIn("HBOT-B1", self.exchange._order_tracker.lost_orders)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "10")])
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, status=400, body=msgs.NOT_FOUND_BODY)
            await self.exchange._cancel_lost_orders()
            await self.settle()
        self.assertEqual(Decimal("10"), order.executed_amount_base)
        self.assertEqual(["T1"], [f.exchange_trade_id for f in self.events[MarketEvent.OrderFilled].event_log])

    async def test_pending_order_found_cancelled_still_records_its_exchange_id(self):
        # MP-L1: no created event fires for PENDING_CREATE -> CANCELED, so the id is recorded here
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, exception=asyncio.TimeoutError())
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_CLIENT_ID, msgs.order_record("HBOT-B1", status="Cancelled", is_active=False))
            await self.poll()
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual("HBOT-B1", self.exchange._exchange_order_ids.get(msgs.EXCHANGE_ORDER_ID))

    async def test_duplicate_client_id_is_looked_up_not_failed(self):
        # C2: 20008 means an order with this id exists
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=400, body=msgs.DUPLICATE_ID_BODY)
        self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    async def test_503_never_sets_a_placeholder_id(self):
        # C2: the old 'UNKNOWN' sentinel is gone
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=503, body=msgs.SERVICE_UNAVAILABLE_BODY)
        self.assertIsNone(order.exchange_order_id)
        self.assertEqual(OrderState.PENDING_CREATE, order.current_state)

    async def test_bad_createorder_reply_is_looked_up(self):
        # 2.18 returns a str body for non-JSON replies; that is not proof of failure
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, body=json.dumps({"unexpected": True}))
        self.assertEqual(OrderState.PENDING_CREATE, order.current_state)
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    async def test_definite_reject_fails_at_once(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, status=400, body=msgs.INSUFFICIENT_FUNDS_BODY)
            sent_methods = [key[0] for key in mock_api.requests]
        self.assertEqual(["POST"], sent_methods)
        self.assertTrue(order.is_failure)
        self.assertEqual(1, self.event_count(MarketEvent.OrderFailure))
        self.assertEqual([], replay.sent("GET", CONSTANTS.ORDER_INFO_PATH_URL))

    # --- status poll errors ---

    async def test_poll_errors_are_not_counted_as_not_found(self):
        # H3 / MP-M3: 429, 5xx, bad bodies and odd statuses never mark a live order lost
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [], repeat=True)
            path = BY_ID
            replay.get(path, status=429, body=msgs.RATE_LIMIT_BODY)
            replay.get(path, status=502, body="<html>bad gateway</html>")
            replay.get(path, status=504, body=msgs.GATEWAY_TIMEOUT_BODY)
            replay.get(path, status=500, body=json.dumps({"error": {"code": 500}}))
            replay.get(path, body="[]")
            replay.get(path, exception=asyncio.TimeoutError())
            replay.get(path, msgs.order_record("HBOT-B1", status="Expired-ish"))
            replay.get(path, {**msgs.order_record("HBOT-B1"), "userProvidedId": "HBOT-OTHER"})
            for _ in range(8):
                await self.poll()
        self.assertIn("HBOT-B1", self.exchange.in_flight_orders)
        self.assertNotIn("HBOT-B1", self.exchange._order_tracker.lost_orders)
        self.assertEqual(0, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))
        self.assertTrue(order.is_open)

    async def test_not_found_count_resets_after_an_answer(self):
        # H3: scattered not-found answers over a long life do not add up
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [], repeat=True)
            path = BY_ID
            for _ in range(3):
                for _ in range(3):
                    replay.get(path, status=400, body=msgs.NOT_FOUND_BODY)
                replay.get(path, msgs.order_record("HBOT-B1"))
            for _ in range(12):
                await self.poll()
        self.assertTrue(order.is_open)
        self.assertEqual(0, self.event_count(MarketEvent.OrderFailure))

    def test_rate_limits_are_low_and_polls_use_their_own_pool(self):
        # H3: no Binance numbers; getorder and trades never spend the placement budget
        limits = {limit.limit_id: limit for limit in CONSTANTS.RATE_LIMITS}
        self.assertNotIn("REQUEST_WEIGHT", limits)
        self.assertTrue(all(limit.limit <= 10 for limit in limits.values() if not limit.linked_limits))

        def pools(limit_id):
            return {pair.limit_id for pair in limits[limit_id].linked_limits}
        self.assertEqual({CONSTANTS.PRIVATE_ORDER_POOL}, pools(CONSTANTS.CREATE_ORDER_PATH_URL))
        self.assertEqual({CONSTANTS.PRIVATE_ORDER_POOL}, pools(CONSTANTS.CANCEL_ORDER_PATH_URL))
        self.assertEqual({CONSTANTS.PRIVATE_QUERY_POOL}, pools(CONSTANTS.ORDER_INFO_PATH_URL))
        self.assertEqual({CONSTANTS.PRIVATE_QUERY_POOL}, pools(CONSTANTS.USER_TRADES_PATH_URL))

    async def test_429_pauses_the_next_request(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.USER_BALANCES_PATH_URL, status=429, body=msgs.RATE_LIMIT_BODY)
            with self.assertRaises(IOError):
                await self.exchange._update_balances()
            replay.get(CONSTANTS.USER_BALANCES_PATH_URL, msgs.BALANCES)
            await self.exchange._update_balances()
        delay = self.exchange._sleep.await_args.args[0]
        self.assertGreater(delay, 0)
        self.assertLessEqual(delay, CONSTANTS.RATE_LIMIT_BACKOFF_SECONDS)

    # --- cancel ---

    async def test_confirmed_cancel_is_not_applied_before_its_trades_are_listed(self):
        # M-1 (probe ZZP1): the leg fills as the cancel lands; NonKYC answers success true and the
        # trade is not listed yet. It went CANCELED at once and the fill was booked after.
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            result = await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
            self.assertIsNone(result)
            self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))
            self.assertNotEqual(OrderState.CANCELED, order.current_state)
            self.assertEqual(1, len(replay.sent("GET", BY_ID)))

            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "10")])
            self.exchange._set_current_timestamp(NOW + 5)
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(Decimal("10"), order.executed_amount_base)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))
        self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))

    async def test_confirmed_cancel_of_a_partial_fill_waits_for_the_trades(self):
        # M-1: partly filled when the cancel lands; CANCELED only once the 4 are booked
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="4", status="Cancelled", is_active=False))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
            self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))

            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "4")])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="4", status="Cancelled", is_active=False))
            self.exchange._set_current_timestamp(NOW + 5)
            await self.poll()
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(Decimal("4"), order.executed_amount_base)
        self.assertEqual(1, self.event_count(MarketEvent.OrderCancelled))
        self.assertEqual(1, self.event_count(MarketEvent.OrderFilled))

    async def test_cancel_without_exchange_id_looks_it_up_first(self):
        # H2: cancel uses NonKYC's id, learnt by client id when the create reply was lost
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay, exception=asyncio.TimeoutError())
            replay.get(BY_CLIENT_ID, msgs.order_record("HBOT-B1"))
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="0", status="Cancelled", is_active=False))
            await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
        self.assertEqual({"id": msgs.EXCHANGE_ORDER_ID},
                         json.loads(replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL)[0]["data"]))
        self.assertEqual(OrderState.CANCELED, order.current_state)

    async def test_confirmed_cancel_defers_when_order_reread_fails(self):
        # If re-reading the order after a confirmed cancel fails (e.g. 503/429/timeout),
        # do not mark CANCELED immediately: keep the order tracked so status polling can
        # verify executedQuantity and book any late fills.
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID, status=503, body="Service Unavailable")
            result = await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
            self.assertIsNone(result)
            self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))
            self.assertNotEqual(OrderState.CANCELED, order.current_state)
            self.assertIn("HBOT-B1", self.exchange.in_flight_orders)

            # Next poll sees the order actually filled before cancel took effect.
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "10")])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            self.exchange._set_current_timestamp(NOW + 5)
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(Decimal("10"), order.executed_amount_base)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))
        self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))

    async def test_confirmed_cancel_defers_when_trade_fetch_fails(self):
        # If fetching order trades after a confirmed cancel fails, defer cancellation to
        # status polling rather than marking CANCELED with unverified fills.
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, status=503, body="Service Unavailable")
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="0", status="Cancelled", is_active=False))
            result = await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
            self.assertIsNone(result)
            self.assertEqual(0, self.event_count(MarketEvent.OrderCancelled))
            self.assertNotEqual(OrderState.CANCELED, order.current_state)

            # Next poll succeeds and applies CANCELED cleanly.
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="0", status="Cancelled", is_active=False))
            self.exchange._set_current_timestamp(NOW + 5)
            await self.poll()
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(1, self.event_count(MarketEvent.OrderCancelled))

    async def test_unconfirmed_cancel_is_not_treated_as_cancelled(self):
        # MP-M2: success false raises and the order stays open; not counted as not found
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": False})
            result = await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
        self.assertIsNone(result)
        self.assertTrue(order.is_open)
        self.assertEqual(0, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))

    async def test_cancel_not_found_matches_the_documented_code(self):
        # H2: not-found on cancel is code 20002, not an invented message
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, status=400, body=msgs.NOT_FOUND_BODY)
            await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
        self.assertEqual(1, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))

    # --- status mapping ---

    async def test_partly_filled_and_inactive_is_cancelled(self):
        # MP-M3: does not stay open for ever
        t1 = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled", is_active=False))
            await self.poll()
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(Decimal("4"), order.executed_amount_base)

    async def test_unknown_status_is_not_an_error(self):
        # L2: no KeyError; isActive decides
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            record = msgs.order_record("HBOT-B1", status="Something New")
            record.pop("isActive")
            replay.get(BY_ID, record)
            await self.poll()
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertTrue(any("Unknown NonKYC order status" in r.getMessage() for r in self.log_records))

    async def test_terminal_status_waits_for_its_trades(self):
        # A Filled record with trades missing does not complete the order short
        t1 = msgs.trade_record("T1", "4")
        t2 = msgs.trade_record("T2", "6", timestamp=1795727720000)
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1])
            await self.poll()
            self.assertEqual(OrderState.PARTIALLY_FILLED, order.current_state)
            self.assertEqual(0, self.event_count(MarketEvent.BuyOrderCompleted))

            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t2, t1])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="10", status="Filled", is_active=False))
            await self.poll()
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(Decimal("10"), order.executed_amount_base)
        self.assertEqual(1, self.event_count(MarketEvent.BuyOrderCompleted))

    # --- polling robustness ---

    async def test_server_time_comes_from_the_documented_endpoint(self):
        # M4: GET /time, serverTime in ms
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.SERVER_TIME_PATH_URL, {"serverTime": 1795727618756})
            server_time = await web_utils.get_current_server_time()
        self.assertEqual(1795727618756.0, server_time)
        self.assertEqual(f"{BASE}/time", replay.calls[0]["url"])

    async def test_time_sync_failure_does_not_stop_the_poll(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.SERVER_TIME_PATH_URL, status=500, body="{}")
            # must not raise: a raise here skipped balances and orders for the whole poll
            await self.exchange._update_time_synchronizer()
        self.assertEqual(f"{BASE}/time", replay.calls[0]["url"])

    async def test_bad_trades_reply_does_not_block_status_or_balances(self):
        # M4: one bad trade reply used to abort every REST update
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, {"not": "a list"})
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", status="Cancelled", is_active=False))
            replay.get(CONSTANTS.USER_BALANCES_PATH_URL, msgs.BALANCES)
            await self.exchange._status_polling_loop_fetch_updates()
            await self.settle()
        self.assertEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(Decimal("110.5"), self.exchange.get_balance("ERG"))

    async def test_trade_cursor_moves_only_after_a_good_reply(self):
        # M5 / MP-M4: a failed fetch does not skip its window
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, status=500, body="{}")
            replay.get(BY_ID, msgs.order_record("HBOT-B1"))
            await self.poll()
            self.assertNotIn(msgs.TRADING_PAIR, self.exchange._trade_cursor_ms)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "4", timestamp=1795727700000)])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            await self.poll()
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(BY_ID,
                       msgs.order_record("HBOT-B1", executed="4", status="Partly Filled"))
            await self.poll()
        urls = [c["url"] for c in replay.sent("GET", CONSTANTS.USER_TRADES_PATH_URL)]
        start = int(NOW * 1e3) - CONSTANTS.TRADES_OVERLAP_MS
        self.assertIn(f"since={start}", urls[0])
        self.assertIn(f"since={start}", urls[1])
        self.assertIn(f"since={1795727700000 - CONSTANTS.TRADES_OVERLAP_MS}", urls[2])

    async def test_trades_are_paged(self):
        # M5: limit 500 and skip paging on the documented endpoint
        page = [msgs.trade_record(f"X{i}", "1", exchange_order_id="someone-else") for i in range(500)]
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, page)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "4")])
            updates = await self.exchange._all_trade_updates_for_order(order)
        urls = [c["url"] for c in replay.sent("GET", CONSTANTS.USER_TRADES_PATH_URL)]
        self.assertEqual(2, len(urls))
        self.assertNotIn("skip=", urls[0])
        self.assertIn("skip=500", urls[1])
        self.assertEqual(["T1"], [u.trade_id for u in updates])

    def test_poll_interval_is_short_while_orders_can_change(self):
        # H1 / MP-M1: websocket pings must not stretch polling to 120 s
        ws = MagicMock()
        ws.last_recv_time = NOW
        self.exchange._user_stream_tracker.data_source._ws_assistant = ws
        self.assertEqual(self.exchange.LONG_POLL_INTERVAL, self.exchange._get_poll_interval(NOW))
        self.exchange.start_tracking_order("HBOT-B9", None, msgs.TRADING_PAIR, TradeType.BUY,
                                           Decimal("0.188"), Decimal("10"), OrderType.LIMIT)
        self.assertEqual(self.exchange.SHORT_POLL_INTERVAL, self.exchange._get_poll_interval(NOW))

    # --- fees ---

    def test_default_fee_is_0_2_percent_and_buy_fee_is_added_to_cost(self):
        # MP-H3 / M6
        buy_fee = self.exchange.get_fee("ERG", "USDT", OrderType.LIMIT, TradeType.BUY, Decimal("10"), Decimal("0.188"))
        sell_fee = self.exchange.get_fee("ERG", "USDT", OrderType.LIMIT, TradeType.SELL, Decimal("10"), Decimal("0.188"))
        self.assertIsInstance(buy_fee, AddedToCostTradeFee)
        self.assertIsInstance(sell_fee, DeductedFromReturnsTradeFee)
        self.assertEqual(Decimal("0.002"), buy_fee.percent)
        self.assertEqual(Decimal("0.002"), sell_fee.percent)

    async def test_fee_is_booked_in_the_asset_the_trade_states(self):
        # M6 / MP-M5: quote fee plus any alternate-asset fee
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
        trade = msgs.trade_record("T1", "4", fee="0", alternate_fee="0.5", alternate_asset="NKYC")
        update = self.exchange._trade_update_from_trade(order, trade)
        self.assertEqual([TokenAmount("NKYC", Decimal("0.5"))], update.fee.flat_fees)
        trade = msgs.trade_record("T2", "4", fee="0.000752")
        self.assertEqual([TokenAmount("USDT", Decimal("0.000752"))],
                         self.exchange._trade_update_from_trade(order, trade).fee.flat_fees)

    async def test_buy_fee_asset_is_read_from_total_with_fee(self):
        # MP-M2: totalWithFee == quantity x price means the fee came out of the base received
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
        in_base = msgs.trade_record("T1", "10", price="0.1880", fee="0.02", total_with_fee="1.88")
        fee = self.exchange._trade_update_from_trade(order, in_base).fee
        self.assertIsInstance(fee, DeductedFromReturnsTradeFee)
        self.assertEqual([TokenAmount("ERG", Decimal("0.02"))], fee.flat_fees)
        in_quote = msgs.trade_record("T2", "10", price="0.1880", fee="0.00376", total_with_fee="1.88376")
        fee = self.exchange._trade_update_from_trade(order, in_quote).fee
        self.assertIsInstance(fee, AddedToCostTradeFee)
        self.assertEqual([TokenAmount("USDT", Decimal("0.00376"))], fee.flat_fees)

    async def test_undecidable_fee_asset_keeps_the_quote_default_and_logs_once(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
        for trade_id in ("T1", "T2"):
            trade = msgs.trade_record(trade_id, "4", fee="0.000752", total_with_fee="0")
            self.assertEqual([TokenAmount("USDT", Decimal("0.000752"))],
                             self.exchange._trade_update_from_trade(order, trade).fee.flat_fees)
        logged = [r for r in self.log_records if "does not show which asset the fee was paid in" in r.getMessage()]
        self.assertEqual(1, len(logged))

    async def test_alternate_asset_fee_is_booked_alone(self):
        # MP-M2 / ADV verdict 6: 'fee' and 'alternateFee' both set must not be booked twice
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
        trade = msgs.trade_record("T1", "4", fee="0.000752", alternate_fee="0.5", alternate_asset="NKYC")
        self.assertEqual([TokenAmount("NKYC", Decimal("0.5"))],
                         self.exchange._trade_update_from_trade(order, trade).fee.flat_fees)

    async def test_signed_requests_are_sent_one_at_a_time_in_nonce_order(self):
        # MP-M3: concurrent signed requests could reach NonKYC with nonces out of order
        events = []

        async def slow_reply(url, **kwargs):
            nonce = int(kwargs["headers"]["X-API-NONCE"])
            events.append(("in", nonce))
            await asyncio.sleep(0.01)
            events.append(("out", nonce))
        with aioresponses() as mock_api:
            mock_api.get(path_re(CONSTANTS.USER_BALANCES_PATH_URL), body=json.dumps(msgs.BALANCES),
                         callback=slow_reply, repeat=True)
            await asyncio.gather(*(self.exchange._api_get(
                path_url=CONSTANTS.USER_BALANCES_PATH_URL, is_auth_required=True,
                limit_id=CONSTANTS.USER_BALANCES_PATH_URL) for _ in range(3)))
        self.assertEqual(["in", "out"] * 3, [kind for kind, _ in events])
        nonces = [nonce for kind, nonce in events if kind == "in"]
        self.assertEqual(sorted(nonces), nonces)

    async def test_a_burst_of_reports_does_not_use_up_the_wait_for_trades(self):
        # ADV MIN-3: seven reports in one second used to apply CANCELED with 4 of 6 booked
        t1 = msgs.trade_record("T1", "4")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.get(BY_ID, msgs.order_record("HBOT-B1", executed="6", status="Cancelled", is_active=False),
                       repeat=True)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [t1], repeat=True)
            await self.push_ws(*[msgs.ws_report("HBOT-B1", executed="6", status="Cancelled")] * 7)
        self.assertNotEqual(OrderState.CANCELED, order.current_state)
        self.assertEqual(1, self.exchange._terminal_deferrals["HBOT-B1"][0])
        self.assertEqual(Decimal("4"), order.executed_amount_base)

    async def test_trade_paging_cap_is_reported_to_the_operator(self):
        # ADV MIN-4: the cap was a plain warning; it is a network warning with an app message now
        page = [msgs.trade_record(f"X{i}", "1", exchange_order_id="someone-else") for i in range(500)]
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, page)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, page)
            trades = await self.exchange._fetch_account_trades(symbol=msgs.SYMBOL, since_ms=0)
        self.assertEqual(1000, len(trades))
        capped = [r for r in self.log_records if "paging cap" in r.getMessage()]
        self.assertEqual([NETWORK], [r.levelno for r in capped])

    async def test_cancel_timeout_is_not_counted_as_not_found(self):
        # MP-L4: the base class counts a cancel timeout toward lost
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            order = await self.place(replay)
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, exception=asyncio.TimeoutError())
            await self.exchange._execute_cancel(msgs.TRADING_PAIR, "HBOT-B1")
            await self.settle()
        self.assertEqual(0, self.exchange._order_tracker._order_not_found_records.get("HBOT-B1", 0))
        self.assertTrue(order.is_open)

    def test_unknown_outcome_marker_is_the_one_strategies_hold_on(self):
        # MP-H1: amm_arb's outcome hold starts only on this exact text
        try:
            from hummingbot.strategy_v2.utils.outcome_hold import SUBMIT_OUTCOME_UNKNOWN
        except ImportError:
            self.skipTest("outcome_hold.py not mounted (stock image)")
        self.assertEqual(SUBMIT_OUTCOME_UNKNOWN, CONSTANTS.SUBMIT_OUTCOME_UNKNOWN)

    def test_pause_flags_are_refreshed_every_minute(self):
        # MP-L2: the flags come with the trading rules, which refreshed every 30 min
        self.assertEqual(60, self.exchange.TRADING_RULES_INTERVAL)

    def test_check_order_locally_is_public_and_sends_nothing(self):
        # MP-C1: amm_arb runs this before the Gateway leg
        check = self.exchange.check_order_locally
        # [P5]: 5.4 ERG at the buffered 0.185024 is 0.99913 USDT, under minQuote 1
        self.assertIn("below the NonKYC minimum", check(msgs.TRADING_PAIR, False, Decimal("5.4"), Decimal("0.185024")))
        self.assertIsNone(check(msgs.TRADING_PAIR, True, Decimal("6"), Decimal("0.18")))
        # rounded down to the steps first, as a placement would
        self.assertIsNone(check(msgs.TRADING_PAIR, False, Decimal("6.123456789"), Decimal("0.1800004")))
        self.assertIn("paused", check("XPE-USDT", True, Decimal("100"), Decimal("0.5")))
        self.assertIn("buys paused", check("ERG-BTC", True, Decimal("100"), Decimal("0.000003")))
        self.assertIsNone(check("ERG-BTC", False, Decimal("100"), Decimal("0.000003")))
        self.assertIsNotNone(check("NOPE-USDT", True, Decimal("100"), Decimal("0.5")))

    # --- restart ---

    async def test_restart_with_in_flight_orders(self):
        # harness: one order with a booked fill, one whose create reply was lost before the restart
        open_order = InFlightOrder(client_order_id="HBOT-R1", trading_pair=msgs.TRADING_PAIR,
                                   order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("10"),
                                   creation_timestamp=NOW - 600, price=Decimal("0.188"),
                                   exchange_order_id="EXR1", initial_state=OrderState.PARTIALLY_FILLED)
        open_order.update_with_trade_update(TradeUpdate(
            trade_id="TR1", client_order_id="HBOT-R1", exchange_order_id="EXR1", trading_pair=msgs.TRADING_PAIR,
            fill_timestamp=NOW - 500, fill_price=Decimal("0.188"), fill_base_amount=Decimal("4"),
            fill_quote_amount=Decimal("0.752"),
            fee=AddedToCostTradeFee(flat_fees=[TokenAmount("USDT", Decimal("0.0015"))])))
        pending_order = InFlightOrder(client_order_id="HBOT-R2", trading_pair=msgs.TRADING_PAIR,
                                      order_type=OrderType.LIMIT, trade_type=TradeType.BUY, amount=Decimal("10"),
                                      creation_timestamp=NOW - 300, price=Decimal("0.188"))
        saved = {o.client_order_id: o.to_json() for o in (open_order, pending_order)}

        restarted = NonkycExchange(nonkyc_api_key=API_KEY, nonkyc_api_secret=SECRET,
                                   trading_pairs=[msgs.TRADING_PAIR], trading_required=True)
        restarted._time_synchronizer.add_time_offset_ms_sample(wall_clock_offset_ms())
        restarted._set_current_timestamp(NOW)
        restarted._sleep = AsyncMock()
        restarted._initialize_trading_pair_symbols_from_exchange_info(msgs.copy_of(msgs.MARKETS))
        restarted._trading_rules = {r.trading_pair: r for r in await restarted._format_trading_rules(
            msgs.copy_of(msgs.MARKETS))}
        fills = EventLogger()
        restarted.add_listener(MarketEvent.OrderFilled, fills)
        restarted.restore_tracking_states(saved)

        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.USER_TRADES_PATH_URL,
                       [msgs.trade_record("TR2", "3", exchange_order_id="EXR1", timestamp=1795727650000),
                        msgs.trade_record("TR1", "4", exchange_order_id="EXR1", timestamp=1795727100000)])
            replay.get(f"{CONSTANTS.ORDER_INFO_PATH_URL}/EXR1",
                       msgs.order_record("HBOT-R1", executed="7", status="Partly Filled", exchange_order_id="EXR1"))
            replay.get(f"{CONSTANTS.ORDER_INFO_PATH_URL}/HBOT-R2",
                       msgs.order_record("HBOT-R2", exchange_order_id="EXR2"))
            await restarted._update_order_status()
            await self.settle()

        r1 = restarted.in_flight_orders["HBOT-R1"]
        r2 = restarted.in_flight_orders["HBOT-R2"]
        self.assertEqual(Decimal("7"), r1.executed_amount_base)
        self.assertEqual({"TR1", "TR2"}, set(r1.order_fills.keys()))
        self.assertEqual(["TR2"], [f.exchange_trade_id for f in fills.event_log])
        self.assertEqual("EXR2", r2.exchange_order_id)
        self.assertEqual(OrderState.OPEN, r2.current_state)
        since = int((NOW - 600) * 1e3) - CONSTANTS.TRADES_OVERLAP_MS
        self.assertIn(f"since={since}", replay.sent("GET", CONSTANTS.USER_TRADES_PATH_URL)[0]["url"])

    # --- balances and secrets ---

    async def test_balances_are_available_plus_held_and_replaced_whole(self):
        self.exchange._account_balances["OLD"] = Decimal("1")
        self.exchange._account_available_balances["OLD"] = Decimal("1")
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            replay.get(CONSTANTS.USER_BALANCES_PATH_URL, msgs.BALANCES)
            await self.exchange._update_balances()
            replay.get(CONSTANTS.USER_BALANCES_PATH_URL, {"error": {"code": 500}})
            with self.assertRaises(IOError):
                await self.exchange._update_balances()
        self.assertEqual(Decimal("110.5"), self.exchange.get_balance("ERG"))
        self.assertEqual(Decimal("100.5"), self.exchange._account_available_balances["ERG"])
        self.assertEqual(Decimal("27.13"), self.exchange.get_balance("USDT"))
        self.assertNotIn("OLD", self.exchange._account_balances)

    async def test_secrets_stay_out_of_logs_urls_and_errors(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)
            await self.place(replay)
            await self.place(replay, order_id="HBOT-B2", status=401,
                             body=json.dumps({"error": {"code": 1002, "message": "Authorisation failed"}}))
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, status=500, body="{}")
            replay.get(BY_ID, status=400, body=msgs.NOT_FOUND_BODY)
            await self.poll()
        texts = [r.getMessage() for r in self.log_records]
        texts += [str(r.exc_info[1]) for r in self.log_records if r.exc_info]
        texts += [c["url"] for c in replay.calls]
        texts += [repr(self.exchange._auth), repr(self.exchange)]
        for text in texts:
            self.assertNotIn(SECRET, text)
            self.assertNotIn(API_KEY, text)
        for call in replay.calls:
            self.assertNotIn(SECRET, json.dumps(call["headers"]))
            self.assertNotIn(SECRET, call["data"] or "")

    def test_no_print_and_source_credit(self):
        # L4, and the package credits the source and licence
        folder = Path(nonkyc_package.__file__).parent
        for source in folder.glob("*.py"):
            self.assertNotIn("print(", source.read_text(), source.name)
        doc = nonkyc_package.__doc__
        self.assertIn("NonKYCExchange/hummingbot", doc)
        self.assertIn("4d9a98e1e", doc)
        self.assertIn("Apache License", doc)
        self.assertTrue((folder / "dummy.pyx").exists())
        self.assertTrue((folder / "dummy.pxd").exists())
