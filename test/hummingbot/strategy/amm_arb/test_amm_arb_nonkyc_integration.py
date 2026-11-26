"""
amm_arb against a real NonkycExchange on the keyless replay harness.

The Gateway leg is a stub that completes at once. The NonKYC leg goes through the
real strategy order path, the real connector, its auth and REST stack; only aiohttp
is replaced by aioresponses. Order events reach the strategy through its real
listeners. The connector's clock is set by hand; the strategy's waits are real
seconds, kept short: cex_leg_timeout 0.2 s and the connector's SHORT_POLL_INTERVAL
0.05 s (so the wait after a cancel is 0.2 + 6 x 0.05 s instead of 30 + 6 x 5 s).
"""
import asyncio
import re
from decimal import Decimal
from test.hummingbot.connector.exchange.nonkyc import nonkyc_replay_messages as msgs
from test.hummingbot.connector.exchange.nonkyc.test_nonkyc_exchange import (
    API_KEY,
    BASE,
    NOW,
    SECRET,
    Replay,
    wall_clock_offset_ms,
)
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from aioresponses import aioresponses

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.connector.exchange.nonkyc import nonkyc_constants as CONSTANTS
from hummingbot.connector.exchange.nonkyc.nonkyc_exchange import NonkycExchange
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from hummingbot.strategy.amm_arb import amm_arb
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.data_types import ArbProposal, ArbProposalSide
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_base import StrategyBase

BY_ID = f"{CONSTANTS.ORDER_INFO_PATH_URL}/{msgs.EXCHANGE_ORDER_ID}"
ANY_CLIENT_ID_GETORDER = re.compile(r"^" + re.escape(f"{BASE}{CONSTANTS.ORDER_INFO_PATH_URL}/HBOT-") + r".*$")


def without_client_id(record):
    # the client id is generated at placement; a reply without it is accepted (no echo to check)
    return {k: v for k, v in record.items() if k != "userProvidedId"}


class AmmArbNonkycIntegrationTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        amm_arb._PROCESS_OUTCOME_HOLDS.clear()
        self.addCleanup(amm_arb._PROCESS_OUTCOME_HOLDS.clear)
        self.now = NOW

        self.exchange = NonkycExchange(nonkyc_api_key=API_KEY, nonkyc_api_secret=SECRET,
                                       trading_pairs=[msgs.TRADING_PAIR], trading_required=True)
        self.exchange._time_synchronizer.add_time_offset_ms_sample(wall_clock_offset_ms())
        self.exchange._set_current_timestamp(NOW)
        self.exchange._sleep = AsyncMock()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(msgs.copy_of(msgs.MARKETS))
        self.exchange._trading_rules = {r.trading_pair: r for r in await self.exchange._format_trading_rules(
            msgs.copy_of(msgs.MARKETS))}
        self.exchange.SHORT_POLL_INTERVAL = 0.05
        self.events = {}
        for event in (MarketEvent.OrderFilled, MarketEvent.OrderCancelled, MarketEvent.OrderFailure,
                      MarketEvent.BuyOrderCompleted):
            self.events[event] = EventLogger()
            self.exchange.add_listener(event, self.events[event])

        self.gateway = MagicMock()
        self.gateway.name = "ergo-mainnet"
        self.gateway.display_name = "ergo-mainnet"
        self.gateway.order_failure_reason = MagicMock(return_value=None)
        self.gateway.get_balance = MagicMock(return_value=Decimal("100"))
        self.info_1 = MarketTradingPairTuple(self.gateway, "ERG-USDT", "ERG", "USDT")
        self.info_2 = MarketTradingPairTuple(self.exchange, "ERG-USDT", "ERG", "USDT")

        add_markets = StrategyBase.add_markets
        self.notify = MagicMock()
        patchers = [
            # only the real connector is a ConnectorBase the strategy can listen to and order on
            patch.object(AmmArbStrategy, "add_markets", new=lambda strategy, markets: add_markets(
                strategy, [m for m in markets if isinstance(m, ConnectorBase)])),
            patch.object(AmmArbStrategy, "current_timestamp", new_callable=PropertyMock,
                         side_effect=lambda: self.now),
            patch("hummingbot.strategy.amm_arb.amm_arb.AllConnectorSettings.get_gateway_amm_connector_names",
                  return_value={"ergo-mainnet"}),
            patch.object(AmmArbStrategy, "notify_hb_app_with_timestamp", self.notify),
        ]
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)
        AmmArbStrategy.is_gateway_market.cache_clear()
        self.addCleanup(AmmArbStrategy.is_gateway_market.cache_clear)

        self.strategy = AmmArbStrategy()
        self.strategy.init_params(
            market_info_1=self.info_1, market_info_2=self.info_2, min_profitability=Decimal("0.01"),
            order_amount=Decimal("10"), concurrent_orders_submission=False, rate_source=MagicMock(),
            cex_leg_timeout=0.2)
        # no clock runs: the strategy's order tracker stamps its limit orders with this
        self.strategy._set_current_timestamp(NOW)
        self.strategy._sb_order_tracker._set_current_timestamp(NOW)
        self.strategy.CEX_CANCEL_SETTLE_MIN_SECONDS = 0
        self.strategy.CREATE_SENT_POLL_SECONDS = 0.01
        self.set_loggers([self.strategy.logger()])
        self.gateway_orders = 0

    def proposal(self) -> ArbProposal:
        # Gateway sells 10 ERG, NonKYC buys 10 ERG
        return ArbProposal(
            ArbProposalSide(self.info_1, False, Decimal("0.19"), Decimal("0.19"), Decimal("10"), []),
            ArbProposalSide(self.info_2, True, Decimal("0.188"), Decimal("0.188"), Decimal("10"), []),
        )

    def start_proposal(self) -> asyncio.Task:
        real_place = self.strategy.place_arb_order

        async def place(market_info, is_buy, amount, price):
            if market_info.market is self.gateway:
                self.gateway_orders += 1
                order_id = f"GW-{self.gateway_orders}"
                asyncio.get_event_loop().call_soon(self.strategy.set_order_completed, order_id)
                return order_id
            return await real_place(market_info, is_buy, amount, price)
        patcher = patch.object(self.strategy, "place_arb_order", side_effect=place)
        patcher.start()
        self.addCleanup(patcher.stop)
        return asyncio.ensure_future(self.strategy.execute_arb_proposals([self.proposal()]))

    @staticmethod
    async def until(condition, timeout: float = 3.0):
        loop = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while not condition():
            if loop.time() > deadline:
                raise AssertionError("condition not met in time")
            await asyncio.sleep(0.01)
        for _ in range(10):
            await asyncio.sleep(0)

    def count(self, event: MarketEvent) -> int:
        return len(self.events[event].event_log)

    def halt_messages(self):
        return [r.getMessage() for r in self.log_records
                if r.levelname == "ERROR" and r.getMessage().startswith("Arbitrage halted")]

    async def test_cancel_race_then_unknown_outcome_default_sequence(self):
        with aioresponses() as mock_api:
            replay = Replay(mock_api)

            # --- 1. M-1: the leg fills as the cancel lands; NonKYC confirms the cancel, the trade
            # is not listed yet. The order must not go CANCELED, and the strategy must not halt.
            replay.post(CONSTANTS.CREATE_ORDER_PATH_URL, without_client_id(msgs.order_record("x")))
            replay.post(CONSTANTS.CANCEL_ORDER_PATH_URL, {"success": True, "id": msgs.EXCHANGE_ORDER_ID})
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [])
            filled_record = msgs.order_record("x", executed="10", status="Filled", is_active=False)
            replay.get(BY_ID, without_client_id(filled_record), repeat=True)
            first = self.start_proposal()
            # the cancel: fills read, the order re-read by NonKYC id, its trades read again
            await self.until(lambda: len(replay.sent("GET", BY_ID)) == 1
                             and len(replay.sent("GET", CONSTANTS.USER_TRADES_PATH_URL)) == 2)
            order_1 = next(iter(self.exchange.in_flight_orders.values()))
            self.assertEqual(1, len(replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL)))
            self.assertEqual(0, self.count(MarketEvent.OrderCancelled))
            self.assertNotEqual(OrderState.CANCELED, order_1.current_state)

            # the trade is listed: the next status poll books it and the order completes
            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [msgs.trade_record("T1", "10")])
            self.exchange._set_current_timestamp(NOW + 5)
            await self.exchange._update_order_status()
            await asyncio.wait_for(first, timeout=3.0)
            self.assertEqual(OrderState.FILLED, order_1.current_state)
            self.assertEqual(1, self.count(MarketEvent.BuyOrderCompleted))
            self.assertEqual(0, self.count(MarketEvent.OrderCancelled))
            self.assertEqual(["T1"], [f.exchange_trade_id for f in self.events[MarketEvent.OrderFilled].event_log])
            self.assertFalse(self.strategy._halted)
            self.assertEqual([], self.halt_messages())

            # --- 2. L-1, the default sequence: the createorder outcome is unknown (504), the cancel
            # is not sent while it is looked up, the strategy halts after the waits, then the
            # connector fails the order with the marker and the hold starts. The halt stays.
            replay.post(CONSTANTS.CREATE_ORDER_PATH_URL, status=504, body=msgs.GATEWAY_TIMEOUT_BODY)
            mock_api.get(ANY_CLIENT_ID_GETORDER, status=400, body=msgs.NOT_FOUND_BODY, repeat=True)
            second = self.start_proposal()
            await asyncio.wait_for(second, timeout=3.0)
            order_2 = next(iter(self.exchange.in_flight_orders.values()))
            self.assertEqual(OrderState.PENDING_CREATE, order_2.current_state)
            self.assertTrue(self.exchange.create_request_sent(order_2.client_order_id))
            self.assertEqual(1, len(replay.sent("POST", CONSTANTS.CANCEL_ORDER_PATH_URL)))
            self.assertTrue(self.strategy._halted)
            halts = self.halt_messages()
            self.assertEqual(1, len(halts))
            self.assertIn(f"{order_2.client_order_id} (buy) was not cancelled within", halts[0])
            self.assertIn("and may still be open: 0 of 10 ERG booked so far, so 10 ERG is unhedged", halts[0])

            replay.get(CONSTANTS.USER_TRADES_PATH_URL, [], repeat=True)
            replay.get(CONSTANTS.ACCOUNT_ORDERS_PATH_URL, [], repeat=True)
            self.exchange._set_current_timestamp(NOW + 10)
            await self.exchange._update_order_status()
            self.assertEqual(0, self.count(MarketEvent.OrderFailure))
            # the grace period started when the create failed, at NOW + 5
            self.exchange._set_current_timestamp(NOW + 5 + CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS + 1)
            await self.exchange._update_order_status()
            await self.until(lambda: self.count(MarketEvent.OrderFailure) == 1)
            self.assertIn(CONSTANTS.SUBMIT_OUTCOME_UNKNOWN,
                          self.exchange.order_failure_reason(order_2.client_order_id))
            self.assertIn(order_2.client_order_id, self.strategy._outcome_holds)
            self.assertTrue(self.strategy._halted)

            # the hold ending does not resume trading
            self.now += 7200
            create = AsyncMock(return_value=[])
            with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
                await self.strategy.main()
            create.assert_not_awaited()
            self.assertEqual(1, len(self.halt_messages()))
            account_order_urls = [c["url"] for c in replay.sent("GET", CONSTANTS.ACCOUNT_ORDERS_PATH_URL)]
        self.assertTrue(any("status=filled" in url for url in account_order_urls))
