import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import BuyOrderCompletedEvent, MarketOrderFailureEvent, SellOrderCompletedEvent
from hummingbot.strategy.amm_arb import amm_arb
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.data_types import ArbProposal, ArbProposalSide
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

UNKNOWN = "ClientError() [code: SUBMIT_OUTCOME_UNKNOWN]"
DEFINITE = "Gateway error 409 [code: SLIPPAGE_EXCEEDED]"
T0 = 1_700_000_000.0


class AmmArbOutcomeHoldTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """A leg failed with SUBMIT_OUTCOME_UNKNOWN holds the strategy until the outcome is settled."""

    def setUp(self):
        super().setUp()
        self.now = T0
        self.reasons = {}
        self.balances = {"ERG": Decimal("10"), "USDT": Decimal("100")}
        amm_arb._PROCESS_OUTCOME_HOLDS.clear()
        self.addCleanup(amm_arb._PROCESS_OUTCOME_HOLDS.clear)

        # Gateway leg: knows the network's block timing and remembers failure reasons.
        self.gateway = MagicMock()
        self.gateway.name = "ergo-mainnet"
        self.gateway.display_name = "ergo-mainnet"
        self.gateway._block_time_seconds = 120.0
        self.gateway._block_confirmations = 1
        self.gateway.order_failure_reason = MagicMock(side_effect=lambda oid: self.reasons.get(oid))
        self.gateway._update_balances = AsyncMock()
        self.gateway.get_balance = MagicMock(side_effect=lambda asset: self.balances[asset])

        # CEX leg: no failure reasons.
        self.cex = MagicMock()
        self.cex.name = "binance"
        self.cex.display_name = "binance"
        del self.cex.order_failure_reason
        self.cex._update_balances = AsyncMock()

        self.info_1 = MarketTradingPairTuple(self.gateway, "ERG-USDT", "ERG", "USDT")
        self.info_2 = MarketTradingPairTuple(self.cex, "ERG-USDT", "ERG", "USDT")

        patchers = [
            patch.object(AmmArbStrategy, "add_markets"),
            patch.object(AmmArbStrategy, "current_timestamp", new_callable=PropertyMock,
                         side_effect=lambda: self.now),
            patch("hummingbot.strategy.amm_arb.amm_arb.AllConnectorSettings.get_gateway_amm_connector_names",
                  return_value={"ergo-mainnet"}),
        ]
        self.notify = MagicMock()
        patchers.append(patch.object(AmmArbStrategy, "notify_hb_app_with_timestamp", self.notify))
        for p in patchers:
            p.start()
            self.addCleanup(p.stop)

        self.strategy = AmmArbStrategy()
        self.strategy.init_params(
            market_info_1=self.info_1,
            market_info_2=self.info_2,
            min_profitability=Decimal("0.01"),
            order_amount=Decimal("1"),
            concurrent_orders_submission=False,
            rate_source=MagicMock(),
        )
        self.set_loggers([self.strategy.logger()])
        self.placed = []

    def new_strategy(self) -> AmmArbStrategy:
        strategy = AmmArbStrategy()
        strategy.init_params(
            market_info_1=self.info_1,
            market_info_2=self.info_2,
            min_profitability=Decimal("0.01"),
            order_amount=Decimal("1"),
            concurrent_orders_submission=False,
            rate_source=MagicMock(),
        )
        return strategy

    def proposal(self) -> ArbProposal:
        return ArbProposal(
            ArbProposalSide(self.info_1, False, Decimal("2"), Decimal("2"), Decimal("1"), []),
            ArbProposalSide(self.info_2, True, Decimal("1"), Decimal("1"), Decimal("1"), []),
        )

    def fail_later(self, order_id: str, reason: str):
        self.reasons[order_id] = reason
        asyncio.get_event_loop().call_soon(
            self.strategy.did_fail_order, MarketOrderFailureEvent(self.now, order_id, OrderType.MARKET))

    def place_failing(self, reason: str):
        """place_arb_order stub: the Gateway leg fails with `reason`, the CEX leg completes."""
        async def place(market_info, is_buy, amount, price):
            order_id = f"OID-{len(self.placed) + 1}"
            self.placed.append((market_info.market.name, order_id))
            if market_info.market is self.gateway:
                self.fail_later(order_id, reason)
            else:
                asyncio.get_event_loop().call_soon(self.strategy.set_order_completed, order_id)
            return order_id
        return place

    async def run_main(self, create_mock: AsyncMock):
        with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create_mock):
            await self.strategy.main()

    async def hold_after_unknown_failure(self):
        with patch.object(self.strategy, "place_arb_order", side_effect=self.place_failing(UNKNOWN)):
            await self.strategy.execute_arb_proposals([self.proposal(), self.proposal()])

    async def test_unknown_outcome_drops_proposal_and_holds(self):
        await self.hold_after_unknown_failure()

        # Only the Gateway leg was placed; the CEX leg and the second proposal were not.
        self.assertEqual([("ergo-mainnet", "OID-1")], self.placed)
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.assertEqual(T0 + 120 * (1 + 10), hold_end)
        warnings = [r for r in self.log_records
                    if r.levelname == "WARNING" and "OID-1" in r.getMessage() and "unknown outcome" in r.getMessage()]
        self.assertEqual(1, len(warnings))
        self.assertIn(AmmArbStrategy._format_ts(hold_end), warnings[0].getMessage())

        # No new proposal while held.
        create = AsyncMock(return_value=[])
        self.now = hold_end - 1
        await self.run_main(create)
        create.assert_not_awaited()
        self.gateway._update_balances.assert_not_awaited()

    async def test_definite_failure_keeps_old_behaviour(self):
        with patch.object(self.strategy, "place_arb_order", side_effect=self.place_failing(DEFINITE)):
            await self.strategy.execute_arb_proposals([self.proposal()])

        self.assertEqual([("ergo-mainnet", "OID-1")], self.placed)
        self.assertEqual({}, self.strategy._outcome_holds)
        create = AsyncMock(return_value=[])
        await self.run_main(create)
        create.assert_awaited_once()

    async def test_definite_failure_runs_next_proposal_in_concurrent_mode(self):
        self.strategy._concurrent_orders_submission = True
        with patch.object(self.strategy, "place_arb_order", side_effect=self.place_failing(DEFINITE)):
            await self.strategy.execute_arb_proposals([self.proposal(), self.proposal()])
        self.assertEqual(4, len(self.placed))

    async def test_unknown_outcome_in_concurrent_mode_keeps_cex_leg_and_stops_list(self):
        self.strategy._concurrent_orders_submission = True
        with patch.object(self.strategy, "place_arb_order", side_effect=self.place_failing(UNKNOWN)):
            await self.strategy.execute_arb_proposals([self.proposal(), self.proposal()])

        # Both legs of the first proposal went out as today; the second proposal did not.
        self.assertEqual([("ergo-mainnet", "OID-1"), ("binance", "OID-2")], self.placed)
        self.assertIn("OID-1", self.strategy._outcome_holds)

    async def test_hold_ends_after_window_and_balance_refresh(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]

        self.now = hold_end
        create = AsyncMock(return_value=[])
        await self.run_main(create)

        self.gateway._update_balances.assert_awaited_once()
        create.assert_awaited_once()
        self.assertEqual({}, self.strategy._outcome_holds)
        self.assertTrue(self.is_partially_logged("WARNING", "Hold for order OID-1 on ergo-mainnet ended"))

    async def test_failed_balance_refresh_keeps_hold(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.now = hold_end + 5
        self.gateway._update_balances.side_effect = Exception("gateway down")

        create = AsyncMock(return_value=[])
        await self.run_main(create)
        create.assert_not_awaited()
        self.assertIn("OID-1", self.strategy._outcome_holds)
        self.assertTrue(self.is_partially_logged("WARNING", "Hold kept"))

        self.gateway._update_balances.side_effect = None
        self.now += 10
        await self.run_main(create)
        create.assert_awaited_once()

    async def test_default_window_without_block_time(self):
        self.gateway._block_time_seconds = None
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.assertEqual(T0 + 1800.0, hold_end)

    async def test_reported_fill_ends_window_then_refresh(self):
        await self.hold_after_unknown_failure()
        self.now = T0 + 60

        tracker = MagicMock()
        tracker.get_market_pair_from_order_id.return_value = self.info_1
        with patch.object(AmmArbStrategy, "order_tracker", new_callable=PropertyMock, return_value=tracker), \
                patch.object(self.strategy, "notify_hb_app_with_timestamp"):
            self.strategy.did_complete_sell_order(SellOrderCompletedEvent(
                self.now, "OID-1", "ERG", "USDT", Decimal("1"), Decimal("2"), OrderType.MARKET))

        create = AsyncMock(return_value=[])
        await self.run_main(create)
        self.gateway._update_balances.assert_awaited_once()
        create.assert_awaited_once()

    async def test_later_failure_report_for_held_order_changes_nothing(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.now = T0 + 60
        self.reasons["OID-1"] = DEFINITE
        self.strategy.did_fail_order(MarketOrderFailureEvent(self.now, "OID-1", OrderType.MARKET))

        self.assertEqual(hold_end, self.strategy._outcome_holds["OID-1"][1])
        create = AsyncMock(return_value=[])
        await self.run_main(create)
        create.assert_not_awaited()
        self.gateway._update_balances.assert_not_awaited()

    async def test_failed_refresh_is_retried_at_most_every_10_seconds(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.gateway._update_balances.side_effect = Exception("gateway down")
        create = AsyncMock(return_value=[])

        self.now = hold_end
        await self.run_main(create)
        for step in range(1, 10):
            self.now = hold_end + step
            await self.run_main(create)
        self.assertEqual(1, self.gateway._update_balances.await_count)

        self.now = hold_end + 10
        await self.run_main(create)
        self.assertEqual(2, self.gateway._update_balances.await_count)
        create.assert_not_awaited()

    async def test_strategy_stop_and_start_keeps_the_hold(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.strategy.stop(MagicMock())

        restarted = self.new_strategy()
        self.assertEqual(hold_end, restarted._outcome_holds["OID-1"][1])
        create = AsyncMock(return_value=[])
        self.now = hold_end - 1
        with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
            await restarted.main()
        create.assert_not_awaited()

        self.now = hold_end
        with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
            await restarted.main()
        self.gateway._update_balances.assert_awaited_once()
        create.assert_awaited_once()
        self.assertEqual({}, amm_arb._PROCESS_OUTCOME_HOLDS)

    async def test_hold_end_reports_landed_swap_with_other_leg_never_placed(self):
        await self.hold_after_unknown_failure()
        self.assertTrue(self.is_partially_logged("WARNING", "The other leg was not placed"))
        _, hold_end = self.strategy._outcome_holds["OID-1"]
        self.balances["ERG"] = Decimal("9")  # the Gateway leg sold 1 ERG: the swap landed

        self.now = hold_end
        await self.run_main(AsyncMock(return_value=[]))

        warnings = [r.getMessage() for r in self.log_records
                    if r.levelname == "WARNING" and "Hold for order OID-1" in r.getMessage()]
        self.assertEqual(1, len(warnings))
        self.assertIn("the swap appears to have landed", warnings[0])
        self.assertIn("never placed: the position is one-sided by 1 ERG", warnings[0])
        self.notify.assert_called_once_with(warnings[0])

    async def test_hold_end_reports_no_balance_change(self):
        await self.hold_after_unknown_failure()
        _, hold_end = self.strategy._outcome_holds["OID-1"]

        self.now = hold_end
        await self.run_main(AsyncMock(return_value=[]))

        self.assertTrue(self.is_partially_logged("WARNING", "no balance change seen"))
        self.assertTrue(self.is_partially_logged("WARNING", "the swap appears not to have landed"))
        self.notify.assert_called_once()

    async def test_concurrent_mode_warning_says_the_other_leg_was_sent(self):
        self.strategy._concurrent_orders_submission = True
        await self.hold_after_unknown_failure()
        self.assertTrue(self.is_partially_logged("WARNING", "The other leg OID-2 on binance was sent"))
        _, hold_end = self.strategy._outcome_holds["OID-1"]

        self.now = hold_end
        await self.run_main(AsyncMock(return_value=[]))
        self.assertTrue(self.is_partially_logged(
            "WARNING", "The other leg OID-2 on binance was placed: its fill is unhedged"))

    async def test_hold_end_judges_against_the_balance_before_placement(self):
        # The swap's change is already in the balance when the failure arrives (PROBE-C).
        def place_and_sell(reason):
            stub = self.place_failing(reason)

            async def place(market_info, is_buy, amount, price):
                order_id = await stub(market_info, is_buy, amount, price)
                if market_info.market is self.gateway:
                    self.balances["ERG"] = Decimal("9")
                return order_id
            return place

        with patch.object(self.strategy, "place_arb_order", side_effect=place_and_sell(UNKNOWN)):
            await self.strategy.execute_arb_proposals([self.proposal()])
        record = amm_arb._PROCESS_OUTCOME_HOLDS["OID-1"]
        self.assertEqual(Decimal("10"), record.base_at_placement)
        self.assertEqual(Decimal("9"), record.base_before)
        self.assertEqual({}, self.strategy._placement_balances)
        _, hold_end = self.strategy._outcome_holds["OID-1"]

        self.now = hold_end
        await self.run_main(AsyncMock(return_value=[]))

        warnings = [r.getMessage() for r in self.log_records
                    if r.levelname == "WARNING" and "Hold for order OID-1" in r.getMessage()]
        self.assertEqual(1, len(warnings))
        self.assertIn("the balance had already moved before the failure was reported", warnings[0])
        self.assertIn("the swap appears to have landed", warnings[0])
        self.assertIn("one-sided by 1 ERG", warnings[0])
        self.assertNotIn("no balance change", warnings[0])

    async def test_definite_failure_drops_the_placement_snapshot(self):
        with patch.object(self.strategy, "place_arb_order", side_effect=self.place_failing(DEFINITE)):
            await self.strategy.execute_arb_proposals([self.proposal()])
        self.assertEqual({}, self.strategy._placement_balances)

    async def test_stop_and_start_with_another_wallet_does_not_take_the_hold(self):
        self.gateway.address = "9fWalletA"
        await self.hold_after_unknown_failure()
        self.assertEqual("9fWalletA", amm_arb._PROCESS_OUTCOME_HOLDS["OID-1"].address)
        self.strategy.stop(MagicMock())

        self.gateway.address = "9fWalletB"
        self.assertNotIn("OID-1", self.new_strategy()._outcome_holds)

        self.gateway.address = "9fWalletA"
        self.assertIn("OID-1", self.new_strategy()._outcome_holds)

    async def test_late_completion_after_order_tracker_eviction_does_not_crash(self):
        # After did_fail_order, StrategyBase.order_tracker no longer tracks the order id and
        # get_market_pair_from_order_id returns None. Late buy/sell completion must fall back
        # to _order_id_side_map (and tolerate missing side entries too) without AttributeError.
        await self.hold_after_unknown_failure()
        self.now = T0 + 60

        tracker = MagicMock()
        tracker.get_market_pair_from_order_id.return_value = None
        with patch.object(AmmArbStrategy, "order_tracker", new_callable=PropertyMock, return_value=tracker):
            self.strategy.did_complete_sell_order(SellOrderCompletedEvent(
                self.now, "OID-1", "ERG", "USDT", Decimal("1"), Decimal("2"), OrderType.MARKET,
                exchange_order_id="0xtx1"))
            self.strategy.did_complete_buy_order(BuyOrderCompletedEvent(
                self.now, "OID-UNKNOWN", "ERG", "USDT", Decimal("1"), Decimal("2"), OrderType.MARKET,
                exchange_order_id="0xtx2"))

        self.assertTrue(self.is_partially_logged(
            "INFO", "Sell order completed on ergo-mainnet: OID-1. txHash: 0xtx1"))
        self.assertTrue(self.is_partially_logged(
            "INFO", "Buy order completed on unknown market: OID-UNKNOWN."))
        create = AsyncMock(return_value=[])
        await self.run_main(create)
        self.gateway._update_balances.assert_awaited_once()
        create.assert_awaited_once()
