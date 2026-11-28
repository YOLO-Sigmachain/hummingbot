import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee
from hummingbot.core.event.events import MarketOrderFailureEvent, OrderCancelledEvent, OrderFilledEvent
from hummingbot.strategy.amm_arb import amm_arb
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.data_types import ArbProposal, ArbProposalSide
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple

UNKNOWN = "createorder outcome unknown [code: SUBMIT_OUTCOME_UNKNOWN]"
T0 = 1_700_000_000.0
REFUSAL = "Order value 0.99913 is below the NonKYC minimum 1 for ERG-USDT"


class AmmArbSecondLegTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """MP-C1 / MP-H2: the non-Gateway second leg is checked before the Gateway leg, bounded in time,
    and an unhedged first leg halts the strategy."""

    def setUp(self):
        super().setUp()
        self.now = T0
        amm_arb._PROCESS_OUTCOME_HOLDS.clear()
        self.addCleanup(amm_arb._PROCESS_OUTCOME_HOLDS.clear)

        self.gateway = MagicMock()
        self.gateway.name = "ergo-mainnet"
        self.gateway.display_name = "ergo-mainnet"
        self.gateway._block_time_seconds = 120.0
        self.gateway._block_confirmations = 1
        self.gateway.order_failure_reason = MagicMock(return_value=None)
        self.gateway.get_balance = MagicMock(return_value=Decimal("10"))

        # CEX leg: a NonKYC-like connector with its own local check
        self.cex = MagicMock()
        self.cex.name = "nonkyc"
        self.cex.display_name = "nonkyc"
        self.cex.check_order_locally = MagicMock(return_value=None)
        self.cex.order_failure_reason = MagicMock(return_value=None)
        self.cex.get_balance = MagicMock(return_value=Decimal("100"))

        self.info_1 = MarketTradingPairTuple(self.gateway, "ERG-USDT", "ERG", "USDT")
        self.info_2 = MarketTradingPairTuple(self.cex, "ERG-USDT", "ERG", "USDT")

        self.notify = MagicMock()
        patchers = [
            patch.object(AmmArbStrategy, "add_markets"),
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
        self.strategy = self.new_strategy()
        self.set_loggers([self.strategy.logger()])
        self.placed = []
        self.cancelled = []
        self.fill_delay = 0.0

    def new_strategy(self, **kwargs) -> AmmArbStrategy:
        strategy = AmmArbStrategy()
        strategy.init_params(
            market_info_1=self.info_1,
            market_info_2=self.info_2,
            min_profitability=Decimal("0.01"),
            order_amount=Decimal("1"),
            concurrent_orders_submission=False,
            rate_source=MagicMock(),
            **kwargs,
        )
        return strategy

    def proposal(self, amount: Decimal = Decimal("1")) -> ArbProposal:
        # Gateway sells, NonKYC buys
        return ArbProposal(
            ArbProposalSide(self.info_1, False, Decimal("0.19"), Decimal("0.19"), amount, []),
            ArbProposalSide(self.info_2, True, Decimal("0.1888"), Decimal("0.1888"), amount, []),
        )

    def placer(self, cex_outcome: str):
        """place_arb_order stub: the Gateway leg completes; the CEX leg completes, fails, is cancelled or rests."""
        async def place(market_info, is_buy, amount, price):
            order_id = f"OID-{len(self.placed) + 1}"
            self.placed.append((market_info.market.name, order_id))
            loop = asyncio.get_event_loop()
            if market_info.market is self.gateway:
                loop.call_soon(self.strategy.set_order_completed, order_id)
            elif cex_outcome == "fill":
                loop.call_soon(self.strategy.set_order_completed, order_id)
            elif cex_outcome == "fail":
                loop.call_soon(self.strategy.did_fail_order,
                               MarketOrderFailureEvent(self.now, order_id, OrderType.LIMIT))
            elif cex_outcome == "cancelled":
                loop.call_soon(self.strategy.did_cancel_order, OrderCancelledEvent(self.now, order_id))
            elif cex_outcome == "fill_later":
                loop.call_later(self.fill_delay, self.strategy.set_order_completed, order_id)
            return order_id
        return place

    def fill(self, order_id: str, amount: str):
        self.strategy.did_fill_order(OrderFilledEvent(
            self.now, order_id, "ERG-USDT", TradeType.BUY, OrderType.LIMIT, Decimal("0.1888"), Decimal(amount),
            AddedToCostTradeFee()))

    async def execute(self, proposals, cex_outcome: str, timeout: float = 2.0):
        with patch.object(self.strategy, "place_arb_order", side_effect=self.placer(cex_outcome)):
            await asyncio.wait_for(self.strategy.execute_arb_proposals(proposals), timeout=timeout)

    def halt_messages(self):
        return [r.getMessage() for r in self.log_records
                if r.levelname == "ERROR" and r.getMessage().startswith("Arbitrage halted")]

    # --- MP-C1: the check before the Gateway leg ---

    async def test_refused_second_leg_skips_the_proposal_before_the_gateway_leg(self):
        self.cex.check_order_locally.return_value = REFUSAL
        await self.execute([self.proposal()], "fill")
        self.assertEqual([], self.placed)
        self.cex.check_order_locally.assert_called_once_with("ERG-USDT", True, Decimal("1"), Decimal("0.1888"))
        self.assertTrue(self.is_partially_logged("INFO", f"Skipping arbitrage proposal: nonkyc would refuse the "
                                                         f"buy of 1 ERG at 0.1888: {REFUSAL}"))
        self.assertFalse(self.strategy._halted)

    async def test_trading_rules_check_when_the_connector_has_no_check(self):
        del self.cex.check_order_locally
        self.cex.trading_rules = {"ERG-USDT": TradingRule(
            "ERG-USDT", min_order_size=Decimal("0.0001"), min_price_increment=Decimal("0.000001"),
            min_base_amount_increment=Decimal("0.0001"), min_notional_size=Decimal("1"))}
        self.cex.quantize_order_amount = MagicMock(side_effect=lambda pair, amount: amount.quantize(Decimal("0.0001")))
        self.cex.quantize_order_price = MagicMock(side_effect=lambda pair, price: price)
        await self.execute([self.proposal(Decimal("5"))], "fill")      # 5 x 0.1888 = 0.944 < 1
        self.assertEqual([], self.placed)
        self.assertTrue(self.is_partially_logged("INFO", "below the minimum notional 1"))
        await self.execute([self.proposal(Decimal("0.00001"))], "fill")  # rounds to zero
        self.assertEqual([], self.placed)
        self.assertTrue(self.is_partially_logged("INFO", "rounds to zero"))
        await self.execute([self.proposal(Decimal("6"))], "fill")
        self.assertEqual([("ergo-mainnet", "OID-1"), ("nonkyc", "OID-2")], self.placed)

    async def test_definite_second_leg_failure_halts_and_names_the_unhedged_amount(self):
        await self.execute([self.proposal(), self.proposal()], "fail")
        # the second proposal was not started
        self.assertEqual([("ergo-mainnet", "OID-1"), ("nonkyc", "OID-2")], self.placed)
        self.assertTrue(self.strategy._halted)
        halts = self.halt_messages()
        self.assertEqual(1, len(halts))
        self.assertIn("the ergo-mainnet leg sold 1 ERG", halts[0])
        self.assertIn("nonkyc leg OID-2 (buy) failed: 0 of 1 ERG booked so far, so 1 ERG is unhedged", halts[0])
        self.notify.assert_called_once_with(halts[0])

        create = AsyncMock(return_value=[])
        with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
            await self.strategy.main()
        create.assert_not_awaited()

    async def test_unknown_outcome_second_leg_holds_and_halts(self):
        # L-1: the outcome hold (FORK-NOTES 2.2) starts, and the halt too: the hold ending used to
        # resume trading while the Gateway leg may be unhedged
        self.cex.order_failure_reason = MagicMock(return_value=UNKNOWN)
        await self.execute([self.proposal()], "fail")
        self.assertIn("OID-2", self.strategy._outcome_holds)
        self.assertTrue(self.strategy._halted)
        self.assertIn("OID-2 (buy) failed with an unknown outcome and may still fill: 0 of 1 ERG booked so far",
                      self.halt_messages()[0])

    async def test_unknown_outcome_after_the_cancel_halts_whatever_the_timeout(self):
        # L-1: with a long cex_leg_timeout the marker failure lands inside the second wait; it held
        # and then resumed trading
        self.strategy = self.new_strategy(cex_leg_timeout=0.05)
        self.cex.order_failure_reason = MagicMock(return_value=UNKNOWN)
        self.cex._update_balances = AsyncMock()

        def cancel(market_info, order_id):
            asyncio.get_event_loop().call_soon(self.strategy.did_fail_order,
                                               MarketOrderFailureEvent(self.now, order_id, OrderType.LIMIT))
        with patch.object(self.strategy, "cancel_order", side_effect=cancel):
            await self.execute([self.proposal()], "rest")
        self.assertIn("OID-2", self.strategy._outcome_holds)
        self.assertTrue(self.strategy._halted)
        self.assertIn("failed with an unknown outcome and may still fill", self.halt_messages()[0])
        # the hold ending does not clear the halt
        self.now += 7200
        create = AsyncMock(return_value=[])
        with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
            await self.strategy.main()
        create.assert_not_awaited()

    async def test_filled_second_leg_does_not_halt(self):
        await self.execute([self.proposal()], "fill")
        self.assertFalse(self.strategy._halted)
        self.assertEqual([], self.halt_messages())

    # --- MP-H2: a cancelled or resting second leg ---

    async def test_cancelled_second_leg_ends_the_wait_and_halts(self):
        # amm_arb had no did_cancel_order, so a manual cancel hung arb_proposal.wait()
        await self.execute([self.proposal()], "cancelled")
        self.assertTrue(self.strategy._halted)
        self.assertIn("so 1 ERG is unhedged", self.halt_messages()[0])

    async def test_resting_second_leg_is_cancelled_after_the_timeout_and_the_remainder_reported(self):
        self.strategy = self.new_strategy(cex_leg_timeout=0.05)
        self.strategy.CEX_CANCEL_SETTLE_MIN_SECONDS = 0.05

        def cancel(market_info, order_id):
            self.cancelled.append((market_info, order_id))
            self.fill(order_id, "0.4")
            asyncio.get_event_loop().call_soon(self.strategy.did_cancel_order, OrderCancelledEvent(self.now, order_id))
        with patch.object(self.strategy, "cancel_order", side_effect=cancel):
            await self.execute([self.proposal()], "rest")
        self.assertEqual([(self.info_2, "OID-2")], self.cancelled)
        self.assertTrue(self.strategy._halted)
        halts = self.halt_messages()
        self.assertEqual(1, len(halts))
        self.assertIn("OID-2 (buy) was cancelled after the timeout: 0.4 of 1 ERG booked so far, so 0.6 ERG is unhedged",
                      halts[0])
        self.assertTrue(self.is_partially_logged("WARNING", "not done after 0 s. Cancelling it."))

    async def test_second_leg_that_cannot_be_cancelled_halts_as_possibly_open(self):
        self.strategy = self.new_strategy(cex_leg_timeout=0.05)
        self.strategy.CEX_CANCEL_SETTLE_MIN_SECONDS = 0.05
        with patch.object(self.strategy, "cancel_order", side_effect=lambda *args: self.cancelled.append(args)):
            await self.execute([self.proposal()], "rest")
        self.assertEqual(1, len(self.cancelled))
        self.assertTrue(self.strategy._halted)
        self.assertIn("was not cancelled within 0 s and may still be open: 0 of 1 ERG booked so far",
                      self.halt_messages()[0])

    def test_default_cex_leg_timeout_is_30_seconds(self):
        self.assertEqual(30.0, self.strategy._cex_leg_timeout)
        # M-1: the wait after the cancel adds at least 6 deferrals x 5 s
        self.assertEqual(30.0, self.strategy._cancel_settle_seconds(self.cex))
        self.cex.MAX_TERMINAL_DEFERRALS = 6
        self.cex.SHORT_POLL_INTERVAL = 10.0
        self.assertEqual(60.0, self.strategy._cancel_settle_seconds(self.cex))

    # --- M-1: a cancel whose fills are listed late ---

    async def test_fill_booked_after_the_halt_corrects_the_halt_message(self):
        # M-1: a fill of the halted leg booked later changed _filled_base silently
        await self.execute([self.proposal()], "cancelled")
        self.assertIn("0 of 1 ERG booked so far, so 1 ERG is unhedged", self.halt_messages()[0])
        self.fill("OID-2", "0.7")
        self.fill("OID-9", "1")  # another order: no correction
        corrections = [r.getMessage() for r in self.log_records
                       if r.levelname == "ERROR" and r.getMessage().startswith("Correction: Arbitrage halted")]
        self.assertEqual(1, len(corrections))
        self.assertIn("nonkyc leg OID-2 (buy) failed:", corrections[0])
        self.assertIn("0.7 of 1 ERG booked so far, so 0.3 ERG is unhedged", corrections[0])
        self.assertEqual(2, self.notify.call_count)
        self.notify.assert_called_with(corrections[0])
        self.assertEqual(corrections[0], self.strategy._halt_reason)

    async def test_wait_after_the_cancel_covers_the_connector_deferrals(self):
        # M-1: the second wait was cex_leg_timeout only, shorter than the connector holds back a
        # terminal state while the trades are listed (MAX_TERMINAL_DEFERRALS x SHORT_POLL_INTERVAL)
        self.strategy = self.new_strategy(cex_leg_timeout=0.05)
        self.strategy.CEX_CANCEL_SETTLE_MIN_SECONDS = 0
        self.cex.MAX_TERMINAL_DEFERRALS = 3
        self.cex.SHORT_POLL_INTERVAL = 0.1

        def cancel(market_info, order_id):
            # the trades are listed 0.2 s after the cancel and the order completes
            asyncio.get_event_loop().call_later(0.2, self.strategy.set_order_completed, order_id)
        with patch.object(self.strategy, "cancel_order", side_effect=cancel):
            await self.execute([self.proposal()], "rest")
        self.assertFalse(self.strategy._halted)
        self.assertEqual([], self.halt_messages())

    # --- L-1: the timer starts when the request goes out ---

    async def test_leg_timer_starts_when_the_order_request_goes_out(self):
        # L-1: the timer started at placement, while createorder could still wait for the
        # signed-request lock, so the order rested on the exchange for less than cex_leg_timeout
        # The request goes out 0.2 s after placement and the order fills 0.6 s after placement.
        # Timed from placement (the old code) the cancel comes at 0.5 s, before the fill; timed
        # from the request going out it comes at 0.7 s, after it. 0.1 s margin either way.
        self.strategy = self.new_strategy(cex_leg_timeout=0.5)
        self.strategy.CREATE_SENT_POLL_SECONDS = 0.01
        loop = asyncio.get_event_loop()
        placed_at = []
        self.cex.create_request_sent = MagicMock(
            side_effect=lambda order_id: bool(placed_at) and loop.time() - placed_at[0] >= 0.2)
        self.fill_delay = 0.6
        place = self.placer("fill_later")

        async def place_and_note(market_info, *args):
            if market_info.market is self.cex:
                placed_at.append(loop.time())
            return await place(market_info, *args)
        with patch.object(self.strategy, "cancel_order", side_effect=lambda *args: self.cancelled.append(args)), \
                patch.object(self.strategy, "place_arb_order", side_effect=place_and_note):
            await asyncio.wait_for(self.strategy.execute_arb_proposals([self.proposal()]), timeout=2.0)
        self.assertEqual([], self.cancelled)
        self.assertFalse(self.strategy._halted)
        self.cex.create_request_sent.assert_called_with("OID-2")

    async def test_unknown_outcome_on_gateway_second_leg_also_halts(self):
        # DEX-to-DEX sequential arb: leg 1 filled on Gateway #1, leg 2 on Gateway #2 fails with
        # SUBMIT_OUTCOME_UNKNOWN. An outcome hold starts, and the strategy must also halt so the
        # hold expiring does not resume trading while leg 1 is unhedged.
        gw2 = MagicMock()
        gw2.name = "ergo-mainnet-2"
        gw2.display_name = "ergo-mainnet-2"
        gw2._block_time_seconds = 120.0
        gw2._block_confirmations = 1
        reasons = {}
        self.gateway.order_failure_reason = MagicMock(side_effect=lambda oid: reasons.get(oid))
        gw2.order_failure_reason = MagicMock(side_effect=lambda oid: reasons.get(oid))
        gw2.get_balance = MagicMock(return_value=Decimal("100"))
        gw2._update_balances = AsyncMock()
        info_2 = MarketTradingPairTuple(gw2, "ERG-USDT", "ERG", "USDT")

        with patch("hummingbot.strategy.amm_arb.amm_arb.AllConnectorSettings.get_gateway_amm_connector_names",
                   return_value={"ergo-mainnet", "ergo-mainnet-2"}):
            AmmArbStrategy.is_gateway_market.cache_clear()
            strategy = AmmArbStrategy()
            strategy.init_params(
                market_info_1=self.info_1,
                market_info_2=info_2,
                min_profitability=Decimal("0.01"),
                order_amount=Decimal("1"),
                concurrent_orders_submission=False,
                rate_source=MagicMock(),
            )
            self.set_loggers([strategy.logger()])
            proposal = ArbProposal(
                ArbProposalSide(self.info_1, False, Decimal("0.19"), Decimal("0.19"), Decimal("1"), []),
                ArbProposalSide(info_2, True, Decimal("0.1888"), Decimal("0.1888"), Decimal("1"), []),
            )

            async def place(market_info, is_buy, amount, price):
                order_id = f"OID-{len(self.placed) + 1}"
                self.placed.append((market_info.market.name, order_id))
                loop = asyncio.get_event_loop()
                if len(self.placed) == 1:
                    loop.call_soon(strategy.set_order_completed, order_id)
                else:
                    reasons[order_id] = UNKNOWN
                    loop.call_soon(strategy.did_fail_order,
                                   MarketOrderFailureEvent(self.now, order_id, OrderType.MARKET))
                return order_id

            with patch.object(strategy, "place_arb_order", side_effect=place):
                await asyncio.wait_for(strategy.execute_arb_proposals([proposal]), timeout=2.0)

            self.assertIn("OID-2", strategy._outcome_holds)
            self.assertTrue(strategy._halted)
            self.assertEqual(1, len(self.halt_messages()))
            self.assertIn("OID-2 (", self.halt_messages()[0])
            self.assertIn("failed with an unknown outcome and may still fill",
                          self.halt_messages()[0])

            # Expiry of the outcome hold must not resume trading while halted.
            self.now += 7200
            create = AsyncMock(return_value=[])
            with patch("hummingbot.strategy.amm_arb.amm_arb.create_arb_proposals", create):
                await strategy.main()
            create.assert_not_awaited()
