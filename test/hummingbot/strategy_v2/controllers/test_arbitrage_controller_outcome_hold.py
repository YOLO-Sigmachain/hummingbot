import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, patch

from controllers.generic.arbitrage_controller import ArbitrageController, ArbitrageControllerConfig
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.strategy_v2.utils import outcome_hold

T0 = 1_700_000_000.0
WINDOW = 120.0 * (1 + 10)


class ArbitrageControllerOutcomeHoldTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """An executor that reported an order of unknown outcome holds the controller."""

    def setUp(self):
        super().setUp()
        self.now = T0
        outcome_hold.reset_process_outcome_holds()
        self.addCleanup(outcome_hold.reset_process_outcome_holds)
        self.ergo_pair = ConnectorPair(connector_name="ergo-mainnet", trading_pair="ERG-USDT")
        self.cex_pair = ConnectorPair(connector_name="kucoin", trading_pair="ERG-USDT")

        self.ergo = MagicMock()
        self.ergo._block_time_seconds = 120.0
        self.ergo._block_confirmations = 1
        self.ergo._update_balances = AsyncMock()
        self.cex = MagicMock()
        self.cex._update_balances = AsyncMock()

        self.mdp = MagicMock()
        self.mdp.time.side_effect = lambda: self.now
        self.mdp.get_rate.return_value = Decimal("1")
        self.mdp.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        self.mdp.connectors = {"ergo-mainnet": self.ergo, "kucoin": self.cex}

        for name in ("_initialize_gas_tokens", "initialize_rate_sources"):
            p = patch.object(ArbitrageController, name)
            p.start()
            self.addCleanup(p.stop)

        self.controller = self.new_controller()
        self.set_loggers([self.controller.logger()])

    def new_controller(self, controller_id: str = "arb-1", pair_1: ConnectorPair = None) -> ArbitrageController:
        config = ArbitrageControllerConfig(
            id=controller_id,
            exchange_pair_1=pair_1 or self.ergo_pair,
            exchange_pair_2=self.cex_pair,
            total_amount_quote=Decimal("10"),
            rate_connector="kucoin",
        )
        return ArbitrageController(config, market_data_provider=self.mdp, actions_queue=asyncio.Queue())

    def executor(self, buying: ConnectorPair, selling: ConnectorPair, status: RunnableStatus,
                 close_timestamp=None, close_type=None, custom_info=None) -> ExecutorInfo:
        config = ArbitrageExecutorConfig(timestamp=T0 - 500, buying_market=buying, selling_market=selling,
                                         order_amount=Decimal("1"), min_profitability=Decimal("0.01"))
        return ExecutorInfo(
            id=f"exec-{buying.connector_name}-{status.name}", timestamp=T0 - 500, type="arbitrage_executor",
            status=status, config=config, net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal("0"),
            cum_fees_quote=Decimal("0"), filled_amount_quote=Decimal("0"),
            is_active=status != RunnableStatus.TERMINATED, is_trading=False, custom_info=custom_info or {},
            close_timestamp=close_timestamp, close_type=close_type, controller_id="arb-1")

    def unknown_info(self, close_timestamp=None):
        return {"outcome_unknown_order_id": "OID-1", "outcome_unknown_connector": "ergo-mainnet",
                "outcome_unknown_close_timestamp": close_timestamp}

    def history_with_unknown_outcome(self):
        """The other direction closed earlier; the Ergo-buy executor closed FAILED on an unknown outcome."""
        self.controller.executors_info = [
            self.executor(self.cex_pair, self.ergo_pair, RunnableStatus.TERMINATED, T0 - 100, CloseType.COMPLETED),
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.TERMINATED, T0, CloseType.FAILED,
                          self.unknown_info(T0)),
        ]

    async def tick(self):
        await self.controller.update_processed_data()
        return self.controller.determine_executor_actions()

    async def test_no_executor_within_the_window(self):
        self.history_with_unknown_outcome()
        for self.now in (T0 + 11, T0 + 600, T0 + WINDOW - 1):
            self.assertEqual([], await self.tick())
        self.ergo._update_balances.assert_not_awaited()

    async def test_executors_resume_after_window_and_balance_refresh(self):
        self.history_with_unknown_outcome()
        self.now = T0 + WINDOW
        actions = await self.tick()
        self.ergo._update_balances.assert_awaited_once()
        self.assertEqual(2, len(actions))
        self.assertTrue(self.is_partially_logged("INFO", "Hold for order OID-1 on ergo-mainnet ended"))

    async def test_failed_refresh_keeps_hold_and_retries_every_10_seconds(self):
        self.history_with_unknown_outcome()
        self.ergo._update_balances.side_effect = Exception("gateway down")
        self.now = T0 + WINDOW
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("WARNING", "Hold kept: balance refresh of ergo-mainnet"))
        self.now += 9
        self.assertEqual([], await self.tick())
        self.assertEqual(1, self.ergo._update_balances.await_count)

        self.ergo._update_balances.side_effect = None
        self.now += 1
        actions = await self.tick()
        self.assertEqual(2, self.ergo._update_balances.await_count)
        self.assertEqual(2, len(actions))

    async def test_running_executor_with_unknown_outcome_holds_both_directions(self):
        # The flagged executor still waits for its other leg; the opposite direction is idle.
        self.controller.executors_info = [
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.SHUTTING_DOWN,
                          custom_info=self.unknown_info()),
        ]
        self.now = T0 + 3600
        self.assertEqual([], await self.tick())
        self.ergo._update_balances.assert_not_awaited()

    async def test_default_window_without_block_time(self):
        self.ergo._block_time_seconds = None
        self.history_with_unknown_outcome()
        self.now = T0 + 1799
        self.assertEqual([], await self.tick())
        self.now = T0 + 1800
        self.assertEqual(2, len(await self.tick()))

    async def test_failed_executor_without_marker_keeps_old_behaviour(self):
        self.controller.executors_info = [
            self.executor(self.cex_pair, self.ergo_pair, RunnableStatus.TERMINATED, T0 - 100, CloseType.COMPLETED),
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.TERMINATED, T0, CloseType.FAILED),
        ]
        self.now = T0 + 11
        self.assertEqual(2, len(await self.tick()))

    async def test_hold_survives_the_executor_leaving_executors_info_and_a_new_controller(self):
        self.history_with_unknown_outcome()
        self.now = T0 + 11
        self.assertEqual([], await self.tick())

        # The closed-executor buffer stores the flagged executor: it leaves executors_info.
        self.controller.executors_info = self.controller.executors_info[:1]
        self.now = T0 + 300
        self.assertEqual([], await self.tick())

        # A strategy stop/start builds a new controller with no executors.
        self.controller = self.new_controller()
        self.controller.executors_info = []
        self.now = T0 + 600
        self.assertEqual([], await self.tick())
        self.ergo._update_balances.assert_not_awaited()

        self.now = T0 + WINDOW
        self.assertEqual(2, len(await self.tick()))
        self.ergo._update_balances.assert_awaited_once()

    async def test_running_executor_gone_after_a_stop_counts_as_closed_when_noticed(self):
        self.controller.executors_info = [
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.SHUTTING_DOWN,
                          custom_info=self.unknown_info()),
        ]
        self.assertEqual([], await self.tick())

        # Stop/start at T0+600: the executor is gone before it was seen closed.
        self.controller = self.new_controller()
        self.controller.executors_info = []
        self.now = T0 + 600
        self.assertEqual([], await self.tick())
        self.now = T0 + 600 + WINDOW - 1
        self.assertEqual([], await self.tick())
        self.now = T0 + 600 + WINDOW
        self.assertEqual(2, len(await self.tick()))

    async def test_sibling_controller_on_the_same_connector_is_held(self):
        self.controller.executors_info = [
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.SHUTTING_DOWN,
                          custom_info=self.unknown_info()),
        ]
        self.assertEqual([], await self.tick())

        sibling = self.new_controller("arb-2", ConnectorPair(connector_name="ergo-mainnet", trading_pair="ERG-SigUSD"))
        sibling.executors_info = []
        self.now = T0 + 3600
        await sibling.update_processed_data()
        self.assertEqual([], sibling.determine_executor_actions())
        # The sibling does not take the first controller's running executor as closed.
        self.now = T0 + 3600 + WINDOW
        await sibling.update_processed_data()
        self.assertEqual([], sibling.determine_executor_actions())
        self.ergo._update_balances.assert_not_awaited()

    async def test_both_legs_unknown_hold_and_refresh_both_connectors(self):
        info = self.unknown_info(T0)
        info.update({"outcome_unknown_other_order_id": "OID-2", "outcome_unknown_other_connector": "kucoin"})
        self.controller.executors_info = [
            self.executor(self.cex_pair, self.ergo_pair, RunnableStatus.TERMINATED, T0 - 100, CloseType.COMPLETED),
            self.executor(self.ergo_pair, self.cex_pair, RunnableStatus.TERMINATED, T0, CloseType.FAILED, info),
        ]
        self.cex._update_balances.side_effect = Exception("cex down")
        # Each connector has its own window: kucoin knows no block time, so 1800 s.
        self.now = T0 + WINDOW
        self.assertEqual([], await self.tick())
        self.ergo._update_balances.assert_awaited_once()
        self.cex._update_balances.assert_not_awaited()
        self.now = T0 + 1800
        self.assertEqual([], await self.tick())
        self.cex._update_balances.assert_awaited_once()

        self.cex._update_balances.side_effect = None
        self.now += 10
        self.assertEqual(2, len(await self.tick()))
        self.assertEqual(2, self.cex._update_balances.await_count)
        self.assertTrue(self.is_partially_logged("INFO", "Hold for order OID-2 on kucoin ended"))
