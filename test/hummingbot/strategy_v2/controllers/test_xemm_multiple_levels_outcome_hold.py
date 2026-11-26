import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock, patch

from controllers.generic.xemm_multiple_levels import XEMMMultipleLevels, XEMMMultipleLevelsConfig
from hummingbot.core.data_type.common import TradeType
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.strategy_v2.utils import outcome_hold

T0 = 1_700_000_000.0
WINDOW = 120.0 * (1 + 10)


class XEMMMultipleLevelsOutcomeHoldTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    """An XEMM executor that reported a taker swap of unknown outcome holds every level."""

    def setUp(self):
        super().setUp()
        self.now = T0
        outcome_hold.reset_process_outcome_holds()
        self.addCleanup(outcome_hold.reset_process_outcome_holds)
        self.ergo = MagicMock()
        self.ergo._block_time_seconds = 120.0
        self.ergo._block_confirmations = 1
        self.ergo._update_balances = AsyncMock()
        self.cex = MagicMock()
        self.cex._update_balances = AsyncMock()

        self.mdp = MagicMock()
        self.mdp.time.side_effect = lambda: self.now
        self.mdp.get_price_by_type.return_value = Decimal("1")
        self.mdp.connectors = {"ergo-mainnet": self.ergo, "kucoin": self.cex}

        for name in ("_initialize_gas_tokens", "initialize_rate_sources"):
            p = patch.object(XEMMMultipleLevels, name)
            p.start()
            self.addCleanup(p.stop)

        config = XEMMMultipleLevelsConfig(
            id="xemm-1",
            maker_connector="kucoin",
            maker_trading_pair="ERG-USDT",
            taker_connector="ergo-mainnet",
            taker_trading_pair="ERG-USDT",
            buy_levels_targets_amount="0.01,1",
            sell_levels_targets_amount="0.01,1",
            min_profitability=Decimal("0.003"),
            max_profitability=Decimal("0.01"),
            total_amount_quote=Decimal("10"),
        )
        self.controller = XEMMMultipleLevels(config, market_data_provider=self.mdp, actions_queue=asyncio.Queue())
        self.set_loggers([self.controller.logger()])

    def failed_executor(self, status: RunnableStatus = RunnableStatus.TERMINATED, close_timestamp=T0) -> ExecutorInfo:
        config = XEMMExecutorConfig(
            timestamp=T0 - 500,
            buying_market=ConnectorPair(connector_name="kucoin", trading_pair="ERG-USDT"),
            selling_market=ConnectorPair(connector_name="ergo-mainnet", trading_pair="ERG-USDT"),
            maker_side=TradeType.BUY, order_amount=Decimal("1"), min_profitability=Decimal("0.007"),
            target_profitability=Decimal("0.01"), max_profitability=Decimal("0.02"))
        done = status == RunnableStatus.TERMINATED
        return ExecutorInfo(
            id="exec-1", timestamp=T0 - 500, type="xemm_executor", status=status, config=config,
            net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal("0"), cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"), is_active=not done, is_trading=False,
            custom_info={"side": TradeType.BUY, "outcome_unknown_order_id": "OID-TAKER-1",
                         "outcome_unknown_connector": "ergo-mainnet",
                         "outcome_unknown_close_timestamp": close_timestamp if done else None},
            close_timestamp=close_timestamp if done else None, close_type=CloseType.FAILED if done else None,
            controller_id="xemm-1")

    async def tick(self):
        await self.controller.update_processed_data()
        return self.controller.determine_executor_actions()

    async def test_no_level_is_recreated_within_the_window(self):
        self.controller.executors_info = [self.failed_executor()]
        for self.now in (T0 + 1, T0 + WINDOW - 1):
            self.assertEqual([], await self.tick())
        self.ergo._update_balances.assert_not_awaited()

    async def test_levels_resume_after_window_and_balance_refresh(self):
        self.controller.executors_info = [self.failed_executor()]
        self.now = T0 + WINDOW
        actions = await self.tick()
        self.ergo._update_balances.assert_awaited_once()
        self.assertEqual(2, len(actions))

    async def test_failed_refresh_keeps_the_hold(self):
        self.controller.executors_info = [self.failed_executor()]
        self.ergo._update_balances.side_effect = Exception("gateway down")
        self.now = T0 + WINDOW
        self.assertEqual([], await self.tick())
        self.now += 5
        self.assertEqual([], await self.tick())
        self.assertEqual(1, self.ergo._update_balances.await_count)

    async def test_running_executor_with_unknown_outcome_holds_every_level(self):
        self.controller.executors_info = [self.failed_executor(status=RunnableStatus.SHUTTING_DOWN)]
        self.now = T0 + 3600
        self.assertEqual([], await self.tick())

    async def test_hold_survives_the_executor_leaving_executors_info(self):
        self.controller.executors_info = [self.failed_executor()]
        self.now = T0 + 1
        self.assertEqual([], await self.tick())
        self.controller.executors_info = []
        self.now = T0 + 300
        self.assertEqual([], await self.tick())
        self.now = T0 + WINDOW
        self.assertEqual(2, len(await self.tick()))
