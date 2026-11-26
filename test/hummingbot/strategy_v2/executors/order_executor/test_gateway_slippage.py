"""The order executor's slippage ramp, which applies to Gateway swaps and nothing else.

A CEX order has a price and a book and nothing to be tolerant about. A Gateway order is a
swap against a pool, and it had no slippage setting at any executor level: the request
omitted slippagePct, so every attempt used the connector's configured value and every
retry sent the identical request.

The reason a failure can be told apart at all is that the connector now remembers what
Gateway said. The order tracker records FAILED and nothing else.
"""
import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.core.gateway.gateway_error import GatewayError
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


def a_config(**overrides) -> OrderExecutorConfig:
    fields = dict(
        id="test-order",
        timestamp=1234567890,
        trading_pair="SOL-USDC",
        connector_name="solana-mainnet-beta",
        side=TradeType.SELL,
        amount=Decimal("1"),
        execution_strategy=ExecutionStrategy.MARKET,
    )
    fields.update(overrides)
    return OrderExecutorConfig(**fields)


class TestTheRampIsGatewayOnly(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.strategy = MagicMock()
        self.strategy.current_timestamp = 1234567890

    def an_executor(self, connector, config=None) -> OrderExecutor:
        # ExecutorBase takes its connectors from the strategy at construction, so the
        # strategy hands over the one under test.
        self.strategy.connectors = {"solana-mainnet-beta": connector}
        executor = OrderExecutor(self.strategy, config or a_config(), update_interval=1.0)
        executor._order = TrackedOrder(order_id="order-1")
        return executor

    @staticmethod
    def _failure(order_id="order-1"):
        return MarketOrderFailureEvent(timestamp=1234567890, order_id=order_id, order_type=OrderType.MARKET)

    def test_a_gateway_slippage_failure_widens_the_next_attempt(self):
        connector = MagicMock(spec=Gateway)
        connector.order_failure_reason.return_value = (
            "Gateway error: Price slippage check failed. [code: SLIPPAGE_EXCEEDED]"
        )
        executor = self.an_executor(connector)

        executor.process_order_failed_event(None, MagicMock(), self._failure())

        self.assertEqual(executor._slippage_pct, Decimal("0.25"))

    def test_any_other_gateway_failure_retries_at_the_same_tolerance(self):
        connector = MagicMock(spec=Gateway)
        connector.order_failure_reason.return_value = "Gateway error: insufficient balance"
        executor = self.an_executor(connector)

        executor.process_order_failed_event(None, MagicMock(), self._failure())

        self.assertEqual(executor._slippage_pct, Decimal("0.05"))

    def test_a_cex_connector_never_moves_the_ramp(self):
        # A plain connector remembers no reason, so there is nothing to widen on — which
        # is the correct behaviour rather than an accident: a CEX order was not refused
        # for slippage.
        connector = MagicMock(spec=[])
        executor = self.an_executor(connector)

        executor.process_order_failed_event(None, MagicMock(), self._failure())

        self.assertEqual(executor._slippage_pct, Decimal("0.05"))

    def test_it_stops_widening_at_the_ceiling(self):
        connector = MagicMock(spec=Gateway)
        connector.order_failure_reason.return_value = "[code: SLIPPAGE_EXCEEDED]"
        executor = self.an_executor(connector)
        executor._slippage_pct = Decimal("5")

        executor.process_order_failed_event(None, MagicMock(), self._failure())

        self.assertEqual(executor._slippage_pct, Decimal("5"))

    def test_a_gateway_order_carries_the_tolerance_to_the_connector(self):
        connector = MagicMock(spec=Gateway)
        connector.place_order.return_value = "order-1"
        executor = self.an_executor(connector)

        executor.place_open_order()

        _, kwargs = connector.place_order.call_args
        self.assertEqual(kwargs["slippage_pct"], 0.05)
        self.assertEqual(kwargs["trading_pair"], "SOL-USDC")
        self.assertIs(kwargs["is_buy"], False)

    def test_a_cex_order_is_placed_through_the_strategy_as_before(self):
        connector = MagicMock(spec=[])
        executor = self.an_executor(connector)
        executor.place_order = MagicMock(return_value="order-1")

        executor.place_open_order()

        executor.place_order.assert_called_once()
        self.assertEqual(
            executor.place_order.call_args.kwargs["position_action"], PositionAction.OPEN
        )

    async def test_a_swap_that_may_have_been_submitted_is_never_placed_again(self):
        # The reply to the submit was lost: the swap may be on its way to the chain.
        connector = MagicMock(spec=Gateway)
        connector.order_failure_reason.return_value = (
            "ServerDisconnectedError('Server disconnected') [code: SUBMIT_OUTCOME_UNKNOWN]"
        )
        executor = self.an_executor(connector)
        executor._status = RunnableStatus.RUNNING

        executor.process_order_failed_event(None, MagicMock(), self._failure())
        # One tick of the control loop, in its own order: the control task, then the retry check.
        await executor.control_task()
        executor.evaluate_max_retries()

        connector.place_order.assert_not_called()
        self.assertGreater(executor._current_retries, executor._max_retries)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(CloseType.FAILED, executor.close_type)

    async def test_the_running_control_loop_places_a_possibly_submitted_swap_only_once(self):
        connector = MagicMock(spec=Gateway)
        connector.place_order.return_value = "order-1"
        connector.order_failure_reason.return_value = (
            "ServerDisconnectedError('Server disconnected') [code: SUBMIT_OUTCOME_UNKNOWN]"
        )
        executor = self.an_executor(connector)
        executor._order = None
        executor.update_interval = 0.01
        executor._status = RunnableStatus.RUNNING
        loop = asyncio.ensure_future(executor.control_loop())
        for _ in range(100):
            await asyncio.sleep(0.001)
            if connector.place_order.called:
                break
        self.assertEqual(1, connector.place_order.call_count)

        # The order fails while the loop sleeps between ticks, as the order tracker reports it.
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        await asyncio.wait_for(loop, timeout=1)

        self.assertEqual(1, connector.place_order.call_count)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(CloseType.FAILED, executor.close_type)

    async def test_a_swap_gateway_says_was_not_submitted_is_placed_again(self):
        # Gateway answered 503: the node could not be reached and nothing was sent.
        connector = MagicMock(spec=Gateway)
        connector.place_order.return_value = "order-2"
        connector.order_failure_reason.return_value = str(GatewayError(
            "Failed to execute ergodex swap: Transaction 4f was not submitted: the node could not be reached "
            "(connect ECONNREFUSED 127.0.0.1:9053); nothing was sent, so it cannot confirm. "
            "Retry once the node is reachable", status=503, http_error="HttpError",
        ))
        executor = self.an_executor(connector)
        executor._status = RunnableStatus.RUNNING

        executor.process_order_failed_event(None, MagicMock(), self._failure())
        await executor.control_task()
        executor.evaluate_max_retries()

        connector.place_order.assert_called_once()
        self.assertEqual(1, executor._current_retries)
        self.assertEqual(RunnableStatus.RUNNING, executor.status)

    def test_any_other_failure_leaves_the_retry_budget_as_it_was(self):
        connector = MagicMock(spec=Gateway)
        connector.order_failure_reason.return_value = "Gateway error: insufficient balance"
        executor = self.an_executor(connector)
        executor.stop = MagicMock()

        executor.process_order_failed_event(None, MagicMock(), self._failure())
        executor.evaluate_max_retries()

        self.assertEqual(1, executor._current_retries)
        executor.stop.assert_not_called()


class TestConfigValidation(IsolatedAsyncioWrapperTestCase):
    def test_a_start_above_the_ceiling_is_rejected(self):
        with self.assertRaises(ValueError):
            a_config(slippage_pct=Decimal("10"), max_slippage_pct=Decimal("5"))

    def test_a_multiplier_that_never_widens_is_rejected(self):
        with self.assertRaises(ValueError):
            a_config(slippage_multiplier=Decimal("1"))
