"""The order executor's pool per order, its own retry budget, and its unknown-outcome report."""
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import MagicMock

from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.executors.order_executor.order_executor import OrderExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

POOL = "3c" * 32


def a_config(**overrides) -> OrderExecutorConfig:
    fields = dict(
        id="test-order", timestamp=1234567890, trading_pair="ERG-SigUSD", connector_name="ergo-mainnet",
        side=TradeType.BUY, amount=Decimal("1"), price=Decimal("2"), execution_strategy=ExecutionStrategy.LIMIT,
    )
    fields.update(overrides)
    return OrderExecutorConfig(**fields)


class OrderExecutorPoolTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self):
        super().setUp()
        self.strategy = MagicMock()
        self.strategy.current_timestamp = 1234567890
        self.connector = MagicMock(spec=Gateway)
        self.connector.place_order.return_value = "order-1"
        self.connector.order_failure_reason.return_value = "Gateway error: pool moved"

    def an_executor(self, config=None, connector=None) -> OrderExecutor:
        self.strategy.connectors = {"ergo-mainnet": connector or self.connector}
        return OrderExecutor(self.strategy, config or a_config(), update_interval=1.0)

    @staticmethod
    def _failure(order_id="order-1"):
        return MarketOrderFailureEvent(timestamp=1234567890, order_id=order_id, order_type=OrderType.LIMIT)

    def test_pool_is_sent_with_the_swap(self):
        executor = self.an_executor(a_config(pool_address=POOL))
        executor.place_open_order()
        self.assertEqual(POOL, self.connector.place_order.call_args.kwargs["pool_address"])
        self.assertEqual(POOL, executor.get_custom_info()["pool_address"])

    def test_no_pool_sends_the_old_arguments(self):
        executor = self.an_executor()
        executor.place_open_order()
        self.assertEqual({"is_buy", "trading_pair", "amount", "price", "slippage_pct", "max_retries"},
                         set(self.connector.place_order.call_args.kwargs))
        self.assertNotIn("pool_address", executor.get_custom_info())

    def test_a_retry_after_a_failure_uses_the_same_pool(self):
        executor = self.an_executor(a_config(pool_address=POOL))
        executor.place_open_order()
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        executor.place_open_order()
        self.assertEqual(2, self.connector.place_order.call_count)
        for call in self.connector.place_order.call_args_list:
            self.assertEqual(POOL, call.kwargs["pool_address"])

    def test_pool_on_a_non_gateway_connector_places_nothing(self):
        cex = MagicMock()  # not a Gateway connector
        executor = self.an_executor(a_config(pool_address=POOL), connector=cex)
        executor.place_order = MagicMock()
        executor.place_open_order()
        executor.place_order.assert_not_called()
        self.assertEqual(CloseType.FAILED, executor.close_type)

    def test_blank_pool_is_refused(self):
        with self.assertRaises(ValueError):
            a_config(pool_address=" ")

    def test_negative_retry_budget_is_refused(self):
        with self.assertRaises(ValueError):
            a_config(max_retries=-1)

    def test_config_budget_of_zero_stops_on_the_first_failure(self):
        executor = self.an_executor(a_config(max_retries=0))
        executor.place_open_order()
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(CloseType.FAILED, executor.close_type)
        self.assertEqual(1, self.connector.place_order.call_count)

    def test_default_budget_retries_as_before(self):
        executor = self.an_executor()
        executor.place_open_order()
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        self.assertNotEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(10, executor._max_retries)

    def test_unknown_outcome_is_reported_for_the_controller_hold(self):
        self.connector.order_failure_reason.return_value = "timeout [code: SUBMIT_OUTCOME_UNKNOWN]"
        executor = self.an_executor(a_config(pool_address=POOL))
        executor._order = TrackedOrder(order_id="order-1")
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        info = executor.get_custom_info()
        self.assertEqual("order-1", info["outcome_unknown_order_id"])
        self.assertEqual("ergo-mainnet", info["outcome_unknown_connector"])
        self.assertEqual(1234567890, info["outcome_unknown_close_timestamp"])
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)

    def test_definite_failure_reports_no_unknown_outcome(self):
        executor = self.an_executor()
        executor._order = TrackedOrder(order_id="order-1")
        executor.process_order_failed_event(None, MagicMock(), self._failure())
        self.assertNotIn("outcome_unknown_order_id", executor.get_custom_info())
