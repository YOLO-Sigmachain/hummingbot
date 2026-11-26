from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import MagicMock, Mock, PropertyMock, patch

from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.event.events import MarketOrderFailureEvent
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.arbitrage_executor.arbitrage_executor import ArbitrageExecutor
from hummingbot.strategy_v2.executors.arbitrage_executor.data_types import ArbitrageExecutorConfig
from hummingbot.strategy_v2.executors.data_types import ConnectorPair
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder


class TestArbitrageExecutor(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.strategy = self.create_mock_strategy()
        self.arbitrage_config = MagicMock(spec=ArbitrageExecutorConfig)
        self.arbitrage_config.buying_market = ConnectorPair(connector_name='binance', trading_pair='POL-USDT')
        self.arbitrage_config.selling_market = ConnectorPair(connector_name='uniswap_polygon_mainnet', trading_pair='WPOL-USDT')
        self.arbitrage_config.min_profitability = Decimal('0.01')
        self.arbitrage_config.order_amount = Decimal('1')
        self.arbitrage_config.max_retries = 3
        self.update_interval = 0.5
        self.executor = ArbitrageExecutor(self.strategy, self.arbitrage_config, self.update_interval)
        self.set_loggers(loggers=[self.executor.logger()])

    @staticmethod
    def create_mock_strategy():
        market = MagicMock()
        market_info = MagicMock()
        market_info.market = market

        strategy = MagicMock(spec=StrategyV2Base)
        type(strategy).market_info = PropertyMock(return_value=market_info)
        type(strategy).trading_pair = PropertyMock(return_value="ETH-USDT")
        strategy.buy.side_effect = ["OID-BUY-1", "OID-BUY-2", "OID-BUY-3"]
        strategy.sell.side_effect = ["OID-SELL-1", "OID-SELL-2", "OID-SELL-3"]
        strategy.cancel.return_value = None
        strategy.connectors = {
            "binance": MagicMock(spec=ConnectorBase),
        }
        return strategy

    def test_is_arbitrage_valid(self):
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-BUSD', 'ETH-USDT'))
        self.assertTrue(self.executor.is_arbitrage_valid('ETH-USDT', 'WETH-USDT'))
        self.assertFalse(self.executor.is_arbitrage_valid('ETH-USDT', 'BTC-USDT'))

    def test_net_pnl_quote(self):
        self.executor.close_type = CloseType.COMPLETED
        self.executor._buy_order = Mock(spec=TrackedOrder)
        self.executor._sell_order = Mock(spec=TrackedOrder)
        self.executor._buy_order.order.executed_amount_base = Decimal('1')
        self.executor._sell_order.order.executed_amount_base = Decimal('1')
        self.executor._buy_order.average_executed_price = Decimal('100')
        self.executor._sell_order.average_executed_price = Decimal('200')
        self.executor._buy_order.cum_fees_quote = Decimal('1')
        self.executor._sell_order.cum_fees_quote = Decimal('1')
        self.executor._status = RunnableStatus.TERMINATED
        self.assertEqual(self.executor.get_net_pnl_quote(), Decimal('98'))
        self.assertEqual(self.executor.get_net_pnl_pct(), Decimal('98'))

    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    @patch.object(ArbitrageExecutor, "get_tx_cost_in_asset")
    async def test_control_task_not_started_not_profitable(self, tx_cost_mock, resulting_price_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.side_effect = [Decimal('100'), Decimal('102')]
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.RUNNING)

    @patch.object(ArbitrageExecutor, "place_order")
    @patch.object(ArbitrageExecutor, "get_resulting_price_for_amount")
    @patch.object(ArbitrageExecutor, "get_tx_cost_in_asset")
    async def test_control_task_profitable(self, tx_cost_mock, resulting_price_mock, place_order_mock):
        tx_cost_mock.return_value = Decimal('0.01')
        resulting_price_mock.side_effect = [Decimal('100'), Decimal('104')]
        place_order_mock.side_effect = ['OID-BUY', 'OID-SELL']
        self.executor._status = RunnableStatus.RUNNING
        await self.executor.control_task()
        self.assertEqual(self.executor._status, RunnableStatus.SHUTTING_DOWN)
        self.assertEqual(self.executor.buy_order.order_id, 'OID-BUY')
        self.assertEqual(self.executor.sell_order.order_id, 'OID-SELL')

    async def test_control_task_max_retries(self):
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor._cumulative_failures = 4
        await self.executor.control_task()
        self.assertEqual(self.executor.close_type, CloseType.FAILED)
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    async def test_control_task_complete(self):
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor._cumulative_failures = 0
        self.executor._buy_order = Mock(spec=TrackedOrder)
        self.executor._sell_order = Mock(spec=TrackedOrder)
        self.executor._buy_order.order.is_filled = True
        self.executor._sell_order.order.is_filled = True
        await self.executor.control_task()
        self.assertEqual(self.executor.close_type, CloseType.COMPLETED)
        self.assertEqual(self.executor._status, RunnableStatus.TERMINATED)

    def test_to_format_status(self):
        self.executor._status = RunnableStatus.RUNNING
        self.executor._last_buy_price = Decimal('100')
        self.executor._last_sell_price = Decimal('102')
        self.executor._last_tx_cost = Decimal('0.01')
        format_status = "".join(self.executor.to_format_status())
        self.assertIn(f"Arbitrage Status: {RunnableStatus.RUNNING}", format_status)
        self.assertIn("Trade PnL (%): 2.00 % | TX Cost (%): -1.00 % | Net PnL (%): 1.00 %", format_status)

    @patch.object(ArbitrageExecutor, "place_order")
    def test_process_order_failed_event_increments_cumulative_failures(self, _):
        self.executor._cumulative_failures = 0
        self.executor.buy_order.order_id = "123"
        self.executor.sell_order.order_id = "321"
        market = MagicMock()
        buy_order_failed_event = MarketOrderFailureEvent(
            timestamp=123456789,
            order_id=self.executor.buy_order.order_id,
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event("102", market, buy_order_failed_event)
        self.assertEqual(self.executor._cumulative_failures, 1)

        sell_order_failed_event = MarketOrderFailureEvent(
            timestamp=123456789,
            order_id=self.executor.sell_order.order_id,
            order_type=OrderType.MARKET,
        )
        self.executor.process_order_failed_event("102", market, sell_order_failed_event)
        self.assertEqual(self.executor._cumulative_failures, 2)

    def _fail(self, order_id: str):
        event = MarketOrderFailureEvent(timestamp=123456789, order_id=order_id, order_type=OrderType.MARKET)
        self.executor.process_order_failed_event("102", MagicMock(), event)

    def _gateway_leg(self, connector_name: str, reason: str) -> MagicMock:
        connector = MagicMock()
        connector.order_failure_reason = MagicMock(return_value=reason)
        self.executor.connectors[connector_name] = connector
        return connector

    @staticmethod
    def _done_order(executed: str) -> MagicMock:
        order = MagicMock()
        order.is_done = True
        order.is_filled = True
        order.executed_amount_base = Decimal(executed)
        return order

    @patch.object(ArbitrageExecutor, "place_order")
    async def test_failed_buy_with_unknown_outcome_waits_for_the_other_leg(self, place_order_mock):
        self._gateway_leg("binance", "TimeoutError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.buy_order.order_id = "OID-BUY-1"
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-BUY-1")
        place_order_mock.assert_not_called()
        self.assertEqual("OID-BUY-1", self.executor.buy_order.order_id)
        self.assertTrue(self.is_partially_logged("ERROR", "Order OID-BUY-1 on binance may have been submitted"))
        self.assertTrue(self.is_partially_logged("ERROR", "The other leg OID-SELL-1 on uniswap_polygon_mainnet "
                                                          "is still open"))

        # The other leg is still open: the executor keeps listening and places nothing.
        await self.executor.control_task()
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, self.executor.status)
        info = self.executor.get_custom_info()
        self.assertEqual("OID-BUY-1", info["outcome_unknown_order_id"])
        self.assertEqual("binance", info["outcome_unknown_connector"])
        self.assertEqual("OID-SELL-1", info["outcome_unknown_other_order_id"])

        # The other leg fills: the executor stops FAILED and reports the fill.
        self.executor.sell_order.order = self._done_order("1")
        await self.executor.control_task()
        place_order_mock.assert_not_called()
        self.assertEqual(CloseType.FAILED, self.executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, self.executor.status)
        info = self.executor.get_custom_info()
        self.assertEqual(Decimal("1"), info["outcome_unknown_other_executed_base"])
        self.assertIn("outcome_unknown_close_timestamp", info)
        self.assertTrue(self.is_partially_logged("ERROR", "the other leg OID-SELL-1 is done with 1 base executed"))

    @patch.object(ArbitrageExecutor, "place_order")
    def test_failed_sell_with_unknown_outcome_stops_at_once_when_other_leg_never_placed(self, place_order_mock):
        self._gateway_leg("uniswap_polygon_mainnet", "ClientError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-SELL-1")
        place_order_mock.assert_not_called()
        self.assertEqual("OID-SELL-1", self.executor.sell_order.order_id)
        self.assertEqual(CloseType.FAILED, self.executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, self.executor.status)
        self.assertEqual("OID-SELL-1", self.executor.get_custom_info()["outcome_unknown_order_id"])

    @patch.object(ArbitrageExecutor, "place_order")
    def test_other_leg_failing_after_unknown_outcome_is_not_placed_again(self, place_order_mock):
        self._gateway_leg("uniswap_polygon_mainnet", "ClientError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.buy_order.order_id = "OID-BUY-1"
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-SELL-1")
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, self.executor.status)

        # The CEX leg then fails with a definite reason: it is not placed again either.
        self.executor.connectors["binance"].order_failure_reason = MagicMock(return_value="rejected")
        self._fail("OID-BUY-1")
        place_order_mock.assert_not_called()
        self.assertEqual("OID-BUY-1", self.executor.buy_order.order_id)
        self.assertEqual(CloseType.FAILED, self.executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, self.executor.status)

    @patch.object(ArbitrageExecutor, "place_order")
    def test_failed_leg_with_definite_reason_is_placed_again(self, place_order_mock):
        place_order_mock.return_value = "OID-SELL-2"
        self._gateway_leg("uniswap_polygon_mainnet", "Gateway error 409 [code: SLIPPAGE_EXCEEDED]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-SELL-1")
        place_order_mock.assert_called_once()
        self.assertEqual("OID-SELL-2", self.executor.sell_order.order_id)
        self.assertEqual(1, self.executor._cumulative_failures)
        self.assertIsNone(self.executor.close_type)
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, self.executor.status)

    @patch.object(ArbitrageExecutor, "place_order")
    def test_both_legs_failing_with_unknown_outcome_report_both(self, place_order_mock):
        self._gateway_leg("binance", "TimeoutError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self._gateway_leg("uniswap_polygon_mainnet", "ClientError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.buy_order.order_id = "OID-BUY-1"
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-BUY-1")
        self._fail("OID-SELL-1")
        place_order_mock.assert_not_called()
        self.assertEqual(CloseType.FAILED, self.executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, self.executor.status)
        info = self.executor.get_custom_info()
        self.assertEqual("OID-BUY-1", info["outcome_unknown_order_id"])
        self.assertEqual("binance", info["outcome_unknown_connector"])
        self.assertEqual("OID-SELL-1", info["outcome_unknown_other_order_id"])
        self.assertEqual("uniswap_polygon_mainnet", info["outcome_unknown_other_connector"])
        self.assertTrue(self.is_partially_logged(
            "ERROR", "The other leg OID-SELL-1 on uniswap_polygon_mainnet also failed with an unknown outcome"))
        self.assertTrue(self.is_partially_logged("ERROR", "both swaps may still land"))
        self.assertFalse(self.is_partially_logged("ERROR", "is done with 0 base executed"))

    @patch.object(ArbitrageExecutor, "place_order")
    def test_other_leg_failing_with_definite_reason_reports_no_second_unknown(self, place_order_mock):
        self._gateway_leg("binance", "TimeoutError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        self._gateway_leg("uniswap_polygon_mainnet", "Gateway error 409 [code: SLIPPAGE_EXCEEDED]")
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.buy_order.order_id = "OID-BUY-1"
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-BUY-1")
        self._fail("OID-SELL-1")
        self.assertNotIn("outcome_unknown_other_connector", self.executor.get_custom_info())

    @patch.object(ArbitrageExecutor, "place_order")
    async def test_other_leg_still_open_after_the_window_logs_an_error_every_600_seconds(self, place_order_mock):
        connector = self._gateway_leg("binance", "TimeoutError() [code: SUBMIT_OUTCOME_UNKNOWN]")
        connector._block_time_seconds = 120.0
        connector._block_confirmations = 1
        self.strategy.current_timestamp = 1000.0
        self.executor._status = RunnableStatus.SHUTTING_DOWN
        self.executor.buy_order.order_id = "OID-BUY-1"
        self.executor.sell_order.order_id = "OID-SELL-1"
        self._fail("OID-BUY-1")
        self.assertEqual(1000.0, self.executor.get_custom_info()["outcome_unknown_waiting_since"])

        def waiting_errors():
            return [r for r in self.log_records
                    if r.levelname == "ERROR" and "is still not done" in r.getMessage()]

        self.strategy.current_timestamp = 1000.0 + 1320 - 1
        await self.executor.control_task()
        self.assertEqual([], waiting_errors())

        self.strategy.current_timestamp = 1000.0 + 1320
        await self.executor.control_task()
        self.assertEqual(1, len(waiting_errors()))
        message = waiting_errors()[0].getMessage()
        self.assertIn("the other leg OID-SELL-1", message)
        self.assertIn("state never acknowledged", message)
        self.assertIn("executed 0 base", message)
        self.assertIn("stays held until this executor is stopped by hand", message)
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, self.executor.status)

        self.strategy.current_timestamp += 599
        await self.executor.control_task()
        self.assertEqual(1, len(waiting_errors()))
        self.strategy.current_timestamp += 1
        await self.executor.control_task()
        self.assertEqual(2, len(waiting_errors()))
        self.assertEqual(RunnableStatus.SHUTTING_DOWN, self.executor.status)
        place_order_mock.assert_not_called()
