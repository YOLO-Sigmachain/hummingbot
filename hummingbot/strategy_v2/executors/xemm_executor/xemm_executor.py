import asyncio
import logging
from decimal import Decimal
from typing import Dict, Optional

from hummingbot.connector.connector_base import ConnectorBase, Union
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, PriceType, TradeType
from hummingbot.core.data_type.order_candidate import OrderCandidate
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    MarketOrderFailureEvent,
    SellOrderCompletedEvent,
    SellOrderCreatedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.validation import are_tokens_interchangeable
from hummingbot.strategy_v2.executors.xemm_executor.data_types import XEMMExecutorConfig
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder
from hummingbot.strategy_v2.utils.outcome_hold import (
    OUTCOME_UNKNOWN_CLOSE_TIMESTAMP,
    OUTCOME_UNKNOWN_CONNECTOR,
    OUTCOME_UNKNOWN_ORDER_ID,
    OUTCOME_UNKNOWN_WAITING_LOG_SECONDS,
    OUTCOME_UNKNOWN_WAITING_SINCE,
    SUBMIT_OUTCOME_UNKNOWN,
    outcome_hold_seconds,
)


class XEMMExecutor(ExecutorBase):
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    @staticmethod
    def _are_tokens_interchangeable(first_token: str, second_token: str):
        return are_tokens_interchangeable(first_token, second_token)

    def is_arbitrage_valid(self, pair1, pair2):
        base_asset1, _ = split_hb_trading_pair(pair1)
        base_asset2, _ = split_hb_trading_pair(pair2)
        return self._are_tokens_interchangeable(base_asset1, base_asset2)

    def __init__(self, strategy: StrategyV2Base, config: XEMMExecutorConfig, update_interval: float = 1.0,
                 max_retries: int = 10):
        # The markets being interchangeable is validated by XEMMExecutorConfig.
        self.config = config
        self.rate_oracle = RateOracle.get_instance()
        if config.maker_side == TradeType.BUY:
            self.maker_connector = config.buying_market.connector_name
            self.maker_trading_pair = config.buying_market.trading_pair
            self.maker_order_side = TradeType.BUY
            self.taker_connector = config.selling_market.connector_name
            self.taker_trading_pair = config.selling_market.trading_pair
            self.taker_order_side = TradeType.SELL
        else:
            self.maker_connector = config.selling_market.connector_name
            self.maker_trading_pair = config.selling_market.trading_pair
            self.maker_order_side = TradeType.SELL
            self.taker_connector = config.buying_market.connector_name
            self.taker_trading_pair = config.buying_market.trading_pair
            self.taker_order_side = TradeType.BUY

        # Set up quote conversion pair
        _, maker_quote = split_hb_trading_pair(self.maker_trading_pair)
        _, taker_quote = split_hb_trading_pair(self.taker_trading_pair)
        self.quote_conversion_pair = f"{taker_quote}-{maker_quote}"

        taker_connector = strategy.connectors[self.taker_connector]
        if not self.is_amm_connector(exchange=self.taker_connector):
            if OrderType.MARKET not in taker_connector.supported_order_types():
                raise ValueError(f"{self.taker_connector} does not support market orders.")
        self._taker_result_price = Decimal("1")
        self._maker_target_price = Decimal("1")
        self._tx_cost = Decimal("1")
        self._tx_cost_pct = Decimal("1")
        self._current_trade_profitability = Decimal("0")
        self.maker_order = None
        self.taker_order = None
        self.failed_orders = []
        # Set when an order failed with SUBMIT_OUTCOME_UNKNOWN: no order is placed again after that.
        self._outcome_unknown_order_id: Optional[str] = None
        self._outcome_unknown_connector: Optional[str] = None
        self._outcome_other_leg: Optional[TrackedOrder] = None
        self._outcome_other_leg_failed = False
        # When the order was flagged, and when the executor last logged that it still waits.
        self._outcome_flagged_at: Optional[float] = None
        self._outcome_last_waiting_log: Optional[float] = None
        super().__init__(strategy=strategy,
                         connectors=[config.buying_market.connector_name, config.selling_market.connector_name],
                         config=config, update_interval=update_interval, max_retries=max_retries)

    async def validate_sufficient_balance(self):
        mid_price = self.get_price(self.maker_connector, self.maker_trading_pair,
                                   price_type=PriceType.MidPrice)
        maker_order_candidate = OrderCandidate(
            trading_pair=self.maker_trading_pair,
            is_maker=True,
            order_type=OrderType.LIMIT,
            order_side=self.maker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        taker_order_candidate = OrderCandidate(
            trading_pair=self.taker_trading_pair,
            is_maker=False,
            order_type=OrderType.MARKET,
            order_side=self.taker_order_side,
            amount=self.config.order_amount,
            price=mid_price,)
        maker_adjusted_candidate = self.adjust_order_candidates(self.maker_connector, [maker_order_candidate])[0]
        taker_adjusted_candidate = self.adjust_order_candidates(self.taker_connector, [taker_order_candidate])[0]
        if maker_adjusted_candidate.amount == Decimal("0") or taker_adjusted_candidate.amount == Decimal("0"):
            self.close_type = CloseType.INSUFFICIENT_BALANCE
            self.logger().error("Not enough budget to open position.")
            self.stop()

    async def control_task(self):
        if self._outcome_unknown_order_id is not None:
            self._stop_once_other_leg_done()
        elif self.status == RunnableStatus.RUNNING:
            await self.update_prices_and_tx_costs()
            await self.control_maker_order()
        elif self.status == RunnableStatus.SHUTTING_DOWN:
            await self.control_shutdown_process()

    async def control_maker_order(self):
        if self.maker_order is None:
            await self.create_maker_order()
        else:
            await self.control_update_maker_order()

    async def update_prices_and_tx_costs(self):
        self._taker_result_price = await self.get_resulting_price_for_amount(
            connector=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount)
        await self.update_tx_costs()
        if self.taker_order_side == TradeType.BUY:
            # Maker is SELL: profitability = (maker_price - taker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 - target_profitability - tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") - self.config.target_profitability - self._tx_cost_pct)
        else:
            # Maker is BUY: profitability = (taker_price - maker_price) / maker_price
            # To achieve target: maker_price = taker_price / (1 + target_profitability + tx_cost_pct)
            self._maker_target_price = self._taker_result_price / (Decimal("1") + self.config.target_profitability + self._tx_cost_pct)

    async def update_tx_costs(self):
        base, quote = split_hb_trading_pair(trading_pair=self.config.buying_market.trading_pair)
        base_without_wrapped = base[1:] if base.startswith("W") else base
        taker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=OrderType.MARKET,
            is_buy=self.taker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount,
            asset=base_without_wrapped
        ))
        maker_fee_task = asyncio.create_task(self.get_tx_cost_in_asset(
            exchange=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            is_buy=self.maker_order_side == TradeType.BUY,
            order_amount=self.config.order_amount,
            asset=base_without_wrapped
        ))

        taker_fee, maker_fee = await asyncio.gather(taker_fee_task, maker_fee_task)
        self._tx_cost = taker_fee + maker_fee
        self._tx_cost_pct = self._tx_cost / self.config.order_amount

    async def get_tx_cost_in_asset(self, exchange: str, trading_pair: str, is_buy: bool, order_amount: Decimal,
                                   asset: str, order_type: OrderType = OrderType.MARKET):
        connector = self.connectors[exchange]
        if self.is_amm_connector(exchange=exchange):
            gas_cost = connector.network_transaction_fee
            conversion_price = RateOracle.get_instance().get_pair_rate(f"{asset}-{gas_cost.token}")
            if conversion_price is None:
                self.logger().warning(f"Could not get conversion rate for {asset}-{gas_cost.token}")
                return Decimal("0")
            return gas_cost.amount / conversion_price
        else:
            fee = connector.get_fee(
                base_currency=asset,
                quote_currency=trading_pair.split("-")[1],
                order_type=order_type,
                order_side=TradeType.BUY if is_buy else TradeType.SELL,
                amount=order_amount,
                price=self._taker_result_price,
                is_maker=order_type.is_limit_type(),
            )
            return fee.fee_amount_in_token(
                trading_pair=trading_pair,
                price=self._taker_result_price,
                order_amount=order_amount,
                token=asset,
            )

    async def get_resulting_price_for_amount(self, connector: str, trading_pair: str, is_buy: bool,
                                             order_amount: Decimal):
        return await self.connectors[connector].get_quote_price(trading_pair, is_buy, order_amount)

    async def create_maker_order(self):
        order_id = self.place_order(
            connector_name=self.maker_connector,
            trading_pair=self.maker_trading_pair,
            order_type=OrderType.LIMIT,
            side=self.maker_order_side,
            amount=self.config.order_amount,
            price=self._maker_target_price)
        self.maker_order = TrackedOrder(order_id=order_id)
        self.logger().info(f"Created maker order {order_id} at price {self._maker_target_price}.")

    async def control_shutdown_process(self):
        if self.maker_order.is_done and self.taker_order.is_done:
            self.logger().info("Both orders are done, executor terminated.")
            self.stop()

    async def control_update_maker_order(self):
        await self.update_current_trade_profitability()
        if self._current_trade_profitability - self._tx_cost_pct < self.config.min_profitability:
            self.logger().info(f"Order {self.maker_order.order_id} profitability {self._current_trade_profitability - self._tx_cost_pct} is below minimum profitability {self.config.min_profitability}. Cancelling order.")
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
            self.maker_order = None
        elif self._current_trade_profitability - self._tx_cost_pct > self.config.max_profitability:
            self.logger().info(f"Order {self.maker_order.order_id} profitability {self._current_trade_profitability - self._tx_cost_pct} is above maximum profitability {self.config.max_profitability}. Cancelling order.")
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
            self.maker_order = None

    async def update_current_trade_profitability(self):
        trade_profitability = Decimal("0")
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            maker_price = self.maker_order.order.price
            # Get the conversion rate to normalize prices to the same quote asset
            try:
                conversion_rate = await self.get_quote_asset_conversion_rate()
                if self.maker_order_side == TradeType.BUY:
                    # If maker is buying, normalize taker (sell) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (normalized_taker_price - maker_price) / maker_price
                else:
                    # If maker is selling, normalize taker (buy) price to maker quote asset
                    normalized_taker_price = self._taker_result_price * conversion_rate
                    trade_profitability = (maker_price - normalized_taker_price) / maker_price
            except Exception as e:
                self.logger().error(f"Error calculating trade profitability: {e}")
                return Decimal("0")
        self._current_trade_profitability = trade_profitability
        return trade_profitability

    def process_order_created_event(self,
                                    event_tag: int,
                                    market: ConnectorBase,
                                    event: Union[BuyOrderCreatedEvent, SellOrderCreatedEvent]):
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} created.")
            self.maker_order.order = self.get_in_flight_order(self.maker_connector, event.order_id)
        elif self.taker_order and event.order_id == self.taker_order.order_id:
            self.logger().info(f"Taker order {event.order_id} created.")
            self.taker_order.order = self.get_in_flight_order(self.taker_connector, event.order_id)

    def process_order_completed_event(self,
                                      event_tag: int,
                                      market: ConnectorBase,
                                      event: Union[BuyOrderCompletedEvent, SellOrderCompletedEvent]):
        if self._outcome_unknown_order_id is not None:
            # An order's outcome is unknown: no order is placed again.
            return
        if self.maker_order and event.order_id == self.maker_order.order_id:
            self.logger().info(f"Maker order {event.order_id} completed. Executing taker order.")
            self.place_taker_order()
            self._status = RunnableStatus.SHUTTING_DOWN

    def place_taker_order(self):
        taker_order_id = self.place_order(
            connector_name=self.taker_connector,
            trading_pair=self.taker_trading_pair,
            order_type=OrderType.MARKET,
            side=self.taker_order_side,
            amount=self.config.order_amount)
        self.taker_order = TrackedOrder(order_id=taker_order_id)

    def process_order_failed_event(self, _, market, event: MarketOrderFailureEvent):
        if self._outcome_unknown_order_id is not None:
            # An order's outcome is unknown: no order is placed again.
            if self._outcome_other_leg is not None and self._outcome_other_leg.order_id == event.order_id:
                self._outcome_other_leg_failed = True
                self._stop_once_other_leg_done()
            return
        if self.maker_order and self.maker_order.order_id == event.order_id:
            self.failed_orders.append(self.maker_order)
            self._current_retries += 1
            if self._flag_if_outcome_unknown(self.maker_connector, event.order_id,
                                             self.taker_order, self.taker_connector):
                return
            self.maker_order = None
        elif self.taker_order and self.taker_order.order_id == event.order_id:
            self.failed_orders.append(self.taker_order)
            self._current_retries += 1
            if self._flag_if_outcome_unknown(self.taker_connector, event.order_id,
                                             self.maker_order, self.maker_connector):
                return
            self.place_taker_order()

    def _flag_if_outcome_unknown(self, connector_name: str, order_id: str,
                                 other_leg: Optional[TrackedOrder], other_connector: str) -> bool:
        """Never place an order again when its swap may have reached the chain.

        Gateway has no duplicate guard, so a second order could trade twice. The executor keeps the
        failed order's id, waits until the other order is done (or stops now if it was never placed),
        and then ends FAILED. The chain must be checked by hand.
        """
        connector = self.connectors.get(connector_name)
        reason = getattr(connector, "order_failure_reason", lambda _: None)(order_id)
        if not (isinstance(reason, str) and SUBMIT_OUTCOME_UNKNOWN in reason):
            return False
        self._outcome_unknown_order_id = order_id
        self._outcome_unknown_connector = connector_name
        self._outcome_other_leg = other_leg
        self._outcome_flagged_at = self._strategy.current_timestamp
        if other_leg is None or other_leg.order_id is None:
            other = "The other order was never placed."
        else:
            other = (f"The other order {other_leg.order_id} on {other_connector} is "
                     f"{'done' if other_leg.is_done else 'still open'}, executed {other_leg.executed_amount_base} "
                     "base; the executor stops once it is done.")
        self.logger().error(
            f"Order {order_id} on {connector_name} may have been submitted ({reason}); not placing it again. "
            f"{other} The xemm_multiple_levels controller then starts no executor on {connector_name} until "
            f"the hold window has passed and its balances are refreshed; other callers may not wait. "
            f"Check order {order_id} on chain."
        )
        self._stop_once_other_leg_done()
        return True

    def _other_leg_done(self) -> bool:
        other = self._outcome_other_leg
        return other is None or other.order_id is None or self._outcome_other_leg_failed or other.is_done

    def _stop_once_other_leg_done(self):
        """After an unknown outcome: end FAILED once the other order is done, never re-placing either."""
        if self.status == RunnableStatus.TERMINATED:
            return
        if not self._other_leg_done():
            self._log_still_waiting()
            return
        other = self._outcome_other_leg
        if other is not None and other.order_id is not None:
            self.logger().error(
                f"Order {self._outcome_unknown_order_id} has an unknown outcome; the other order {other.order_id} "
                f"is done with {other.executed_amount_base} base executed. Stopping the executor."
            )
        self.close_type = CloseType.FAILED
        self.stop()

    def _log_still_waiting(self):
        """Past the hold window with the other order still not done: log an error at most every 600 s.

        The executor is not stopped: a stop would start the controller's window while the other
        order's state is still unknown.
        """
        now = self._strategy.current_timestamp
        flagged_at = self._outcome_flagged_at
        if not all(isinstance(t, (int, float)) and not isinstance(t, bool) for t in (now, flagged_at)):
            return
        if now < flagged_at + outcome_hold_seconds(self.connectors.get(self._outcome_unknown_connector)):
            return
        last = self._outcome_last_waiting_log
        if last is not None and now - last < OUTCOME_UNKNOWN_WAITING_LOG_SECONDS:
            return
        self._outcome_last_waiting_log = now
        other = self._outcome_other_leg
        state = other.order.current_state if other.order is not None else "never acknowledged"
        self.logger().error(
            f"Order {self._outcome_unknown_order_id} has an unknown outcome and the other order {other.order_id} "
            f"is still not done (state {state}, executed {other.executed_amount_base} base) after the hold "
            "window. The controller stays held until this executor is stopped by hand after the chain is "
            "checked."
        )

    def get_custom_info(self) -> Dict:
        info = self._base_custom_info()
        if self._outcome_unknown_order_id is not None:
            other = self._outcome_other_leg
            info.update({
                OUTCOME_UNKNOWN_ORDER_ID: self._outcome_unknown_order_id,
                OUTCOME_UNKNOWN_CONNECTOR: self._outcome_unknown_connector,
                OUTCOME_UNKNOWN_CLOSE_TIMESTAMP: self.close_timestamp,
                "outcome_unknown_other_order_id": other.order_id if other is not None else None,
                "outcome_unknown_other_executed_base": (other.executed_amount_base if other is not None
                                                        else Decimal("0")),
                OUTCOME_UNKNOWN_WAITING_SINCE: self._outcome_flagged_at,
            })
        return info

    def _base_custom_info(self) -> Dict:
        # Since we can't make this method async, we'll skip the profitability calculation
        # The profitability will still be shown in the status message which is async
        return {
            "side": self.config.maker_side,
            "maker_connector": self.maker_connector,
            "maker_trading_pair": self.maker_trading_pair,
            "taker_connector": self.taker_connector,
            "taker_trading_pair": self.taker_trading_pair,
            "min_profitability": self.config.min_profitability,
            "target_profitability_pct": self.config.target_profitability,
            "max_profitability": self.config.max_profitability,
            "trade_profitability": self._current_trade_profitability,
            "tx_cost": self._tx_cost,
            "tx_cost_pct": self._tx_cost_pct,
            "taker_price": self._taker_result_price,
            "maker_target_price": self._maker_target_price,
            "net_profitability": self._current_trade_profitability - self._tx_cost_pct,
            "order_amount": self.config.order_amount,
        }

    def early_stop(self, keep_position: bool = False):
        if self.maker_order and self.maker_order.order and self.maker_order.order.is_open:
            self.logger().info(f"Cancelling maker order {self.maker_order.order_id}.")
            self._strategy.cancel(self.maker_connector, self.maker_trading_pair, self.maker_order.order_id)
        self.close_type = CloseType.POSITION_HOLD if keep_position else CloseType.EARLY_STOP
        self.stop()

    def get_cum_fees_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order:
            return self.maker_order.cum_fees_quote + self.taker_order.cum_fees_quote
        else:
            return Decimal("0")

    def get_net_pnl_quote(self) -> Decimal:
        if self.is_closed and self.maker_order and self.taker_order and self.maker_order.is_done and self.taker_order.is_done:
            maker_pnl = self.maker_order.executed_amount_base * self.maker_order.average_executed_price
            taker_pnl = self.taker_order.executed_amount_base * self.taker_order.average_executed_price
            return taker_pnl - maker_pnl - self.get_cum_fees_quote()
        else:
            return Decimal("0")

    def get_net_pnl_pct(self) -> Decimal:
        pnl_quote = self.get_net_pnl_quote()
        return pnl_quote / self.config.order_amount

    async def get_quote_asset_conversion_rate(self) -> Decimal:
        """
        Fetch the conversion rate between the quote assets of the buying and selling markets.
        Example: For M3M3/USDT and M3M3/USDC, fetch the USDC/USDT rate.
        """
        try:
            conversion_rate = self.rate_oracle.get_pair_rate(self.quote_conversion_pair)
            if conversion_rate is None:
                self.logger().error(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
                raise ValueError(f"Could not fetch conversion rate for {self.quote_conversion_pair}")
            return conversion_rate
        except Exception as e:
            self.logger().error(f"Error fetching conversion rate for {self.quote_conversion_pair}: {e}")
            raise

    def to_format_status(self):
        return f"""
Maker Side: {self.maker_order_side}
-----------------------------------------------------------------------------------------------------------------------
    - Maker: {self.maker_connector} {self.maker_trading_pair} | Taker: {self.taker_connector} {self.taker_trading_pair}
    - Min profitability: {self.config.min_profitability * 100:.2f}% | Target profitability: {self.config.target_profitability * 100:.2f}% | Max profitability: {self.config.max_profitability * 100:.2f}% | Current profitability: {(self._current_trade_profitability - self._tx_cost_pct) * 100:.2f}%
    - Trade profitability: {self._current_trade_profitability * 100:.2f}% | Tx cost: {self._tx_cost_pct * 100:.2f}%
    - Taker result price: {self._taker_result_price:.3f} | Tx cost: {self._tx_cost:.3f} {self.maker_trading_pair.split('-')[-1]} | Order amount (Base): {self.config.order_amount:.2f}
-----------------------------------------------------------------------------------------------------------------------
"""
