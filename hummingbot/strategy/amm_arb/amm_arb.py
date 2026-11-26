import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Tuple, cast

import pandas as pd

from hummingbot.client.performance import PerformanceMetrics
from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.clock import Clock
from hummingbot.core.data_type.limit_order import LimitOrder
from hummingbot.core.data_type.market_order import MarketOrder
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.core.event.events import (
    BuyOrderCompletedEvent,
    MarketOrderFailureEvent,
    OrderExpiredEvent,
    OrderType,
    SellOrderCompletedEvent,
)
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.amm_arb.data_types import ArbProposalSide
from hummingbot.strategy.amm_arb.utils import ArbProposal, create_arb_proposals
from hummingbot.strategy.market_trading_pair_tuple import MarketTradingPairTuple
from hummingbot.strategy.strategy_py_base import StrategyPyBase
from hummingbot.strategy_v2.utils.outcome_hold import (
    OUTCOME_HOLD_REFRESH_LOG_SECONDS,
    OUTCOME_HOLD_REFRESH_RETRY_SECONDS,
    SUBMIT_OUTCOME_UNKNOWN,
    outcome_hold_seconds,
)

NaN = float("nan")
s_decimal_zero = Decimal(0)
amm_logger = None


@dataclass
class OutcomeHoldRecord:
    """What amm_arb knows about an order of unknown outcome, kept for the hold end report."""
    market_name: str
    hold_end: float
    base_asset: str
    quote_asset: str
    is_buy: Optional[bool]
    amount: Optional[Decimal]
    base_before: Optional[Decimal]
    quote_before: Optional[Decimal]
    other_leg_id: Optional[str]
    other_leg_market: Optional[str]
    # Balances read right before the order was placed; None when not known.
    base_at_placement: Optional[Decimal] = None
    quote_at_placement: Optional[Decimal] = None
    # The market's wallet address (Gateway connectors), when known.
    address: Optional[str] = None


# order_id -> record of each hold. It lives in the process, not in the strategy instance, so a
# strategy stop/start keeps the holds; a process restart forgets them.
_PROCESS_OUTCOME_HOLDS: Dict[str, OutcomeHoldRecord] = {}


class AmmArbStrategy(StrategyPyBase):
    """
    This is a basic arbitrage strategy which can be used for most types of connectors (CEX, DEX or AMM).
    For a given order amount, the strategy checks both sides of the trade (market_1 and market_2) for arb opportunity.
    If presents, the strategy submits taker orders to both market.
    """

    _market_info_1: MarketTradingPairTuple
    _market_info_2: MarketTradingPairTuple
    _min_profitability: Decimal
    _order_amount: Decimal
    _market_1_slippage_buffer: Decimal
    _market_2_slippage_buffer: Decimal
    _concurrent_orders_submission: bool
    _last_no_arb_reported: float
    _arb_proposals: Optional[List[ArbProposal]]
    _all_markets_ready: bool
    _ev_loop: asyncio.AbstractEventLoop
    _main_task: Optional[asyncio.Task]
    _last_timestamp: float
    _status_report_interval: float
    _quote_eth_rate_fetch_loop_task: Optional[asyncio.Task]
    _market_1_quote_eth_rate: None          # XXX (martin_kou): Why are these here?
    _market_2_quote_eth_rate: None          # XXX (martin_kou): Why are these here?
    _rate_source: Optional[RateOracle]

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global amm_logger
        if amm_logger is None:
            amm_logger = logging.getLogger(__name__)
        return amm_logger

    def init_params(self,
                    market_info_1: MarketTradingPairTuple,
                    market_info_2: MarketTradingPairTuple,
                    min_profitability: Decimal,
                    order_amount: Decimal,
                    market_1_slippage_buffer: Decimal = Decimal("0"),
                    market_2_slippage_buffer: Decimal = Decimal("0"),
                    concurrent_orders_submission: bool = True,
                    status_report_interval: float = 900,
                    rate_source: Optional[RateOracle] = RateOracle.get_instance(),
                    ):
        """
        Assigns strategy parameters, this function must be called directly after init.
        The reason for this is to make the parameters discoverable on introspect (it is not possible on init of
        a Cython class).
        :param market_info_1: The first market
        :param market_info_2: The second market
        :param min_profitability: The minimum profitability for execute trades (e.g. 0.0003 for 0.3%)
        :param order_amount: The order amount
        :param market_1_slippage_buffer: The buffer for which to adjust order price for higher chance of
        the order getting filled. This is quite important for AMM which transaction takes a long time where a slippage
        is acceptable rather having the transaction get rejected. The submitted order price will be adjust higher
        for buy order and lower for sell order.
        :param market_2_slippage_buffer: The slipper buffer for market_2
        :param concurrent_orders_submission: whether to submit both arbitrage taker orders (buy and sell) simultaneously
        If false, the bot will wait for first exchange order filled before submitting the other order.
        :param status_report_interval: Amount of seconds to wait to refresh the status report
        :param rate_source: The rate source to use for conversion rate - (RateOracle or FixedRateSource) - default is FixedRateSource
        """
        self._market_info_1 = market_info_1
        self._market_info_2 = market_info_2
        self._min_profitability = min_profitability
        self._order_amount = order_amount
        self._market_1_slippage_buffer = market_1_slippage_buffer
        self._market_2_slippage_buffer = market_2_slippage_buffer
        self._concurrent_orders_submission = concurrent_orders_submission
        self._last_no_arb_reported = 0
        self._all_arb_proposals = None
        self._all_markets_ready = False

        self._ev_loop = asyncio.get_event_loop()
        self._main_task = None

        self._last_timestamp = 0
        self._status_report_interval = status_report_interval
        self.add_markets([market_info_1.market, market_info_2.market])
        self._quote_eth_rate_fetch_loop_task = None

        self._rate_source = rate_source

        self._order_id_side_map: Dict[str, ArbProposalSide] = {}
        # The proposal whose legs are being placed, to find the other leg of a failed one.
        self._current_arb_proposal: Optional[ArbProposal] = None
        # order_id -> (market, hold end) for each order that failed with an unknown outcome.
        # While any is held no new proposal starts. See did_fail_order.
        self._outcome_holds: Dict[str, Tuple[ConnectorBase, float]] = {}
        self._last_hold_refresh_error = 0.0
        self._next_hold_refresh_at = 0.0
        # order_id -> (base, quote) balance read right before placing it; dropped when the order ends
        # unless it is held.
        self._placement_balances: Dict[str, Tuple[Optional[Decimal], Optional[Decimal]]] = {}
        # A stop/start of the strategy in this process keeps the holds of its markets: same
        # connector name, and the same wallet address when both are known.
        for order_id, record in _PROCESS_OUTCOME_HOLDS.items():
            for market_info in (market_info_1, market_info_2):
                address = self._market_address(market_info.market)
                if market_info.market.name == record.market_name and (
                        record.address is None or address is None or address == record.address):
                    self._outcome_holds[order_id] = (market_info.market, record.hold_end)
                    self.logger().warning(
                        f"Order {order_id} on {record.market_name} still has an unknown outcome: no new "
                        f"arbitrage proposal until {self._format_ts(record.hold_end)} and a balance refresh after it.")
                    break

    @property
    def all_markets_ready(self) -> bool:
        return self._all_markets_ready

    @all_markets_ready.setter
    def all_markets_ready(self, value: bool):
        self._all_markets_ready = value

    @property
    def min_profitability(self) -> Decimal:
        return self._min_profitability

    @property
    def order_amount(self) -> Decimal:
        return self._order_amount

    @order_amount.setter
    def order_amount(self, value: Decimal):
        self._order_amount = value

    @property
    def rate_source(self) -> Optional[RateOracle]:
        return self._rate_source

    @rate_source.setter
    def rate_source(self, src: Optional[RateOracle]):
        self._rate_source = src

    @property
    def market_info_to_active_orders(self) -> Dict[MarketTradingPairTuple, List[LimitOrder]]:
        return self._sb_order_tracker.market_pair_to_active_orders

    @staticmethod
    @lru_cache(maxsize=10)
    def is_gateway_market(market_info: MarketTradingPairTuple) -> bool:
        return market_info.market.name in sorted(
            AllConnectorSettings.get_gateway_amm_connector_names()
        )

    @staticmethod
    @lru_cache(maxsize=10)
    def is_gateway_market_evm_compatible(market_info: MarketTradingPairTuple) -> bool:
        # Gateway connectors are now managed by Gateway
        # Assume all gateway connectors are EVM compatible
        # This can be enhanced later by querying Gateway API
        connector_name = market_info.market.name
        connector_settings = AllConnectorSettings.get_connector_settings().get(connector_name)
        if connector_settings and connector_settings.uses_gateway_generic_connector():
            # For now, assume all gateway connectors are EVM compatible
            return True
        return False

    def tick(self, timestamp: float):
        """
        Clock tick entry point, is run every second (on normal tick setting).
        :param timestamp: current tick timestamp
        """
        if not self.all_markets_ready:
            self.all_markets_ready = all([market.ready for market in self.active_markets])
            if not self.all_markets_ready:
                if int(timestamp) % 10 == 0:  # prevent spamming by logging every 10 secs
                    unready_markets = [market for market in self.active_markets if market.ready is False]
                    for market in unready_markets:
                        msg = ', '.join([k for k, v in market.status_dict.items() if v is False])
                        self.logger().warning(f"{market.name} not ready: waiting for {msg}.")
                return
            else:
                self.logger().info("Markets are ready. Trading started.")

        if self.ready_for_new_arb_trades():
            if self._main_task is None or self._main_task.done():
                self._main_task = safe_ensure_future(self.main())

    async def main(self):
        """
        The main procedure for the arbitrage strategy. It first creates arbitrage proposals, filters for ones that meets
        min profitability required, applies the slippage buffer, applies budget constraint, then finally execute the
        arbitrage.
        """
        if not await self.outcome_holds_settled():
            return
        self._all_arb_proposals = await create_arb_proposals(
            market_info_1=self._market_info_1,
            market_info_2=self._market_info_2,
            market_1_extra_flat_fees=(
                [getattr(self._market_info_1.market, "network_transaction_fee")]
                if hasattr(self._market_info_1.market, "network_transaction_fee")
                else []
            ),
            market_2_extra_flat_fees=(
                [getattr(self._market_info_2.market, "network_transaction_fee")]
                if hasattr(self._market_info_2.market, "network_transaction_fee")
                else []
            ),
            order_amount=self._order_amount,
        )
        profitable_arb_proposals: List[ArbProposal] = [
            t.copy() for t in self._all_arb_proposals
            if t.profit_pct(
                rate_source=self._rate_source,
                account_for_fee=True,
            ) >= self._min_profitability
        ]
        if len(profitable_arb_proposals) == 0:
            if self._last_no_arb_reported < self.current_timestamp - 20.:
                self.logger().info("No arbitrage opportunity.\n" +
                                   "\n".join(self.short_proposal_msg(self._all_arb_proposals, False)))
                self._last_no_arb_reported = self.current_timestamp
            return
        await self.apply_slippage_buffers(profitable_arb_proposals)
        self.apply_budget_constraint(profitable_arb_proposals)
        await self.execute_arb_proposals(profitable_arb_proposals)

    async def apply_slippage_buffers(self, arb_proposals: List[ArbProposal]):
        """
        Updates arb_proposals by adjusting order price for slipper buffer percentage.
        E.g. if it is a buy order, for an order price of 100 and 1% slipper buffer, the new order price is 101,
        for a sell order, the new order price is 99.
        :param arb_proposals: the arbitrage proposal
        """
        for arb_proposal in arb_proposals:
            for arb_side in (arb_proposal.first_side, arb_proposal.second_side):
                market = arb_side.market_info.market
                arb_side.amount = market.quantize_order_amount(arb_side.market_info.trading_pair, arb_side.amount)
                s_buffer = self._market_1_slippage_buffer if market == self._market_info_1.market \
                    else self._market_2_slippage_buffer
                if not arb_side.is_buy:
                    s_buffer *= Decimal("-1")
                arb_side.order_price *= Decimal("1") + s_buffer
                arb_side.order_price = market.quantize_order_price(arb_side.market_info.trading_pair,
                                                                   arb_side.order_price)

    def apply_budget_constraint(self, arb_proposals: List[ArbProposal]):
        """
        Updates arb_proposals by setting proposal amount to 0 if there is not enough balance to submit order with
        required order amount.
        :param arb_proposals: the arbitrage proposal
        """
        for arb_proposal in arb_proposals:
            for arb_side in (arb_proposal.first_side, arb_proposal.second_side):
                market = arb_side.market_info.market
                token = arb_side.market_info.quote_asset if arb_side.is_buy else arb_side.market_info.base_asset
                balance = market.get_available_balance(token)
                required = arb_side.amount * arb_side.order_price if arb_side.is_buy else arb_side.amount
                if balance < required:
                    arb_side.amount = s_decimal_zero
                    self.logger().info(f"Can't arbitrage, {market.display_name} "
                                       f"{token} balance "
                                       f"({balance}) is below required order amount ({required}).")
                    continue

    def prioritize_evm_exchanges(self, arb_proposal: ArbProposal) -> ArbProposal:
        """
        Prioritize the EVM exchanges in the arbitrage proposals

        :param arb_proposal: The arbitrage proposal from which the sides are to be prioritized.
        :type arb_proposal: ArbProposal
        :return: A new ArbProposal object with evm exchanges prioritized.
        :rtype: ArbProposal
        """

        results = []
        for side in [arb_proposal.first_side, arb_proposal.second_side]:
            if self.is_gateway_market(side.market_info):
                results.insert(0, side)
            else:
                results.append(side)

        return ArbProposal(first_side=results[0], second_side=results[1])

    async def execute_arb_proposals(self, arb_proposals: List[ArbProposal]):
        """
        Execute both sides of the arbitrage trades. If concurrent_orders_submission is False, it will wait for the
        first order to fill before submit the second order.
        :param arb_proposals: the arbitrage proposal
        """
        for arb_proposal in arb_proposals:
            if self._outcome_holds:
                # An earlier leg may still land: start no further proposal.
                return
            if any(p.amount <= s_decimal_zero for p in (arb_proposal.first_side, arb_proposal.second_side)):
                continue

            if not self._concurrent_orders_submission:
                arb_proposal = self.prioritize_evm_exchanges(arb_proposal)
            self._current_arb_proposal = arb_proposal

            self.logger().info(f"Found arbitrage opportunity!: {arb_proposal}")

            for arb_side in (arb_proposal.first_side, arb_proposal.second_side):
                side: str = "BUY" if arb_side.is_buy else "SELL"
                self.log_with_clock(logging.INFO,
                                    f"Placing {side} order for {arb_side.amount} {arb_side.market_info.base_asset} "
                                    f"at {arb_side.market_info.market.display_name} at {arb_side.order_price} price")

                # Read before placing: a balance read at failure time may already include the swap.
                placement_balances = self._read_balances(arb_side.market_info)
                order_id: str = await self.place_arb_order(
                    arb_side.market_info,
                    arb_side.is_buy,
                    arb_side.amount,
                    arb_side.order_price
                )

                self._order_id_side_map.update({
                    order_id: arb_side
                })
                if order_id is not None:
                    self._placement_balances[order_id] = placement_balances

                if not self._concurrent_orders_submission:
                    await arb_side.completed_event.wait()
                    if arb_side.is_failed:
                        self.log_with_clock(logging.ERROR,
                                            f"Order {order_id} seems to have failed in this arbitrage opportunity. "
                                            f"Dropping Arbitrage Proposal. ")
                        return

            await arb_proposal.wait()

    async def place_arb_order(
            self,
            market_info: MarketTradingPairTuple,
            is_buy: bool,
            amount: Decimal,
            order_price: Decimal) -> str:
        place_order_fn: Callable[[MarketTradingPairTuple, Decimal, OrderType, Decimal], str] = \
            cast(Callable, self.buy_with_specific_market if is_buy else self.sell_with_specific_market)

        return place_order_fn(market_info, amount, market_info.market.get_taker_order_type(), order_price)

    def ready_for_new_arb_trades(self) -> bool:
        """
        Returns True if there is no outstanding unfilled order.
        """
        # outstanding_orders = self.market_info_to_active_orders.get(self._market_info, [])
        for market_info in [self._market_info_1, self._market_info_2]:
            if len(self.market_info_to_active_orders.get(market_info, [])) > 0:
                return False
        return True

    def short_proposal_msg(self, arb_proposal: List[ArbProposal], indented: bool = True) -> List[str]:
        """
        Composes a short proposal message.
        :param arb_proposal: The arbitrage proposal
        :param indented: If the message should be indented (by 4 spaces)
        :return A list of messages
        """
        lines = []
        for proposal in arb_proposal:
            side1: str = "buy" if proposal.first_side.is_buy else "sell"
            side2: str = "buy" if proposal.second_side.is_buy else "sell"
            market_1_name: str = proposal.first_side.market_info.market.display_name
            market_2_name: str = proposal.second_side.market_info.market.display_name
            profit_pct = proposal.profit_pct(
                rate_source=self._rate_source,
                account_for_fee=True,
            )
            lines.append(f"{'    ' if indented else ''}{side1} at {market_1_name}"
                         f", {side2} at {market_2_name}: "
                         f"{profit_pct:.2%}")
        return lines

    def get_fixed_rates_df(self):
        columns = ["Pair", "Rate"]
        quotes_pair: str = f"{self._market_info_2.quote_asset}-{self._market_info_1.quote_asset}"
        bases_pair: str = f"{self._market_info_2.base_asset}-{self._market_info_1.base_asset}"
        data = [[quotes_pair, PerformanceMetrics.smart_round(self._rate_source.get_pair_rate(quotes_pair))],
                [bases_pair, PerformanceMetrics.smart_round(self._rate_source.get_pair_rate(bases_pair))]]
        return pd.DataFrame(data=data, columns=columns)

    async def format_status(self) -> str:
        """
        Returns a status string formatted to display nicely on terminal. The strings composes of 4 parts: markets,
        assets, profitability and warnings(if any).
        """

        if self._all_arb_proposals is None:
            return "  The strategy is not ready, please try again later."
        columns = ["Exchange", "Market", "Sell Price", "Buy Price", "Mid Price"]
        data = []
        for market_info in [self._market_info_1, self._market_info_2]:
            market, trading_pair, base_asset, quote_asset = market_info
            buy_price = await market.get_quote_price(trading_pair, True, self._order_amount)
            sell_price = await market.get_quote_price(trading_pair, False, self._order_amount)

            # check for unavailable price data
            buy_price = PerformanceMetrics.smart_round(Decimal(str(buy_price)), 8) if buy_price is not None else '-'
            sell_price = PerformanceMetrics.smart_round(Decimal(str(sell_price)), 8) if sell_price is not None else '-'
            mid_price = PerformanceMetrics.smart_round(((buy_price + sell_price) / 2), 8) if '-' not in [buy_price, sell_price] else '-'

            data.append([
                market.display_name,
                trading_pair,
                sell_price,
                buy_price,
                mid_price
            ])
        markets_df = pd.DataFrame(data=data, columns=columns)
        lines = []
        lines.extend(["", "  Markets:"] + ["    " + line for line in markets_df.to_string(index=False).split("\n")])

        columns = ["Exchange", "Gas Fees"]
        data = []
        for market_info in [self._market_info_1, self._market_info_2]:
            if hasattr(market_info.market, "network_transaction_fee"):
                transaction_fee: TokenAmount = getattr(market_info.market, "network_transaction_fee")
                data.append([market_info.market.display_name, f"{transaction_fee.amount} {transaction_fee.token}"])
        network_fees_df = pd.DataFrame(data=data, columns=columns)
        if len(data) > 0:
            lines.extend(
                ["", "  Network Fees:"] +
                ["    " + line for line in network_fees_df.to_string(index=False).split("\n")]
            )

        assets_df = self.wallet_balance_data_frame([self._market_info_1, self._market_info_2])
        lines.extend(["", "  Assets:"] +
                     ["    " + line for line in str(assets_df).split("\n")])

        lines.extend(["", "  Profitability:"] + self.short_proposal_msg(self._all_arb_proposals))

        fixed_rates_df = self.get_fixed_rates_df()
        lines.extend(["", f"  Exchange Rates: ({str(self._rate_source)})"] +
                     ["    " + line for line in str(fixed_rates_df).split("\n")])

        warning_lines = self.network_warning([self._market_info_1])
        warning_lines.extend(self.network_warning([self._market_info_2]))
        warning_lines.extend(self.balance_warning([self._market_info_1]))
        warning_lines.extend(self.balance_warning([self._market_info_2]))
        for order_id, (_, hold_end) in self._outcome_holds.items():
            warning_lines.append(f"  Order {order_id} has an unknown outcome. No new proposal until "
                                 f"{self._format_ts(hold_end)} and a balance refresh after it.")
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)

        return "\n".join(lines)

    def set_order_completed(self, order_id: str):
        self._placement_balances.pop(order_id, None)
        arb_side: Optional[ArbProposalSide] = self._order_id_side_map.get(order_id)
        if arb_side:
            arb_side.set_completed()

    def set_order_failed(self, order_id: str):
        self._placement_balances.pop(order_id, None)
        arb_side: Optional[ArbProposalSide] = self._order_id_side_map.get(order_id)
        if arb_side:
            arb_side.set_failed()
            arb_side.set_completed()

    def did_complete_buy_order(self, order_completed_event: BuyOrderCompletedEvent):
        self._settle_outcome_hold(order_completed_event.order_id, "filled")
        self.set_order_completed(order_id=order_completed_event.order_id)

        market_info: MarketTradingPairTuple = self.order_tracker.get_market_pair_from_order_id(
            order_completed_event.order_id
        )
        log_msg: str = f"Buy order completed on {market_info.market.name}: {order_completed_event.order_id}."
        if self.is_gateway_market(market_info):
            log_msg += f" txHash: {order_completed_event.exchange_order_id}"
        self.log_with_clock(logging.INFO, log_msg)
        self.notify_hb_app_with_timestamp(f"Bought {order_completed_event.base_asset_amount:.8f} "
                                          f"{order_completed_event.base_asset}-{order_completed_event.quote_asset} "
                                          f"on {market_info.market.name}.")

    def did_complete_sell_order(self, order_completed_event: SellOrderCompletedEvent):
        self._settle_outcome_hold(order_completed_event.order_id, "filled")
        self.set_order_completed(order_id=order_completed_event.order_id)

        market_info: MarketTradingPairTuple = self.order_tracker.get_market_pair_from_order_id(
            order_completed_event.order_id
        )
        log_msg: str = f"Sell order completed on {market_info.market.name}: {order_completed_event.order_id}."
        if self.is_gateway_market(market_info):
            log_msg += f" txHash: {order_completed_event.exchange_order_id}"
        self.log_with_clock(logging.INFO, log_msg)
        self.notify_hb_app_with_timestamp(f"Sold {order_completed_event.base_asset_amount:.8f} "
                                          f"{order_completed_event.base_asset}-{order_completed_event.quote_asset} "
                                          f"on {market_info.market.name}.")

    def did_fail_order(self, order_failed_event: MarketOrderFailureEvent):
        order_id = order_failed_event.order_id
        if order_id in self._outcome_holds:
            # Already held: only a fill or the window plus a balance refresh ends the hold.
            return
        market_info = self._market_with_unknown_outcome(order_id)
        if market_info is not None:
            self._hold_for_unknown_outcome(order_id, market_info)
        self.set_order_failed(order_id=order_id)

    def _market_with_unknown_outcome(self, order_id: str) -> Optional[MarketTradingPairTuple]:
        """The market whose connector says the order failed with an unknown outcome, or None."""
        for market_info in (self._market_info_1, self._market_info_2):
            reason_fn = getattr(market_info.market, "order_failure_reason", None)
            reason = reason_fn(order_id) if callable(reason_fn) else None
            if isinstance(reason, str) and SUBMIT_OUTCOME_UNKNOWN in reason:
                return market_info
        return None

    @staticmethod
    def _outcome_hold_seconds(market: ConnectorBase) -> float:
        """How long to hold after an unknown outcome on this market (see outcome_hold_seconds)."""
        return outcome_hold_seconds(market)

    @staticmethod
    def _market_address(market: ConnectorBase) -> Optional[str]:
        address = getattr(market, "address", None)
        return address if isinstance(address, str) and address else None

    @staticmethod
    def _read_balances(market_info: MarketTradingPairTuple) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        try:
            return (market_info.market.get_balance(market_info.base_asset),
                    market_info.market.get_balance(market_info.quote_asset))
        except Exception:
            return None, None

    @staticmethod
    def _format_ts(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _other_leg(self, side: Optional[ArbProposalSide]) -> Tuple[Optional[str], Optional[str]]:
        """(order id, market name) of the other leg of the proposal that placed `side`, if it was placed."""
        proposal = self._current_arb_proposal
        if side is None or proposal is None or not any(side is s for s in (proposal.first_side, proposal.second_side)):
            return None, None
        other = proposal.second_side if side is proposal.first_side else proposal.first_side
        for order_id, placed_side in self._order_id_side_map.items():
            if placed_side is other:
                return order_id, other.market_info.market.name
        return None, None

    def _hold_for_unknown_outcome(self, order_id: str, market_info: MarketTradingPairTuple):
        market = market_info.market
        hold_end = self.current_timestamp + self._outcome_hold_seconds(market)
        self._outcome_holds[order_id] = (market, hold_end)
        side = self._order_id_side_map.get(order_id)
        other_id, other_market = self._other_leg(side)
        base_before, quote_before = self._read_balances(market_info)
        base_at_placement, quote_at_placement = self._placement_balances.get(order_id, (None, None))
        _PROCESS_OUTCOME_HOLDS[order_id] = OutcomeHoldRecord(
            market_name=market.name,
            hold_end=hold_end,
            base_asset=market_info.base_asset,
            quote_asset=market_info.quote_asset,
            is_buy=side.is_buy if side is not None else None,
            amount=side.amount if side is not None else None,
            base_before=base_before,
            quote_before=quote_before,
            other_leg_id=other_id,
            other_leg_market=other_market,
            base_at_placement=base_at_placement,
            quote_at_placement=quote_at_placement,
            address=self._market_address(market),
        )
        if other_id is not None:
            other_leg = (f"The other leg {other_id} on {other_market} was sent: if this swap does not land, "
                         "that leg is unhedged.")
        else:
            other_leg = "The other leg was not placed: if this swap lands, the position is one-sided."
        self.logger().warning(
            f"Order {order_id} on {market.name} failed with an unknown outcome: the swap may still land. "
            f"{other_leg} No new arbitrage proposal until {self._format_ts(hold_end)} and a balance refresh "
            "after it. Check the wallet on chain."
        )

    def _settle_outcome_hold(self, order_id: str, verdict: str):
        """The connector gave a verdict on a held order: end its window now.

        The entry stays until outcome_holds_settled() has refreshed balances.
        """
        held = self._outcome_holds.get(order_id)
        if held is None:
            return
        market, hold_end = held
        hold_end = min(hold_end, self.current_timestamp)
        self._outcome_holds[order_id] = (market, hold_end)
        record = _PROCESS_OUTCOME_HOLDS.get(order_id)
        if record is not None:
            record.hold_end = hold_end
        self.logger().info(f"Order {order_id} of unknown outcome is now reported {verdict}; "
                           "the hold ends after a balance refresh.")

    def _report_hold_end(self, order_id: str, market: ConnectorBase):
        """Say whether the held market's balances moved, i.e. whether the swap appears to have landed."""
        record = _PROCESS_OUTCOME_HOLDS.get(order_id)
        prefix = f"Hold for order {order_id} on {market.name} ended: "
        moved = None
        try:
            if record is not None and record.is_buy is not None and record.amount:
                base_after = market.get_balance(record.base_asset)
                quote_after = market.get_balance(record.quote_asset)

                def qualifies(before: Optional[Decimal]) -> bool:
                    if before is None:
                        return False
                    d = base_after - before
                    return bool((d if record.is_buy else -d) >= record.amount / 2)

                # Judge against the balance read before placing: the one read at failure time may
                # already include the swap.
                since_placement = qualifies(record.base_at_placement)
                since_failure = qualifies(record.base_before)
                if record.base_at_placement is not None and (since_placement or not since_failure):
                    base_ref, quote_ref, since = record.base_at_placement, record.quote_at_placement, "placement"
                elif record.base_before is not None:
                    base_ref, quote_ref, since = record.base_before, record.quote_before, "the failure"
                if record.base_at_placement is not None or record.base_before is not None:
                    moved = since_placement or since_failure
                    moved_before_failure = since_placement and not since_failure
                    delta = base_after - base_ref
        except Exception:
            moved = None
        if moved is None:
            msg = prefix + "the balances at the hold start are not known. Check the wallet on chain."
        else:
            balances = (f"{record.base_asset} {base_ref} -> {base_after}, "
                        f"{record.quote_asset} {quote_ref} -> {quote_after}, since {since}")
            if moved:
                if moved_before_failure:
                    msg = prefix + (f"the balance had already moved before the failure was reported ({balances}): "
                                    "the swap appears to have landed.")
                else:
                    msg = prefix + f"balances moved ({balances}); the swap appears to have landed."
                if record.other_leg_id is None:
                    msg += (f" The other leg was never placed: the position is one-sided by {abs(delta)} "
                            f"{record.base_asset}.")
                else:
                    msg += f" The other leg {record.other_leg_id} on {record.other_leg_market} was placed."
            else:
                msg = prefix + (f"no balance change seen since {since} ({balances}); the swap appears not to "
                                "have landed (a heuristic, not a verdict).")
                if record.other_leg_id is not None:
                    msg += (f" The other leg {record.other_leg_id} on {record.other_leg_market} was placed: "
                            "its fill is unhedged.")
                else:
                    msg += " The other leg was never placed."
        self.logger().warning(msg)
        self.notify_hb_app_with_timestamp(msg)

    async def outcome_holds_settled(self) -> bool:
        """True when no order of unknown outcome holds the strategy.

        A hold ends once its window has passed and the held markets' balances have been read
        again after that, so the next budget check sees the swap if it landed.
        """
        if not self._outcome_holds:
            return True
        now = self.current_timestamp
        if any(now < hold_end for _, hold_end in self._outcome_holds.values()):
            return False
        if now < self._next_hold_refresh_at:
            return False
        ended = dict(self._outcome_holds)
        markets: List[ConnectorBase] = []
        for market, _ in ended.values():
            if not any(market is m for m in markets):
                markets.append(market)
        try:
            for market in markets:
                await market._update_balances()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._next_hold_refresh_at = now + OUTCOME_HOLD_REFRESH_RETRY_SECONDS
            if now - self._last_hold_refresh_error >= OUTCOME_HOLD_REFRESH_LOG_SECONDS:
                self._last_hold_refresh_error = now
                self.logger().warning(f"Hold kept: balance refresh after the hold window failed: {e!r}")
            return False
        for order_id, (market, _) in ended.items():
            self._outcome_holds.pop(order_id, None)
            self._report_hold_end(order_id, market)
            _PROCESS_OUTCOME_HOLDS.pop(order_id, None)
        return not self._outcome_holds

    def did_expire_order(self, expired_event: OrderExpiredEvent):
        self.set_order_completed(order_id=expired_event.order_id)

    @property
    def tracked_limit_orders(self) -> List[Tuple[ConnectorBase, LimitOrder]]:
        return self._sb_order_tracker.tracked_limit_orders

    @property
    def tracked_market_orders(self) -> List[Tuple[ConnectorBase, MarketOrder]]:
        return self._sb_order_tracker.tracked_market_orders

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        if self._main_task is not None:
            self._main_task.cancel()
            self._main_task = None
        super().stop(clock)
