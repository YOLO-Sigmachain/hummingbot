import asyncio
import logging
import re
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Dict, List, Optional, Tuple, Union

from hummingbot.connector.gateway.gateway import AMMPoolInfo, AMMPositionInfo, CLMMPoolInfo
from hummingbot.connector.utils import split_hb_trading_pair
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.event.events import RangePositionLiquidityAddedEvent, RangePositionLiquidityRemovedEvent
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient, exact_json_number
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.executor_base import ExecutorBase
from hummingbot.strategy_v2.executors.gateway_utils import (
    is_slippage_failure,
    next_slippage_pct,
    parse_provider,
    validate_and_normalize_connector,
)
from hummingbot.strategy_v2.executors.lp_executor.data_types import LPExecutorConfig, LPExecutorState, LPExecutorStates
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType, TrackedOrder

# Default native currency fallback when connector doesn't have _native_currency set
DEFAULT_NATIVE_CURRENCY = "SOL"

# Markers of a liquidity request that may have reached the chain without the client learning
# its outcome. Such a request is never sent again: a second add or remove could land beside it.
OUTCOME_UNKNOWN_MARKERS = ("[code: TX_UNRESOLVED]", "[code: SUBMIT_OUTCOME_UNKNOWN]")
# A transaction the connector gave up on after its submissions. Its outcome is unknown only
# where NOT_FOUND does not prove a transaction can no longer land (not_found_proves_dropped).
DROPPED_MARKER = "TRANSACTION_TIMEOUT"

# Gateway's refusal (400) of an AMM remove the pool cannot pay in full, which names the most LP
# tokens that can be removed now, or says none can.
AMM_CANNOT_PAY_REFUSAL = "can be removed now"
AMM_MOST_REMOVABLE = re.compile(r"at most ([0-9]+(?:\.[0-9]+)?) LP tokens")

# Gateway reads an AMM remove's percentageToRemove to 8 decimal places.
AMM_PERCENT_QUANTUM = Decimal("0.00000001")


class LPExecutor(ExecutorBase):
    """
    Executor for a single LP position lifecycle.

    - Opens position on start (direct await, no events)
    - Monitors and reports state (IN_RANGE, OUT_OF_RANGE)
    - Tracks out_of_range_since timestamp for rebalancing decisions
    - Closes position when stopped (unless keep_position=True)

    Rebalancing is handled by Controller (stops this executor, creates new one).

    Note: This executor directly awaits gateway operations instead of using
    the fire-and-forget pattern with events. This makes it work in environments
    without the Clock/tick mechanism (like hummingbot-api).
    """
    _logger: Optional[HummingbotLogger] = None

    # AMM only, for the whole process: which executor holds each (chain, network, wallet, pool),
    # the hold reason of one that stopped and kept it, and a lock held from the reading before
    # through the reading after of every AMM add and remove on it. See _amm_claim_pool.
    _amm_pool_claims: Dict[Tuple[str, str, str, str], str] = {}
    _amm_pool_claim_reasons: Dict[Tuple[str, str, str, str], Optional[str]] = {}
    _amm_pool_locks: Dict[Tuple[str, str, str, str], asyncio.Lock] = {}
    # AMM only: the fewest readings, one position_refresh_interval apart, that look for the LP of
    # an add that was sent but could not be booked, before the executor holds with the marker.
    AMM_OPEN_REREADS = 10
    # AMM only: on a network whose block time the connector knows, such an add is followed until
    # this many blocks beyond the confirmations it needs have passed since it was sent.
    AMM_OPEN_FOLLOW_BLOCKS = 10
    # AMM only: hold reasons under which an add or remove of this executor may still land, or its
    # LP is still in the pool. The claim on the pool stays held until it is released by hand.
    AMM_CLAIM_KEPT_REASONS = ("open_outcome_unknown", "open_unmeasured", "forced_stop_in_flight",
                              "forced_stop_lp_in_pool", "close_outcome_unknown", "pool_cannot_pay_in_full")

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(
        self,
        strategy: StrategyV2Base,
        config: LPExecutorConfig,
        update_interval: float = 1.0,
        max_retries: int = 10,
    ):
        # Extract connector names from config for ExecutorBase
        connectors = [config.connector_name]
        super().__init__(strategy, connectors, config, update_interval, max_retries=max_retries)
        self.config: LPExecutorConfig = config
        self._max_retries = max_retries
        self.lp_position_state = LPExecutorState()
        self._pool_info: Optional[Union[CLMMPoolInfo, AMMPoolInfo]] = None
        # One warning, not one per tick, when the pool reports no fee rate and the volume
        # this position generated therefore cannot be derived. See filled_amount_quote.
        self._warned_missing_fee_pct = False
        self._current_price: Optional[Decimal] = None  # Updated from pool_info or position_info
        self._max_retries_reached = False  # True when max retries reached, requires intervention
        # The tolerance the next Gateway request will carry. Starts at the configured
        # value and widens only on a failure Gateway attributed to slippage, never past
        # max_slippage_pct. Reset at each phase boundary: an open and the close that
        # follows it are separated by the position's whole lifetime, so a close should
        # start tight again rather than inherit however wide the open had to go.
        self._slippage_pct: Decimal = config.slippage_pct
        self._last_attempted_signature: Optional[str] = None  # Track for retry logging
        # Position tracking - store LP position for position aggregation when keep_position=True
        self._held_position_orders: List[Dict] = []
        # Swap tracking for close-out flow
        self._swap_not_found_count: int = 0
        # Consecutive definitive "position does not exist" reads while monitoring;
        # guards against a single lagging RPC node being read as an external close.
        # Uses get_position_info_fresh so each miss is an independent Gateway read
        # (the cached get_position_info would serve one 404 for several ticks).
        self._position_not_found_count: int = 0
        # External-close detection is only trusted once the position has been
        # observed on-chain at least once; before that, a not-found streak is far
        # more likely RPC lag on a just-created position than a real close.
        self._position_seen_onchain: bool = False
        # Keep the executor lifecycle responsive while bounding the expensive
        # Gateway/RPC reads made by each active LP position. None makes the first
        # read immediate, including when a position has just been created.
        self._last_position_refresh_timestamp: Optional[float] = None
        # Backoff between close attempts (set by _handle_close_failure) so a burst
        # of instant transport failures (e.g. a Gateway restart) cannot burn the
        # whole retry budget in seconds
        self._close_backoff_until: float = 0.0
        # Why a POSITION_HOLD terminal was involuntary (e.g. "close_retries_exhausted");
        # None for voluntary holds (keep_position=True stops). Consumers use this,
        # not close_type, to distinguish stranded exposure from a requested hold.
        self._hold_reason: Optional[str] = None
        # early_stop() arrived while the create was in flight: let the create
        # resolve, then route straight to CLOSING (killing the state mid-await used
        # to strand a landed position behind a FAILED terminal).
        self._close_after_open: bool = False
        # Close-out swap attempts (each failure re-places with a fresh quote)
        self._swap_failure_count: int = 0
        # Parse lp_provider into dex_name and trading_type for gateway calls
        self.lp_dex_name, self.lp_trading_type = parse_provider(config.lp_provider)
        # An "amm" trading type makes the position a share of a constant-product pool. See
        # _amm_create_position; every CLMM path below is left as it was.
        self._is_amm = self.lp_trading_type == "amm"
        self.lp_position_state.has_price_range = not self._is_amm
        # AMM only: the wallet's LP balance in the pool read right before this executor's add,
        # which measures the LP the add acquired when Gateway's answer does not state it.
        self._amm_lp_before_open: Optional[Decimal] = None
        # AMM only: the reason an add may still confirm, when its outcome was never learned.
        self._amm_open_outcome_unknown: Optional[str] = None
        self._warned_lp_shortfall = False
        # AMM only: the (chain, network, wallet, pool) this executor holds; see _amm_claim_pool.
        self._amm_pool_key: Optional[Tuple[str, str, str, str]] = None
        # AMM only: whether Gateway stated the LP the add acquired, so the close returns exactly
        # that; and whether the amounts booked at open and at close are estimates from readings.
        self._amm_lp_exact = False
        self._amm_open_estimated = False
        self._amm_close_estimated = False
        # AMM only: an add that was sent and may hold LP, but is not booked yet. "kind" is
        # "unmeasured" (it confirmed) or "outcome_unknown"; with its order id, signature, the
        # readings left and the time until which it is followed before the executor holds with
        # the marker. See _amm_try_book_open and _amm_reread_unbooked_open.
        self._amm_unbooked_open: Optional[Dict] = None
        # AMM only: the limit the last position reading showed the price past, when that reading
        # was taken, and whether a later one, at least one block (or position_refresh_interval)
        # later, showed it too. See _amm_note_limit_reading.
        self._amm_limit_crossing: Optional[Tuple[str, float, bool]] = None
        # AMM only: the most LP Gateway said the pool can pay for now, when it refused the remove.
        self._amm_most_removable: Optional[Decimal] = None

    def _validate_and_normalize_connector(self, connector_name: str) -> Optional[str]:
        """
        Validate and normalize connector name for LP executor.

        - If connector already has /clmm suffix, validates it exists
        - If connector is base name only (e.g., "meteora"), auto-appends /clmm
        - Uses GATEWAY_DEXS list populated at gateway startup

        Args:
            connector_name: Connector name from config

        Returns:
            Normalized connector name, or None if validation failed (executor stopped)
        """
        normalized, success = validate_and_normalize_connector(
            connector_name, "clmm", self.logger().error
        )
        if not success:
            self.close_type = CloseType.FAILED
            self.stop()
            return None
        return normalized

    async def on_start(self):
        """Start executor - resolves providers and creates position."""
        await super().on_start()

        # Log LP provider info
        self.logger().info(
            f"Using LP provider: {self.config.lp_provider} "
            f"(dex={self.lp_dex_name}, type={self.lp_trading_type})"
        )

        if self._is_amm and not self._amm_claim_pool():
            return

        # Resolve swap_provider up front when the config already expects to unwind,
        # so a missing provider surfaces at start instead of mid-close-out.
        if not self.config.keep_position:
            await self._resolve_swap_provider()

    async def _resolve_swap_provider(self) -> bool:
        """Fill in swap_provider from the network default if it is not set.

        Also called lazily from the close-out path: early_stop(keep_position=False)
        can unwind an executor whose config said keep_position=True, which never
        resolved a provider at start.

        Returns True if a provider is available.
        """
        if self.config.swap_provider:
            return True

        gateway = GatewayHttpClient.get_instance()
        default_provider = await gateway.get_default_swap_provider(self.config.connector_name)
        if default_provider:
            # The network default comes straight from Gateway's config, which is not
            # covered by LPExecutorConfig's validator — reject an untyped one here
            # instead of letting it become a 400 mid close-out swap.
            if "/" not in default_provider:
                self.logger().error(
                    f"Network default swap provider '{default_provider}' for "
                    f"{self.config.connector_name} is not in 'name/type' form "
                    "(e.g. 'jupiter/router'). Fix swapProvider in the Gateway network config."
                )
                return False
            self.config = self.config.model_copy(update={'swap_provider': default_provider})
            self.logger().info(f"Using network default swap provider: {default_provider}")
            return True

        self.logger().warning(
            f"No swap provider found for {self.config.connector_name}. "
            "Close-out swaps will not be available."
        )
        return False

    async def control_task(self):
        """Main control loop - simple state machine with direct await operations"""
        current_time = self._strategy.current_timestamp

        # Fetch position info when position exists (includes current price)
        # This avoids redundant pool_info call since position_info has price
        if self.lp_position_state.position_address:
            if self._is_position_refresh_due(current_time):
                # Record the attempt before awaiting it. A failed read must still be
                # throttled or a degraded RPC endpoint turns the control loop into a
                # retry storm across all active LP executors.
                self._last_position_refresh_timestamp = current_time
                await self._update_position_info()
        else:
            # Only fetch pool info when no position exists (for price during creation)
            await self.update_pool_info()

        current_price = self._current_price
        self.lp_position_state.update_state(current_price, current_time)

        match self.lp_position_state.state:
            case LPExecutorStates.NOT_ACTIVE:
                # Start opening position
                self.lp_position_state.state = LPExecutorStates.OPENING
                await self._create_position()

            case LPExecutorStates.OPENING:
                # Position creation in progress (connector retries timeouts;
                # any other open failure is terminal — see _handle_create_failure)
                await self._create_position()

            case LPExecutorStates.CLOSING:
                # This re-entry IS the close retry loop: _handle_close_failure counts
                # the failed attempt and backs off, evaluate_max_retries terminates
                await self._close_position()

            case LPExecutorStates.SWAPPING:
                # Close-out swap in progress (keep_position=False)
                # Similar to grid executor placing close order to rebalance
                await self._execute_closeout_swap()

            case LPExecutorStates.FAILED:
                # Stop with failure — but never clobber a more specific close_type
                # already assigned (e.g. EARLY_STOP set by early_stop() before the
                # create failed).
                if self.close_type is None:
                    self.close_type = CloseType.FAILED
                self.stop()

            case LPExecutorStates.IN_RANGE | LPExecutorStates.OUT_OF_RANGE:
                # Position active - close if price exceeds limit prices (like grid
                # executor). Checked in BOTH range states: limit prices are
                # independent of the position's bounds, and the on-chain bounds can
                # be wider than the configured ones (bin rounding at open), so a
                # price beyond a limit can still be inside the position's range.
                self._check_limit_prices()

            case LPExecutorStates.COMPLETE:
                # Position closed - close_type already set by early_stop()
                self.stop()

    def _is_position_refresh_due(self, current_time: float) -> bool:
        """Whether the active position should be read from Gateway this cycle."""
        last_refresh = self._last_position_refresh_timestamp
        return (
            last_refresh is None
            or current_time < last_refresh
            or current_time - last_refresh >= self.config.position_refresh_interval
        )

    def _check_limit_prices(self):
        """Close the position when the price crosses a configured limit price."""
        if self._current_price is None:
            return

        if self.config.upper_limit_price is not None and self._current_price >= self.config.upper_limit_price:
            direction = "above upper limit"
        elif self.config.lower_limit_price is not None and self._current_price <= self.config.lower_limit_price:
            direction = "below lower limit"
        else:
            return
        if self._is_amm and not self._amm_limit_held(direction):
            # One reading of one pool is not enough to withdraw on: see _amm_note_limit_reading.
            return

        self.logger().info(
            f"Price {self._current_price} {direction} "
            f"(upper_limit={self.config.upper_limit_price}, lower_limit={self.config.lower_limit_price}), closing"
        )
        # Respect keep_position config - use POSITION_HOLD to track net position, EARLY_STOP otherwise
        self.close_type = CloseType.POSITION_HOLD if self.config.keep_position else CloseType.EARLY_STOP
        self.lp_position_state.state = LPExecutorStates.CLOSING

    async def _update_position_info(self):
        """Fetch current position info from connector to update amounts and fees"""
        if not self.lp_position_state.position_address:
            return

        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            return

        if self._is_amm:
            await self._amm_update_position_info(connector)
            return

        try:
            # Fresh (uncached) read: this method makes existence decisions, and the
            # cached variant would serve a single 404 for several consecutive ticks
            position_info = await connector.get_position_info_fresh(
                trading_pair=self.config.trading_pair,
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                position_address=self.lp_position_state.position_address
            )

            if position_info:
                self._position_not_found_count = 0
                self._position_seen_onchain = True
                # Update amounts and fees from live position data
                self.lp_position_state.base_amount = Decimal(str(position_info.base_token_amount))
                self.lp_position_state.quote_amount = Decimal(str(position_info.quote_token_amount))
                self.lp_position_state.base_fee = Decimal(str(position_info.base_fee_amount))
                self.lp_position_state.quote_fee = Decimal(str(position_info.quote_fee_amount))
                # Update price bounds from actual position (may differ slightly from config)
                self.lp_position_state.lower_price = Decimal(str(position_info.lower_price))
                self.lp_position_state.upper_price = Decimal(str(position_info.upper_price))
                # Update current price from position_info (avoids separate pool_info call)
                self._current_price = Decimal(str(position_info.price))
            else:
                # None from the connector is a definitive Gateway 404 (transient errors
                # raise and are handled below). Two gates before acting on it: the
                # position must have been observed on-chain at least once (a miss on a
                # just-created position is RPC lag, not a close), and the miss must
                # repeat across independent reads.
                self._position_not_found_count += 1
                if self._position_seen_onchain and self._position_not_found_count >= 3:
                    self.logger().info(
                        f"Position {self.lp_position_state.position_address} no longer exists on-chain "
                        "(closed externally or by a prior attempt) - marking complete"
                    )
                    # An external close still needs a terminal type — and under
                    # keep_position=False it still owes the close-out swap (the
                    # withdrawn tokens landed in the wallet either way).
                    if self.close_type is None:
                        self.close_type = (CloseType.POSITION_HOLD if self.config.keep_position
                                           else CloseType.EARLY_STOP)
                    self._handle_position_gone()
                else:
                    self.logger().warning(
                        f"Position {self.lp_position_state.position_address} not found "
                        f"({self._position_not_found_count} consecutive reads, "
                        f"seen_onchain={self._position_seen_onchain})"
                    )
        except Exception as e:
            # Gateway returns HttpError with message patterns:
            # - "Position closed: {addr}" (404) - position was closed on-chain
            # - "Position not found: {addr}" (404) - position never existed
            # - "Position not found or closed: {addr}" (404) - combined check
            error_msg = str(e).lower()
            if "position closed" in error_msg:
                self.logger().info(
                    f"Position {self.lp_position_state.position_address} confirmed closed on-chain"
                )
                self._emit_already_closed_event()
                self.lp_position_state.state = LPExecutorStates.COMPLETE
                self.lp_position_state.active_close_order = None
                return
            elif "position not found" in error_msg:
                # Position-specific 404 only: a bare "not found" also matches the HTTP
                # client's "(Not Found)" suffix on unrelated 404s (missing route/wallet)
                self.logger().error(
                    f"Position {self.lp_position_state.position_address} not found - "
                    "position may never have been created. Check position tracking."
                )
                return
            self.logger().warning(f"Error fetching position info: {e}")

    async def _create_position(self):
        """
        Create position by directly awaiting the gateway operation.
        No events needed - result is available immediately after await.

        Uses the price bounds provided in config directly.
        """
        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            self.logger().error(f"Connector {self.config.connector_name} not found")
            self._handle_create_failure(ValueError(f"Connector {self.config.connector_name} not found"))
            return

        if self._is_amm:
            await self._amm_create_position(connector)
            return

        # Use config bounds directly
        lower_price = self.config.lower_price
        upper_price = self.config.upper_price
        mid_price = (lower_price + upper_price) / Decimal("2")

        self.logger().info(f"Creating position with bounds: [{lower_price:.6f} - {upper_price:.6f}]")

        # Generate order_id (same as add_liquidity does internally)
        order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
        self.lp_position_state.active_open_order = TrackedOrder(order_id=order_id)

        try:
            # Directly await the async operation - connector handles retry for timeouts
            self.logger().info(f"Calling gateway to open position with order_id={order_id}")
            signature = await connector._clmm_add_liquidity(
                trade_type=TradeType.RANGE,
                order_id=order_id,
                trading_pair=self.config.trading_pair,
                price=float(mid_price),
                lower_price=float(lower_price),
                upper_price=float(upper_price),
                base_token_amount=float(self.config.base_amount),
                quote_token_amount=float(self.config.quote_amount),
                pool_address=self.config.pool_address,
                extra_params=self.config.extra_params,
                slippage_pct=float(self._slippage_pct),
                max_retries=self._max_retries,
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
            )
            # Note: If operation fails after all retries, connector re-raises the exception
            # so it will be caught by the except block below

            self.logger().info(f"Gateway returned signature={signature}")

            # Extract position_address from connector's metadata
            # Gateway response: {"signature": "...", "data": {"positionAddress": "...", ...}}
            metadata = connector._lp_orders_metadata.get(order_id, {})
            position_address = metadata.get("position_address", "")

            if not position_address and metadata.get("data_unavailable"):
                # The open transaction CONFIRMED on-chain, but its confirmation was
                # reconciled by polling the signature, and Gateway fills the response
                # `data` (which carries positionAddress) only on the original confirmed
                # reply. The position is LIVE — recover its address from chain state
                # instead of reporting a creation failure and walking away from real funds.
                self.lp_position_state.open_data_unavailable = True
                position_address = await self._recover_position_address(connector)
                if not position_address:
                    self._handle_create_failure(ValueError(
                        f"Open transaction {signature} CONFIRMED on-chain but its position address "
                        f"could not be recovered from pool {self.config.pool_address}. A position is "
                        "likely LIVE and unmanaged - reconcile this wallet by hand."
                    ))
                    return

            if not position_address:
                self.logger().error(f"No position_address in metadata: {metadata}")
                self._handle_create_failure(ValueError("Position creation failed - no position address in response"))
                return

            # Store position address, rent, tx_fee, and transaction hash from response
            self.lp_position_state.position_address = position_address
            self.lp_position_state.position_rent = metadata.get("position_rent", Decimal("0"))
            self.lp_position_state.tx_fee = metadata.get("tx_fee", Decimal("0"))
            self.lp_position_state.open_tx_hash = signature

            # Position is created - clear open order
            self.lp_position_state.active_open_order = None
            # The open is done; the close is a separate phase and starts tight again.
            self._reset_slippage()

            # Clean up connector metadata
            if order_id in connector._lp_orders_metadata:
                del connector._lp_orders_metadata[order_id]

            # Fetch full position info from chain to get actual amounts and bounds.
            # This enriches bookkeeping only - the position is already open, so a
            # transient fetch error must not fail the create (which would route to
            # CLOSING); fall back to config values instead.
            try:
                # Fresh read: caching a lagging 404 here would poison the next ticks'
                # existence checks with "position gone" for a position that is live
                position_info = await connector.get_position_info_fresh(
                    trading_pair=self.config.trading_pair,
                    dex_name=self.lp_dex_name,
                    trading_type=self.lp_trading_type,
                    position_address=position_address
                )
            except Exception as info_error:
                self.logger().warning(f"Position info fetch after create failed: {info_error}")
                position_info = None

            if position_info:
                self._position_seen_onchain = True

            if position_info:
                self.lp_position_state.base_amount = Decimal(str(position_info.base_token_amount))
                self.lp_position_state.quote_amount = Decimal(str(position_info.quote_token_amount))
                self.lp_position_state.lower_price = Decimal(str(position_info.lower_price))
                self.lp_position_state.upper_price = Decimal(str(position_info.upper_price))
                self.lp_position_state.base_fee = Decimal(str(position_info.base_fee_amount))
                self.lp_position_state.quote_fee = Decimal(str(position_info.quote_fee_amount))
                # Store initial amounts for accurate P&L calculation (these don't change as price moves)
                self.lp_position_state.initial_base_amount = self.lp_position_state.base_amount
                self.lp_position_state.initial_quote_amount = self.lp_position_state.quote_amount
                # Use price from position_info (avoids separate pool_info call)
                current_price = Decimal(str(position_info.price))
                self._current_price = current_price
                self.lp_position_state.add_mid_price = current_price
            else:
                # Fallback to config values if position_info fetch failed (e.g., rate limit)
                self.logger().warning("Position info fetch failed, using config values as fallback")
                self.lp_position_state.base_amount = self.config.base_amount
                self.lp_position_state.quote_amount = self.config.quote_amount
                self.lp_position_state.lower_price = lower_price
                self.lp_position_state.upper_price = upper_price
                self.lp_position_state.initial_base_amount = self.config.base_amount
                self.lp_position_state.initial_quote_amount = self.config.quote_amount
                current_price = mid_price
                self._current_price = current_price
                self.lp_position_state.add_mid_price = current_price

            self.logger().info(
                f"Position created: {position_address}, "
                f"rent: {self.lp_position_state.position_rent} SOL, "
                f"base: {self.lp_position_state.base_amount}, quote: {self.lp_position_state.quote_amount}, "
                f"bounds: [{self.lp_position_state.lower_price} - {self.lp_position_state.upper_price}]"
            )

            # Trigger event for database recording (lphistory command)
            # Note: mid_price is the current MARKET price, not the position range midpoint
            # Create trade_fee with tx_fee in native currency for proper tracking
            native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
            trade_fee = TradeFeeBase.new_spot_fee(
                fee_schema=connector.trade_fee_schema(),
                trade_type=TradeType.RANGE,
                flat_fees=[TokenAmount(amount=self.lp_position_state.tx_fee, token=native_currency)]
            )
            event = connector._trigger_add_liquidity_event(
                order_id=order_id,
                exchange_order_id=signature,
                trading_pair=self.config.trading_pair,
                lower_price=self.lp_position_state.lower_price,
                upper_price=self.lp_position_state.upper_price,
                amount=self.lp_position_state.base_amount + self.lp_position_state.quote_amount / current_price,
                fee_tier=self.config.pool_address,
                creation_timestamp=self._strategy.current_timestamp,
                trade_fee=trade_fee,
                position_address=position_address,
                base_amount=self.lp_position_state.base_amount,
                quote_amount=self.lp_position_state.quote_amount,
                mid_price=current_price,
                position_rent=self.lp_position_state.position_rent,
            )

            # Record the deposit unconditionally. This is bookkeeping, not a
            # decision: whether the round trip is kept as a hold is settled at
            # stop time by early_stop(keep_position=...), long after this runs.
            # Gating it on config.keep_position left a runtime keep_position=True
            # with no deposit to net against, booking the entire withdrawn
            # balance as a BUY.
            self._store_lp_event_from_add(event)

            if self._close_after_open:
                # early_stop() arrived while this create was in flight: the position
                # is live and funded, so close it now (close_type was already set by
                # early_stop from its keep_position argument).
                self.logger().info(
                    f"Create resolved after early_stop: closing {position_address} immediately."
                )
                self.lp_position_state.state = LPExecutorStates.CLOSING
                return

            # Update state immediately (don't wait for next tick)
            self.lp_position_state.update_state(current_price, self._strategy.current_timestamp)

        except Exception as e:
            self._handle_create_failure(e)

    async def _recover_position_address(self, connector) -> str:
        """
        Identify the position an open created, when Gateway's response data was unavailable.

        Reads the wallet's live positions in the configured pool. A single position in the
        pool is unambiguous; with several, the configured bounds pick one out (the chain
        snaps bounds to ticks/bins, so match within a small relative tolerance). Adopting
        the wrong position would later close someone else's, so anything ambiguous returns
        "" and the caller escalates for manual reconciliation rather than guessing.
        """
        try:
            positions = await connector.get_user_positions(
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                pool_address=self.config.pool_address,
            )
        except Exception as e:
            self.logger().error(f"Could not list positions to recover the opened position address: {e}")
            return ""

        if len(positions) == 1:
            recovered = positions[0].address
            self.logger().warning(
                f"Recovered position address {recovered} from chain state after the open's "
                "response data was unavailable."
            )
            return recovered

        matches = [
            p for p in positions
            if self._bounds_match(Decimal(str(p.lower_price)), Decimal(str(p.upper_price)))
        ]
        if len(matches) == 1:
            recovered = matches[0].address
            self.logger().warning(
                f"Recovered position address {recovered} by matching the configured bounds "
                f"among {len(positions)} positions in pool {self.config.pool_address}."
            )
            return recovered

        self.logger().error(
            f"Cannot identify the opened position in pool {self.config.pool_address}: "
            f"{len(positions)} positions owned, {len(matches)} match the configured bounds "
            f"[{self.config.lower_price} - {self.config.upper_price}]."
        )
        return ""

    def _bounds_match(self, lower_price: Decimal, upper_price: Decimal) -> bool:
        """Whether on-chain bounds are the configured ones, within tick/bin snapping."""
        tolerance = Decimal("0.01")  # 1% - wider than any connector's tick/bin spacing
        for actual, configured in ((lower_price, self.config.lower_price),
                                   (upper_price, self.config.upper_price)):
            if configured <= 0:
                return False
            if abs(actual - configured) / configured > tolerance:
                return False
        return True

    # ------------------------------------------------------------------------------------
    # AMM pool share
    #
    # On an AMM (constant-product) pool a position has no range and no account of its own: it
    # is the LP tokens this executor's add acquired, in one pool, and position_address holds
    # the pool's address. The wallet may hold more LP of the same pool (an earlier deposit, a
    # manual one); none of that is this position's. Fees accrue inside the reserves, so there
    # is nothing to collect: base_fee and quote_fee stay 0, and the fees show in base_amount
    # and quote_amount.
    #
    # One AMM executor in this process holds a (chain, network, wallet, pool) at a time (see
    # _amm_claim_pool). Nothing here sees what happens outside this process: another bot or a
    # manual add or remove on the same wallet and pool.
    #
    # Amounts are booked only once the connector returns the transaction as confirmed, from
    # what Gateway's answer stated (a PENDING answer's `pendingData`, or a confirmed answer's
    # `data`), kept with the order. When the add's answer states the LP it acquired, the
    # position is exact: its close returns exactly that LP (lpTokenAmount), and only it is
    # removed. Otherwise the LP is measured as the growth of the wallet's confirmed LP of the
    # pool between a reading before and after the add, and the close removes a percentage of
    # the wallet's LP. Gateway takes that percentage of the LP the wallet can spend, including
    # unconfirmed transactions, while the reading counts confirmed LP only. So anything else
    # moving the wallet's LP of the pool between the readings, or while the remove waits to
    # confirm, can make such a close take LP that is not this position's, or leave some of its
    # own. Amounts worked out from readings are estimates, and are reported as such.

    @staticmethod
    def _amm_claim_key(chain, network, wallet_address, pool_address) -> Tuple[str, str, str, str]:
        """The claim key of a pool: the chain, network and wallet, and the pool id in lower case
        (Gateway reads pool ids in any case), so two spellings of one pool are one key."""
        return str(chain), str(network), str(wallet_address), str(pool_address).lower()

    @classmethod
    def release_amm_pool_claim(cls, chain: str, network: str, wallet_address: str, pool_address: str) -> Optional[str]:
        """Release the claim on one pool, for the person running the bot, and return the executor that held it.

        An AMM executor that stopped while an add or remove of its own may still land, or with LP
        of its own still in the pool, keeps the claim for the rest of the process, so no other AMM
        executor deposits into or removes from that pool and wallet beside it. Call this only
        after checking the wallet's transactions and LP of the pool on chain, for example from a
        script or the Python console of the running bot. Returns None when nothing held it.
        """
        key = cls._amm_claim_key(chain, network, wallet_address, pool_address)
        holder = cls._amm_pool_claims.pop(key, None)
        reason = cls._amm_pool_claim_reasons.pop(key, None)
        if holder is not None:
            cls.logger().warning(
                f"Releasing the claim of AMM executor {holder} (hold_reason={reason}) on pool {key[3]} for wallet "
                f"{key[2]} on {key[0]}-{key[1]}: another AMM executor may now deposit into or remove from it."
            )
        return holder

    def _amm_claim_pool(self) -> bool:
        """Claim this executor's (chain, network, wallet, pool) for the process, or refuse to run.

        Gateway's remove and the wallet's position read count the wallet's LP of a pool, not an
        executor's, so two AMM executors on one pool and wallet cannot tell their LP apart. A
        second one ends FAILED, as for a config it cannot run, before it sends anything. The key
        comes from the connector's chain and network, not its name, so two connector names for
        one wallet are one key. The claim is released by stop() only when nothing of this
        executor's is or may still get into the pool (see _amm_release_pool), and otherwise by
        hand (see release_amm_pool_claim).
        """
        if self._amm_pool_key is not None:
            return True
        connector = self.connectors.get(self.config.connector_name)
        key = self._amm_claim_key(getattr(connector, "chain", ""), getattr(connector, "network", ""),
                                  getattr(connector, "address", ""), self.config.pool_address)
        holder = LPExecutor._amm_pool_claims.setdefault(key, self.config.id)
        if holder != self.config.id:
            stopped = key in LPExecutor._amm_pool_claim_reasons
            reason = LPExecutor._amm_pool_claim_reasons.get(key)
            self.logger().error(
                f"AMM executor {holder} already holds pool {key[3]} for wallet {key[2]} on {key[0]}-{key[1]} "
                f"({f'it stopped with hold_reason={reason}' if stopped else 'it is still running'}). This executor "
                "could not tell its LP apart from that one's, so it does not run; nothing was sent. Once the "
                "wallet's transactions and LP of this pool have been checked on chain, the claim can be released "
                f"with LPExecutor.release_amm_pool_claim({key[0]!r}, {key[1]!r}, {key[2]!r}, {key[3]!r})."
            )
            self.lp_position_state.state = LPExecutorStates.FAILED
            self.close_type = CloseType.FAILED
            self.stop()
            return False
        self._amm_pool_key = key
        return True

    def _amm_pool_lock(self) -> asyncio.Lock:
        """The lock held from the reading before through the reading after of an add or remove.

        Anything else in this process that takes it for the same key waits for it; nothing
        outside this process does.
        """
        return LPExecutor._amm_pool_locks.setdefault(self._amm_pool_key, asyncio.Lock())

    def _amm_lp_at_stake(self) -> bool:
        """Whether LP of this executor's may be in the pool: a position, an add or remove still
        awaited, or an add that was sent and is not booked."""
        state = self.lp_position_state
        return bool(state.position_address or state.active_open_order is not None
                    or state.active_close_order is not None or self._amm_unbooked_open is not None)

    def _amm_marker_written(self) -> bool:
        return any(order.get("lp_unclosed_position") for order in self._held_position_orders)

    def _amm_release_pool(self):
        """Give up the claim on the pool on a clean end only: nothing sent, or a confirmed close that left nothing.

        While an add or remove of this executor's may still land, or LP of its own is still in
        the pool (a hold, or a close by percentage that left a residue), the claim stays held for
        the rest of the process, marker or not: a second executor would deposit beside an add that
        may still confirm, or take this executor's LP as its own. It is then released by hand.
        """
        key = self._amm_pool_key
        if key is None or LPExecutor._amm_pool_claims.get(key) != self.config.id:
            return
        if (self._amm_lp_at_stake() or self.lp_position_state.lp_token_residue > 0
                or self._hold_reason in self.AMM_CLAIM_KEPT_REASONS):
            LPExecutor._amm_pool_claim_reasons[key] = self._hold_reason
            self.logger().warning(
                f"Keeping the claim on pool {key[3]} for wallet {key[2]} on {key[0]}-{key[1]} "
                f"(hold_reason={self._hold_reason}, lp_token_residue={self.lp_position_state.lp_token_residue}): "
                "an add or remove of this executor's may still land, or LP of its own is still in the pool, so no "
                "other AMM executor in this process may use the pool and wallet. Check the wallet on chain, then "
                f"release it with LPExecutor.release_amm_pool_claim({key[0]!r}, {key[1]!r}, {key[2]!r}, {key[3]!r})."
            )
            return
        del LPExecutor._amm_pool_claims[key]
        LPExecutor._amm_pool_claim_reasons.pop(key, None)

    def stop(self):
        super().stop()
        if self._is_amm:
            self._amm_release_pool()

    def _amm_limit_direction(self, price: Optional[Decimal]) -> Optional[str]:
        """The limit `price` is past, in _check_limit_prices' words, or None."""
        if price is None:
            return None
        if self.config.upper_limit_price is not None and price >= self.config.upper_limit_price:
            return "above upper limit"
        if self.config.lower_limit_price is not None and price <= self.config.lower_limit_price:
            return "below lower limit"
        return None

    @staticmethod
    def _amm_block_time(connector) -> Optional[float]:
        """The network's block time in seconds as the connector read it from Gateway, or None if it does not know it."""
        block_time = getattr(connector, "_block_time_seconds", None)
        if isinstance(block_time, (int, float)) and not isinstance(block_time, bool) and block_time > 0:
            return float(block_time)
        return None

    def _amm_note_limit_reading(self, now: float, connector=None):
        """Record what a position reading at `now` says about the limits.

        A limit closes an AMM position only once the readings show the price past it without a
        break from a first reading to one at least one block time later (position_refresh_interval
        where the connector knows no block time, and never less than that). One pool's price can be
        pushed past a limit for a block by anyone willing to pay two pool fees, and a close
        followed by a close-out swap at that price is theirs to profit from; two readings of the
        same confirmed pool box are one reading of it. Gateway's pool and position info name no
        pool box, so the readings are told apart by the time between them only.
        """
        direction = self._amm_limit_direction(self._current_price)
        crossing = self._amm_limit_crossing
        spacing = max(self.config.position_refresh_interval, self._amm_block_time(connector) or 0.0)
        if direction is None:
            self._amm_limit_crossing = None
        elif crossing is None or crossing[0] != direction:
            self._amm_limit_crossing = (direction, now, False)
        elif now - crossing[1] >= spacing:
            self._amm_limit_crossing = (direction, crossing[1], True)

    def _amm_limit_held(self, direction: str) -> bool:
        """Whether the price has stayed past this limit over two readings (see _amm_note_limit_reading)."""
        crossing = self._amm_limit_crossing
        return crossing is not None and crossing[0] == direction and crossing[2]

    async def _read_amm_holding(self, connector) -> Optional[AMMPositionInfo]:
        """The wallet's whole holding in the pool, read fresh. None when Gateway says it holds none."""
        return await connector.get_position_info_fresh(
            trading_pair=self.config.trading_pair,
            dex_name=self.lp_dex_name,
            trading_type=self.lp_trading_type,
            position_address=self.config.pool_address,
        )

    @staticmethod
    def _held_lp(holding: Optional[AMMPositionInfo]) -> Decimal:
        """The LP tokens of the pool a holding reading counts, 0 for none."""
        return Decimal(str(holding.lp_token_amount)) if holding is not None else Decimal("0")

    @staticmethod
    def _share_of(holding: AMMPositionInfo, lp: Decimal) -> Tuple[Decimal, Decimal]:
        """What `lp` of the LP in a holding reading is worth, in base and quote, at that reading."""
        held = Decimal(str(holding.lp_token_amount))
        if held <= 0:
            return Decimal("0"), Decimal("0")
        return (Decimal(str(holding.base_token_amount)) * lp / held,
                Decimal(str(holding.quote_token_amount)) * lp / held)

    @staticmethod
    def _is_outcome_unknown(reason: str, proves_dropped: bool) -> bool:
        """Whether a failure says the request may have reached the chain all the same.

        A transaction the connector gave up on as dropped counts only where NOT_FOUND does not
        prove it can no longer land. `proves_dropped` is the connector's not_found_proves_dropped,
        read once before the request was sent, as the connector's own retry loop reads it.
        """
        if any(marker in reason for marker in OUTCOME_UNKNOWN_MARKERS):
            return True
        return DROPPED_MARKER in reason and not proves_dropped

    @staticmethod
    def _proves_dropped(connector) -> bool:
        """The connector's not_found_proves_dropped; anything but True is read as False."""
        return getattr(connector, "not_found_proves_dropped", False) is True

    @staticmethod
    def _amm_order_signature(connector, order_id: Optional[str], reason: Optional[str] = None) -> Optional[str]:
        """The transaction an AMM add or remove was sent as, when the client knows it.

        The connector keeps it with the order from Gateway's answer. A failure that names a
        transaction ("Transaction <id> ...") is read when the order does not have it.
        """
        order = connector.get_order(order_id) if order_id else None
        kept = getattr(order, "pending_swap_data", None)
        if isinstance(kept, (tuple, list)) and len(kept) == 2 and isinstance(kept[0], str) and kept[0]:
            return kept[0]
        match = re.search(r"Transaction ([0-9A-Za-z]+)", reason or "")
        return match.group(1) if match else None

    def _amm_pending_amounts(self, connector, order_id: str, signature: str, operation: str) -> Dict[str, Decimal]:
        """The amounts Gateway's answer to a confirmed AMM add or remove states, by what they measure.

        The connector keeps a PENDING answer's `pendingData`, or a CONFIRMED answer's `data`,
        with the order and the signature it came with, and returns the signature only once that
        transaction is confirmed. The keys read are `baseTokenAmountAdded`,
        `quoteTokenAmountAdded` and `lpTokenAmountAdded` for an add, `baseTokenAmountRemoved`,
        `quoteTokenAmountRemoved` and `lpTokenAmountRemoved` for a remove, and `fee`; numbers or
        strings. A `poolAddress` other than this executor's pool, in any case, discards them all. A key that is
        missing or not a finite amount (an LP amount above 0) is left out, and the caller
        works that amount out from the pool holding instead.
        """
        order = connector.get_order(order_id)
        kept = getattr(order, "pending_swap_data", None)
        if (not isinstance(kept, (tuple, list)) or len(kept) != 2
                or kept[0] != signature or not isinstance(kept[1], dict)):
            return {}
        stated_pool = kept[1].get("poolAddress")
        if stated_pool is not None and str(stated_pool).lower() != self.config.pool_address.lower():
            self.logger().warning(
                f"Gateway's answer for {signature} names pool {kept[1].get('poolAddress')}, not "
                f"{self.config.pool_address}: its amounts are not used.")
            return {}
        suffix = "Added" if operation == "add" else "Removed"
        keys = {"base": f"baseTokenAmount{suffix}", "quote": f"quoteTokenAmount{suffix}",
                "lp": f"lpTokenAmount{suffix}", "fee": "fee"}
        amounts = {}
        for name, key in keys.items():
            value = kept[1].get(key)
            if value is None or isinstance(value, bool):
                continue
            try:
                value = Decimal(str(value))
            except (InvalidOperation, ValueError):
                continue
            if value.is_finite() and value >= 0 and (name != "lp" or value > 0):
                amounts[name] = value
        return amounts

    def _amm_position_lp(self, held: Decimal) -> Decimal:
        """This position's LP among the `held` LP the wallet holds now, 0 when there is none."""
        lp = self.lp_position_state.lp_token_amount
        if lp <= 0 or held <= 0:
            return Decimal("0")
        if held < lp:
            if not self._warned_lp_shortfall:
                self._warned_lp_shortfall = True
                self.logger().warning(
                    f"The wallet holds {held} LP of pool {self.config.pool_address}, less than the {lp} this "
                    "position acquired: some of it left the wallet outside this executor."
                )
            return held
        return lp

    def _amm_position_record(self) -> Dict:
        """What this executor holds, or may hold, in the pool: for the unclosed-position marker and custom info."""
        state = self.lp_position_state
        unmeasured = self._amm_unbooked_open is not None or (
            state.active_open_order is not None and state.position_address is None)
        record = {
            "pool_address": self.config.pool_address,
            "lp_token_amount": "unmeasured" if unmeasured else str(state.lp_token_amount),
            "lp_token_amount_exact": self._amm_lp_exact,
            "open_tx_hash": state.open_tx_hash,
            "close_tx_hash": state.close_tx_hash,
        }
        if unmeasured:
            record["offered_base_amount"] = str(self.config.base_amount)
            record["offered_quote_amount"] = str(self.config.quote_amount)
            # What whoever recovers the add by hand needs: the wallet's LP of the pool before it,
            # its order, and the LP Gateway's answer stated for its transaction, when it was kept.
            unbooked = self._amm_unbooked_open or {}
            order_id = unbooked.get("order_id") or (
                state.active_open_order.order_id if state.active_open_order is not None else None)
            record["lp_before_open"] = (str(self._amm_lp_before_open)
                                        if self._amm_lp_before_open is not None else None)
            record["open_order_id"] = order_id
            connector = self.connectors.get(self.config.connector_name)
            signature = state.open_tx_hash
            stated = (self._amm_pending_amounts(connector, order_id, signature, "add").get("lp")
                      if connector is not None and order_id and signature else None)
            if stated is None:
                stated = (unbooked.get("pending") or {}).get("lp")
            record["stated_lp_token_amount"] = str(stated) if stated is not None else None
            if signature is None and self._amm_open_outcome_unknown is not None:
                record["open_tx_note"] = (
                    "the add's reply was lost; its transaction id is unknown; check the wallet's recent "
                    f"transactions for a deposit into pool {self.config.pool_address}")
        return record

    async def _amm_create_position(self, connector):
        """Add the configured amounts to the pool; the position is the LP tokens the add acquires."""
        if self._amm_unbooked_open is not None:
            # An add was sent and is not booked: look for its LP again, and never send another.
            await self._amm_reread_unbooked_open(connector)
            return
        if self._close_after_open:
            # early_stop() came while no add had been sent (a failed reading or a refused add left
            # the state at OPENING), so there is nothing to close and nothing is sent.
            self.logger().info("early_stop before the AMM add was sent: not sending it.")
            self.lp_position_state.active_open_order = None
            self.lp_position_state.state = LPExecutorStates.COMPLETE
            return
        if not self._amm_claim_pool():
            return
        async with self._amm_pool_lock():
            await self._amm_add(connector)

    async def _amm_add(self, connector):
        """Read the wallet's holding in the pool, send the add, and book it once it is known."""
        pool_address = self.config.pool_address
        try:
            before = await self._read_amm_holding(connector)
        except Exception as e:
            # Nothing was sent. Without this reading the LP the add acquires could not be told
            # apart from LP the wallet already holds, so the add waits for the next tick; the
            # retry budget ends a reading that keeps failing.
            self._current_retries += 1
            self.logger().warning(
                f"Reading the wallet's holding in pool {pool_address} before the AMM add failed "
                f"({self._current_retries}/{self._max_retries} retries used): {e}"
            )
            return
        lp_before = self._held_lp(before)
        self._amm_lp_before_open = lp_before
        if before is not None:
            self._current_price = Decimal(str(before.price))
        price = self._current_price or Decimal("0")
        beyond = self._amm_limit_direction(price) if price > 0 else None
        if beyond is not None:
            # The position would be withdrawn again as soon as it is read.
            self._handle_create_failure(ValueError(
                f"The price of pool {pool_address} ({price}) is already {beyond} (upper_limit="
                f"{self.config.upper_limit_price}, lower_limit={self.config.lower_limit_price}); the AMM add is not sent"
            ))
            return

        order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
        self.lp_position_state.active_open_order = TrackedOrder(order_id=order_id)
        self.logger().info(
            f"Adding at most {self.config.base_amount} base and {self.config.quote_amount} quote to AMM pool "
            f"{pool_address} (order_id={order_id}); the wallet holds {lp_before} LP of it before the add"
        )
        proves_dropped = self._proves_dropped(connector)
        sent_at = self._strategy.current_timestamp
        try:
            # Returns the signature only once the transaction is confirmed (a PENDING answer is
            # polled to a final state first), and None on a failure, which it records on the order.
            signature = await connector._amm_add_liquidity(
                trade_type=TradeType.RANGE,
                order_id=order_id,
                trading_pair=self.config.trading_pair,
                price=float(price),
                base_token_amount=float(self.config.base_amount),
                quote_token_amount=float(self.config.quote_amount),
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                slippage_pct=float(self._slippage_pct),
                max_retries=self._max_retries,
                pool_address=pool_address,
            )
        except Exception as e:
            # Raised only before anything was sent.
            self._handle_create_failure(e)
            return

        kind = "unmeasured"
        if not signature:
            reason = connector.order_failure_reason(order_id) or "AMM add liquidity failed"
            if not self._is_outcome_unknown(reason, proves_dropped):
                self._handle_create_failure(ValueError(reason))
                return
            # It may still confirm. It is never sent again, and the executor never ends FAILED
            # for it: a controller that re-creates an executor after FAILED would deposit twice.
            kind = "outcome_unknown"
            self._amm_open_outcome_unknown = reason
            signature = self._amm_order_signature(connector, order_id, reason)
        follow_until = self._amm_open_follow_until(connector, sent_at)
        if kind == "outcome_unknown":
            self.logger().error(
                f"The AMM add on pool {pool_address} may still confirm ({reason}). It is not sent again; it is "
                f"followed until {follow_until:.0f} ({follow_until - sent_at:.0f}s after it was sent), by its "
                "transaction's status when its id is known and by the wallet's LP of the pool, and then this "
                "executor holds with the unclosed-position marker."
            )
        pending = (self._amm_pending_amounts(connector, order_id, signature, "add")
                   if signature else {})
        self.lp_position_state.active_open_order = None
        self.lp_position_state.open_tx_hash = signature
        self._amm_unbooked_open = {"kind": kind, "order_id": order_id, "signature": signature,
                                   "pending": pending, "reads_left": self.AMM_OPEN_REREADS,
                                   "follow_until": follow_until}
        await self._amm_book_or_wait(connector)

    def _amm_open_follow_until(self, connector, sent_at: float) -> float:
        """Until when an add sent at `sent_at` that is not booked is followed before the executor holds.

        As long as it can still land: the blocks the connector's confirmation needs plus
        AMM_OPEN_FOLLOW_BLOCKS more, when the connector knows the block time, and never less than
        AMM_OPEN_REREADS readings.
        """
        least = sent_at + self.AMM_OPEN_REREADS * self.config.position_refresh_interval
        block_time = self._amm_block_time(connector)
        if block_time is None:
            return least
        confirmations = getattr(connector, "_block_confirmations", 1)
        if not isinstance(confirmations, int) or isinstance(confirmations, bool) or confirmations < 1:
            confirmations = 1
        return max(least, sent_at + block_time * (confirmations + self.AMM_OPEN_FOLLOW_BLOCKS))

    async def _amm_open_verdict(self, connector, signature: str) -> Optional[int]:
        """The add's transaction status by its signature, as the connector polls it: 1 confirmed,
        -1 failed, anything else (or a poll that failed) None."""
        try:
            poll = await connector._get_gateway_instance().get_transaction_status(
                connector.chain, connector.network, signature, fail_silently=True)
        except Exception as e:
            self.logger().warning(f"Polling the AMM add {signature} on pool {self.config.pool_address} failed: {e}")
            return None
        status = poll.get("txStatus") if isinstance(poll, dict) else None
        return status if status in (1, -1) else None

    async def _amm_reread_unbooked_open(self, connector):
        """Follow an add that was sent and is not booked; hold once it has been followed as long as it can land.

        An add whose outcome is unknown is polled by its transaction id when that is known, as the
        connector polls it. Confirmed: it is booked as a confirmed add, from the amounts Gateway's
        answer stated for it when they were kept, else from the readings. Failed: nothing landed,
        and the open fails with nothing booked. Otherwise, and without an id, the wallet's LP of the
        pool is read for it.
        """
        now = self._strategy.current_timestamp
        if not self._is_position_refresh_due(now):
            return
        self._last_position_refresh_timestamp = now
        unbooked = self._amm_unbooked_open
        unbooked["reads_left"] -= 1
        if unbooked["kind"] == "outcome_unknown" and unbooked["signature"]:
            verdict = await self._amm_open_verdict(connector, unbooked["signature"])
            if verdict == 1:
                unbooked["kind"] = "unmeasured"
            elif verdict == -1:
                self._amm_unbooked_open = None
                self._handle_create_failure(ValueError(
                    f"The AMM add {unbooked['signature']} on pool {self.config.pool_address} failed on-chain: "
                    "nothing was deposited"))
                return
        async with self._amm_pool_lock():
            if await self._amm_book_or_wait(connector):
                return
        if unbooked["reads_left"] <= 0 and now >= unbooked["follow_until"]:
            self._amm_hold_unbooked_open()

    async def _amm_book_or_wait(self, connector) -> bool:
        """Book the add that was sent, if what it acquired is now known. False while it is not."""
        try:
            return await self._amm_try_book_open(connector)
        except Exception as e:
            if self.lp_position_state.position_address:
                # Booked, and something after it failed: the position is live (see _handle_create_failure).
                self._handle_create_failure(e)
                return True
            self.logger().error(f"Booking the AMM add on pool {self.config.pool_address} failed: {e}")
            return False

    async def _amm_try_book_open(self, connector) -> bool:
        """Book the add that was sent, once what it acquired is known; False while it is not.

        The LP comes from Gateway's answer when it states it (the position is then exact), and
        otherwise from the growth of the wallet's LP of the pool over the reading before the add.
        What it paid comes from Gateway's answer, and otherwise from what that LP is worth at the
        reading taken now, an estimate. The offer is never booked: an AMM deposit takes at most
        each side, in the pool's ratio, so what was offered is not what was paid.
        """
        unbooked = self._amm_unbooked_open
        pool_address = self.config.pool_address
        state = self.lp_position_state
        confirmed = unbooked["kind"] == "unmeasured"
        pending = {}
        if confirmed:
            pending = (self._amm_pending_amounts(connector, unbooked["order_id"], unbooked["signature"], "add")
                       or unbooked.get("pending") or {})
        after, after_read = None, True
        try:
            after = await self._read_amm_holding(connector)
        except Exception as e:
            after_read = False
            self.logger().warning(f"Reading the wallet's holding in pool {pool_address} after the AMM add failed: {e}")
        held_after = self._held_lp(after)
        lp_before = self._amm_lp_before_open if self._amm_lp_before_open is not None else Decimal("0")

        lp = pending.get("lp")
        if lp is None and after_read and held_after > lp_before:
            lp = held_after - lp_before
        # What this LP is worth at the reading, not the change in the whole holding's worth: the
        # pool moving between the reads changes the worth of LP the wallet already held.
        share = self._share_of(after, lp) if lp is not None and held_after > 0 else None
        base = pending.get("base", share[0] if share else None)
        quote = pending.get("quote", share[1] if share else None)
        if lp is None or base is None or quote is None:
            self.logger().warning(
                f"The AMM add {unbooked['signature']} on pool {pool_address} is not booked yet: "
                f"{'the LP it acquired' if lp is None else 'what it paid'} is not stated by Gateway or seen in a "
                f"reading ({lp_before} LP before the add, {held_after if after_read else 'no reading'} now)."
            )
            return False
        if after is not None:
            self._current_price = Decimal(str(after.price))
        current_price = self._current_price or Decimal("0")
        signature = unbooked["signature"] or "outcome-unknown"
        order_id = unbooked["order_id"]

        state.position_address = pool_address
        state.lp_token_amount = lp
        state.base_amount = state.initial_base_amount = base
        state.quote_amount = state.initial_quote_amount = quote
        state.base_fee = state.quote_fee = Decimal("0")
        state.tx_fee = pending.get("fee", Decimal("0"))
        state.open_tx_hash = signature
        state.add_mid_price = current_price
        state.active_open_order = None
        # Adopted from a reading after an add whose outcome was never learned.
        state.open_data_unavailable = not confirmed
        self._amm_lp_exact = "lp" in pending
        self._amm_open_estimated = "base" not in pending or "quote" not in pending
        self._amm_unbooked_open = None
        if after_read and held_after >= lp:
            self._position_seen_onchain = True
        self._reset_slippage()
        # The close has a retry budget of its own: readings that failed while opening do not spend it.
        self._current_retries = 0
        self.logger().info(
            f"AMM position opened in pool {pool_address}: {lp} LP ({'stated by Gateway' if self._amm_lp_exact else 'measured'}) "
            f"for base: {base}, quote: {quote} ({'estimated' if self._amm_open_estimated else 'stated by Gateway'})"
        )

        native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
        trade_fee = TradeFeeBase.new_spot_fee(
            fee_schema=connector.trade_fee_schema(),
            trade_type=TradeType.RANGE,
            flat_fees=[TokenAmount(amount=state.tx_fee, token=native_currency)]
        )
        event = connector._trigger_add_liquidity_event(
            order_id=order_id,
            exchange_order_id=signature,
            trading_pair=self.config.trading_pair,
            lower_price=Decimal("0"),
            upper_price=Decimal("0"),
            amount=base + (quote / current_price if current_price > 0 else Decimal("0")),
            fee_tier=pool_address,
            creation_timestamp=self._strategy.current_timestamp,
            trade_fee=trade_fee,
            position_address=pool_address,
            base_amount=base,
            quote_amount=quote,
            mid_price=current_price,
            position_rent=Decimal("0"),
        )
        # Recorded unconditionally, for the reason _create_position gives.
        self._store_lp_event_from_add(event)

        if self._close_after_open:
            self.logger().info(f"Add resolved after early_stop: removing this position's LP from {pool_address} now.")
            state.state = LPExecutorStates.CLOSING
            return True
        state.update_state(current_price, self._strategy.current_timestamp)
        return True

    def _amm_hold_unbooked_open(self):
        """End as an involuntary POSITION_HOLD for an add whose LP was never seen, with the marker.

        Not FAILED: the add confirmed, or may still, and a controller that re-creates an executor
        after FAILED would deposit a second time beside it.
        """
        kind = self._amm_unbooked_open["kind"]
        self._hold_reason = "open_unmeasured" if kind == "unmeasured" else "open_outcome_unknown"
        self.lp_position_state.active_open_order = None
        self._record_unclosed_position_marker()
        self.close_type = CloseType.POSITION_HOLD
        self.logger().error(
            f"The AMM add {self._amm_unbooked_open['signature']} on pool {self.config.pool_address} "
            f"{'confirmed' if kind == 'unmeasured' else 'may still confirm'}, but its LP was not seen while it was "
            f"followed (until {self._amm_unbooked_open['follow_until']:.0f}). Terminating as an involuntary "
            f"POSITION_HOLD (hold_reason={self._hold_reason}) - check the wallet's LP of this pool on-chain before "
            "trading it. The claim on the pool stays held until it is released by hand."
        )
        self.stop()

    async def _amm_update_position_info(self, connector):
        """Read the pool holding and value this position's LP in it."""
        try:
            holding = await self._read_amm_holding(connector)
        except Exception as e:
            self.logger().warning(f"Error fetching position info: {e}")
            return
        if holding is not None:
            self._current_price = Decimal(str(holding.price))
        lp = self._amm_position_lp(self._held_lp(holding))
        if lp <= 0:
            # The wallet holds none of this position's LP. The same two gates as a CLMM
            # position that reads as gone: seen on-chain once, and missing three reads running.
            self._position_not_found_count += 1
            if self._position_seen_onchain and self._position_not_found_count >= 3:
                self.logger().info(
                    f"The wallet no longer holds this position's LP of pool {self.config.pool_address} "
                    "(removed externally or by a prior attempt) - marking complete"
                )
                if self.close_type is None:
                    self.close_type = (CloseType.POSITION_HOLD if self.config.keep_position
                                       else CloseType.EARLY_STOP)
                self._handle_position_gone()
            else:
                self.logger().warning(
                    f"This position's LP of pool {self.config.pool_address} not found "
                    f"({self._position_not_found_count} consecutive reads, seen_onchain={self._position_seen_onchain})"
                )
            return
        self._position_not_found_count = 0
        self._position_seen_onchain = True
        self._amm_note_limit_reading(self._strategy.current_timestamp, connector)
        state = self.lp_position_state
        state.base_amount, state.quote_amount = self._share_of(holding, lp)
        state.base_fee = state.quote_fee = Decimal("0")

    async def _amm_close_position(self, connector):
        """Remove this position's LP from the pool: exactly it for an exact position, else by percentage."""
        if self._hold_reason in ("close_outcome_unknown", "pool_cannot_pay_in_full"):
            # A remove that may still confirm, or that the pool cannot pay, is never sent again
            # (see _amm_hold_unclosed and _amm_hold_cannot_pay).
            return
        if not self._amm_claim_pool():
            return
        async with self._amm_pool_lock():
            await self._amm_remove(connector)

    async def _amm_remove(self, connector):
        """Read the wallet's holding in the pool, send the remove, and book it once confirmed."""
        pool_address = self.config.pool_address
        state = self.lp_position_state
        try:
            before = await self._read_amm_holding(connector)
        except Exception as e:
            # The pre-flight is worked out from this reading, so nothing is sent without it.
            self._handle_close_failure(e)
            return
        held = self._held_lp(before)
        lp = self._amm_position_lp(held)
        if lp <= 0:
            # "Is the position still there" is "does the wallet still hold this position's LP".
            # One miss is never proof, as for a CLMM close.
            self._position_not_found_count += 1
            if self._position_not_found_count >= 3:
                self.logger().info(
                    f"This position's LP of pool {pool_address} definitively absent "
                    f"({self._position_not_found_count} consecutive misses) - already removed"
                )
                self._handle_position_gone()
                return
            self.logger().warning(
                f"Pre-flight miss {self._position_not_found_count}/3 for this position's LP of pool "
                f"{pool_address}; not removing this tick"
            )
            return
        self._position_not_found_count = 0
        self._position_seen_onchain = True
        state.base_amount, state.quote_amount = self._share_of(before, lp)
        self._current_price = Decimal(str(before.price))

        # An exact position returns exactly its LP. Otherwise the remove route takes a percentage
        # of the wallet's LP, rounded down: of the LP Gateway can spend, while this reading counts
        # confirmed LP, so it takes this position's LP and no more only while nothing else moves
        # the wallet's LP of the pool (see the section comment). What it leaves of this position's
        # LP stays in the wallet and is reported as lp_token_residue.
        exact = self._amm_lp_exact
        if exact and exact_json_number(lp) is None:
            exact = False
            self.logger().warning(
                f"This position's {lp} LP cannot be sent to Gateway as an exact number (more "
                f"than 2^53-1, or more than 15 significant digits): removing it by percentage instead."
            )
        percentage = (lp * Decimal("100") / held).quantize(AMM_PERCENT_QUANTUM, rounding=ROUND_DOWN)
        if not exact and percentage <= 0:
            self._handle_close_failure(ValueError(
                f"This position's {lp} LP is too small a share of the wallet's {held} LP of pool {pool_address} "
                "to remove by percentage"
            ))
            return

        order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
        state.active_close_order = TrackedOrder(order_id=order_id)
        proves_dropped = self._proves_dropped(connector)
        try:
            # One Gateway request per attempt, as for a CLMM close: this CLOSING loop owns retries.
            signature = await connector._amm_remove_liquidity(
                trade_type=TradeType.RANGE,
                order_id=order_id,
                trading_pair=self.config.trading_pair,
                percentage=float(percentage),
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                max_retries=0,
                pool_address=pool_address,
                slippage_pct=float(self._slippage_pct),
                lp_token_amount=lp if exact else None,
            )
        except Exception as e:
            self._handle_close_failure(e)
            return
        if not signature:
            reason = connector.order_failure_reason(order_id) or "AMM remove liquidity failed"
            if self._is_outcome_unknown(reason, proves_dropped):
                state.close_tx_hash = self._amm_order_signature(connector, order_id, reason)
                self._amm_hold_unclosed(reason)
            elif AMM_CANNOT_PAY_REFUSAL in reason:
                self._amm_hold_cannot_pay(reason)
            else:
                self._handle_close_failure(ValueError(reason))
            return

        # Confirmed: from here on nothing may send the remove again, since a second one would take
        # LP the wallet holds beyond this position. A reading that fails is booked as not measured.
        pending = self._amm_pending_amounts(connector, order_id, signature, "remove")
        try:
            after = await self._read_amm_holding(connector)
            after_read = True
        except Exception as e:
            self.logger().warning(
                f"Reading the wallet's holding in pool {pool_address} after the AMM remove failed: {e}")
            after, after_read = None, False
        removed = pending.get("lp")
        if removed is None and exact:
            # Gateway returns exactly the LP it is given, or refuses the remove.
            removed = lp
        if removed is None and after_read:
            removed = held - self._held_lp(after)
        if removed is None or removed <= 0:
            state.close_data_unavailable = True
            removed = held * percentage / Decimal("100")
            self.logger().error(
                f"AMM remove {signature} confirmed, but the LP it removed is not stated or measurable: booking the "
                f"{removed} LP it asked for. Reported amounts are not measured and PnL is incomplete."
            )
        # What the removed LP was worth at the reading before the remove, when Gateway's answer
        # does not state it: an estimate, since the remove pays at the pool's state when it
        # executes, which the slippage bound keeps close to that reading.
        share = self._share_of(before, removed)
        state.base_amount = pending.get("base", share[0])
        state.quote_amount = pending.get("quote", share[1])
        self._amm_close_estimated = "base" not in pending or "quote" not in pending
        state.base_fee = state.quote_fee = Decimal("0")
        state.lp_token_residue = lp - removed
        if state.lp_token_residue > 0:
            self.logger().warning(
                f"{state.lp_token_residue} of this position's LP stays in the wallet: the remove took less than "
                f"the position's LP. The claim on pool {pool_address} stays held until it is released by hand "
                "(LPExecutor.release_amm_pool_claim): that LP cannot be told apart from LP another AMM executor "
                "on this pool and wallet would acquire, and its close by percentage would take it."
            )
        elif state.lp_token_residue < 0:
            self.logger().error(
                f"The remove took {removed} LP of pool {pool_address}, more than this position's {lp}: "
                "LP the wallet held beyond this position left the pool as well."
            )
        close_tx_fee = pending.get("fee", Decimal("0"))
        state.tx_fee += close_tx_fee
        state.close_tx_hash = signature
        self.logger().info(
            f"AMM position removed from pool {pool_address}: {removed} LP for "
            f"base: {state.base_amount}, quote: {state.quote_amount}"
        )

        current_price = self._current_price if self._current_price else Decimal("0")
        native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
        trade_fee = TradeFeeBase.new_spot_fee(
            fee_schema=connector.trade_fee_schema(),
            trade_type=TradeType.RANGE,
            flat_fees=[TokenAmount(amount=close_tx_fee, token=native_currency)]
        )
        event = connector._trigger_remove_liquidity_event(
            order_id=order_id,
            exchange_order_id=signature,
            trading_pair=self.config.trading_pair,
            token_id="0",
            creation_timestamp=self._strategy.current_timestamp,
            trade_fee=trade_fee,
            position_address=pool_address,
            lower_price=Decimal("0"),
            upper_price=Decimal("0"),
            mid_price=current_price,
            base_amount=state.base_amount,
            quote_amount=state.quote_amount,
            base_fee=Decimal("0"),
            quote_fee=Decimal("0"),
            position_rent_refunded=Decimal("0"),
        )
        # The same routing as a CLMM close (see _close_position).
        if self.close_type == CloseType.POSITION_HOLD:
            self._store_lp_event_from_remove(event)
        state.active_close_order = None
        state.position_address = None
        if self.close_type == CloseType.EARLY_STOP and abs(self._calculate_net_base_difference()) > Decimal("0.000001"):
            self._reset_slippage()
            state.state = LPExecutorStates.SWAPPING
        else:
            state.state = LPExecutorStates.COMPLETE

    def _amm_hold_unclosed(self, reason: str):
        """End as an involuntary POSITION_HOLD after an AMM remove whose outcome is unknown.

        It may still confirm, so it is never sent again: once the first lands, a second remove
        would take LP the wallet holds beyond this position. The position is recorded as a hold
        with the unclosed position marker, as a CLMM close whose retries ran out is.
        """
        self.lp_position_state.active_close_order = None
        self._hold_reason = "close_outcome_unknown"
        self._record_unclosed_position_marker()
        self.close_type = CloseType.POSITION_HOLD
        self.logger().error(
            f"The AMM remove of this position's {self.lp_position_state.lp_token_amount} LP from pool "
            f"{self.config.pool_address} may still confirm ({reason}). It is not sent again. Terminating as an "
            "involuntary POSITION_HOLD (hold_reason=close_outcome_unknown) - check the chain, and remove the LP "
            "by hand if it did not."
        )
        self.stop()

    def _amm_hold_cannot_pay(self, reason: str):
        """End as an involuntary POSITION_HOLD after Gateway refused a remove the pool cannot pay in full.

        The refusal is definite, and the same remove would be refused again, so it is not sent
        again. The most LP Gateway said can be removed now is reported for whoever removes it.
        """
        match = AMM_MOST_REMOVABLE.search(reason)
        self._amm_most_removable = Decimal(match.group(1)) if match else Decimal("0")
        self.lp_position_state.active_close_order = None
        self._hold_reason = "pool_cannot_pay_in_full"
        self._record_unclosed_position_marker()
        self.close_type = CloseType.POSITION_HOLD
        self.logger().error(
            f"Pool {self.config.pool_address} cannot pay for this position's {self.lp_position_state.lp_token_amount} "
            f"LP in full; at most {self._amm_most_removable} LP can be removed now ({reason}). It is not sent again. "
            "Terminating as an involuntary POSITION_HOLD (hold_reason=pool_cannot_pay_in_full)."
        )
        self.stop()

    def _widen_slippage(self, error: Exception) -> bool:
        """Widen the tolerance for the next attempt, if this failure was slippage.

        Returns True when the next attempt will carry a wider tolerance than this one
        did, which is the only case where retrying is doing something different.

        Only a slippage failure widens. A wrong tick, an insufficient balance or a
        transport error says nothing about the tolerance, and loosening one that was
        never too tight spends the operator's money to fix the wrong problem.
        """
        if not is_slippage_failure(error):
            return False

        widened = next_slippage_pct(
            self._slippage_pct, self.config.slippage_multiplier, self.config.max_slippage_pct
        )
        if widened is None:
            self.logger().warning(
                f"Slippage failure at {self._slippage_pct}%, which is max_slippage_pct "
                f"({self.config.max_slippage_pct}%). Not widening further: the market has moved "
                "past what this executor was configured to accept."
            )
            return False

        self.logger().info(
            f"Slippage failure at {self._slippage_pct}%; retrying at {widened}% "
            f"(x{self.config.slippage_multiplier}, ceiling {self.config.max_slippage_pct}%)."
        )
        self._slippage_pct = widened
        return True

    def _reset_slippage(self):
        """Start the next phase at the configured tolerance.

        An open and the close that follows it are separated by the position's whole
        lifetime. Carrying a widened tolerance across that boundary would silently
        accept 5% on an exit because an entry once needed it.
        """
        if self._slippage_pct != self.config.slippage_pct:
            self.logger().info(f"Resetting slippage to {self.config.slippage_pct}% for the next phase.")
        self._slippage_pct = self.config.slippage_pct

    def _handle_create_failure(self, error: Exception):
        """Handle position creation failure.

        A position_address means add_liquidity already landed on-chain and only
        the bookkeeping after it threw, so the funds are real. FAILED would stop
        the executor without ever calling _close_position, stranding them: the
        add-liquidity event that records the position for lphistory is emitted
        after the code most likely to throw, so the position would survive only
        in this log line. Close it instead, and name the address either way.

        Retrying the open is not an option here -- the add succeeded, so a retry
        would deposit a second position on top of the first.
        """
        self.lp_position_state.active_open_order = None

        # A slippage failure is the one open failure that can be retried safely. Gateway
        # raises it either from the pre-flight simulation, where nothing was sent, or
        # from a transaction that landed and reverted — and a reverted open deposits
        # nothing. Both mean there is no position to deposit a second one on top of,
        # which is the risk the terminal rule below exists to prevent. Counted against
        # the retry budget so evaluate_max_retries still ends it.
        if not self.lp_position_state.position_address and self._widen_slippage(error):
            self._current_retries += 1
            self.logger().warning(
                f"Position creation failed on slippage ({self._current_retries}/{self._max_retries} "
                f"retries used): {error}. Retrying at {self._slippage_pct}%."
            )
            self.lp_position_state.state = LPExecutorStates.OPENING
            return

        if self.lp_position_state.position_address:
            self.logger().error(
                f"Position creation failed after the position was opened at "
                f"{self.lp_position_state.position_address} ({self.config.trading_pair}): "
                f"{error}. Closing it to recover the funds."
            )
            self.close_type = CloseType.FAILED
            self.lp_position_state.state = LPExecutorStates.CLOSING
            return

        self.logger().error(f"Position creation failed: {error}")
        self.lp_position_state.state = LPExecutorStates.FAILED

    async def _close_position(self):
        """
        Close position by directly awaiting the gateway operation.
        No events needed - result is available immediately after await.
        """
        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            self.logger().error(f"Connector {self.config.connector_name} not found")
            self._handle_close_failure(ValueError(f"Connector {self.config.connector_name} not found"))
            return

        # Respect the backoff set by a failed attempt: without it, a burst of instant
        # transport failures (e.g. Gateway restarting) would burn the whole retry
        # budget in as many seconds as there are retries.
        if self._strategy.current_timestamp < self._close_backoff_until:
            return

        if self._is_amm:
            await self._amm_close_position(connector)
            return

        # Verify position still exists before trying to close (handles timeout-but-succeeded
        # case). Fresh read; but ONE miss is never proof (a lagging RPC node's 404
        # would otherwise declare a freshly funded position closed with a
        # success-shaped event). Same 3-consecutive-miss gate as the monitoring
        # path (retry-architecture §3.2).
        try:
            position_info = await connector.get_position_info_fresh(
                trading_pair=self.config.trading_pair,
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                position_address=self.lp_position_state.position_address
            )
        except Exception as e:
            # Gateway returns HttpError with message patterns for a definitively
            # missing position; treat those exactly like a None read (a miss).
            error_msg = str(e).lower()
            if "position closed" in error_msg or "position not found" in error_msg:
                position_info = None
            else:
                # Transient error - proceed with the close attempt
                position_info = False

        if position_info is None:
            self._position_not_found_count += 1
            if self._position_not_found_count >= 3:
                self.logger().info(
                    f"Position {self.lp_position_state.position_address} definitively absent "
                    f"({self._position_not_found_count} consecutive misses) - already closed"
                )
                self._handle_position_gone()
                return
            self.logger().warning(
                f"Pre-flight miss {self._position_not_found_count}/3 for "
                f"{self.lp_position_state.position_address}; not closing this tick"
            )
            # Don't submit a close against a position we cannot currently see —
            # re-enter next tick; misses do not burn the close retry budget.
            return
        if position_info is not False:
            self._position_not_found_count = 0
            self._position_seen_onchain = True

        # Generate order_id for tracking
        order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
        self.lp_position_state.active_close_order = TrackedOrder(order_id=order_id)

        try:
            # One gateway request per attempt (max_retries=0): this CLOSING loop is the
            # only owner of close retries. Each re-entry rebuilds from fresh on-chain
            # state, and the pre-flight above reconciles an attempt that landed after a
            # timeout — a connector-level retry would re-submit without that check.
            signature = await connector._clmm_close_position(
                trade_type=TradeType.RANGE,
                order_id=order_id,
                trading_pair=self.config.trading_pair,
                position_address=self.lp_position_state.position_address,
                max_retries=0,
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
                slippage_pct=float(self._slippage_pct),
            )
            # Note: on failure the connector re-raises and _handle_close_failure counts it
            # so it will be caught by the except block below

            self.logger().info(f"Position close confirmed, signature={signature}")

            # Success - extract close data from connector's metadata
            metadata = connector._lp_orders_metadata.get(order_id, {})
            if metadata.get("data_unavailable"):
                # The close CONFIRMED on-chain but its response data is unrecoverable, so
                # there are no collected-fee / removed-amount / rent-refund figures to
                # book. Keep the last values read from chain while the position was open
                # rather than overwriting them with zeros, and say plainly that this
                # executor's close accounting is incomplete.
                self.lp_position_state.close_data_unavailable = True
                self.logger().error(
                    f"Close transaction {signature} confirmed but Gateway returned no close data: "
                    "collected fees, removed amounts and the rent refund are UNKNOWN for this "
                    "close. Reported amounts are the last on-chain reading and PnL is incomplete."
                )
            else:
                self.lp_position_state.position_rent_refunded = metadata.get("position_rent_refunded", Decimal("0"))
                self.lp_position_state.base_amount = metadata.get("base_amount", Decimal("0"))
                self.lp_position_state.quote_amount = metadata.get("quote_amount", Decimal("0"))
                self.lp_position_state.base_fee = metadata.get("base_fee", Decimal("0"))
                self.lp_position_state.quote_fee = metadata.get("quote_fee", Decimal("0"))
            # Add close tx_fee to cumulative total (open tx_fee + close tx_fee)
            close_tx_fee = metadata.get("tx_fee", Decimal("0"))
            self.lp_position_state.tx_fee += close_tx_fee
            self.lp_position_state.close_tx_hash = signature

            # Clean up connector metadata
            if order_id in connector._lp_orders_metadata:
                del connector._lp_orders_metadata[order_id]

            self.logger().info(
                f"Position closed: {self.lp_position_state.position_address}, "
                f"rent refunded: {self.lp_position_state.position_rent_refunded} SOL, "
                f"base: {self.lp_position_state.base_amount}, quote: {self.lp_position_state.quote_amount}, "
                f"fees: {self.lp_position_state.base_fee} base / {self.lp_position_state.quote_fee} quote"
            )

            # Trigger event for database recording (lphistory command)
            # Note: mid_price is the current MARKET price, not the position range midpoint
            current_price = self._current_price if self._current_price else Decimal("0")
            # Create trade_fee with close tx_fee in native currency for proper tracking
            native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
            trade_fee = TradeFeeBase.new_spot_fee(
                fee_schema=connector.trade_fee_schema(),
                trade_type=TradeType.RANGE,
                flat_fees=[TokenAmount(amount=close_tx_fee, token=native_currency)]
            )
            event = connector._trigger_remove_liquidity_event(
                order_id=order_id,
                exchange_order_id=signature,
                trading_pair=self.config.trading_pair,
                token_id="0",
                creation_timestamp=self._strategy.current_timestamp,
                trade_fee=trade_fee,
                position_address=self.lp_position_state.position_address,
                lower_price=self.lp_position_state.lower_price,
                upper_price=self.lp_position_state.upper_price,
                mid_price=current_price,
                base_amount=self.lp_position_state.base_amount,
                quote_amount=self.lp_position_state.quote_amount,
                base_fee=self.lp_position_state.base_fee,
                quote_fee=self.lp_position_state.quote_fee,
                position_rent_refunded=self.lp_position_state.position_rent_refunded,
            )

            # Store REMOVE event for position tracking (like spot grid stores orders).
            # Keyed off close_type alone: that is the runtime decision made by
            # early_stop(keep_position=...), which overrides config.keep_position.
            if self.close_type == CloseType.POSITION_HOLD:
                self._store_lp_event_from_remove(event)

            self.lp_position_state.active_close_order = None
            self.lp_position_state.position_address = None

            # Not holding the net means swapping back to the original position.
            # Same runtime decision as the REMOVE gate above, and stated
            # positively so an unexpected close_type skips the on-chain swap
            # rather than firing one nobody asked for. Both transitions into
            # CLOSING set close_type to POSITION_HOLD or EARLY_STOP first.
            if self.close_type == CloseType.EARLY_STOP:
                # Calculate net base change using helper (same calculation as position_hold)
                base_diff = self._calculate_net_base_difference()
                if abs(base_diff) > Decimal("0.000001"):  # Non-trivial difference
                    self.logger().info(
                        f"Close-out swap needed: base_diff={base_diff:.6f} "
                        f"(received={self.lp_position_state.base_amount + self.lp_position_state.base_fee:.6f}, "
                        f"initial={self.lp_position_state.initial_base_amount:.6f})"
                    )
                    # A third phase, and a third chance to start tight.
                    self._reset_slippage()
                    self.lp_position_state.state = LPExecutorStates.SWAPPING
                else:
                    self.logger().info("No close-out swap needed (base amounts match)")
                    self.lp_position_state.state = LPExecutorStates.COMPLETE
            else:
                self.lp_position_state.state = LPExecutorStates.COMPLETE

        except Exception as e:
            self._handle_close_failure(e)

    def _handle_close_failure(self, error: Exception):
        """Handle position close failure.

        The connector retries transport and stale-state errors within a single
        attempt; this handles an attempt that failed after those retries. The
        position is still open on-chain, so going terminal here would strand it
        (see gateway#678). Count the attempt, arm the backoff, and stay in
        CLOSING — control_task re-enters _close_position on the next tick, and
        each re-entry re-POSTs to Gateway, which rebuilds the transaction from
        fresh on-chain state (the pre-flight reconciles a close that already
        landed). Termination on exhaustion is decided by evaluate_max_retries,
        the family hook the base control_loop calls right after this
        control_task returns.
        """
        self._current_retries += 1
        self.lp_position_state.active_close_order = None
        # Widen for the next attempt when the close failed on slippage. Unlike an open,
        # an exit at the ceiling keeps retrying: the position is real and has to come
        # out, and the alternative is stranding it.
        self._widen_slippage(error)

        if self._current_retries > self._max_retries:
            # evaluate_max_retries terminates after this control_task returns.
            self.logger().warning(f"Position close attempt failed: {error}. Retry budget exhausted.")
            return

        # Exponential backoff (capped at 30s) so instant failures — e.g. connection
        # errors while Gateway restarts — spread the retry budget over minutes
        # instead of burning it in seconds
        backoff = min(30.0, 2.0 ** self._current_retries)
        self._close_backoff_until = self._strategy.current_timestamp + backoff
        self.logger().warning(
            f"Position close attempt failed ({self._current_retries}/{self._max_retries} retries used): "
            f"{error}. Rebuilding with fresh state in {backoff:.0f}s."
        )
        # State stays CLOSING - control_task re-enters _close_position after backoff.

    def evaluate_max_retries(self):
        """Terminate when the close-retry budget is exhausted.

        Base-class semantics (``> max_retries``, i.e. ``max_retries + 1`` total
        attempts — the family convention) decide *when*; this override decides
        *how*. A position still on-chain is residual exposure, and the family
        contract (see ``force_stop_with_position_hold``) ends residual exposure
        as POSITION_HOLD, reserving FAILED for executors with nothing left
        behind. PositionHold cannot book a live LP position as spot amounts, so
        the hold carries a zero-amount marker order with the position's identity
        instead — visible and durable in the hold store on both the bot and the
        hummingbot-api topology, with accounting untouched.
        """
        if self._current_retries <= self._max_retries:
            return
        self._max_retries_reached = True
        position_address = self.lp_position_state.position_address
        if position_address:
            self._hold_reason = "close_retries_exhausted"
            self._record_unclosed_position_marker()
            self.close_type = CloseType.POSITION_HOLD
            self.logger().error(
                f"Position close failed after {self._current_retries} attempts: position "
                f"{position_address} ({self.config.trading_pair}) is still open on-chain. "
                "Terminating as an involuntary POSITION_HOLD (hold_reason=close_retries_exhausted) - "
                "close it via the gateway and resolve the orphan record."
            )
        else:
            self.close_type = CloseType.FAILED
            self.logger().error(
                f"Retry budget exhausted after {self._current_retries} attempts with no position "
                "on-chain - terminating as FAILED."
            )
        self.stop()

    def _record_unclosed_position_marker(self):
        """Append a zero-amount marker order carrying the unclosed position's identity.

        A live on-chain position cannot be booked as spot amounts — the tokens
        are in the pool, not the wallet — but an empty held_position_orders would
        make the POSITION_HOLD invisible to the hold store. Zero amounts keep
        accounting untouched (PositionHold._process_order skips them) while the
        marker carries the address every recovery path needs.
        """
        self._held_position_orders.append({
            "client_order_id": f"{self.config.id}-unclosed-lp",
            "trading_pair": self.config.trading_pair,
            "trade_type": "BUY",  # dummy; zero amounts never reach accounting
            "price": float(self._current_price) if self._current_price else 0.0,
            "executed_amount_base": 0.0,
            "executed_amount_quote": 0.0,
            "cumulative_fee_paid_quote": getattr(self, '_add_tx_fee_quote', 0.0),
            "lp_source": True,
            "lp_unclosed_position": True,
            "position_address": self.lp_position_state.position_address,
        })
        if self._is_amm:
            # The pool address alone does not say how much of the wallet's LP is this position's:
            # the marker names the pool, the LP (or "unmeasured"), and the transactions.
            self._held_position_orders[-1].update(self._amm_position_record(),
                                                  position_address=self.config.pool_address,
                                                  hold_reason=self._hold_reason)

    async def _execute_closeout_swap(self):
        """
        Execute close-out swap to return to original position when keep_position=False.
        Similar to grid executor's place_close_order_and_cancel_open_orders().

        This sells excess base tokens or buys back base tokens to match the initial position.
        """
        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            self.logger().error(f"Connector {self.config.connector_name} not found")
            self._handle_swap_failure(ValueError(f"Connector {self.config.connector_name} not found"))
            return

        if not await self._resolve_swap_provider():
            self.logger().error("No swap_provider configured for close-out swap")
            self._handle_swap_failure(ValueError("No swap_provider configured"))
            return

        # Check if we already have an active swap order
        if self.lp_position_state.active_swap_order is not None:
            # Check swap order status
            order = connector.get_order(self.lp_position_state.active_swap_order.order_id)
            if order is None:
                # Order not found - might have completed or failed
                self._swap_not_found_count += 1
                if self._swap_not_found_count >= 3:
                    self.logger().warning(
                        f"Swap order {self.lp_position_state.active_swap_order.order_id} not found after "
                        f"{self._swap_not_found_count} checks. Assuming completed."
                    )
                    self.lp_position_state.active_swap_order = None
                    self.lp_position_state.state = LPExecutorStates.COMPLETE
                return

            from hummingbot.core.data_type.in_flight_order import OrderState
            if order.current_state == OrderState.FILLED:
                self.logger().info(f"Close-out swap completed: {order.client_order_id}")
                self.lp_position_state.active_swap_order = None
                self.lp_position_state.state = LPExecutorStates.COMPLETE
            elif order.current_state == OrderState.FAILED:
                # The order tracker records FAILED and no reason, so ask the connector
                # what Gateway said. Without it the slippage ramp cannot act on this leg:
                # every close-out swap failure would look the same, and widening on all
                # of them would loosen a tolerance that was never the problem.
                reason = connector.order_failure_reason(order.client_order_id) or "Swap order failed"
                self.logger().error(f"Close-out swap failed: {order.client_order_id}: {reason}")
                self._handle_swap_failure(ValueError(reason))
            elif order.current_state == OrderState.CANCELED:
                self.logger().warning(f"Close-out swap cancelled: {order.client_order_id}")
                self._handle_swap_failure(ValueError("Swap order cancelled"))
            # Otherwise still pending - wait for next tick
            return

        # Calculate swap amount and direction using helper (consistent with _close_position)
        base_diff = self._calculate_net_base_difference()

        if abs(base_diff) < Decimal("0.000001"):
            # No swap needed
            self.lp_position_state.state = LPExecutorStates.COMPLETE
            return

        # Determine trade direction
        # If base_diff > 0: We received more base than deposited → SELL excess base
        # If base_diff < 0: We received less base than deposited → BUY base to restore
        is_buy = base_diff < 0
        amount = abs(base_diff)
        side = TradeType.BUY if is_buy else TradeType.SELL

        self.logger().info(
            f"Executing close-out swap: {side.name} {amount:.6f} base (diff={base_diff:.6f})"
        )

        try:
            # The executor's tolerance rather than the connector's configured one, so a
            # close-out swap starts as tight as everything else this executor does and
            # widens the same way. Hardcoding 1.0 here used to override the operator's
            # Gateway config; omitting it entirely left this one leg unable to widen.
            order_id = connector.place_order(
                is_buy=is_buy,
                trading_pair=self.config.trading_pair,
                amount=amount,
                price=Decimal("0"),  # Market order
                dex_name=self.config.swap_provider,
                slippage_pct=float(self._slippage_pct),
                max_retries=self._max_retries,
            )
            self.lp_position_state.active_swap_order = TrackedOrder(order_id=order_id)
            self._swap_not_found_count = 0
            self.logger().info(f"Close-out swap order placed: {order_id}")

        except Exception as e:
            self.logger().error(f"Failed to place close-out swap: {e}")
            self._handle_swap_failure(e)

    def _handle_position_gone(self):
        """The position is definitively absent on-chain: a prior close attempt
        landed, or it was closed externally. Emit the synthetic close event and
        route exactly like a successful close — EARLY_STOP (keep_position=False)
        still owes the close-out swap, since the withdrawn tokens are in the
        wallet either way. Going straight to COMPLETE here used to leave that
        net base change unswapped and unrecorded.
        """
        self._emit_already_closed_event()
        self.lp_position_state.active_close_order = None
        self.lp_position_state.position_address = None

        if self.close_type == CloseType.EARLY_STOP:
            base_diff = self._calculate_net_base_difference()
            if abs(base_diff) > Decimal("0.000001"):
                self.logger().info(
                    f"Close-out swap needed after already-closed reconcile: base_diff={base_diff:.6f}"
                )
                self.lp_position_state.state = LPExecutorStates.SWAPPING
                return
        self.lp_position_state.state = LPExecutorStates.COMPLETE

    # Close-out swap attempts before the executor stops fighting the market and
    # holds the withdrawn tokens instead (each attempt re-quotes at market).
    MAX_CLOSEOUT_SWAP_ATTEMPTS = 3

    def _handle_swap_failure(self, error: Exception):
        """Handle close-out swap failure.

        Bounded retries with a fresh quote each attempt; on exhaustion the
        executor terminates POSITION_HOLD carrying the withdrawn tokens. The old
        first-failure jump to FAILED reported "no residual exposure" while the
        REMOVE's proceeds sat in the wallet, unrecorded.
        """
        self.lp_position_state.active_swap_order = None
        self._swap_failure_count += 1
        # A swap whose submission may have reached the chain is never placed again:
        # spending the close-out swap's attempts holds the tokens instead.
        if "[code: SUBMIT_OUTCOME_UNKNOWN]" in str(error):
            self._swap_failure_count = self.MAX_CLOSEOUT_SWAP_ATTEMPTS
            self._hold_reason = "closeout_swap_outcome_unknown"
        # Widen for the next quote when the market moved further than the tolerance.
        # The swap has its own small budget, so the ramp has to fit inside it — with the
        # default multiplier of 5 it reaches the ceiling in three attempts, which is the
        # budget exactly.
        self._widen_slippage(error)

        if self._swap_failure_count < self.MAX_CLOSEOUT_SWAP_ATTEMPTS:
            self.logger().error(
                f"Close-out swap failed (attempt {self._swap_failure_count}/"
                f"{self.MAX_CLOSEOUT_SWAP_ATTEMPTS}): {error}. Retrying with a fresh quote."
            )
            return  # stay in SWAPPING; next tick re-places at market

        if self._hold_reason == "closeout_swap_outcome_unknown":
            self.logger().error(
                f"Close-out swap may have been submitted ({error}); holding the withdrawn tokens. "
                "Check the chain before trading them."
            )
        else:
            self.logger().error(
                f"Close-out swap failed {self._swap_failure_count} times: {error}. "
                "Holding the withdrawn tokens instead of reporting FAILED with hidden exposure."
            )
        if self._hold_reason is None:
            self._hold_reason = "closeout_swap_failed"
        self.close_type = CloseType.POSITION_HOLD
        connector = self.connectors.get(self.config.connector_name)
        current_price = self._current_price if self._current_price else Decimal("0")
        if connector is not None:
            order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
            self._store_net_trade_from_withdrawal(
                total_base_returned=self.lp_position_state.base_amount + self.lp_position_state.base_fee,
                total_quote_returned=self.lp_position_state.quote_amount + self.lp_position_state.quote_fee,
                mid_price=current_price,
                remove_tx_fee_quote=0.0,
                order_id=order_id,
                exchange_order_id="closeout-swap-failed",
                trading_pair=self.config.trading_pair,
            )
        self.lp_position_state.state = LPExecutorStates.COMPLETE

    def _emit_already_closed_event(self):
        """
        Emit a synthetic RangePositionLiquidityRemovedEvent for positions that were
        closed on-chain but we didn't receive the confirmation (e.g., timeout-but-succeeded).
        Uses last known position data. This ensures the database is updated.
        """
        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            return

        # Generate a synthetic order_id for this event
        order_id = connector.create_market_order_id(TradeType.RANGE, self.config.trading_pair)
        # Note: mid_price is the current MARKET price, not the position range midpoint
        current_price = self._current_price if self._current_price else Decimal("0")

        self.logger().info(
            f"Emitting synthetic close event for already-closed position: "
            f"{self.lp_position_state.position_address}, "
            f"base: {self.lp_position_state.base_amount}, quote: {self.lp_position_state.quote_amount}, "
            f"fees: {self.lp_position_state.base_fee} base / {self.lp_position_state.quote_fee} quote"
        )

        # For synthetic events, we don't have the actual close tx_fee, so use 0
        native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
        trade_fee = TradeFeeBase.new_spot_fee(
            fee_schema=connector.trade_fee_schema(),
            trade_type=TradeType.RANGE,
            flat_fees=[TokenAmount(amount=Decimal("0"), token=native_currency)]
        )
        connector._trigger_remove_liquidity_event(
            order_id=order_id,
            exchange_order_id="already-closed",
            trading_pair=self.config.trading_pair,
            token_id="0",
            creation_timestamp=self._strategy.current_timestamp,
            trade_fee=trade_fee,
            position_address=self.lp_position_state.position_address,
            lower_price=self.lp_position_state.lower_price,
            upper_price=self.lp_position_state.upper_price,
            mid_price=current_price,
            base_amount=self.lp_position_state.base_amount,
            quote_amount=self.lp_position_state.quote_amount,
            base_fee=self.lp_position_state.base_fee,
            quote_fee=self.lp_position_state.quote_fee,
            position_rent_refunded=self.lp_position_state.position_rent,
        )

        # Record the hold from the same last-known amounts. Without this the
        # executor completes as POSITION_HOLD reporting no orders at all, and
        # consumers fall back to filled_amount_base -- which for an LP executor
        # is the base sitting in the pool, not base the executor acquired.
        if self.close_type == CloseType.POSITION_HOLD:
            self._store_net_trade_from_withdrawal(
                total_base_returned=self.lp_position_state.base_amount + self.lp_position_state.base_fee,
                total_quote_returned=self.lp_position_state.quote_amount + self.lp_position_state.quote_fee,
                mid_price=current_price,
                remove_tx_fee_quote=0.0,
                order_id=order_id,
                exchange_order_id="already-closed",
                trading_pair=self.config.trading_pair,
            )

    def _store_lp_event_from_add(self, event: RangePositionLiquidityAddedEvent):
        """Store ADD event data for later net trade calculation at REMOVE.

        Instead of recording ADD as a separate trade, we store the deposit amounts
        and calculate the net trade when the position is closed (REMOVE).
        """
        # Store ADD data for net trade calculation at REMOVE
        self._add_base_amount = event.base_amount
        self._add_quote_amount = event.quote_amount
        self._add_order_id = event.exchange_order_id

        # Store actual TX fee for ADD (from trade_fee, NOT position_rent which is refundable)
        native_to_quote = self._get_native_to_quote_rate()
        tx_fee = sum(fee.amount for fee in event.trade_fee.flat_fees) if event.trade_fee.flat_fees else Decimal("0")
        self._add_tx_fee_quote = float(tx_fee * native_to_quote)

    def _store_lp_event_from_remove(self, event: RangePositionLiquidityRemovedEvent):
        """Calculate net trade from ADD/REMOVE and store single order."""
        # TX fee for REMOVE
        native_to_quote = self._get_native_to_quote_rate()
        tx_fee = sum(fee.amount for fee in event.trade_fee.flat_fees) if event.trade_fee.flat_fees else Decimal("0")
        remove_tx_fee_quote = float(tx_fee * native_to_quote)
        self._store_net_trade_from_withdrawal(
            total_base_returned=event.base_amount + event.base_fee,
            total_quote_returned=event.quote_amount + event.quote_fee,
            mid_price=event.mid_price,
            remove_tx_fee_quote=remove_tx_fee_quote,
            order_id=event.order_id,
            exchange_order_id=event.exchange_order_id,
            trading_pair=event.trading_pair,
        )

    def _store_net_trade_from_withdrawal(self, total_base_returned: Decimal, total_quote_returned: Decimal,
                                         mid_price: Decimal, remove_tx_fee_quote: float,
                                         order_id: str, exchange_order_id: str, trading_pair: str):
        """Store the net trade of a liquidity withdrawal against the recorded ADD.

        The LP position net change determines if this was effectively a BUY or SELL:
        - net_base > 0, net_quote < 0: BUY (gained base, spent quote)
        - net_base < 0, net_quote > 0: SELL (spent base, gained quote)
        - net_base ≈ 0, net_quote ≈ 0: No trade (same assets in/out)
        """
        # Get ADD data (stored when position was opened)
        add_base = getattr(self, '_add_base_amount', Decimal("0"))
        add_quote = getattr(self, '_add_quote_amount', Decimal("0"))
        add_tx_fee = getattr(self, '_add_tx_fee_quote', 0.0)

        # Calculate net change (REMOVE - ADD)
        # Include LP fees earned in the returned amounts
        net_base = total_base_returned - add_base
        net_quote = total_quote_returned - add_quote

        # Total TX fees for this LP position
        total_tx_fee_quote = add_tx_fee + remove_tx_fee_quote

        # Determine trade type based on net change
        threshold = Decimal("0.0001")

        if abs(net_base) < threshold and abs(net_quote) < threshold:
            # No significant conversion - record a zero-amount order carrying only
            # the fees. Appended even when there are no fees: an empty
            # held_position_orders is indistinguishable from "this executor does
            # not report orders", and consumers then fall back to
            # filled_amount_base, which for an LP executor is the pool balance
            # rather than acquired base. A zero-amount order says "nothing" plainly.
            self._held_position_orders.append({
                "client_order_id": exchange_order_id,
                "trade_type": "BUY",  # Dummy, won't affect P&L with 0 amounts
                "price": float(mid_price),
                "executed_amount_base": 0.0,
                "executed_amount_quote": 0.0,
                "cumulative_fee_paid_quote": total_tx_fee_quote,
                "lp_source": True,
                "lp_net_trade": True,
            })
            return

        if net_base > threshold and net_quote < -threshold:
            # Gained base, lost quote = BUY
            trade_type = "BUY"
            amount_base = float(net_base)
            amount_quote = float(abs(net_quote))
            price = amount_quote / amount_base if amount_base > 0 else float(mid_price)
        elif net_base < -threshold and net_quote > threshold:
            # Lost base, gained quote = SELL
            trade_type = "SELL"
            amount_base = float(abs(net_base))
            amount_quote = float(net_quote)
            price = amount_quote / amount_base if amount_base > 0 else float(mid_price)
        elif abs(net_base) > threshold:
            # Base changed but quote didn't significantly - use mid_price
            # This happens when LP fees are collected in the same asset
            if net_base > 0:
                trade_type = "BUY"
                amount_base = float(net_base)
            else:
                trade_type = "SELL"
                amount_base = float(abs(net_base))
            amount_quote = amount_base * float(mid_price)
            price = float(mid_price)
        else:
            # Only quote changed - record as 0-base trade (fees only)
            self._held_position_orders.append({
                "client_order_id": exchange_order_id,
                "trade_type": "BUY",
                "price": float(mid_price),
                "executed_amount_base": 0.0,
                "executed_amount_quote": float(abs(net_quote)),
                "cumulative_fee_paid_quote": total_tx_fee_quote,
                "lp_source": True,
                "lp_net_trade": True,
            })
            return

        # Create single order representing the net trade
        self._held_position_orders.append({
            "client_order_id": exchange_order_id,
            "order_id": order_id,
            "exchange_order_id": exchange_order_id,
            "trading_pair": trading_pair,
            "trade_type": trade_type,
            "price": price,
            "amount": amount_base,
            "executed_amount_base": amount_base,
            "executed_amount_quote": amount_quote,
            "cumulative_fee_paid_quote": total_tx_fee_quote,
            "lp_source": True,
            "lp_net_trade": True,
        })

    def _collect_held_position_orders(self) -> List[Dict]:
        """Snapshot residual exposure for a forced stop at the shutdown deadline.

        Mid-SWAPPING the liquidity is already out of the pool but the close-out swap
        has not confirmed, so the withdrawn tokens sit in the wallet as spot. Record
        the same net trade the keep_position path would have stored at REMOVE, so the
        exposure becomes a tracked hold instead of invisible dust. (A swap submitted
        in the same tick can still land after the stop; next-start reconciliation
        absorbs that one fill.)

        A position still on-chain (address set, REMOVE not confirmed) cannot be
        represented as spot orders — log it loudly so it can be recovered.
        """
        if not self._held_position_orders and self.lp_position_state.state == LPExecutorStates.SWAPPING:
            mid_price = self._current_price if self._current_price else Decimal("0")
            self._store_net_trade_from_withdrawal(
                total_base_returned=self.lp_position_state.base_amount + self.lp_position_state.base_fee,
                total_quote_returned=self.lp_position_state.quote_amount + self.lp_position_state.quote_fee,
                mid_price=mid_price,
                remove_tx_fee_quote=0.0,
                order_id=f"{self.config.id}-forced-hold",
                exchange_order_id=f"{self.config.id}-forced-hold",
                trading_pair=self.config.trading_pair,
            )
        if self._is_amm and self._amm_lp_at_stake() and not self._amm_marker_written():
            # An add or remove still being awaited, or LP still in the pool: an AMM share has no
            # on-chain object of its own to find later, so record what this executor holds, with
            # its transactions, before the executor is stored.
            state = self.lp_position_state
            connector = self.connectors.get(self.config.connector_name)
            in_flight = state.active_open_order is not None or state.active_close_order is not None
            if connector is not None and state.active_open_order is not None:
                state.open_tx_hash = self._amm_order_signature(connector, state.active_open_order.order_id)
            if connector is not None and state.active_close_order is not None:
                state.close_tx_hash = self._amm_order_signature(connector, state.active_close_order.order_id)
            if self._hold_reason is None:
                self._hold_reason = "forced_stop_in_flight" if in_flight else "forced_stop_lp_in_pool"
            self._record_unclosed_position_marker()
        if not self._held_position_orders and self.lp_position_state.position_address:
            self.logger().error(
                f"Forced stop with LP position still on-chain at {self.lp_position_state.position_address} "
                f"({self.config.trading_pair}). An on-chain position cannot be held as spot orders; "
                f"recover it on the next start or manually."
            )
        return list(self._held_position_orders)

    def early_stop(self, keep_position: bool = True):
        """Stop executor - transitions to CLOSING state.

        Args:
            keep_position: If True (default), after closing the LP position on-chain,
                          the net token change will be tracked as a spot position.
                          This matches spot grid executor behavior where keep_position
                          means "track the net position" not "keep orders open".
        """
        self._status = RunnableStatus.SHUTTING_DOWN
        # Use parameter directly like grid/position executors (controller decides keep_position)
        self.close_type = CloseType.POSITION_HOLD if keep_position else CloseType.EARLY_STOP

        # ALWAYS close the LP position on-chain
        # If keep_position=True, we'll capture the difference after closing
        if self.lp_position_state.state in [LPExecutorStates.IN_RANGE, LPExecutorStates.OUT_OF_RANGE]:
            self.lp_position_state.state = LPExecutorStates.CLOSING
        elif self.lp_position_state.state == LPExecutorStates.OPENING:
            # The create is in flight and its transaction may LAND — killing the
            # state here used to strand a live, funded position behind a FAILED
            # terminal ("nothing on-chain"). Let the create resolve; its success
            # tail routes straight to CLOSING, and its failure path ends normally.
            self._close_after_open = True
            self.logger().info(
                "early_stop during OPENING: waiting for the in-flight create to "
                "resolve, then closing."
            )
        elif self.lp_position_state.state == LPExecutorStates.NOT_ACTIVE:
            # No position was created, just complete
            self.lp_position_state.state = LPExecutorStates.COMPLETE

    def _calculate_net_base_difference(self) -> Decimal:
        """
        Calculate net base token difference from LP position lifecycle.

        This is the difference between what we received when closing the position
        (including fees) and what we initially deposited.

        Deliberately the net, NOT the entire withdrawn balance. The executor owns
        the LP round trip, not the base it was handed: the deposit was funded by
        whoever opened the slot (typically an entry order_executor that recorded a
        PositionHold), so unwinding to the net leaves that hold accurate and the
        executor position-neutral. Selling the full balance instead disposes of
        base the ledger still counts as held -- and since the keep_position=False
        path does not call _store_lp_event_from_remove, that sale is recorded
        nowhere, stranding a phantom hold. Ending flat is the entry executor's
        job, via its own keep_position=False.

        Returns:
            Positive: We have more base than we started with (need to SELL)
            Negative: We have less base than we started with (need to BUY)
            Zero: Position is balanced

        Used by:
            - _close_position: To determine if close-out swap is needed
            - _execute_closeout_swap: To determine swap amount and direction
            - position_hold: Uses same calculation (ADD as SELL, REMOVE+fees as BUY)
        """
        # What we received when closing: base_amount + base_fee
        received_base = self.lp_position_state.base_amount + self.lp_position_state.base_fee
        # What we deposited when opening: initial_base_amount
        initial_base = self.lp_position_state.initial_base_amount
        return received_base - initial_base

    def _get_quote_to_global_rate(self) -> Decimal:
        """
        Get conversion rate from pool quote currency to USDT.

        For pools like COIN-SOL, the quote is SOL. This method returns the
        SOL-USDT rate to convert values to USD for consistent P&L reporting.

        Returns Decimal("1") if rate is not available.
        """
        _, quote_token = split_hb_trading_pair(self.config.trading_pair)

        try:
            rate = RateOracle.get_instance().get_pair_rate(f"{quote_token}-USDT")
            if rate is not None and rate > 0:
                return rate
        except Exception as e:
            self.logger().debug(f"Could not get rate for {quote_token}-USDT: {e}")

        return Decimal("1")  # Fallback to no conversion

    def _get_native_to_quote_rate(self) -> Decimal:
        """
        Get conversion rate from native currency (SOL) to pool quote currency.

        Used to convert transaction fees (paid in native currency) to quote.

        Returns Decimal("1") if rate is not available.
        """
        connector = self.connectors.get(self.config.connector_name)
        native_currency = getattr(connector, '_native_currency', DEFAULT_NATIVE_CURRENCY) or DEFAULT_NATIVE_CURRENCY
        _, quote_token = split_hb_trading_pair(self.config.trading_pair)

        # If native currency is the quote token, no conversion needed
        if native_currency == quote_token:
            return Decimal("1")

        try:
            rate = RateOracle.get_instance().get_pair_rate(f"{native_currency}-{quote_token}")
            if rate is not None and rate > 0:
                return rate
        except Exception as e:
            self.logger().debug(f"Could not get rate for {native_currency}-{quote_token}: {e}")

        return Decimal("1")  # Fallback to no conversion

    @property
    def filled_amount_base(self) -> Decimal:
        """Returns current base token amount in the LP position.

        Used for position tracking aggregation.
        """
        return self.lp_position_state.base_amount

    @property
    def filled_amount_quote(self) -> Decimal:
        """The swap volume that flowed through this position, in quote.

        Volume, not capital. This used to return the initial deposit, which made an LP
        executor incomparable with every other one: a position that put up $100 and
        traded nothing reported $100 the moment it opened, and the performance report
        added it again when the executor finished. Depositing and withdrawing is not
        volume; it is the same money moving in and back out.

        What a position does generate is the swaps that crossed its range, and it holds a
        direct measurement of those: the fees it earned are a fixed fraction of the volume
        that paid them. So

            volume = fees_earned_in_quote / fee_rate

        Both fee sides invert to the same expression, which is why one division does it:
        base fees were paid by base-denominated volume and quote fees by quote, and
        (base_fee / rate) x price + (quote_fee / rate) == (base_fee x price + quote_fee) / rate.

        The figure is the position's whole life, not a window, because this executor never
        collects mid-life — pending fees accumulate until the close collects them, and the
        close writes the collected total back to the same fields.

        Returns 0 when the pool's fee rate is unknown, which is a missing measurement
        rather than an absence of volume. Guessing a rate would put a number here that
        reads as measured and is not.

        An AMM share collects nothing: its fees stay in the reserves, so they are read from
        the deposit and the share's value now, or at withdrawal (see _amm_fees_earned_quote),
        and divided by the same fee rate.
        """
        if self._pool_info is None:
            # Pool info not fetched yet — this is read on the very first tick, before
            # update_pool_info() has run. Nothing is wrong and nothing is known, so say
            # nothing: warning here would assert that the pool reports no fee rate, which
            # is a claim about the pool made before ever having asked it.
            return Decimal("0")

        fee_pct = getattr(self._pool_info, "fee_pct", None)
        if not fee_pct or Decimal(str(fee_pct)) <= 0:
            # Asked, and the answer really is no rate. That is worth saying once.
            if not self._warned_missing_fee_pct:
                self._warned_missing_fee_pct = True
                self.logger().warning(
                    f"Pool {self.config.pool_address} reports a fee rate of {fee_pct!r}, so the "
                    "volume this position generated cannot be derived from its fees. "
                    "Reporting 0 volume."
                )
            return Decimal("0")

        price = self._current_price if self._current_price else Decimal("0")
        fees_quote = self.lp_position_state.base_fee * price + self.lp_position_state.quote_fee
        if self._is_amm:
            fees_quote = self._amm_fees_earned_quote(price)
        if fees_quote <= 0:
            return Decimal("0")

        # fee_pct is a PERCENT on every Gateway surface (see GW-2), not a fraction.
        return fees_quote / (Decimal(str(fee_pct)) / Decimal("100"))

    def _amm_fees_earned_quote(self, price: Decimal) -> Decimal:
        """The pool fees this AMM share earned, in quote, from its deposit and its value now or at withdrawal.

        Trades move a constant-product share along base x quote = constant, and fees grow that
        constant, so fees grow L = sqrt(base x quote). A share of L at price p is worth 2 x L x
        sqrt(p) in quote, so the fees are 2 x sqrt(p) x (L now - L at deposit). 0 when a side
        is not known.
        """
        state = self.lp_position_state
        sides = (state.initial_base_amount, state.initial_quote_amount, state.base_amount, state.quote_amount)
        if price <= 0 or min(sides) <= 0:
            return Decimal("0")
        grown = ((state.base_amount * state.quote_amount).sqrt()
                 - (state.initial_base_amount * state.initial_quote_amount).sqrt())
        return max(Decimal("0"), Decimal("2") * price.sqrt() * grown)

    def get_custom_info(self) -> Dict:
        """Report LP position state to controller"""
        price_float = float(self._current_price) if self._current_price else 0.0
        current_time = self._strategy.current_timestamp

        # Calculate total value in quote
        total_value = (
            float(self.lp_position_state.base_amount) * price_float +
            float(self.lp_position_state.quote_amount)
        )

        # Calculate fees earned in quote
        fees_earned = (
            float(self.lp_position_state.base_fee) * price_float +
            float(self.lp_position_state.quote_fee)
        )

        info = {
            "side": self.config.side,
            "state": self.lp_position_state.state.value,
            "position_address": self.lp_position_state.position_address,
            "current_price": price_float if self._current_price else None,
            "lower_price": float(self.lp_position_state.lower_price),
            "upper_price": float(self.lp_position_state.upper_price),
            "base_amount": float(self.lp_position_state.base_amount),
            "quote_amount": float(self.lp_position_state.quote_amount),
            "base_fee": float(self.lp_position_state.base_fee),
            "quote_fee": float(self.lp_position_state.quote_fee),
            "fees_earned_quote": fees_earned,
            "total_value_quote": total_value,
            "unrealized_pnl_quote": float(self.get_net_pnl_quote()),
            "position_rent": float(self.lp_position_state.position_rent),
            "position_rent_refunded": float(self.lp_position_state.position_rent_refunded),
            "tx_fee": float(self.lp_position_state.tx_fee),
            "out_of_range_seconds": self.lp_position_state.get_out_of_range_seconds(current_time),
            # Initial amounts (actual deposited) for inventory tracking, fallback to config
            "initial_base_amount": float(
                self.lp_position_state.initial_base_amount
                if self.lp_position_state.initial_base_amount > 0 or self.lp_position_state.initial_quote_amount > 0
                else self.config.base_amount
            ),
            "initial_quote_amount": float(
                self.lp_position_state.initial_quote_amount
                if self.lp_position_state.initial_base_amount > 0 or self.lp_position_state.initial_quote_amount > 0
                else self.config.quote_amount
            ),
            # Position tracking fields (consistent with grid/position/swap executors)
            "filled_amount_base": float(self.lp_position_state.base_amount),
            "filled_amount_quote": float(self.lp_position_state.quote_amount),
            "held_position_orders": self._held_position_orders,
            # Retry observability (consistent with order executor)
            "current_retries": self._current_retries,
            "max_retries": self._max_retries,
            "max_retries_reached": self._max_retries_reached,
            # What the next Gateway request will ask for. Worth reporting because it
            # changes under the executor's own control: a value above slippage_pct means
            # attempts have already failed on slippage and this one is paying for it.
            "slippage_pct": float(self._slippage_pct),
            "max_slippage_pct": float(self.config.max_slippage_pct),
            "hold_reason": self._hold_reason,
            # A transaction reconciled from its signature comes back without Gateway's
            # response data, so some figures above were never available for this position.
            "open_data_unavailable": self.lp_position_state.open_data_unavailable,
            "close_data_unavailable": self.lp_position_state.close_data_unavailable,
        }
        if self._is_amm:
            # The pool, this position's LP (or "unmeasured") and its transactions, as the marker has them.
            # LP amounts stay Decimal: the stored executor writes them out as exact text.
            record = self._amm_position_record()
            if record["lp_token_amount"] != "unmeasured":
                record["lp_token_amount"] = self.lp_position_state.lp_token_amount
            info.update(record)
            info.update({
                "pool_type": "amm",
                "lp_token_residue": self.lp_position_state.lp_token_residue,
                "open_outcome_unknown": self._amm_open_outcome_unknown,
                # True where the amounts were worked out from the wallet's holding in the pool read
                # around the transaction, not stated by Gateway: estimates, not measurements.
                "open_amounts_estimated": self._amm_open_estimated,
                "close_amounts_estimated": self._amm_close_estimated,
                "largest_removable_lp_token_amount": self._amm_most_removable,
                "fees_note": "AMM fees accrue inside the pool's reserves and are part of base_amount and "
                             "quote_amount; there is nothing to collect, so base_fee and quote_fee are always 0.",
            })
        return info

    # Required abstract methods from ExecutorBase
    async def validate_sufficient_balance(self):
        """Validate sufficient balance for LP position. ExecutorBase calls this in on_start()."""
        # LP connector handles balance validation during add_liquidity
        pass

    def get_net_pnl_quote(self) -> Decimal:
        """
        Returns net P&L in pool quote currency.

        P&L = (current_position_value + fees_earned) - initial_value - tx_fees

        Uses stored initial amounts and add_mid_price for accurate calculation.
        Works for both open positions and closed positions (using final returned amounts).
        Falls back to config values if initial amounts not yet set.
        """
        if self._current_price is None or self._current_price == 0:
            return Decimal("0")
        current_price = self._current_price

        # If executor failed before creating a position, P&L is 0
        if (self.lp_position_state.state == LPExecutorStates.FAILED and
                not self.lp_position_state.position_address):
            return Decimal("0")

        # Use stored add_mid_price for initial value, fall back to current price if not set
        add_price = self.lp_position_state.add_mid_price if self.lp_position_state.add_mid_price > 0 else current_price

        # Use stored initial amounts, fall back to config if not set (position not yet created)
        initial_base = self.lp_position_state.initial_base_amount
        initial_quote = self.lp_position_state.initial_quote_amount
        if initial_base == 0 and initial_quote == 0:
            initial_base = self.config.base_amount
            initial_quote = self.config.quote_amount

        # Initial value (actual deposited amounts, valued at ADD time price)
        initial_value = initial_base * add_price + initial_quote

        # Current position value (tokens in position, valued at current price)
        current_value = (
            self.lp_position_state.base_amount * current_price +
            self.lp_position_state.quote_amount
        )

        # Fees earned (LP swap fees, not transaction costs)
        fees_earned = (
            self.lp_position_state.base_fee * current_price +
            self.lp_position_state.quote_fee
        )

        # P&L in pool quote currency (before tx fees)
        pnl_in_quote = current_value + fees_earned - initial_value

        # Subtract transaction fees (tx_fee is in native currency, convert to quote)
        tx_fee_quote = self.lp_position_state.tx_fee * self._get_native_to_quote_rate()

        return pnl_in_quote - tx_fee_quote

    def get_net_pnl_pct(self) -> Decimal:
        """Returns net P&L ratio relative to initial investment.

        Both P&L and initial value are in quote currency.
        Falls back to config values if initial amounts not yet set.
        """
        pnl_quote = self.get_net_pnl_quote()
        if pnl_quote == Decimal("0"):
            return Decimal("0")

        if self._current_price is None or self._current_price == 0:
            return Decimal("0")
        current_price = self._current_price

        # Use stored add_mid_price for initial value to match get_net_pnl_quote()
        add_price = self.lp_position_state.add_mid_price if self.lp_position_state.add_mid_price > 0 else current_price

        # Use stored initial amounts, fall back to config if not set
        initial_base = self.lp_position_state.initial_base_amount
        initial_quote = self.lp_position_state.initial_quote_amount
        if initial_base == 0 and initial_quote == 0:
            initial_base = self.config.base_amount
            initial_quote = self.config.quote_amount

        # Initial value in pool quote currency
        initial_value_quote = initial_base * add_price + initial_quote

        if initial_value_quote == Decimal("0"):
            return Decimal("0")

        return pnl_quote / initial_value_quote

    def get_cum_fees_quote(self) -> Decimal:
        """
        Returns cumulative transaction costs in quote currency.

        NOTE: This is for transaction/gas costs, NOT LP fees earned.
        LP fees earned are included in get_net_pnl_quote() calculation.
        Transaction fees are paid in native currency (SOL) and converted to quote.
        """
        return self.lp_position_state.tx_fee * self._get_native_to_quote_rate()

    async def update_pool_info(self):
        """Fetch and store current pool info"""
        connector = self.connectors.get(self.config.connector_name)
        if connector is None:
            return

        try:
            self._pool_info = await connector.get_pool_info_by_address(
                self.config.pool_address,
                dex_name=self.lp_dex_name,
                trading_type=self.lp_trading_type,
            )
            if self._pool_info:
                self._current_price = Decimal(str(self._pool_info.price))
        except Exception as e:
            self.logger().warning(f"Error fetching pool info: {e}")
