"""
Combined Gateway connector for all gateway operations (swap and LP).

This module provides a unified connector for interacting with DEXes through Gateway.
The connector handles both swap operations and liquidity provision (AMM/CLMM).

Architecture:
- connector_name: Network identifier (e.g., "solana-mainnet-beta")
- dex_name: DEX protocol name passed to methods (e.g., "orca", "jupiter")
- trading_type: Pool type passed to methods (e.g., "clmm", "amm", "router")
"""

import asyncio
import re
import time
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN, Decimal, InvalidOperation, localcontext
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
from pydantic import BaseModel, Field

from hummingbot.connector.gateway.gateway_base import (
    TX_DATA_UNAVAILABLE,
    GatewayBase,
    TransactionUnresolvedError,
    extract_error_code,
)
from hummingbot.connector.gateway.gateway_in_flight_order import GatewayInFlightOrder
from hummingbot.core.data_type.common import LPType, OrderType, PriceType, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import (
    MarketEvent,
    RangePositionLiquidityAddedEvent,
    RangePositionLiquidityRemovedEvent,
    RangePositionUpdateFailureEvent,
)
from hummingbot.core.gateway.gateway_error import GatewayError
from hummingbot.core.rate_oracle.rate_oracle import RateOracle
from hummingbot.core.utils import async_ttl_cache
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather

# AMM connectors whose execute-swap declares minAmountOut/maxAmountIn, and so binds the swap
# to the price limit sent with it. Every other connector is sent none, unless Gateway states
# `binds_price_limit: true` for it in /config/connectors. A stated false never removes one
# of these: a limit sent to a connector that ignores it costs nothing.
PRICE_LIMIT_DEXES = frozenset({"ergodex"})

# The id under which Gateway names Ergo's native coin (ERG) in answers (`tokenIn`, `baseTokenAddress`).
ERGO_NATIVE_TOKEN_ID = "0" * 64

# Gateway's refusal of an AMM deposit or redeem whose pool moved past the request's bound
# (answered 409, with no code): nothing that can still execute was submitted.
POOL_MOVED_REFUSAL = re.compile(
    r"The pool moved: .*Nothing that can still execute was submitted; retry with a fresh quote\.$", re.DOTALL)


class TokenInfo(BaseModel):
    address: str
    symbol: str
    decimals: int


class AMMPoolInfo(BaseModel):
    address: str
    base_token_address: str = Field(alias="baseTokenAddress")
    quote_token_address: str = Field(alias="quoteTokenAddress")
    price: float
    fee_pct: float = Field(alias="feePct")
    base_token_amount: float = Field(alias="baseTokenAmount")
    quote_token_amount: float = Field(alias="quoteTokenAmount")


class CLMMPoolInfo(BaseModel):
    address: str
    base_token_address: str = Field(alias="baseTokenAddress")
    quote_token_address: str = Field(alias="quoteTokenAddress")
    # Optional in Gateway's CLMM PoolInfo schema ("Optional - Meteora-specific"): only
    # bin-based CLMMs report a bin step. Requiring it here turned every other connector's
    # pool-info response into a pydantic ValidationError, which get_pool_info_by_address
    # swallows into None and the user sees as a misleading "pool not found".
    bin_step: Optional[int] = Field(default=None, alias="binStep")
    fee_pct: float = Field(alias="feePct")
    price: float
    base_token_amount: float = Field(alias="baseTokenAmount")
    quote_token_amount: float = Field(alias="quoteTokenAmount")
    active_bin_id: int = Field(alias="activeBinId")


class AMMPositionDetail(BaseModel):
    """One individually addressable position, for AMMs whose LP is non-fungible.

    Meteora DAMM v2 positions are NFTs and a wallet may hold several in the same pool;
    remove-liquidity / add-liquidity need this address to name one of them.
    """
    position_address: str = Field(alias="positionAddress")
    lp_token_amount: Decimal = Field(alias="lpTokenAmount")
    base_token_amount: float = Field(alias="baseTokenAmount")
    quote_token_amount: float = Field(alias="quoteTokenAmount")


class AMMPositionInfo(BaseModel):
    pool_address: str = Field(alias="poolAddress")
    wallet_address: str = Field(alias="walletAddress")
    base_token_address: str = Field(alias="baseTokenAddress")
    quote_token_address: str = Field(alias="quoteTokenAddress")
    lp_token_amount: Decimal = Field(alias="lpTokenAmount")
    base_token_amount: float = Field(alias="baseTokenAmount")
    quote_token_amount: float = Field(alias="quoteTokenAmount")
    price: float
    # Per-position breakdown; omitted by fungible-LP AMMs, where the pool address
    # identifies the whole holding.
    positions: Optional[List[AMMPositionDetail]] = None
    base_token: Optional[str] = None
    quote_token: Optional[str] = None


class CLMMPositionInfo(BaseModel):
    address: str
    pool_address: str = Field(alias="poolAddress")
    base_token_address: str = Field(alias="baseTokenAddress")
    quote_token_address: str = Field(alias="quoteTokenAddress")
    base_token_amount: float = Field(alias="baseTokenAmount")
    quote_token_amount: float = Field(alias="quoteTokenAmount")
    base_fee_amount: float = Field(alias="baseFeeAmount")
    quote_fee_amount: float = Field(alias="quoteFeeAmount")
    lower_bin_id: int = Field(alias="lowerBinId")
    upper_bin_id: int = Field(alias="upperBinId")
    lower_price: float = Field(alias="lowerPrice")
    upper_price: float = Field(alias="upperPrice")
    price: float
    base_token: Optional[str] = None
    quote_token: Optional[str] = None


class Gateway(GatewayBase):
    """
    Unified Gateway connector for swap and LP operations.

    This connector handles:
    - Swap operations (buy, sell, get_quote_price)
    - LP operations (add_liquidity, remove_liquidity, get_pool_info)

    The dex_name and trading_type are passed as parameters to methods rather than
    being derived from the connector_name.
    """

    # Error code from gateway for transaction confirmation timeout
    TRANSACTION_TIMEOUT_CODE = "TRANSACTION_TIMEOUT"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Store LP operation metadata for triggering proper events
        self._lp_orders_metadata: Dict[str, Dict] = {}
        # Whether each connector binds a price limit, as Gateway states it; None until read.
        self._price_limit_connectors: Optional[Dict[str, bool]] = None
        self._last_connectors_attempt = 0.0

    async def start_network(self):
        await super().start_network()
        await self._read_price_limit_connectors()

    async def _read_price_limit_connectors(self):
        """Read which connectors Gateway says bind a price limit (`binds_price_limit`), once.

        Only a connector whose entry carries the key as true or false is recorded; only true
        changes anything (see _binds_price_limit). A read that fails is logged and leaves
        PRICE_LIMIT_DEXES alone in force; it is tried again on a later swap, at most once
        per NETWORK_CONFIG_RETRY_INTERVAL.
        """
        self._last_connectors_attempt = time.time()
        try:
            response = await self._get_gateway_instance().get_connectors(fail_silently=True)
            connectors = response.get("connectors") if isinstance(response, dict) else None
            if not isinstance(connectors, list):
                raise ValueError(f"unexpected answer {response!r}")
            self._price_limit_connectors = {
                entry["name"]: entry["binds_price_limit"]
                for entry in connectors
                if isinstance(entry, dict) and isinstance(entry.get("name"), str)
                and isinstance(entry.get("binds_price_limit"), bool)
            }
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(
                f"Could not read which Gateway connectors bind a price limit ({e}); "
                f"sending one only to {', '.join(sorted(PRICE_LIMIT_DEXES))}."
            )

    def _binds_price_limit(self, dex: str) -> bool:
        """Whether `dex` binds the price limit sent with a swap: in PRICE_LIMIT_DEXES, or stated true by Gateway."""
        return dex in PRICE_LIMIT_DEXES or (self._price_limit_connectors or {}).get(dex) is True

    def get_price_by_type(self, trading_pair: str, price_type: PriceType) -> Decimal:
        """
        Gets price by type for gateway connectors.

        Checks RateOracle for cached prices first (set by MarketDataProvider).
        Falls back to NaN if no price is available.

        :param trading_pair: The market trading pair
        :param price_type: The price type (MidPrice, BestBid, BestAsk, etc.)
        :returns: The price or NaN if not available
        """
        # Try to get price from RateOracle (set by MarketDataProvider for gateway connectors)
        try:
            rate_oracle = RateOracle.get_instance()
            price = rate_oracle.get_pair_rate(trading_pair)
            if price and price > 0:
                return price
        except Exception:
            pass
        # Fall back to NaN if no cached price
        return Decimal("nan")

    async def get_last_traded_prices(self, trading_pairs: List[str]) -> Dict[str, float]:
        """
        Return a dictionary with the trading pair as key and its current price as value,
        for each trading pair passed as parameter.

        ExchangeBase supplies this fan-out for order-book connectors, but Gateway
        extends ConnectorBase directly and so never inherited it. That left
        _get_last_traded_price below with no caller at all, and every consumer of the
        plural form raising AttributeError instead of returning prices.

        A pair that cannot be priced is omitted rather than reported as zero, so a
        caller can tell "no price available" from "the price is 0".

        :param trading_pairs: list of trading pairs to get the prices for
        :returns: Dictionary of associations between token pair and its latest price
        """
        results = await safe_gather(
            *[self._get_last_traded_price(trading_pair=trading_pair) for trading_pair in trading_pairs],
            return_exceptions=True,
        )
        prices: Dict[str, float] = {}
        for trading_pair, price in zip(trading_pairs, results):
            if isinstance(price, Exception):
                self.logger().warning(f"Failed to get last traded price for {trading_pair}: {price}")
                continue
            if price and price > 0:
                prices[trading_pair] = price
        return prices

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Gets the last traded price for a trading pair.

        Prefers the RateOracle cache (set by MarketDataProvider) and falls back to a
        Gateway quote.

        :param trading_pair: The market trading pair
        :returns: The last traded price or 0 if not available
        """
        try:
            rate_oracle = RateOracle.get_instance()
            price = rate_oracle.get_pair_rate(trading_pair)
            if price and price > 0:
                return float(price)
        except Exception:
            pass
        # The oracle only holds pairs MarketDataProvider was asked to track, so a pair
        # priced on demand -- a pool just opened in a dashboard, a memecoin addressed by
        # mint -- finds nothing cached and used to fall straight through to 0. Quote it
        # instead: Gateway prices anything the configured swap provider can route.
        try:
            quoted = await self.get_quote_price(trading_pair, is_buy=True, amount=Decimal("1"))
            if quoted and quoted > 0:
                return float(quoted)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().debug(f"Could not quote a fallback price for {trading_pair}.", exc_info=True)
        return 0.0

    @staticmethod
    def _parse_dex_name(dex_name: str) -> tuple:
        """
        Parse dex_name into (dex, trading_type) tuple.

        The trading type is never defaulted: Gateway rejects a guessed one with a 400,
        and guessing "router" for e.g. "meteora" only surfaced at execution time — on
        the LP executor's close-out swap, with funds already exposed.

        Args:
            dex_name: DEX identifier in "name/type" form
                - "jupiter/router" -> ("jupiter", "router")
                - "orca/clmm" -> ("orca", "clmm")

        Returns:
            Tuple of (dex, trading_type)

        Raises:
            ValueError: if dex_name does not carry a trading type
        """
        if "/" not in dex_name:
            raise ValueError(
                f"Invalid swap provider '{dex_name}' - expected 'name/type' "
                "(e.g. 'jupiter/router', 'meteora/clmm')"
            )
        parts = dex_name.split("/", 1)
        return parts[0], parts[1]

    # ==================== SWAP OPERATIONS ====================

    @async_ttl_cache(ttl=5, maxsize=10)
    async def get_quote_price(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
            slippage_pct: Optional[Decimal] = None,
            pool_address: Optional[str] = None,
    ) -> Optional[Decimal]:
        """
        Retrieves the volume weighted average price for a swap.

        :param trading_pair: The market trading pair
        :param is_buy: True for an intention to buy, False for an intention to sell
        :param amount: The amount required (in base token unit)
        :param slippage_pct: Maximum allowed slippage percentage
        :param pool_address: Optional pool to quote on (AMM/CLMM). None lets Gateway pick it.
        :return: The quote price.
        """
        price, _ = await self._quote_price_and_pool(trading_pair, is_buy, amount, slippage_pct, pool_address)
        return price

    @async_ttl_cache(ttl=5, maxsize=10)
    async def get_quote_price_and_pool(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
            slippage_pct: Optional[Decimal] = None,
            pool_address: Optional[str] = None,
    ) -> Tuple[Optional[Decimal], Optional[str]]:
        """
        The quote price and the pool Gateway says it priced on (`poolAddress` of its answer).

        The pool is None when Gateway's answer names none. A caller that named a pool can
        check the answer came from it. The price is None on an error, and when a pool was
        named and the answer names another one, or its `tokenIn`/`tokenOut` are not the
        pair's tokens in the side's direction (a pool of another pair).
        """
        return await self._quote_price_and_pool(trading_pair, is_buy, amount, slippage_pct, pool_address)

    async def get_quote_price_and_pool_fresh(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
            slippage_pct: Optional[Decimal] = None,
            pool_address: Optional[str] = None,
    ) -> Tuple[Optional[Decimal], Optional[str]]:
        """get_quote_price_and_pool without its 5 s cache: Gateway is asked on every call.

        For a re-quote that must see the pool as it is now, such as a sell retried after a failure.
        """
        return await self._quote_price_and_pool(trading_pair, is_buy, amount, slippage_pct, pool_address)

    async def _quote_price_and_pool(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
            slippage_pct: Optional[Decimal],
            pool_address: Optional[str],
    ) -> Tuple[Optional[Decimal], Optional[str]]:
        base, quote = trading_pair.split("-")
        side: TradeType = TradeType.BUY if is_buy else TradeType.SELL

        if not self._swap_provider:
            raise ValueError("No swap provider configured for this network. Set swapProvider in Gateway network config.")

        dex, trading_type = self._parse_dex_name(self._swap_provider)
        # Sent only when named, so a quote with no pool is the same request as before.
        pool_kwargs = {"pool_address": pool_address} if pool_address is not None else {}

        try:
            resp: Dict[str, Any] = await self._get_gateway_instance().quote_swap(
                network=self.network,
                chain=self.chain,
                dex=dex,
                trading_type=trading_type,
                base_asset=base,
                quote_asset=quote,
                amount=amount,
                side=side,
                slippage_pct=slippage_pct,
                **pool_kwargs,
            )
            price = resp.get("price", None)
            answered_pool = resp.get("poolAddress")
            answered_pool = answered_pool if isinstance(answered_pool, str) and answered_pool else None
            if pool_address is not None and answered_pool is not None and answered_pool != pool_address:
                self.logger().error(
                    f"Quote for {trading_pair} {side.name} {amount} on pool {pool_address} came from pool "
                    f"{answered_pool}; ignoring it."
                )
                return None, answered_pool
            if pool_address is not None:
                # A named pool is priced as it is, whatever pair was asked for, so its tokens
                # are checked here: a pool of another pair is never priced as this one.
                mismatch = self._quote_tokens_mismatch(trading_pair, side, resp)
                if mismatch is not None:
                    self.logger().error(
                        f"Quote for {trading_pair} {side.name} {amount} on pool {pool_address}: {mismatch}; "
                        "ignoring it."
                    )
                    return None, answered_pool
            return (Decimal(price) if price is not None else None), answered_pool
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Error getting quote price for {trading_pair} {side} order for {amount} amount.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            return None, None

    def _token_address(self, symbol: str) -> Optional[str]:
        """A token's address (lower case) from Gateway's token list, found as in _token_decimals.

        On Ergo the native coin is the all-zero id when the token list does not carry it.
        """
        token = self._token_data.get(symbol)
        if token is None:
            matches = [t for s, t in self._token_data.items() if s.lower() == symbol.lower()]
            token = matches[0] if len(matches) == 1 else None
        if token is None:
            token = self.get_token_by_address(symbol)
        address = token.get("address") if token is not None else None
        if isinstance(address, str) and address:
            return address.lower()
        if (self.chain == "ergo" and self._native_currency is not None
                and symbol.upper() == self._native_currency.upper()):
            return ERGO_NATIVE_TOKEN_ID
        return None

    def pair_token_addresses(self, trading_pair: str) -> Optional[Tuple[str, str]]:
        """The (base, quote) token addresses of a pair, lower case, or None when either is unknown."""
        base, quote = trading_pair.split("-")
        base_address, quote_address = self._token_address(base), self._token_address(quote)
        if base_address is None or quote_address is None:
            return None
        return base_address, quote_address

    def _quote_tokens_mismatch(self, trading_pair: str, side: TradeType, resp: Dict[str, Any]) -> Optional[str]:
        """Why a quote's `tokenIn`/`tokenOut` are not the pair's in the side's direction, or None when they are.

        A BUY pays the quote token for the base token; a SELL the reverse.
        """
        addresses = self.pair_token_addresses(trading_pair)
        if addresses is None:
            return f"the token addresses of {trading_pair} are not in the token list"
        base_address, quote_address = addresses
        expected_in, expected_out = ((quote_address, base_address) if side == TradeType.BUY
                                     else (base_address, quote_address))
        token_in, token_out = resp.get("tokenIn"), resp.get("tokenOut")
        if not isinstance(token_in, str) or not isinstance(token_out, str) or not token_in or not token_out:
            return "the answer names no tokenIn/tokenOut"
        if token_in.lower() != expected_in or token_out.lower() != expected_out:
            return (f"the answer trades {token_in} for {token_out}, not {expected_in} for {expected_out} "
                    f"({trading_pair} {side.name})")
        return None

    async def get_order_price(
            self,
            trading_pair: str,
            is_buy: bool,
            amount: Decimal,
    ) -> Decimal:
        """
        Retrieves the price required for an order of a given amount.
        """
        return await self.get_quote_price(trading_pair, is_buy, amount)

    def buy(self, trading_pair: str, amount: Decimal, order_type: OrderType, price: Decimal, **kwargs) -> str:
        """
        Buys an amount of base token for a given price (or cheaper).
        """
        return self.place_order(True, trading_pair, amount, price, **kwargs)

    def sell(self, trading_pair: str, amount: Decimal, order_type: OrderType, price: Decimal, **kwargs) -> str:
        """
        Sells an amount of base token for a given price (or at a higher price).
        """
        return self.place_order(False, trading_pair, amount, price, **kwargs)

    def place_order(self, is_buy: bool, trading_pair: str, amount: Decimal, price: Decimal, **request_args) -> str:
        """
        Places a swap order.
        """
        side: TradeType = TradeType.BUY if is_buy else TradeType.SELL
        order_id: str = self.create_market_order_id(side, trading_pair)
        safe_ensure_future(self._create_order(side, order_id, trading_pair, amount, price, **request_args))
        return order_id

    def cancel(self, trading_pair: str, client_order_id: str):
        """A swap cannot be cancelled once it is submitted: the order stays open until the chain resolves it."""
        order = self._order_tracker.fetch_order(client_order_id)
        transaction_hash = order.exchange_order_id if order is not None else None
        self.logger().warning(
            f"Swap orders cannot be cancelled; order {client_order_id} stays open on {transaction_hash} "
            "until the chain resolves it."
        )
        return client_order_id

    async def _create_order(
            self,
            trade_type: TradeType,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            price: Decimal,
            **kwargs
    ):
        """
        Executes a swap order through Gateway.

        :param trade_type: BUY or SELL
        :param order_id: Internal order id
        :param trading_pair: The market to place order
        :param amount: The order amount (in base token value)
        :param price: The order price
        :param kwargs: Additional parameters (dex_name, quote_id, slippage_pct, max_retries, pool_address)
        """
        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)

        base, quote = trading_pair.split("-")

        # Check if order is already being tracked
        existing_order = self._order_tracker.fetch_order(order_id)
        if existing_order is not None:
            self.logger().debug(f"Order {order_id} already tracked, skipping")
        else:
            # Start tracking - order tracker will emit BuyOrderCreatedEvent when state transitions
            self.start_tracking_order(order_id=order_id,
                                      trading_pair=trading_pair,
                                      trade_type=trade_type,
                                      price=price,
                                      amount=amount)

        # The pool named for this order is kept on it, so a later quote for it uses the same
        # pool. An order already tracked keeps the pool it was placed with.
        pool_address: Optional[str] = kwargs.get("pool_address")
        tracked_order = self._order_tracker.fetch_tracked_order(order_id)
        if tracked_order is not None:
            if pool_address is None:
                pool_address = getattr(tracked_order, "pool_address", None)
            else:
                tracked_order.pool_address = pool_address

        # A network config that could not be read at start is read again here, so the swap
        # follows its network's rules and provider once Gateway answers.
        await self._ensure_network_timing_read()

        # Extract optional parameters
        quote_id = kwargs.get("quote_id")
        slippage_pct = kwargs.get("slippage_pct")
        max_retries = kwargs.get("max_retries", 10)
        # An explicit dex_name (e.g. the LP executor's configured swap_provider)
        # overrides the network's default provider — it used to be silently ignored.
        swap_provider = kwargs.get("dex_name") or self._swap_provider

        try:
            # Inside the try: a missing provider must fail the ORDER (via
            # _handle_operation_failure) — raising before this block left the order
            # tracked-but-stuck in OPEN forever with no failure event.
            if not swap_provider:
                raise ValueError(f"No swap provider configured for {self.network}.")

            dex, trading_type = self._parse_dex_name(swap_provider)
            if (self._price_limit_connectors is None
                    and time.time() - self._last_connectors_attempt >= self.NETWORK_CONFIG_RETRY_INTERVAL):
                # Read in the background: this swap never waits for it, and uses the fallback.
                # Marked as tried now, so other swaps in this tick start no second read.
                self._last_connectors_attempt = time.time()
                safe_ensure_future(self._read_price_limit_connectors())
            # Only the AMM route takes a price limit; the router route refuses keys it does
            # not declare. A stock Gateway's AMM route accepts and ignores them, so the limit
            # is sent only to the connectors known to bind the swap to it.
            price_limit = (
                self._swap_price_limit(trade_type, trading_pair, amount, price, slippage_pct)
                if trading_type == "amm" and not quote_id and self._binds_price_limit(dex) else {}
            )

            async def execute_gateway_swap() -> Dict[str, Any]:
                if quote_id:
                    return await self._get_gateway_instance().execute_quote(
                        dex=dex,
                        network=self.network,
                        quote_id=quote_id,
                        wallet_address=self.address,
                        chain=self.chain,
                    )
                else:
                    return await self._get_gateway_instance().execute_swap(
                        dex=dex,
                        trading_type=trading_type,
                        base_asset=base,
                        quote_asset=quote,
                        side=trade_type,
                        amount=amount,
                        network=self.network,
                        chain=self.chain,
                        wallet_address=self.address,
                        slippage_pct=slippage_pct,
                        **price_limit,
                        **({"pool_address": pool_address} if pool_address is not None else {}),
                    )

            async def execute_and_keep_pending_data() -> Dict[str, Any]:
                result = await execute_gateway_swap()
                self._keep_pending_swap_data(order_id, result)
                return result

            if quote_id:
                self.logger().info(f"Swap {order_id}: {trade_type.name} {amount} {trading_pair} on quote {quote_id}.")
            else:
                self.logger().info(
                    f"Swap {order_id}: {trade_type.name} {amount} {trading_pair} via {dex}/{trading_type} on "
                    + (f"pool {pool_address}." if pool_address is not None else "the pool Gateway chooses.")
                )

            if not self._is_usable_price(price):
                # A fill confirmed without Gateway's realized amounts is booked at the order's
                # price, and this order has none. Quote one alongside the submission, which
                # never waits for it; without a quote the fill is booked at price 0.
                safe_ensure_future(self._set_quoted_price(order_id, trading_pair, trade_type, amount))

            order_result = await self._execute_with_retry(
                operation=execute_and_keep_pending_data,
                operation_name=f"swap {trade_type.name} {amount} on {trading_pair}",
                max_retries=max_retries,
                # Where the transaction is never re-submitted, attach its hash as soon as it
                # is broadcast, so the order is saved with it and polled by the status loop
                # while it is still being waited for. Not where a re-submission can change
                # the hash: the first one would stick to the order.
                on_broadcast=(
                    (lambda signature: self.update_order_from_hash(order_id, trading_pair, signature, {}))
                    if not self.not_found_proves_dropped else None
                ),
            )

            transaction_hash: Optional[str] = order_result.get("signature")
            self._log_swap_pool(order_id, pool_address, order_result)
            if transaction_hash is not None and transaction_hash != "":
                if self._has_no_hash_yet(order_id):
                    self.update_order_from_hash(order_id, trading_pair, transaction_hash, order_result)
                tracked = self._order_tracker.fetch_tracked_order(order_id)
                kept = getattr(tracked, "pending_swap_data", None) if tracked is not None else None
                if order_result.get(TX_DATA_UNAVAILABLE) and (tracked is None or tracked.is_done):
                    self.logger().info(
                        f"Swap {order_id} confirmed on {transaction_hash}; the order-status loop has already resolved it."
                    )
                elif order_result.get(TX_DATA_UNAVAILABLE) and kept is not None and kept[0] == transaction_hash:
                    self.logger().info(
                        f"Swap {order_id} confirmed on {transaction_hash}; its fill is booked from the amounts "
                        "its transaction fixes once the order-status loop records it."
                    )
                else:
                    self._store_swap_result(order_id, trade_type, trading_pair, amount, order_result, transaction_hash)

        except asyncio.CancelledError:
            raise
        except TransactionUnresolvedError as e:
            # Broadcast, and neither confirmed nor shown to have failed when polling
            # stopped. That is no evidence of failure: the swap may still land, and
            # failing the order here would leave its fill uncounted and invite the
            # strategy to trade again. Keep the order open on its transaction instead;
            # update_order_status() resolves it from the chain (CONFIRMED -> filled,
            # FAILED -> failed). NOT_FOUND counts toward the lost-order rule only on a
            # network where it proves the transaction can no longer land; elsewhere it
            # keeps the order open.
            self.logger().warning(
                f"Swap order {order_id} on {trading_pair}: {e} Keeping the order open on transaction "
                f"{e.signature} for status polling."
            )
            if self._has_no_hash_yet(order_id):
                self.update_order_from_hash(order_id, trading_pair, e.signature, {})
        except aiohttp.ClientConnectorError as e:
            # The connection to Gateway could not be opened, so no request was sent.
            self._fail_swap_order(order_id, trading_pair, trade_type, e)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # The request failed on the way to or from Gateway, so whether Gateway submitted
            # the swap is unknown. Where a transaction never expires, that order must not be
            # traded again before someone checks the chain: the failure says so.
            error = Exception(f"{e!r} [code: SUBMIT_OUTCOME_UNKNOWN]") if not self.not_found_proves_dropped else e
            self._fail_swap_order(order_id, trading_pair, trade_type, error)
        except Exception as e:
            self._fail_swap_order(order_id, trading_pair, trade_type, e)

    def _log_swap_pool(self, order_id: str, pool_address: Optional[str], order_result: Any):
        """Log the pool Gateway's answer names for a swap (its `data.poolAddress`, once confirmed)."""
        data = order_result.get("data") if isinstance(order_result, dict) else None
        used = data.get("poolAddress") if isinstance(data, dict) else None
        if not isinstance(used, str) or not used:
            return
        if pool_address is not None and used != pool_address:
            self.logger().error(f"Swap {order_id} was placed on pool {pool_address} but Gateway names pool {used}.")
        else:
            self.logger().info(f"Swap {order_id} traded on pool {used}.")

    def _fail_swap_order(self, order_id: str, trading_pair: str, trade_type: TradeType, error: Exception):
        """Fail the order, unless the order-status loop has already resolved it from the chain."""
        if self._order_tracker.fetch_tracked_order(order_id) is None:
            self.logger().warning(
                f"Swap order {order_id}: order already resolved by the status loop; ignoring the late failure "
                f"({error})."
            )
            return
        self._handle_operation_failure(order_id, trading_pair, f"submitting {trade_type.name} swap order", error)

    @staticmethod
    def _is_usable_price(price: Optional[Decimal]) -> bool:
        """Whether an order's price can book a fill: finite and positive (a market order's is NaN)."""
        return price is not None and Decimal(str(price)).is_finite() and price > 0

    async def _set_quoted_price(self, order_id: str, trading_pair: str, trade_type: TradeType, amount: Decimal):
        """Give an open order placed without a usable price Gateway's quoted price, if Gateway gives one."""
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        pool_address = getattr(tracked, "pool_address", None) if tracked is not None else None
        try:
            if pool_address is not None:
                quoted = await self.get_quote_price(trading_pair, trade_type == TradeType.BUY, amount,
                                                    pool_address=pool_address)
            else:
                quoted = await self.get_quote_price(trading_pair, trade_type == TradeType.BUY, amount)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().warning(f"Swap order {order_id}: no quote to book its fill at ({e}).")
            return
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if self._is_usable_price(quoted) and tracked is not None and not tracked.is_done:
            tracked.price = self.quantize_order_price(trading_pair, quoted)

    def _has_no_hash_yet(self, order_id: str) -> bool:
        """Whether the order is still open and has no transaction hash attached yet."""
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        return tracked is not None and not tracked.is_done and tracked.exchange_order_id is None

    def _keep_pending_swap_data(self, order_id: str, result: Any):
        """Keep, with the open order, the amounts a PENDING swap answer says its transaction moves.

        Gateway's `pendingData` is what the submitted transaction moves if it confirms. It is
        no evidence that it will; it is kept with the transaction's signature, and only read
        once poll says that transaction confirmed (see process_transaction_confirmation_update).
        """
        if not isinstance(result, dict) or result.get("status") != 0:
            return
        signature, pending_data = result.get("signature"), result.get("pendingData")
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if signature and isinstance(pending_data, dict) and tracked is not None and not tracked.is_done:
            tracked.pending_swap_data = (signature, dict(pending_data))

    def process_transaction_confirmation_update(self, tracked_order: GatewayInFlightOrder, fee: Decimal):
        """Book the fill of a transaction poll has just reported confirmed.

        A swap answered PENDING with `pendingData` for this same transaction is booked from
        those amounts and that fee, as a swap answered with `data` is in _store_swap_result.
        Every other order is booked as before, at its price and amount.
        """
        trade_update = self._trade_update_from_pending_data(tracked_order, fee)
        if trade_update is None:
            super().process_transaction_confirmation_update(tracked_order, fee)
            return
        self.logger().info(
            f"Processing trade update for {tracked_order.client_order_id} from the amounts its transaction fixes: "
            f"fill_amount={trade_update.fill_base_amount}, fill_price={trade_update.fill_price}, "
            f"trade_id={trade_update.trade_id}"
        )
        self._order_tracker.process_trade_update(trade_update)

    def _trade_update_from_pending_data(
        self, tracked_order: GatewayInFlightOrder, poll_fee: Decimal
    ) -> Optional[TradeUpdate]:
        """The fill a confirmed swap's kept `pendingData` gives, or None to book it as before.

        Its fee is `pendingData`'s, or poll's when `pendingData` names none.
        """
        kept = getattr(tracked_order, "pending_swap_data", None)
        if kept is None or tracked_order.trade_type not in (TradeType.BUY, TradeType.SELL):
            return None
        signature, data = kept
        if signature != tracked_order.exchange_order_id:
            return None
        try:
            amount_in = Decimal(str(data.get("amountIn")))
            amount_out = Decimal(str(data.get("amountOut")))
            fee = Decimal(str(data["fee"])) if data.get("fee") is not None else poll_fee
        except (InvalidOperation, TypeError, ValueError):
            return None
        if not (amount_in.is_finite() and amount_out.is_finite() and fee.is_finite()):
            return None
        if amount_in <= 0 or amount_out <= 0 or fee < 0:
            return None
        # A SELL sends base and receives quote; a BUY is the mirror image.
        if tracked_order.trade_type == TradeType.SELL:
            fill_base_amount, fill_quote_amount = amount_in, amount_out
        else:
            fill_base_amount, fill_quote_amount = amount_out, amount_in
        return TradeUpdate(
            trade_id=signature,
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=signature,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=self.current_timestamp,
            fill_price=fill_quote_amount / fill_base_amount,
            fill_base_amount=fill_base_amount,
            fill_quote_amount=fill_quote_amount,
            fee=AddedToCostTradeFee(flat_fees=[TokenAmount(self._native_currency, fee)]),
        )

    # A JSON number holds a decimal exactly up to this many significant digits, so a limit
    # with no more than this reaches Gateway as the value computed here.
    PRICE_LIMIT_SIGNIFICANT_DIGITS = 15

    def _swap_price_limit(
        self,
        trade_type: TradeType,
        trading_pair: str,
        amount: Decimal,
        price: Decimal,
        slippage_pct: Optional[Union[Decimal, float, str]],
    ) -> Dict[str, Decimal]:
        """The price limit for an AMM swap, as execute_swap keyword arguments.

        buy() and sell() take a price the order may not do worse than. Gateway can bind it
        on the swap it builds: a SELL must receive at least amount x price of the quote
        token (`minAmountOut`), a BUY may spend at most amount x price of it
        (`maxAmountIn`), each widened by the slippage tolerance the order was placed with
        (none if it named none; Gateway then also applies its own on its own quote).

        The exact product is first rounded to 15 significant digits, which removes the
        noise a price read from a float carries beyond them. It is then rounded to the quote
        token's decimals, and to 15 significant digits, in the direction that tightens it:
        up for a SELL's minimum, down for a BUY's maximum. So it is never looser than the
        order by more than half a unit in the 15th significant digit, and never finer than
        the token or than a JSON number carries.

        Returns {} (no limit) when the order has no usable price, e.g. a market order's NaN.
        Raises ValueError, before anything is sent, when the quote token's decimals are
        unknown, when the limit is below 1e-6 or 1e15 or more (Gateway cannot read such a JSON
        number), or when a BUY's limit rounds to 0 of the quote token (no swap could pass it).
        """
        if price is None or amount is None:
            return {}
        price, amount = Decimal(str(price)), Decimal(str(amount))
        if not price.is_finite() or not amount.is_finite() or price <= 0 or amount <= 0:
            return {}
        slippage = Decimal(str(slippage_pct)) if slippage_pct is not None else Decimal("0")
        quote = trading_pair.split("-")[1]
        decimals = self._token_decimals(quote)
        if decimals is None:
            raise ValueError(f"Cannot bind this swap's price: decimals of {quote} are unknown to the client")
        with localcontext() as context:
            # Exact products, so the roundings below are the only roundings.
            context.prec = 100
            if trade_type == TradeType.SELL:
                limit = max(amount * price * (Decimal("100") - slippage) / Decimal("100"), Decimal("0"))
                rounding, key = ROUND_CEILING, "min_amount_out"
            else:
                limit = amount * price * (Decimal("100") + slippage) / Decimal("100")
                rounding, key = ROUND_FLOOR, "max_amount_in"
            positive = limit > 0
            if limit != 0:
                limit = limit.quantize(
                    Decimal(1).scaleb(limit.adjusted() - (self.PRICE_LIMIT_SIGNIFICANT_DIGITS - 1)),
                    rounding=ROUND_HALF_EVEN,
                )
        # The finest digit kept: the token's last decimal, or the 15th significant digit
        # when that is coarser. One quantize, so the result never has more digits than that.
        lowest_digit = max(-decimals, limit.adjusted() - (self.PRICE_LIMIT_SIGNIFICANT_DIGITS - 1))
        limit = limit.quantize(Decimal(1).scaleb(lowest_digit), rounding=rounding)
        if 0 < limit < Decimal("1e-6"):
            raise ValueError("price limit below 1e-6 cannot be sent as a JSON number to this Gateway")
        if limit >= Decimal("1e15"):
            raise ValueError("price limit of 1e15 or more cannot be sent as a JSON number to this Gateway")
        if key == "max_amount_in" and limit == 0:
            raise ValueError(f"Cannot bind this swap's price: its quote value rounds to 0 {quote}")
        # A SELL's minimum rounds up, so a positive one is never sent as 0; refused should
        # that ever change, as a minimum of 0 binds nothing. Only a slippage of 100% or more
        # gives a minimum of exactly 0.
        if key == "min_amount_out" and limit == 0 and positive:
            raise ValueError(f"Cannot bind this swap's price: its minimum rounds to 0 {quote}")
        return {key: limit}

    def _token_decimals(self, symbol: str) -> Optional[int]:
        """A token's decimals from Gateway's token list, or for the native coin, from the network config.

        The token is looked up by its exact symbol, then by a symbol that differs only in
        case (Gateway accepts either) if exactly one token has it, then by its address.
        """
        token = self._token_data.get(symbol)
        if token is None:
            matches = [t for s, t in self._token_data.items() if s.lower() == symbol.lower()]
            token = matches[0] if len(matches) == 1 else None
        if token is None:
            token = self.get_token_by_address(symbol)
        if token is not None and token.get("decimals") is not None:
            return int(token["decimals"])
        if (self._native_decimals is not None and self._native_currency is not None
                and symbol.upper() == self._native_currency.upper()):
            return self._native_decimals
        return None

    def _store_swap_result(
        self,
        order_id: str,
        trade_type: TradeType,
        trading_pair: str,
        amount: Decimal,
        order_result: Dict[str, Any],
        transaction_hash: str
    ):
        """Store swap result data by creating a TradeUpdate for proper fill tracking.

        ``amountIn``/``amountOut`` are Gateway's REALIZED amounts, derived from the
        wallet's on-chain pre/post token balance deltas for the settled transaction.
        They are what actually moved, which is not the amount that was requested:
        slippage and fees mean a swap for 62 base tokens may land 61.962753. Callers
        that spend the proceeds downstream (e.g. an LP open's ``base_amount``) must
        see the realized figure, or they will ask the chain for tokens that aren't
        there.

        Base and quote are assigned from the trade side rather than from the token
        identities, so this is independent of the quote asset (SOL, USDC, ...).

        One caveat: for an SPL token Gateway diffs the token balance exactly, but a
        NATIVE SOL leg is measured as a lamport delta that also absorbs the tx fee
        and any rent. So on a SOL-quoted pair the quote leg carries a few thousand
        lamports of noise, and on a SOL-BASE pair the base leg does. The error is
        gas-sized (~1e-5 SOL) and, for a buy, understates what arrived - which is
        the safe direction for anything that spends the proceeds.
        """
        data = order_result.get("data", {})
        amount_in = Decimal(str(data.get("amountIn", "0")))
        amount_out = Decimal(str(data.get("amountOut", "0")))

        # Gateway only populates `data` once the swap is CONFIRMED. Without this
        # guard a pending or failed swap yields zeroed amounts and would still be
        # forced to FILLED below, reporting a phantom 0-amount fill. Leave the order
        # OPEN so update_order_status() resolves it from the chain instead.
        if amount_in <= 0 or amount_out <= 0:
            self.logger().warning(
                f"Swap {order_id} ({transaction_hash}) returned no realized amounts "
                f"(status={order_result.get('status')!r}, amountIn={amount_in}, amountOut={amount_out}); "
                f"not marking as filled. Order stays open for status polling."
            )
            return

        if trade_type == TradeType.SELL:
            executed_price = amount_out / amount_in
        else:
            executed_price = amount_in / amount_out

        tracked_order = self._order_tracker.fetch_order(order_id)
        if not tracked_order:
            return

        fee = Decimal(str(data.get("fee", 0)))
        fee_asset = self._native_currency
        trade_fee = AddedToCostTradeFee(flat_fees=[TokenAmount(fee_asset, fee)])

        # A SELL sends base and receives quote; a BUY is the mirror image.
        if trade_type == TradeType.SELL:
            fill_base_amount, fill_quote_amount = amount_in, amount_out
        else:
            fill_base_amount, fill_quote_amount = amount_out, amount_in

        trade_update = TradeUpdate(
            trade_id=transaction_hash,
            client_order_id=order_id,
            exchange_order_id=transaction_hash,
            trading_pair=trading_pair,
            fill_timestamp=self.current_timestamp,
            fill_price=executed_price,
            fill_base_amount=fill_base_amount,
            fill_quote_amount=fill_quote_amount,
            fee=trade_fee
        )

        self.logger().info(
            f"Processing trade update for {order_id}: requested={amount}, "
            f"realized fill_amount={fill_base_amount}, fill_price={executed_price}, "
            f"trade_id={transaction_hash}"
        )

        # Process order update to mark order as FILLED (triggers OrderCompleted event)
        order_update = OrderUpdate(
            client_order_id=order_id,
            exchange_order_id=transaction_hash,
            trading_pair=trading_pair,
            update_timestamp=self.current_timestamp,
            new_state=OrderState.FILLED,
        )
        self._order_tracker.process_order_update(order_update)

        # Process trade update (triggers OrderFilled event with fill details)
        self._order_tracker.process_trade_update(trade_update)

    # ==================== LP OPERATIONS ====================

    def _trigger_lp_events_if_needed(self, order_id: str, transaction_hash: str):
        """Helper to trigger LP-specific events when an order completes."""
        if order_id not in self._lp_orders_metadata:
            return

        tracked_order = self._order_tracker.fetch_order(order_id)
        if not tracked_order or tracked_order.trade_type != TradeType.RANGE:
            return

        metadata = self._lp_orders_metadata[order_id]

        is_successful = tracked_order.is_done and not tracked_order.is_failure and not tracked_order.is_cancelled

        if is_successful:
            if metadata["operation"] == "add":
                self._trigger_add_liquidity_event(
                    order_id=order_id,
                    exchange_order_id=transaction_hash,
                    trading_pair=tracked_order.trading_pair,
                    lower_price=metadata["lower_price"],
                    upper_price=metadata["upper_price"],
                    amount=metadata["amount"],
                    fee_tier=metadata["fee_tier"],
                    creation_timestamp=tracked_order.creation_timestamp,
                    trade_fee=TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        flat_fees=[TokenAmount(amount=metadata.get("tx_fee", Decimal("0")), token=self._native_currency)]
                    ),
                    position_address=metadata.get("position_address", ""),
                    base_amount=metadata.get("base_amount", Decimal("0")),
                    quote_amount=metadata.get("quote_amount", Decimal("0")),
                    position_rent=metadata.get("position_rent", Decimal("0")),
                )
            elif metadata["operation"] == "remove":
                self._trigger_remove_liquidity_event(
                    order_id=order_id,
                    exchange_order_id=transaction_hash,
                    trading_pair=tracked_order.trading_pair,
                    token_id=metadata["position_address"],
                    creation_timestamp=tracked_order.creation_timestamp,
                    trade_fee=TradeFeeBase.new_spot_fee(
                        fee_schema=self.trade_fee_schema(),
                        trade_type=tracked_order.trade_type,
                        flat_fees=[TokenAmount(amount=metadata.get("tx_fee", Decimal("0")), token=self._native_currency)]
                    ),
                    position_address=metadata.get("position_address", ""),
                    base_amount=metadata.get("base_amount", Decimal("0")),
                    quote_amount=metadata.get("quote_amount", Decimal("0")),
                    base_fee=metadata.get("base_fee", Decimal("0")),
                    quote_fee=metadata.get("quote_fee", Decimal("0")),
                    position_rent_refunded=metadata.get("position_rent_refunded", Decimal("0")),
                )
        elif tracked_order.is_failure:
            operation_type = "add" if metadata["operation"] == "add" else "remove"
            self.logger().error(
                f"LP {operation_type} liquidity transaction failed for order {order_id} (tx: {transaction_hash})"
            )
            self.trigger_event(
                MarketEvent.RangePositionUpdateFailure,
                RangePositionUpdateFailureEvent(
                    timestamp=self.current_timestamp,
                    order_id=order_id,
                    order_action=LPType.ADD if metadata["operation"] == "add" else LPType.REMOVE,
                )
            )
        elif tracked_order.is_cancelled:
            operation_type = "add" if metadata["operation"] == "add" else "remove"
            self.logger().warning(
                f"LP {operation_type} liquidity transaction cancelled for order {order_id} (tx: {transaction_hash})"
            )

        del self._lp_orders_metadata[order_id]
        self.stop_tracking_order(order_id)

    async def update_order_status(self, tracked_orders: List[GatewayInFlightOrder]):
        """Override to trigger RangePosition events after LP transactions complete."""
        await super().update_order_status(tracked_orders)

        for tracked_order in tracked_orders:
            if tracked_order.trade_type == TradeType.RANGE:
                try:
                    tx_hash = await tracked_order.get_exchange_order_id()
                    self._trigger_lp_events_if_needed(tracked_order.client_order_id, tx_hash)
                except Exception as e:
                    self.logger().warning(f"Error triggering LP event for {tracked_order.client_order_id}: {e}", exc_info=True)

    def _handle_operation_failure(self, order_id: str, trading_pair: str, operation_name: str, error: Exception):
        """Override to trigger RangePositionUpdateFailureEvent for LP operations."""
        super()._handle_operation_failure(order_id, trading_pair, operation_name, error)

        # Read the code off the exception when Gateway gave us one, instead of
        # looking for it in the rendered message.
        is_timeout_error = extract_error_code(error) == self.TRANSACTION_TIMEOUT_CODE

        if is_timeout_error and order_id in self._lp_orders_metadata:
            metadata = self._lp_orders_metadata[order_id]
            operation = metadata.get("operation", "")
            self.logger().warning(
                f"Transaction timeout detected for LP {operation} order {order_id} on {trading_pair}. "
                f"Chain may be congested. Triggering retry event..."
            )
            self.trigger_event(
                MarketEvent.RangePositionUpdateFailure,
                RangePositionUpdateFailureEvent(
                    timestamp=self.current_timestamp,
                    order_id=order_id,
                    order_action=LPType.ADD if operation == "add" else LPType.REMOVE,
                )
            )
            del self._lp_orders_metadata[order_id]
        elif order_id in self._lp_orders_metadata:
            self.logger().warning(f"Non-retryable error for {order_id}: {str(error)[:100]}")
            del self._lp_orders_metadata[order_id]

    def _trigger_add_liquidity_event(
        self,
        order_id: str,
        exchange_order_id: str,
        trading_pair: str,
        lower_price: Decimal,
        upper_price: Decimal,
        amount: Decimal,
        fee_tier: str,
        creation_timestamp: float,
        trade_fee: TradeFeeBase,
        position_address: str = "",
        base_amount: Decimal = Decimal("0"),
        quote_amount: Decimal = Decimal("0"),
        mid_price: Decimal = Decimal("0"),
        position_rent: Decimal = Decimal("0"),
    ) -> RangePositionLiquidityAddedEvent:
        """Trigger RangePositionLiquidityAddedEvent and return the event."""
        event = RangePositionLiquidityAddedEvent(
            timestamp=self.current_timestamp,
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            lower_price=lower_price,
            upper_price=upper_price,
            amount=amount,
            fee_tier=fee_tier,
            creation_timestamp=creation_timestamp,
            trade_fee=trade_fee,
            token_id=0,
            position_address=position_address,
            mid_price=mid_price,
            base_amount=base_amount,
            quote_amount=quote_amount,
            position_rent=position_rent,
        )
        self.trigger_event(MarketEvent.RangePositionLiquidityAdded, event)
        self.logger().info(f"Triggered RangePositionLiquidityAddedEvent for order {order_id}")
        return event

    def _trigger_remove_liquidity_event(
        self,
        order_id: str,
        exchange_order_id: str,
        trading_pair: str,
        token_id: str,
        creation_timestamp: float,
        trade_fee: TradeFeeBase,
        position_address: str = "",
        lower_price: Decimal = Decimal("0"),
        upper_price: Decimal = Decimal("0"),
        mid_price: Decimal = Decimal("0"),
        base_amount: Decimal = Decimal("0"),
        quote_amount: Decimal = Decimal("0"),
        base_fee: Decimal = Decimal("0"),
        quote_fee: Decimal = Decimal("0"),
        position_rent_refunded: Decimal = Decimal("0"),
    ) -> RangePositionLiquidityRemovedEvent:
        """Trigger RangePositionLiquidityRemovedEvent and return the event."""
        event = RangePositionLiquidityRemovedEvent(
            timestamp=self.current_timestamp,
            order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            token_id=token_id,
            trade_fee=trade_fee,
            creation_timestamp=creation_timestamp,
            position_address=position_address,
            lower_price=lower_price,
            upper_price=upper_price,
            mid_price=mid_price,
            base_amount=base_amount,
            quote_amount=quote_amount,
            base_fee=base_fee,
            quote_fee=quote_fee,
            position_rent_refunded=position_rent_refunded,
        )
        self.trigger_event(MarketEvent.RangePositionLiquidityRemoved, event)
        self.logger().info(f"Triggered RangePositionLiquidityRemovedEvent for order {order_id}")
        return event

    @async_ttl_cache(ttl=300, maxsize=10)
    async def get_pool_address(
        self,
        trading_pair: str,
        dex_name: str,
        trading_type: str = "clmm"
    ) -> Optional[str]:
        """
        Get pool address for a trading pair (cached for 5 minutes).

        :param trading_pair: Trading pair (e.g., "SOL-USDC")
        :param dex_name: DEX protocol name (e.g., "orca", "meteora", "raydium")
        :param trading_type: Trading type (e.g., "clmm", "amm"). Defaults to "clmm".
        :return: Pool address or None if not found
        """
        try:
            pool_info = await self._get_gateway_instance().get_pool(
                trading_pair=trading_pair,
                chain=self.chain,
                network=self.network,
                trading_type=trading_type,
                connector=dex_name
            )

            pool_address = pool_info.get("address")
            if not pool_address:
                self.logger().warning(f"No pool address found for {trading_pair}")

            return pool_address

        except Exception as e:
            self.logger().error(f"Error getting pool address for {trading_pair}: {e}")
            return None

    @async_ttl_cache(ttl=5, maxsize=10)
    async def get_pool_info_by_address(
        self,
        pool_address: str,
        dex_name: str,
        trading_type: str = "clmm",
    ) -> Optional[Union[AMMPoolInfo, CLMMPoolInfo]]:
        """
        Retrieves pool information by pool address directly.

        :param pool_address: The pool contract address
        :param dex_name: DEX protocol name (e.g., "orca", "meteora")
        :param trading_type: Trading type (e.g., "clmm", "amm"). Defaults to "clmm".
        :return: Pool info object or None if not found
        """
        try:
            resp: Dict[str, Any] = await self._get_gateway_instance().pool_info(
                network=self.network,
                chain=self.chain,
                pool_address=pool_address,
                dex=dex_name,
                trading_type=trading_type,
            )

            if not resp:
                return None

            if trading_type == "clmm":
                return CLMMPoolInfo(**resp)
            elif trading_type == "amm":
                return AMMPoolInfo(**resp)
            else:
                self.logger().warning(f"Unknown trading type: {trading_type}")
                return None

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Error fetching pool info for address {pool_address}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            return None

    async def get_pool_info(
        self,
        trading_pair: str,
        dex_name: str,
        trading_type: str = "clmm"
    ) -> Optional[Union[AMMPoolInfo, CLMMPoolInfo]]:
        """
        Get pool information for a trading pair.

        :param trading_pair: Trading pair (e.g., "SOL-USDC")
        :param dex_name: DEX protocol name (e.g., "orca", "meteora")
        :param trading_type: Trading type (e.g., "clmm", "amm"). Defaults to "clmm".
        :return: Pool info object or None if not found
        """
        try:
            pool_address = await self.get_pool_address(trading_pair, dex_name, trading_type)
            if not pool_address:
                return None

            return await self.get_pool_info_by_address(pool_address, dex_name, trading_type)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error getting pool info for {trading_pair}: {e}")
            return None

    async def resolve_trading_pair_from_pool(
        self,
        pool_address: str,
        dex_name: str,
        trading_type: str = "clmm",
    ) -> Optional[Dict[str, str]]:
        """
        Resolve trading pair information from pool address.
        """
        try:
            pool_info_resp = await self._get_gateway_instance().pool_info(
                network=self.network,
                chain=self.chain,
                pool_address=pool_address,
                dex=dex_name,
                trading_type=trading_type,
            )

            if not pool_info_resp:
                raise ValueError(f"Could not fetch pool info for pool address {pool_address}")

            base_token_address = pool_info_resp.get("baseTokenAddress")
            quote_token_address = pool_info_resp.get("quoteTokenAddress")

            if not base_token_address or not quote_token_address:
                raise ValueError(f"Pool info missing token addresses: {pool_info_resp}")

            base_token_info = self.get_token_by_address(base_token_address)
            quote_token_info = self.get_token_by_address(quote_token_address)

            base_symbol = base_token_info.get("symbol") if base_token_info else base_token_address
            quote_symbol = quote_token_info.get("symbol") if quote_token_info else quote_token_address

            trading_pair = f"{base_symbol}-{quote_symbol}"

            return {
                "trading_pair": trading_pair,
                "base_token": base_symbol,
                "quote_token": quote_symbol,
                "base_token_address": base_token_address,
                "quote_token_address": quote_token_address,
            }

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error resolving trading pair from pool {pool_address}: {str(e)}", exc_info=True)
            return None

    def add_liquidity(
        self,
        trading_pair: str,
        price: float,
        dex_name: str,
        trading_type: str = "clmm",
        **request_args
    ) -> str:
        """
        Adds liquidity to a pool - either concentrated (CLMM) or regular (AMM).

        :param trading_pair: The market trading pair
        :param price: The center price for the position.
        :param dex_name: DEX protocol name (e.g., "orca", "meteora", "raydium")
        :param trading_type: Trading type (e.g., "clmm", "amm"). Defaults to "clmm".
        :param request_args: Additional arguments for liquidity addition
        :return: A newly created order id (internal).
        """
        trade_type: TradeType = TradeType.RANGE
        order_id: str = self.create_market_order_id(trade_type, trading_pair)

        if trading_type == "clmm":
            safe_ensure_future(self._clmm_add_liquidity(trade_type, order_id, trading_pair, price, dex_name=dex_name, trading_type=trading_type, **request_args))
        elif trading_type == "amm":
            safe_ensure_future(self._amm_add_liquidity(trade_type, order_id, trading_pair, price, dex_name=dex_name, trading_type=trading_type, **request_args))
        else:
            raise ValueError(f"Trading type {trading_type} does not support liquidity provision")

        return order_id

    async def _clmm_add_liquidity(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        price: float,
        lower_price: Optional[float] = None,
        upper_price: Optional[float] = None,
        upper_width_pct: Optional[float] = None,
        lower_width_pct: Optional[float] = None,
        base_token_amount: Optional[float] = None,
        quote_token_amount: Optional[float] = None,
        slippage_pct: Optional[float] = None,
        pool_address: Optional[str] = None,
        extra_params: Optional[Dict[str, Any]] = None,
        max_retries: int = 10,
        dex_name: Optional[str] = None,
        trading_type: str = "clmm",
    ):
        """Opens a concentrated liquidity position."""
        if not dex_name:
            raise ValueError("dex_name parameter is required for CLMM operations")

        tokens = trading_pair.split("-")
        if len(tokens) != 2:
            raise ValueError(f"Invalid trading pair format: {trading_pair}")

        base_token, quote_token = tokens

        base_amount = base_token_amount or 0.0
        quote_amount_in_base = (quote_token_amount or 0.0) / price if price > 0 else 0.0
        total_amount_in_base = base_amount + quote_amount_in_base

        existing_order = self._order_tracker.fetch_order(order_id)
        if existing_order is not None:
            self.logger().debug(f"Order {order_id} already tracked, skipping start_tracking_order")
        else:
            self.start_tracking_order(order_id=order_id,
                                      trading_pair=trading_pair,
                                      trade_type=trade_type,
                                      price=Decimal(str(price)),
                                      amount=Decimal(str(total_amount_in_base)),
                                      order_type=OrderType.AMM_ADD)

        if lower_price is not None and upper_price is not None:
            pass
        elif upper_width_pct is not None and lower_width_pct is not None:
            lower_width_decimal = lower_width_pct / 100.0
            upper_width_decimal = upper_width_pct / 100.0
            lower_price = price * (1 - lower_width_decimal)
            upper_price = price * (1 + upper_width_decimal)
        else:
            raise ValueError("Must provide either (lower_price and upper_price) or (upper_width_pct and lower_width_pct)")

        if not pool_address:
            pool_address = await self.get_pool_address(trading_pair, dex_name=dex_name, trading_type=trading_type)
            if not pool_address:
                raise ValueError(f"Could not find pool for {trading_pair}")

        self._lp_orders_metadata[order_id] = {
            "operation": "add",
            "lower_price": Decimal(str(lower_price)),
            "upper_price": Decimal(str(upper_price)),
            "amount": Decimal(str(total_amount_in_base)),
            "fee_tier": pool_address,
        }

        async def execute_open_position() -> Dict[str, Any]:
            return await self._get_gateway_instance().clmm_open_position(
                network=self.network,
                chain=self.chain,
                wallet_address=self.address,
                pool_address=pool_address,
                lower_price=lower_price,
                upper_price=upper_price,
                dex=dex_name,
                trading_type=trading_type,
                base_token_amount=base_token_amount,
                quote_token_amount=quote_token_amount,
                slippage_pct=slippage_pct,
                extra_params=extra_params
            )

        try:
            transaction_result = await self._execute_with_retry(
                operation=execute_open_position,
                operation_name=f"CLMM open position on {trading_pair}",
                max_retries=max_retries,
            )
            transaction_hash: Optional[str] = transaction_result.get("signature")
            if transaction_hash is not None and transaction_hash != "":
                self.update_order_from_hash(order_id, trading_pair, transaction_hash, transaction_result)
                if transaction_result.get(TX_DATA_UNAVAILABLE):
                    # The open landed on-chain but its response data is unrecoverable.
                    # Record the gap rather than writing a blank position address and
                    # zero amounts, which the caller cannot tell apart from a failure —
                    # the position is LIVE and must not be abandoned.
                    self.logger().error(
                        f"CLMM open position {transaction_hash} confirmed on-chain but Gateway's "
                        "response data is unavailable: the position address and deposited amounts "
                        "must be recovered from chain state."
                    )
                    self._lp_orders_metadata[order_id]["data_unavailable"] = True
                else:
                    data = transaction_result.get("data", {})
                    self._lp_orders_metadata[order_id].update({
                        "position_address": data.get("positionAddress", ""),
                        "base_amount": Decimal(str(data.get("baseTokenAmountAdded", 0))),
                        "quote_amount": Decimal(str(data.get("quoteTokenAmountAdded", 0))),
                        "position_rent": Decimal(str(data.get("positionRent", 0))),
                        "tx_fee": Decimal(str(data.get("fee", 0))),
                    })
                return transaction_hash
            else:
                raise ValueError("No transaction hash returned from gateway")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_operation_failure(order_id, trading_pair, "opening CLMM position", e)
            raise

    async def _amm_add_liquidity(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        price: float,
        base_token_amount: float,
        quote_token_amount: float,
        dex_name: str,
        trading_type: str = "amm",
        slippage_pct: Optional[float] = None,
        position_address: Optional[str] = None,
        max_retries: int = 10,
        pool_address: Optional[str] = None,
    ):
        """Opens a regular AMM liquidity position, in `pool_address` or else the pair's listed pool."""
        tokens = trading_pair.split("-")
        if len(tokens) != 2:
            raise ValueError(f"Invalid trading pair format: {trading_pair}")

        quote_amount_in_base = quote_token_amount / price if price > 0 else 0.0
        total_amount_in_base = base_token_amount + quote_amount_in_base

        self.start_tracking_order(order_id=order_id,
                                  trading_pair=trading_pair,
                                  trade_type=trade_type,
                                  price=Decimal(str(price)),
                                  amount=Decimal(str(total_amount_in_base)),
                                  order_type=OrderType.AMM_ADD)

        if not pool_address:
            pool_address = await self.get_pool_address(trading_pair, dex_name=dex_name, trading_type=trading_type)
        if not pool_address:
            raise ValueError(f"Could not find pool for {trading_pair}")

        async def execute_add_liquidity() -> Dict[str, Any]:
            result = await self._get_gateway_instance().amm_add_liquidity(
                network=self.network,
                chain=self.chain,
                wallet_address=self.address,
                pool_address=pool_address,
                base_token_amount=base_token_amount,
                quote_token_amount=quote_token_amount,
                dex=dex_name,
                trading_type=trading_type,
                slippage_pct=slippage_pct,
                position_address=position_address,
            )
            self._keep_pending_lp_data(order_id, result)
            return result

        try:
            # Same chokepoint as the CLMM verbs: a status 0 (broadcast, unconfirmed) or
            # -1 (landed and failed) response carries a signature too, so returning it
            # unchecked would report an unlanded or reverted add as a success.
            transaction_result = await self._execute_with_retry(
                operation=execute_add_liquidity,
                operation_name=f"AMM add liquidity on {trading_pair}",
                max_retries=max_retries,
            )
            transaction_hash: Optional[str] = transaction_result.get("signature")
            if transaction_hash is not None and transaction_hash != "":
                self.update_order_from_hash(order_id, trading_pair, transaction_hash, transaction_result)
                return transaction_hash
            else:
                raise ValueError("No transaction hash returned from gateway")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_operation_failure(order_id, trading_pair, "opening AMM position", self._lp_submit_error(e))

    def remove_liquidity(
        self,
        trading_pair: str,
        dex_name: str,
        trading_type: str = "clmm",
        position_address: Optional[str] = None,
        percentage: float = 100.0,
        **request_args
    ) -> str:
        """
        Removes liquidity from a position.

        :param trading_pair: The market trading pair
        :param dex_name: DEX protocol name (e.g., "orca", "meteora", "raydium")
        :param trading_type: Trading type (e.g., "clmm", "amm"). Defaults to "clmm".
        :param position_address: The address of the position. Required for CLMM, and for
            AMMs whose LP is non-fungible (Meteora DAMM v2 positions are NFTs and a wallet
            may hold several per pool); ignored by fungible-LP AMMs.
        :param percentage: Percentage of liquidity to remove (defaults to 100%)
        :return: A newly created order id (internal).
        """
        if trading_type == "clmm" and position_address is None:
            raise ValueError("position_address is required to close a CLMM position")

        trade_type: TradeType = TradeType.RANGE
        order_id: str = self.create_market_order_id(trade_type, trading_pair)

        if trading_type == "clmm":
            if percentage == 100.0:
                safe_ensure_future(self._clmm_close_position(trade_type, order_id, trading_pair, position_address, dex_name=dex_name, trading_type=trading_type, **request_args))
            else:
                safe_ensure_future(self._clmm_remove_liquidity(trade_type, order_id, trading_pair, position_address, percentage, dex_name=dex_name, trading_type=trading_type, **request_args))
        elif trading_type == "amm":
            safe_ensure_future(self._amm_remove_liquidity(trade_type, order_id, trading_pair, percentage, dex_name=dex_name, trading_type=trading_type, position_address=position_address, **request_args))
        else:
            raise ValueError(f"Trading type {trading_type} does not support liquidity provision")

        return order_id

    async def _clmm_close_position(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        position_address: str,
        fail_silently: bool = False,
        max_retries: int = 10,
        dex_name: Optional[str] = None,
        trading_type: str = "clmm",
        slippage_pct: Optional[float] = None,
    ):
        """Closes a concentrated liquidity position."""
        if not dex_name:
            raise ValueError("dex_name parameter is required for CLMM operations")

        existing_order = self._order_tracker.fetch_order(order_id)
        if existing_order is not None:
            self.logger().debug(f"Order {order_id} already tracked, skipping start_tracking_order")
        else:
            self.start_tracking_order(order_id=order_id,
                                      trading_pair=trading_pair,
                                      trade_type=trade_type,
                                      order_type=OrderType.AMM_REMOVE)

        self._lp_orders_metadata[order_id] = {
            "operation": "remove",
            "position_address": position_address,
        }

        _dex_name = dex_name
        _trading_type = trading_type
        _network = self.network

        _slippage_pct = slippage_pct

        async def execute_close_position() -> Dict[str, Any]:
            return await self._get_gateway_instance().clmm_close_position(
                network=_network,
                chain=self.chain,
                wallet_address=self.address,
                position_address=position_address,
                dex=_dex_name,
                trading_type=_trading_type,
                fail_silently=fail_silently,
                slippage_pct=_slippage_pct,
            )

        try:
            # Close retry policy lives in the LP executor's CLOSING loop, which passes
            # max_retries=0 for a single attempt per re-entry and reconciles a
            # landed-but-unconfirmed attempt against fresh position state before
            # re-submitting — a blind retry here cannot do that check.
            transaction_result = await self._execute_with_retry(
                operation=execute_close_position,
                operation_name=f"CLMM close position {position_address}",
                max_retries=max_retries,
            )
            transaction_hash: Optional[str] = transaction_result.get("signature")
            if transaction_hash is not None and transaction_hash != "":
                self.update_order_from_hash(order_id, trading_pair, transaction_hash, transaction_result)
                if transaction_result.get(TX_DATA_UNAVAILABLE):
                    # The close landed but its response data is unrecoverable. Writing the
                    # six financial keys as 0 here would silently book real collected fees
                    # and a real rent refund as zero, so record the gap instead and let the
                    # caller report incomplete accounting.
                    self.logger().error(
                        f"CLMM close position {transaction_hash} confirmed on-chain but Gateway's "
                        "response data is unavailable: collected fees, removed amounts and rent "
                        "refund could NOT be booked for this close."
                    )
                    self._lp_orders_metadata[order_id]["data_unavailable"] = True
                else:
                    data = transaction_result.get("data", {})
                    # ClosePositionResponse.data declares the collected-fee and rent keys
                    # (unlike RemoveLiquidityResponse) — the LP executor books them from
                    # this metadata, so dropping them would zero close-time fee/rent PnL.
                    self._lp_orders_metadata[order_id].update({
                        "base_amount": Decimal(str(data.get("baseTokenAmountRemoved", 0))),
                        "quote_amount": Decimal(str(data.get("quoteTokenAmountRemoved", 0))),
                        "base_fee": Decimal(str(data.get("baseFeeAmountCollected", 0))),
                        "quote_fee": Decimal(str(data.get("quoteFeeAmountCollected", 0))),
                        "position_rent_refunded": Decimal(str(data.get("positionRentRefunded", 0))),
                        "tx_fee": Decimal(str(data.get("fee", 0))),
                    })
                return transaction_hash
            else:
                raise ValueError("No transaction hash returned from gateway")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_operation_failure(order_id, trading_pair, "closing CLMM position", e)
            raise

    async def _clmm_remove_liquidity(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        position_address: str,
        percentage: float,
        dex_name: str,
        trading_type: str = "clmm",
        fail_silently: bool = False,
        max_retries: int = 10,
    ):
        """Removes liquidity from a CLMM position (partial removal)."""
        existing_order = self._order_tracker.fetch_order(order_id)
        if existing_order is not None:
            self.logger().debug(f"Order {order_id} already tracked, skipping start_tracking_order")
        else:
            self.start_tracking_order(order_id=order_id,
                                      trading_pair=trading_pair,
                                      trade_type=trade_type,
                                      order_type=OrderType.AMM_REMOVE)

        self._lp_orders_metadata[order_id] = {
            "operation": "remove",
            "position_address": position_address,
        }

        async def execute_remove_liquidity() -> Dict[str, Any]:
            return await self._get_gateway_instance().clmm_remove_liquidity(
                network=self.network,
                chain=self.chain,
                wallet_address=self.address,
                position_address=position_address,
                percentage=percentage,
                dex=dex_name,
                trading_type=trading_type,
                fail_silently=fail_silently
            )

        try:
            # Same chokepoint as _clmm_add_liquidity / _clmm_close_position: a status 0
            # (broadcast, unconfirmed) or -1 (landed and failed) response also carries a
            # signature, so returning it unchecked booked an unlanded or reverted removal
            # as a completed one.
            transaction_result = await self._execute_with_retry(
                operation=execute_remove_liquidity,
                operation_name=f"CLMM remove liquidity from {position_address}",
                max_retries=max_retries,
            )
            transaction_hash: Optional[str] = transaction_result.get("signature")
            if transaction_hash is not None and transaction_hash != "":
                self.update_order_from_hash(order_id, trading_pair, transaction_hash, transaction_result)
                if transaction_result.get(TX_DATA_UNAVAILABLE):
                    self.logger().error(
                        f"CLMM remove liquidity {transaction_hash} confirmed on-chain but Gateway's "
                        "response data is unavailable: removed amounts could NOT be booked."
                    )
                    self._lp_orders_metadata[order_id]["data_unavailable"] = True
                else:
                    data = transaction_result.get("data", {})
                    # RemoveLiquidityResponse.data carries only fee + removed amounts.
                    # Fee-collected/rent keys exist only on the CLOSE response — reading
                    # them here always yielded 0 and masqueraded as "no fees collected".
                    self._lp_orders_metadata[order_id].update({
                        "base_amount": Decimal(str(data.get("baseTokenAmountRemoved", 0))),
                        "quote_amount": Decimal(str(data.get("quoteTokenAmountRemoved", 0))),
                        "tx_fee": Decimal(str(data.get("fee", 0))),
                    })
                return transaction_hash
            else:
                raise ValueError("No transaction hash returned from gateway")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_operation_failure(order_id, trading_pair, "removing CLMM liquidity", e)

    async def _amm_remove_liquidity(
        self,
        trade_type: TradeType,
        order_id: str,
        trading_pair: str,
        percentage: float,
        dex_name: str,
        trading_type: str = "amm",
        position_address: Optional[str] = None,
        fail_silently: bool = False,
        max_retries: int = 10,
        pool_address: Optional[str] = None,
        slippage_pct: Optional[float] = None,
        lp_token_amount: Optional[Decimal] = None,
    ):
        """Removes liquidity from an AMM pool: `pool_address`, or else the pair's listed pool.

        With `lp_token_amount`, exactly those LP tokens are returned and `percentage` is not sent.
        """
        if not pool_address:
            pool_address = await self.get_pool_address(trading_pair, dex_name=dex_name, trading_type=trading_type)
        if not pool_address:
            raise ValueError(f"Could not find pool for {trading_pair}")

        self.start_tracking_order(order_id=order_id,
                                  trading_pair=trading_pair,
                                  trade_type=trade_type,
                                  order_type=OrderType.AMM_REMOVE)

        async def execute_remove_liquidity() -> Dict[str, Any]:
            result = await self._get_gateway_instance().amm_remove_liquidity(
                network=self.network,
                chain=self.chain,
                wallet_address=self.address,
                pool_address=pool_address,
                percentage=percentage,
                dex=dex_name,
                trading_type=trading_type,
                position_address=position_address,
                fail_silently=fail_silently,
                slippage_pct=slippage_pct,
                lp_token_amount=lp_token_amount,
            )
            self._keep_pending_lp_data(order_id, result)
            return result

        try:
            # Same chokepoint as the CLMM verbs — see _clmm_remove_liquidity.
            transaction_result = await self._execute_with_retry(
                operation=execute_remove_liquidity,
                operation_name=f"AMM remove liquidity on {trading_pair}",
                max_retries=max_retries,
            )
            transaction_hash: Optional[str] = transaction_result.get("signature")
            if transaction_hash is not None and transaction_hash != "":
                self.update_order_from_hash(order_id, trading_pair, transaction_hash, transaction_result)
                return transaction_hash
            else:
                raise ValueError("No transaction hash returned from gateway")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._handle_operation_failure(order_id, trading_pair, "closing AMM position", self._lp_submit_error(e))

    def _keep_pending_lp_data(self, order_id: str, result: Any):
        """Keep, with the order, the amounts an AMM add or remove answer says its transaction moves.

        A PENDING answer's `pendingData`, or a CONFIRMED answer's `data` (the same names), as for
        a swap (see _keep_pending_swap_data): with the transaction's signature, and read only by a
        caller the connector has returned that transaction to as confirmed. A PENDING answer
        without `pendingData` keeps the signature alone, so the transaction can be named while it
        is polled. Kept whatever the order's done state: a remove is tracked with amount 0, which
        already counts as done.
        """
        if not isinstance(result, dict):
            return
        signature, status = result.get("signature"), result.get("status")
        if status == 0:
            stated = result.get("pendingData") if isinstance(result.get("pendingData"), dict) else {}
        elif status == 1 and isinstance(result.get("data"), dict):
            stated = result["data"]
        else:
            return
        tracked = self._order_tracker.fetch_tracked_order(order_id)
        if signature and tracked is not None:
            tracked.pending_swap_data = (signature, dict(stated))

    def _lp_submit_error(self, error: Exception) -> Exception:
        """The failure to record for an AMM liquidity request, marked when Gateway may have submitted it.

        The rule a swap follows (see _create_order): a request that failed on the way to or from
        Gateway, on a network where a transaction never expires, may have been submitted, and
        must not be sent again before someone checks the chain.

        Gateway refuses a deposit or redeem whose pool moved past the request's bound with a 409
        that carries no code; that refusal, and only it, is marked as the slippage failure it is,
        so a wider tolerance can be tried. Nothing that can still execute was submitted for it.
        """
        if (isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError))
                and not isinstance(error, aiohttp.ClientConnectorError)
                and not self.not_found_proves_dropped):
            return Exception(f"{error!r} [code: SUBMIT_OUTCOME_UNKNOWN]")
        if (isinstance(error, GatewayError) and error.status == 409 and not error.code
                and POOL_MOVED_REFUSAL.match(error.message or "")):
            return Exception(f"{error} [code: SLIPPAGE_EXCEEDED]")
        return error

    @async_ttl_cache(ttl=5, maxsize=10)
    async def get_position_info(
        self,
        trading_pair: str,
        dex_name: str,
        trading_type: str = "clmm",
        position_address: Optional[str] = None
    ) -> Union[AMMPositionInfo, CLMMPositionInfo, None]:
        """Cached position info for display/analytics consumers.

        WARNING: the TTL cache stores None ("position gone") like any other value,
        so a single 404 is served for up to 5s of subsequent calls. Anything that
        makes an existence DECISION from this signal (close pre-flight, external-
        close detection) must use get_position_info_fresh instead — otherwise one
        read can masquerade as several independent confirmations.
        """
        return await self.get_position_info_fresh(
            trading_pair=trading_pair,
            dex_name=dex_name,
            trading_type=trading_type,
            position_address=position_address,
        )

    async def get_position_info_fresh(
        self,
        trading_pair: str,
        dex_name: str,
        trading_type: str = "clmm",
        position_address: Optional[str] = None
    ) -> Union[AMMPositionInfo, CLMMPositionInfo, None]:
        """Uncached position info read.

        Contract: returns None only when Gateway definitively reports the position
        does not exist on-chain; transient errors (RPC/network/5xx, unrelated 404s
        like a missing route or wallet) raise instead of being swallowed into None.
        """
        try:
            tokens = trading_pair.split("-")
            if len(tokens) != 2:
                raise ValueError(f"Invalid trading pair format: {trading_pair}")

            if trading_type == "clmm":
                if position_address is None:
                    raise ValueError("position_address is required for CLMM positions")

                resp: Dict[str, Any] = await self._get_gateway_instance().clmm_position_info(
                    network=self.network,
                    chain=self.chain,
                    position_address=position_address,
                    dex=dex_name,
                    trading_type=trading_type,
                )
                return CLMMPositionInfo(**resp) if resp else None

            elif trading_type == "amm":
                resp: Dict[str, Any] = await self._get_gateway_instance().amm_position_info(
                    network=self.network,
                    chain=self.chain,
                    pool_address=position_address,
                    wallet_address=self.address,
                    dex=dex_name,
                    trading_type=trading_type,
                )
                return AMMPositionInfo(**resp) if resp else None

            else:
                raise ValueError(f"Trading type {trading_type} does not support liquidity positions")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            addr_info = f"position {position_address}" if position_address else trading_pair
            error_msg = str(e).lower()
            # None means "the position definitively does not exist on-chain". Match only
            # position-specific Gateway messages: the HTTP client stamps "(Not Found)" on
            # EVERY 404 (missing route after a redeploy, missing wallet file, ...), so a
            # bare "not found" check would report a live position as gone. Transient
            # errors must NOT be swallowed into None: callers such as
            # LPExecutor._close_position interpret None as "already closed", and a
            # swallowed transient error would silently abandon a live position.
            # A GatewayError also carries the HTTP status: require the 404 a missing
            # position returns, so a 500 whose prose happens to name the position (or a
            # transport error carrying no status at all) still raises.
            status = e.status if isinstance(e, GatewayError) else None
            is_position_gone = (
                "position not found" in error_msg
                or "position closed" in error_msg
                or "position does not exist" in error_msg
            )
            if is_position_gone and status in (None, 404):
                self.logger().info(
                    f"Position info for {addr_info} on {dex_name}/{trading_type}: position does not exist ({e})"
                )
                return None
            self.logger().network(
                f"Error fetching position info for {addr_info} on {dex_name}/{trading_type}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            raise

    async def get_user_positions(
        self,
        dex_name: str,
        trading_type: str = "clmm",
        pool_address: Optional[str] = None
    ) -> List[Union[AMMPositionInfo, CLMMPositionInfo]]:
        """Fetch all user positions for this connector and wallet."""
        positions = []

        try:
            if trading_type == "clmm":
                response = await self._get_gateway_instance().clmm_positions_owned(
                    network=self.network,
                    chain=self.chain,
                    wallet_address=self.address,
                    dex=dex_name,
                    trading_type=trading_type,
                )
            else:
                if not pool_address:
                    self.logger().warning("AMM position fetching requires a pool address")
                    return []

                pool_resp = await self._get_gateway_instance().pool_info(
                    network=self.network,
                    chain=self.chain,
                    pool_address=pool_address,
                    dex=dex_name,
                    trading_type=trading_type,
                )

                if not pool_resp:
                    return []

                resp = await self._get_gateway_instance().amm_position_info(
                    network=self.network,
                    chain=self.chain,
                    pool_address=pool_address,
                    wallet_address=self.address,
                    dex=dex_name,
                    trading_type=trading_type,
                )

                if resp:
                    position = AMMPositionInfo(**resp)
                    base_token_info = self.get_token_by_address(position.base_token_address)
                    quote_token_info = self.get_token_by_address(position.quote_token_address)

                    position.base_token = base_token_info.get("symbol", position.base_token_address) if base_token_info else position.base_token_address
                    position.quote_token = quote_token_info.get("symbol", position.quote_token_address) if quote_token_info else position.quote_token_address
                    return [position]
                else:
                    return []

            positions_list = response if isinstance(response, list) else response.get("positions", [])
            for pos_data in positions_list:
                try:
                    if trading_type == "clmm":
                        position = CLMMPositionInfo(**pos_data)

                        base_token_info = self.get_token_by_address(position.base_token_address)
                        quote_token_info = self.get_token_by_address(position.quote_token_address)

                        position.base_token = base_token_info.get("symbol", position.base_token_address) if base_token_info else position.base_token_address
                        position.quote_token = quote_token_info.get("symbol", position.quote_token_address) if quote_token_info else position.quote_token_address

                        positions.append(position)
                    else:
                        position = AMMPositionInfo(**pos_data)

                        base_token_info = self.get_token_by_address(position.base_token_address)
                        quote_token_info = self.get_token_by_address(position.quote_token_address)

                        position.base_token = base_token_info.get("symbol", position.base_token_address) if base_token_info else position.base_token_address
                        position.quote_token = quote_token_info.get("symbol", position.quote_token_address) if quote_token_info else position.quote_token_address

                        positions.append(position)

                except Exception as e:
                    self.logger().error(f"Error parsing position data: {e}", exc_info=True)
                    continue

            if pool_address and trading_type == "clmm":
                positions = [p for p in positions if hasattr(p, 'pool_address') and p.pool_address == pool_address]

        except Exception as e:
            self.logger().error(f"Error fetching positions: {e}", exc_info=True)

        return positions
