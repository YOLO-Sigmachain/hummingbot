"""
Arbitrage between two pools of the same pair on one Ergo connector (FORK-NOTES section 2.3).

Before trading, each pool is read once (pool-info) and must hold the pair's two tokens; a
config whose slippage tolerance rounds to nothing at `order_amount` is refused too.

Each tick, when nothing is in flight, the controller quotes a BUY on one pool and a SELL on the
other for `order_amount`, in both directions. When the better direction's edge after the
network fee of both swaps is above `min_profitability`, it buys on the cheap pool with an
OrderExecutor pinned to that pool. Only once that buy is filled does it sell the bought amount
on the other pool, pinned too, at a fresh quote of that pool. The buy is placed once.

- One arbitrage at a time.
- A leg of unknown outcome (SUBMIT_OUTCOME_UNKNOWN) holds the controller (ControllerOutcomeHold).
  An unknown sell also halts it: the hedge of the filled buy is unproven. A hold from another
  controller does not hold the sell of a filled buy.
- A sell that failed definitely (with nothing booked) is re-quoted, uncached, one block later
  and placed again, up to `sell_attempts` in all. A fresh quote under the buy cost less
  `max_sell_loss_pct` is quoted again one block later; still under it, the controller halts and
  logs an ERROR naming the unhedged position. The operator decides.
- A halt and the list of possibly unhedged buys live in the process (outcome_hold), by
  controller id, connector, pools and pair: a strategy stop/start keeps them; a new process
  clears them.
"""
import math
import re
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import Field, field_validator, model_validator

from hummingbot.core.data_type.common import MarketDict, TradeType
from hummingbot.strategy_v2.controllers.controller_base import ControllerBase, ControllerConfigBase
from hummingbot.strategy_v2.executors.order_executor.data_types import ExecutionStrategy, OrderExecutorConfig
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction, ExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.utils.outcome_hold import (
    OUTCOME_UNKNOWN_ORDER_ID,
    ControllerOutcomeHold,
    process_controller_halt,
)

POOL_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")
# An executor asked for but not listed by then is taken as never started.
EXECUTOR_APPEAR_TIMEOUT = 120.0
# A missing fee price is warned about at most this often.
WARNING_INTERVAL = 60.0
# The wait before a sell retry and before a loss-floor re-quote when the connector does not know its block time.
DEFAULT_BLOCK_TIME_SECONDS = 120.0


class ErgoPoolArbConfig(ControllerConfigBase):
    controller_type: str = "generic"
    controller_name: str = "ergo_pool_arb"
    total_amount_quote: Decimal = Decimal("0")
    connector_name: str = "ergo-mainnet"
    trading_pair: str = "ERG-SigUSD"
    # Pool NFT ids (64 hex), for example a Spectrum pool and a LithosDex pool of the pair.
    pool_a: str = Field(default="", json_schema_extra={
        "prompt": "Enter the first pool id (64 hex): ", "prompt_on_new": True})
    pool_b: str = Field(default="", json_schema_extra={
        "prompt": "Enter the second pool id (64 hex): ", "prompt_on_new": True})
    # Base amount bought on one pool and sold on the other. No default: the size that trades
    # depends on the pair, and a 2-decimal quote token (SigUSD) refuses a small size, whose
    # slippage tolerance rounds to nothing in the price limit.
    order_amount: Decimal = Field(json_schema_extra={
        "prompt": "Enter the base amount per arbitrage: ", "prompt_on_new": True})
    # Net edge needed, as a fraction of the buy cost (0.02 = 2%).
    min_profitability: Decimal = Field(default=Decimal("0.02"), json_schema_extra={"is_updatable": True})
    # Price tolerance per leg, in percent, bound into each swap's price limit.
    slippage_pct: Decimal = Decimal("0.5")
    # Price of the network fee token in quote, for a pair that holds neither side of it.
    fee_token_price_quote: Optional[Decimal] = None
    # Wait after an arbitrage ends before quoting again.
    delay_between_arbitrages: float = 60.0
    # Tries at selling a filled buy, in all: each re-quotes the sell pool and places the sell at that quote.
    sell_attempts: int = 3
    # A sell is placed only while its fresh quote gives at least the buy cost less this, in percent.
    max_sell_loss_pct: Decimal = Decimal("1")

    @field_validator("pool_a", "pool_b", mode="before")
    @classmethod
    def validate_pool_id(cls, value: Any) -> str:
        pool = str(value).strip().lower() if value is not None else ""
        if not POOL_ID_PATTERN.match(pool):
            raise ValueError(f"pool id must be 64 hex characters, got {value!r}")
        return pool

    @model_validator(mode="after")
    def validate_arb(self):
        if not self.connector_name or not self.connector_name.strip():
            raise ValueError("connector_name must not be empty")
        if len(self.trading_pair.split("-")) != 2:
            raise ValueError(f"trading_pair must be BASE-QUOTE, got {self.trading_pair!r}")
        for name, pool in (("pool_a", self.pool_a), ("pool_b", self.pool_b)):
            if not POOL_ID_PATTERN.match(pool):
                raise ValueError(f"{name} must be a pool id of 64 hex characters, got {pool!r}")
        if self.pool_a == self.pool_b:
            raise ValueError("pool_a and pool_b must be two different pools")
        if self.order_amount <= 0:
            raise ValueError(f"order_amount must be greater than 0, got {self.order_amount}")
        if self.min_profitability <= 0:
            raise ValueError(f"min_profitability must be greater than 0, got {self.min_profitability}")
        if not Decimal("0") < self.slippage_pct < Decimal("100"):
            raise ValueError(f"slippage_pct must be above 0 and below 100, got {self.slippage_pct}")
        if self.fee_token_price_quote is not None and self.fee_token_price_quote <= 0:
            raise ValueError(f"fee_token_price_quote must be greater than 0, got {self.fee_token_price_quote}")
        if self.delay_between_arbitrages < 0:
            raise ValueError("delay_between_arbitrages must be 0 or more")
        # Each leg may fill up to slippage_pct worse than quoted, so the worst edge is about
        # min_profitability - 2 x slippage. min_profitability is a fraction, slippage_pct a percent.
        if self.min_profitability <= 2 * self.slippage_pct / Decimal("100"):
            raise ValueError(f"min_profitability ({self.min_profitability}, a fraction) must be above twice "
                             f"slippage_pct ({self.slippage_pct}%, so {2 * self.slippage_pct / Decimal('100')})")
        if self.sell_attempts < 1:
            raise ValueError(f"sell_attempts must be 1 or more, got {self.sell_attempts}")
        if not Decimal("0") < self.max_sell_loss_pct < Decimal("100"):
            raise ValueError(f"max_sell_loss_pct must be above 0 and below 100, got {self.max_sell_loss_pct}")
        return self

    def update_markets(self, markets: MarketDict) -> MarketDict:
        return markets.add_or_update(self.connector_name, self.trading_pair)


class ErgoPoolArb(ControllerBase):
    def __init__(self, config: ErgoPoolArbConfig, *args, **kwargs):
        self.config = config
        super().__init__(config, *args, **kwargs)
        self.base_asset, self.quote_asset = self.config.trading_pair.split("-")
        self._outcome_hold = ControllerOutcomeHold(self.logger(), controller_id=self.config.id)
        # The arbitrage in flight: pools, quoted prices, amounts and executor ids.
        self._arb: Optional[Dict[str, Any]] = None
        # The proposal found by this tick's quotes, used by this tick's actions.
        self._proposal: Optional[Dict[str, Any]] = None
        # The halt and the possibly unhedged buys live in the process, by controller id, connector,
        # pools and pair (in any case): another config of the same pools keeps selling its own buys.
        pools = "/".join(sorted((self.config.pool_a, self.config.pool_b)))
        self._halt_record = process_controller_halt(
            f"ergo_pool_arb:{self.config.id}:{self.config.connector_name}:{pools}:"
            f"{self.config.trading_pair.upper()}")
        # Set when this config was refused at start (a pool not of the pair, a tolerance of nothing).
        self._refused: Optional[str] = None
        self._pools_checked = False
        # This tick's fresh SELL quote for the filled buy's sell.
        self._sell_quote: Optional[Decimal] = None
        self._last_arb_end = 0.0
        self._last_warning = 0.0

    # ---- helpers ----

    def _now(self) -> float:
        return self.market_data_provider.time()

    def _connector(self):
        return self.market_data_provider.connectors.get(self.config.connector_name)

    def _block_time(self) -> float:
        """The connector's block time in seconds, or DEFAULT_BLOCK_TIME_SECONDS when it is not known."""
        block_time = getattr(self._connector(), "_block_time_seconds", None)
        if (not isinstance(block_time, (int, float)) or isinstance(block_time, bool)
                or not math.isfinite(block_time) or block_time <= 0):
            return DEFAULT_BLOCK_TIME_SECONDS
        return float(block_time)

    def _find_executor(self, executor_id: Optional[str]):
        return next((e for e in self.executors_info if e.id == executor_id), None)

    def _is_held(self) -> bool:
        return self._outcome_hold.is_held(self.executors_info, [self.config.connector_name])

    @property
    def _halted(self) -> Optional[str]:
        """Why the controller stopped creating executors, or None. Kept in the process."""
        return self._halt_record["reason"]

    @property
    def _possibly_unhedged(self) -> List[Dict[str, str]]:
        return self._halt_record["possibly_unhedged"]

    def _halt(self, reason: str, unhedged: Optional[str] = None):
        if unhedged is not None:
            self._note_unhedged(unhedged)
        self._halt_record["reason"] = reason
        self._arb = None
        self._proposal = None
        self.logger().error(f"ergo_pool_arb {self.config.id} stopped creating executors: {reason}")

    def _refuse(self, reason: str):
        self._refused = reason
        self.logger().error(f"ergo_pool_arb {self.config.id} refuses to trade: {reason}")

    def _note_unhedged(self, reason: str):
        """List the arbitrage's buy as possibly unhedged, until a new process."""
        arb = self._arb
        amount = arb["bought"] if arb.get("bought") is not None else arb["amount"]
        self._possibly_unhedged.append({
            "order_id": str(arb.get("buy_order_id") or f"executor {arb['buy_id']}"),
            "amount": f"{amount} {self.base_asset}",
            "pool": arb["buy_pool"],
            "price": str(arb["buy_price"]),
            "reason": reason,
        })

    def _warn(self, message: str):
        now = self._now()
        if now - self._last_warning >= WARNING_INTERVAL:
            self._last_warning = now
            self.logger().warning(message)

    @staticmethod
    def _executed_base(info) -> Decimal:
        try:
            return Decimal(str(info.custom_info.get("executed_amount_base", "0")))
        except Exception:
            return Decimal("0")

    @staticmethod
    def _is_unknown(info) -> bool:
        return bool(info.custom_info.get(OUTCOME_UNKNOWN_ORDER_ID))

    def _idle(self) -> bool:
        if self._halted is not None or self._refused is not None or self._arb is not None:
            return False
        if any(not e.is_done for e in self.executors_info):
            return False
        return self._now() >= self._last_arb_end + self.config.delay_between_arbitrages

    # ---- pools ----

    async def _check_pools(self, connector) -> bool:
        """True once both pools are read to hold the pair and the tolerance binds; refuses the config when not.

        Done once. While a pool or the token list cannot be read yet, nothing is traded and it is
        tried again on the next tick.
        """
        if self._pools_checked:
            return True
        if connector is None or not all(hasattr(connector, name) for name in (
                "get_pool_info_by_address", "pair_token_addresses", "_token_decimals")):
            self._warn(f"{self.config.connector_name} cannot read a pool's tokens; no arbitrage.")
            return False
        addresses = connector.pair_token_addresses(self.config.trading_pair)
        provider = getattr(connector, "swap_provider", None)
        if addresses is None or not provider:
            self._warn(f"Token addresses of {self.config.trading_pair} or the swap provider of "
                       f"{self.config.connector_name} are not known yet; no arbitrage.")
            return False
        dex, trading_type = connector._parse_dex_name(provider)
        base_address, quote_address = addresses
        prices = []
        for pool in (self.config.pool_a, self.config.pool_b):
            info = await connector.get_pool_info_by_address(pool, dex, trading_type)
            if info is None:
                self._warn(f"Pool {pool} could not be read; no arbitrage until it is.")
                return False
            tokens = (str(info.base_token_address).lower(), str(info.quote_token_address).lower())
            price = Decimal(str(info.price))
            if tokens == (base_address, quote_address):
                pass
            elif tokens == (quote_address, base_address):
                price = Decimal("1") / price if price.is_finite() and price > 0 else Decimal("0")
            else:
                self._refuse(f"pool {pool} holds tokens {tokens[0]} and {tokens[1]}, not {self.config.trading_pair} "
                             f"({base_address}, {quote_address}). Fix pool_a/pool_b.")
                return False
            if not price.is_finite() or price <= 0:
                self._warn(f"Pool {pool} reports no usable price; no arbitrage until it does.")
                return False
            prices.append(price)
        decimals = connector._token_decimals(self.quote_asset)
        if decimals is None:
            self._warn(f"Decimals of {self.quote_asset} are not known yet; no arbitrage.")
            return False
        # The price limit of both sides is in the quote token, so the tolerance is measured in it.
        tolerance = self.config.order_amount * self.config.slippage_pct / Decimal("100") * min(prices)
        unit = Decimal(1).scaleb(-decimals)
        if tolerance < unit:
            self._refuse(f"slippage_pct {self.config.slippage_pct}% of order_amount {self.config.order_amount} "
                         f"{self.base_asset} is {tolerance:.6g} {self.quote_asset}, under one unit ({unit}) of "
                         f"{self.quote_asset}: the price limit would bind no tolerance. Raise order_amount or "
                         "slippage_pct.")
            return False
        self._pools_checked = True
        return True

    # ---- quotes ----

    async def _quote(self, connector, is_buy: bool, amount: Decimal, pool: str, fresh: bool = False) -> Optional[Decimal]:
        """A quote on this pool, or None when there is none or it came from another pool.

        `fresh` asks Gateway again, past the connector's quote cache.
        """
        side = "BUY" if is_buy else "SELL"
        quote = connector.get_quote_price_and_pool
        if fresh:
            quote = getattr(connector, "get_quote_price_and_pool_fresh", quote)
        price, used = await quote(self.config.trading_pair, is_buy, amount, pool_address=pool)
        if price is None:
            return None
        if used != pool:
            self._warn(f"{side} quote asked of pool {pool} came from pool {used}; skipped.")
            return None
        if not price.is_finite() or price <= 0:
            return None
        return price

    def _fee_in_quote(self, connector, buy_price: Decimal) -> Optional[Decimal]:
        """The network fee of one swap in quote, or None when it cannot be priced."""
        fee = getattr(connector, "network_transaction_fee", None)
        if fee is None:
            return None
        amount = Decimal(str(fee.amount))
        if fee.token == self.quote_asset:
            return amount
        if fee.token == self.base_asset:
            return amount * buy_price
        if self.config.fee_token_price_quote is not None:
            return amount * self.config.fee_token_price_quote
        return None

    async def _find_proposal(self) -> Optional[Dict[str, Any]]:
        connector = self._connector()
        if connector is None or not hasattr(connector, "get_quote_price_and_pool"):
            self._warn(f"{self.config.connector_name} cannot quote a named pool; no arbitrage.")
            return None
        amount = self.market_data_provider.quantize_order_amount(
            self.config.connector_name, self.config.trading_pair, self.config.order_amount)
        if amount <= 0:
            return None
        best = None
        for buy_pool, sell_pool in ((self.config.pool_a, self.config.pool_b),
                                    (self.config.pool_b, self.config.pool_a)):
            buy_price = await self._quote(connector, True, amount, buy_pool)
            sell_price = await self._quote(connector, False, amount, sell_pool)
            if buy_price is None or sell_price is None:
                continue
            fee = self._fee_in_quote(connector, buy_price)
            if fee is None:
                self._warn(f"Network fee of {self.config.connector_name} cannot be priced in "
                           f"{self.quote_asset}; set fee_token_price_quote. No arbitrage.")
                return None
            cost = amount * buy_price
            edge = (amount * sell_price - cost - 2 * fee) / cost
            self.logger().debug(f"Buy on {buy_pool} at {buy_price}, sell on {sell_pool} at {sell_price}: "
                                f"net edge {edge:.6f}.")
            if edge > self.config.min_profitability and (best is None or edge > best["edge"]):
                best = {"buy_pool": buy_pool, "sell_pool": sell_pool, "buy_price": buy_price,
                        "sell_price": sell_price, "amount": amount, "edge": edge}
        return best

    # ---- executors ----

    def _leg(self, side: TradeType, amount: Decimal, price: Decimal, pool: str) -> OrderExecutorConfig:
        return OrderExecutorConfig(
            timestamp=self._now(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=side,
            amount=amount,
            price=price,
            execution_strategy=ExecutionStrategy.LIMIT,
            pool_address=pool,
            slippage_pct=self.config.slippage_pct,
            max_slippage_pct=self.config.slippage_pct,
            # One attempt per leg; a failed leg is never placed again.
            max_retries=0,
            controller_id=self.config.id,
            level_id=f"ergo_pool_arb_{side.name.lower()}",
        )

    def _start_buy(self, proposal: Dict[str, Any]) -> ExecutorAction:
        config = self._leg(TradeType.BUY, proposal["amount"], proposal["buy_price"], proposal["buy_pool"])
        self._arb = dict(proposal, buy_id=config.id, buy_seen=False, buy_order_id=None, sell_id=None,
                         sell_seen=False, created_at=self._now(), bought=None, sell_attempts_used=0,
                         next_sell_at=0.0, floor_breach_at=None)
        self.logger().info(
            f"Arbitrage: buy {proposal['amount']} {self.base_asset} on pool {proposal['buy_pool']} at "
            f"{proposal['buy_price']}, then sell on pool {proposal['sell_pool']} at {proposal['sell_price']} "
            f"(net edge {proposal['edge']:.4%}).")
        return CreateExecutorAction(executor_config=config, controller_id=self.config.id)

    def _sell_amount(self, bought: Decimal) -> Decimal:
        return self.market_data_provider.quantize_order_amount(
            self.config.connector_name, self.config.trading_pair, bought)

    def _sell_amount_due(self) -> Optional[Decimal]:
        """The amount a sell is due for, before this tick's actions, or None when no sell is due.

        Due once the buy is done with a fill, and again one block after a failed sell attempt or
        a quote under the loss floor.
        """
        arb = self._arb
        if arb is None or arb["sell_id"] is not None or self._now() < arb["next_sell_at"]:
            return None
        bought = arb["bought"]
        if bought is None:
            info = self._find_executor(arb["buy_id"])
            if info is None or not info.is_done or self._is_unknown(info):
                return None
            bought = self._executed_base(info)
            if bought <= 0:
                return None
        return self._sell_amount(bought)

    def _place_sell(self) -> List[ExecutorAction]:
        """Place the sell of the filled buy at this tick's fresh quote of the sell pool."""
        arb = self._arb
        quote, self._sell_quote = self._sell_quote, None
        if self._now() < arb["next_sell_at"]:
            # Waiting one block after a failed attempt or a quote under the loss floor.
            return []
        if quote is None:
            self._warn(f"No fresh SELL quote on pool {arb['sell_pool']} yet: {self._unhedged()}. Quoting again.")
            return []
        amount = self._sell_amount(arb["bought"])
        floor = arb["bought"] * arb["buy_price"] * (Decimal("1") - self.config.max_sell_loss_pct / Decimal("100"))
        if amount * quote < floor:
            if arb["floor_breach_at"] is None:
                # One quote may price an unconfirmed box: quote again one block later before halting.
                wait = self._block_time()
                arb.update(floor_breach_at=self._now(), next_sell_at=self._now() + wait)
                self.logger().warning(
                    f"The fresh SELL quote on pool {arb['sell_pool']} is {quote}, so {amount * quote} "
                    f"{self.quote_asset} for {amount}, under the loss floor {floor}: {self._unhedged()}. "
                    f"Quoting again in one block ({wait:.0f} s).")
                return []
            self._halt(f"the fresh SELL quote on pool {arb['sell_pool']} is {quote}, so {amount * quote} "
                       f"{self.quote_asset} for {amount}, under {floor} (the buy cost less max_sell_loss_pct "
                       f"{self.config.max_sell_loss_pct}%), still one block after the first quote under it: "
                       f"{self._unhedged()}.",
                       unhedged="sell quote under the loss floor")
            return []
        arb["floor_breach_at"] = None
        attempt = arb["sell_attempts_used"] + 1
        config = self._leg(TradeType.SELL, amount, quote, arb["sell_pool"])
        arb.update(sell_id=config.id, sell_seen=False, created_at=self._now(), sell_price=quote,
                   sell_attempts_used=attempt)
        self.logger().info(f"Arbitrage: selling {amount} {self.base_asset} bought on pool {arb['buy_pool']} on pool "
                           f"{arb['sell_pool']} at {quote} (attempt {attempt} of {self.config.sell_attempts}).")
        return [CreateExecutorAction(executor_config=config, controller_id=self.config.id)]

    def _unhedged(self) -> str:
        arb = self._arb
        return (f"{arb['bought']} {self.base_asset} bought on pool {arb['buy_pool']} at {arb['buy_price']} "
                f"(executor {arb['buy_id']}) is not sold on pool {arb['sell_pool']}")

    def _follow_buy(self, held: bool) -> List[ExecutorAction]:
        arb = self._arb
        info = self._find_executor(arb["buy_id"])
        if info is None:
            if arb["buy_seen"]:
                self._halt(f"buy executor {arb['buy_id']} on pool {arb['buy_pool']} disappeared before its "
                           "end was seen; check the wallet.", unhedged="buy executor disappeared")
            elif self._now() - arb["created_at"] > EXECUTOR_APPEAR_TIMEOUT:
                self.logger().warning(f"Buy executor {arb['buy_id']} never started; arbitrage dropped.")
                self._arb, self._last_arb_end = None, self._now()
            return []
        arb["buy_seen"] = True
        arb["buy_order_id"] = (info.custom_info.get(OUTCOME_UNKNOWN_ORDER_ID) or info.custom_info.get("order_id")
                               or arb["buy_order_id"])
        if not info.is_done:
            return []
        if self._is_unknown(info):
            self.logger().error(
                f"Buy on pool {arb['buy_pool']} (executor {arb['buy_id']}) may have been submitted; the sell is "
                f"not placed. If it lands, {arb['amount']} {self.base_asset} is held unhedged. Holding.")
            self._note_unhedged("buy outcome unknown")
            self._arb, self._last_arb_end = None, self._now()
            return []
        bought = self._executed_base(info)
        if bought <= 0:
            if info.close_type in (CloseType.POSITION_HOLD, CloseType.COMPLETED):
                self._halt(f"buy on pool {arb['buy_pool']} (executor {arb['buy_id']}) completed ({info.close_type}) "
                           "but no fill was booked; check the chain.", unhedged="buy completed, no fill booked")
                return []
            self.logger().warning(f"Buy on pool {arb['buy_pool']} (executor {arb['buy_id']}) ended "
                                  f"{info.close_type} with nothing bought; arbitrage dropped.")
            self._arb, self._last_arb_end = None, self._now()
            return []
        arb["bought"] = bought
        if held:
            # The hold stops new exposure; this sell reduces it, so it is not held.
            self.logger().warning(
                f"An unknown outcome on {self.config.connector_name} holds new trades, but not this sell: "
                f"{bought} {self.base_asset} bought on pool {arb['buy_pool']} (executor {arb['buy_id']}) is sold "
                f"now on pool {arb['sell_pool']} at a fresh quote.")
        return self._place_sell()

    def _follow_sell(self) -> List[ExecutorAction]:
        arb = self._arb
        info = self._find_executor(arb["sell_id"])
        if info is None:
            if arb["sell_seen"]:
                self._halt(f"sell executor {arb['sell_id']} disappeared before its end was seen; "
                           f"{self._unhedged()} unless it landed. Check the wallet.",
                           unhedged="sell executor disappeared")
            elif self._now() - arb["created_at"] > EXECUTOR_APPEAR_TIMEOUT:
                self._halt(f"sell executor {arb['sell_id']} never started: {self._unhedged()}.",
                           unhedged="sell executor never started")
            return []
        arb["sell_seen"] = True
        if not info.is_done:
            return []
        if self._is_unknown(info):
            self._halt(f"sell (executor {arb['sell_id']}) may have been submitted; if it did not land, "
                       f"{self._unhedged()}. Check the chain.", unhedged="sell outcome unknown")
            return []
        sold = self._executed_base(info)
        if sold > 0 and info.close_type not in (CloseType.POSITION_HOLD, CloseType.COMPLETED):
            # A retry would sell the full amount again on top of what this attempt booked.
            self._halt(f"sell (executor {arb['sell_id']}) ended {info.close_type} with {sold} {self.base_asset} "
                       f"booked, not placed again: {self._unhedged()} in full. Check the chain.",
                       unhedged=f"sell ended {info.close_type} with {sold} booked")
            return []
        if sold <= 0:
            if arb["sell_attempts_used"] < self.config.sell_attempts:
                wait = self._block_time()
                self.logger().warning(
                    f"Sell attempt {arb['sell_attempts_used']} of {self.config.sell_attempts} failed "
                    f"({info.close_type}, executor {arb['sell_id']}); re-quoting pool {arb['sell_pool']} in one "
                    f"block ({wait:.0f} s).")
                arb.update(sell_id=None, next_sell_at=self._now() + wait)
                return []
            self._halt(f"sell failed {arb['sell_attempts_used']} times (last {info.close_type}, executor "
                       f"{arb['sell_id']}), not placed again: {self._unhedged()}.", unhedged="sell failed")
            return []
        self.logger().info(f"Arbitrage done: bought {arb['bought']} on pool {arb['buy_pool']}, sold {sold} "
                           f"on pool {arb['sell_pool']}.")
        self._arb, self._last_arb_end = None, self._now()
        return []

    # ---- controller ----

    async def update_processed_data(self):
        connector = self._connector()
        connectors = {self.config.connector_name: connector} if connector is not None else {}
        await self._outcome_hold.refresh_due(self.executors_info, connectors, self._now())
        self._proposal = None
        self._sell_quote = None
        if self._halted is not None or self._refused is not None:
            return
        if not await self._check_pools(connector):
            return
        amount = self._sell_amount_due()
        if amount is not None:
            # A fresh quote for the sell, uncached, whatever any hold says: the sell hedges a filled buy.
            self._sell_quote = await self._quote(connector, False, amount, self._arb["sell_pool"], fresh=True)
            return
        if self._idle() and not self._is_held():
            self._proposal = await self._find_proposal()

    def determine_executor_actions(self) -> List[ExecutorAction]:
        if self._halted is not None or self._refused is not None:
            return []
        held = self._is_held()
        if self._arb is not None:
            if self._arb["bought"] is None:
                return self._follow_buy(held)
            if self._arb["sell_id"] is None:
                return self._place_sell()
            return self._follow_sell()
        proposal, self._proposal = self._proposal, None
        if held or proposal is None or not self._idle():
            return []
        return [self._start_buy(proposal)]

    def get_custom_info(self) -> dict:
        return {"halted": self._halted,
                "refused": self._refused,
                "arbitrage": {k: str(v) for k, v in self._arb.items()} if self._arb else None,
                "possibly_unhedged": [dict(entry) for entry in self._possibly_unhedged]}

    def to_format_status(self) -> List[str]:
        lines = [f"ergo_pool_arb {self.config.trading_pair} on {self.config.connector_name}: "
                 f"pools {self.config.pool_a} / {self.config.pool_b}"]
        if self._refused is not None:
            lines.append(f"REFUSED: {self._refused}")
        elif self._halted is not None:
            lines.append(f"HALTED: {self._halted}")
        elif self._arb is not None:
            leg = "buy" if self._arb["bought"] is None else "sell"
            lines.append(f"In flight: {leg} leg, buy pool {self._arb['buy_pool']}, sell pool {self._arb['sell_pool']}")
        else:
            lines.append("Idle")
        if self._possibly_unhedged:
            lines.append("Possibly unhedged buys (kept until a new process; check the chain):")
            for entry in self._possibly_unhedged:
                lines.append(f"  order {entry['order_id']}: {entry['amount']} on pool {entry['pool']} at "
                             f"{entry['price']} ({entry['reason']})")
        return lines
