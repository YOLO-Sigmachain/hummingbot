"""The LP executor holding a share of an AMM (constant-product) pool.

On an AMM pool the position is the LP tokens this executor's add acquired, in one pool. It
has no range, its fees accrue inside the pool's reserves, and the wallet may hold more LP of
the same pool that is not this position's. The pool below is a plain constant-product pool
whose reserves and LP supply move exactly as a deposit, a redeem or a swap moves them, and
whose wallet LP is counted the two ways Gateway counts it: confirmed, which position-info
reports, and spendable, which a remove takes its percentage of.
"""
import asyncio
from contextlib import suppress
from decimal import ROUND_DOWN, Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock

from pydantic import ValidationError

from hummingbot.connector.gateway.gateway import AMMPoolInfo, AMMPositionInfo, Gateway
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.event.events import RangePositionLiquidityAddedEvent, RangePositionLiquidityRemovedEvent
from hummingbot.core.gateway.gateway_error import GatewayError
from hummingbot.strategy.strategy_v2_base import StrategyV2Base
from hummingbot.strategy_v2.executors.lp_executor.data_types import (
    LPExecutorConfig,
    LPExecutorState,
    LPExecutorStates,
)
from hummingbot.strategy_v2.executors.lp_executor.lp_executor import LPExecutor
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executors import CloseType

NETWORK = "ergo-mainnet"
POOL = "pool-nft-1"
WALLET = "9wallet"
# The claim key of the pool: the connector's chain and network, the wallet, the pool id in lower case.
CLAIM = ("ergo", "mainnet", WALLET, POOL)
TX_ADD = "tx-add"
TX_REMOVE = "tx-remove"
SIGNATURE = "6a" * 32
# Gateway's 409 when the pool moved past the request's bound, as its body renders client side.
POOL_MOVED = GatewayError(
    "The pool moved: the pool price is now 2.1 TOK per ERG, more than 0.05% from 2 on pool pool-nft-1. "
    "Nothing that can still execute was submitted; retry with a fresh quote.",
    status=409, http_error="HttpError")


def recorded_reason(error: Exception) -> str:
    """The failure reason the Gateway connector records for this answer to an AMM request."""
    return str(Gateway._lp_submit_error(SimpleNamespace(not_found_proves_dropped=False), error))


class FakePool:
    """A constant-product pool and one wallet's LP of it.

    `wallet_lp` is the wallet's confirmed LP. The wallet's transactions still in the mempool
    (`mempool`, someone else's deposit or redeem from the same wallet) change only what it
    can spend: `spendable_lp` adds their LP outputs and leaves out the LP their inputs spend.
    They change the pool when they confirm. LP is counted in whole units, as an LP token with
    no decimals is.
    """

    LP_UNIT = Decimal("1")

    def __init__(self, base=Decimal("1000"), quote=Decimal("2000"), supply=Decimal("1000"), wallet_lp=Decimal("0")):
        self.base, self.quote, self.supply, self.wallet_lp = base, quote, supply, wallet_lp
        self.mempool = []

    @property
    def price(self) -> Decimal:
        return self.quote / self.base

    @property
    def spendable_lp(self) -> Decimal:
        pending = sum((tx["lp"] if tx["kind"] == "deposit" else -tx["lp"] for tx in self.mempool), Decimal("0"))
        return self.wallet_lp + pending

    def holding(self) -> AMMPositionInfo:
        return AMMPositionInfo(
            poolAddress=POOL, walletAddress=WALLET, baseTokenAddress="ERG", quoteTokenAddress="tok",
            lpTokenAmount=float(self.wallet_lp),
            baseTokenAmount=float(self.base * self.wallet_lp / self.supply),
            quoteTokenAmount=float(self.quote * self.wallet_lp / self.supply),
            price=float(self.price),
        )

    def deposit(self, base: Decimal, quote: Decimal) -> Decimal:
        lp = min(base * self.supply / self.base, quote * self.supply / self.quote)
        self.base += lp * self.base / self.supply
        self.quote += lp * self.quote / self.supply
        self.supply += lp
        self.wallet_lp += lp
        return lp

    def redeem(self, lp: Decimal):
        self.base -= lp * self.base / self.supply
        self.quote -= lp * self.quote / self.supply
        self.supply -= lp
        self.wallet_lp -= lp

    def deposit_unconfirmed(self, base: Decimal, quote: Decimal):
        lp = min(base * self.supply / self.base, quote * self.supply / self.quote)
        self.mempool.append({"kind": "deposit", "base": base, "quote": quote, "lp": lp})

    def redeem_unconfirmed(self, lp: Decimal):
        self.mempool.append({"kind": "redeem", "lp": lp})

    def confirm_mempool(self):
        for tx in self.mempool:
            if tx["kind"] == "deposit":
                self.deposit(tx["base"], tx["quote"])
            else:
                self.redeem(tx["lp"])
        self.mempool = []

    def move_to(self, base: Decimal, quote: Decimal):
        """Someone else's swaps leave the reserves at these amounts."""
        self.base, self.quote = base, quote


class AMMPoolShareTestBase(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self) -> None:
        super().setUp()
        LPExecutor._amm_pool_claims.clear()
        LPExecutor._amm_pool_claim_reasons.clear()
        LPExecutor._amm_pool_locks.clear()
        self.addCleanup(LPExecutor._amm_pool_claims.clear)
        self.addCleanup(LPExecutor._amm_pool_claim_reasons.clear)
        self.addCleanup(LPExecutor._amm_pool_locks.clear)
        self.pool = FakePool()
        self.pending = {}  # order_id -> (signature, stated amounts) the connector kept
        self.add_failure = None  # reason the next add fails with, if any
        self.remove_failure = None
        # Whether the fake Gateway states, in its answers, what each add and remove moved.
        self.gateway_states_amounts = False
        # What Gateway's poll says of a transaction by its id: 0 while it is pending.
        self.tx_status = {"txStatus": 0}
        self.now = 1234567890.0
        self.connector = self.a_connector()
        self.strategy = MagicMock(spec=StrategyV2Base)
        type(self.strategy).current_timestamp = PropertyMock(side_effect=lambda: self.now)
        self.strategy.connectors = {NETWORK: self.connector}

    def a_connector(self):
        connector = MagicMock()
        connector.address = WALLET
        connector.chain, connector.network = "ergo", "mainnet"
        # No block time read from the network config unless a test sets one.
        connector._block_time_seconds, connector._block_confirmations = None, 1
        connector._get_gateway_instance.return_value.get_transaction_status = AsyncMock(
            side_effect=lambda *args, **kwargs: self.tx_status)
        connector.not_found_proves_dropped = False
        ids = iter(f"range-{i}" for i in range(1, 100))
        connector.create_market_order_id.side_effect = lambda *args: next(ids)
        connector._native_currency = "ERG"
        connector._lp_orders_metadata = {}
        connector.get_position_info_fresh = AsyncMock(side_effect=lambda **kwargs: self.pool.holding())
        connector.get_pool_info_by_address = AsyncMock(side_effect=lambda *args, **kwargs: AMMPoolInfo(
            address=POOL, baseTokenAddress="ERG", quoteTokenAddress="tok", price=float(self.pool.price),
            feePct=0.3, baseTokenAmount=float(self.pool.base), quoteTokenAmount=float(self.pool.quote),
        ))
        connector._amm_add_liquidity = AsyncMock(side_effect=self._add)
        connector._amm_remove_liquidity = AsyncMock(side_effect=self._remove)
        connector.get_order.side_effect = lambda order_id: SimpleNamespace(pending_swap_data=self.pending.get(order_id))
        self.failure_reasons = {}
        connector.order_failure_reason.side_effect = lambda order_id: self.failure_reasons.get(order_id)
        connector._trigger_add_liquidity_event = MagicMock(
            side_effect=lambda **kwargs: RangePositionLiquidityAddedEvent(timestamp=0, token_id=0, **kwargs))
        connector._trigger_remove_liquidity_event = MagicMock(
            side_effect=lambda **kwargs: RangePositionLiquidityRemovedEvent(timestamp=0, **kwargs))
        return connector

    async def _add(self, **kwargs):
        if self.add_failure is not None:
            self.failure_reasons[kwargs["order_id"]] = self.add_failure
            return None
        base, quote = self.pool.base, self.pool.quote
        lp = self.pool.deposit(Decimal(str(kwargs["base_token_amount"])), Decimal(str(kwargs["quote_token_amount"])))
        if self.gateway_states_amounts:
            self.pending[kwargs["order_id"]] = (TX_ADD, {
                "fee": "0.0011", "poolAddress": POOL, "baseTokenAmountAdded": str(self.pool.base - base),
                "quoteTokenAmountAdded": str(self.pool.quote - quote), "lpTokenAmountAdded": str(lp)})
        return TX_ADD

    async def _remove(self, **kwargs):
        if self.remove_failure is not None:
            self.failure_reasons[kwargs["order_id"]] = self.remove_failure
            return None
        spendable = self.pool.spendable_lp
        if kwargs.get("lp_token_amount") is not None:
            lp = Decimal(str(kwargs["lp_token_amount"]))
            if lp > spendable:
                self.failure_reasons[kwargs["order_id"]] = (
                    f"Gateway error: lpTokenAmount {lp} is more than the {spendable} LP tokens of pool {POOL} the "
                    "wallet can spend; nothing was removed (HttpError)")
                return None
        else:
            # Gateway takes the percentage of the LP the wallet can spend, unconfirmed transactions
            # included, rounded down to a whole LP unit.
            lp = (spendable * Decimal(str(kwargs["percentage"])) / Decimal("100")).quantize(
                FakePool.LP_UNIT, rounding=ROUND_DOWN)
        # The remove and the wallet's transactions in the mempool confirm in one block.
        self.pool.confirm_mempool()
        base, quote = self.pool.base, self.pool.quote
        self.pool.redeem(lp)
        if self.gateway_states_amounts:
            self.pending[kwargs["order_id"]] = (TX_REMOVE, {
                "fee": "0.0012", "poolAddress": POOL, "baseTokenAmountRemoved": str(base - self.pool.base),
                "quoteTokenAmountRemoved": str(quote - self.pool.quote), "lpTokenAmountRemoved": str(lp)})
        return TX_REMOVE

    def a_config(self, **overrides) -> LPExecutorConfig:
        fields = dict(
            id="amm-lp-1", timestamp=1234567890, connector_name=NETWORK, lp_provider="ergodex/amm",
            swap_provider="ergodex/amm", trading_pair="ERG-TOK", pool_address=POOL,
            base_amount=Decimal("10"), quote_amount=Decimal("20"), side=TradeType.RANGE,
            upper_limit_price=Decimal("2.5"), keep_position=True,
        )
        fields.update(overrides)
        return LPExecutorConfig(**fields)

    def an_executor(self, **overrides) -> LPExecutor:
        executor = LPExecutor(self.strategy, self.a_config(**overrides), update_interval=1.0)
        executor._get_native_to_quote_rate = MagicMock(return_value=Decimal("1"))
        executor._status = RunnableStatus.RUNNING
        self.set_loggers(loggers=[executor.logger()])
        return executor

    async def open(self, executor: LPExecutor):
        await executor.control_task()  # NOT_ACTIVE -> OPENING -> add -> confirmed

    async def next_refresh(self, executor: LPExecutor):
        """One tick, one position_refresh_interval after the last."""
        self.now += executor.config.position_refresh_interval
        await executor.control_task()

    async def run_to_limit_exit_and_close(self, executor: LPExecutor):
        await executor.control_task()  # holding at 2 per base: in range, below the limit
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)
        self.pool.move_to(Decimal("808"), Decimal("2525"))  # 3.125 per base, above 2.5
        await self.next_refresh(executor)  # one reading past the limit does not close
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)
        await self.next_refresh(executor)  # the next, one interval later, still past it: closes
        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        await executor.control_task()  # removes this position's LP


class TestAMMPoolShareCycle(AMMPoolShareTestBase):

    async def test_open_hold_limit_exit_and_close_are_booked_from_pool_readings(self):
        executor = self.an_executor()

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.IN_RANGE, state.state)
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(Decimal("10"), state.initial_base_amount)
        self.assertEqual(Decimal("20"), state.initial_quote_amount)
        self.assertEqual(Decimal("2"), state.add_mid_price)
        add = self.connector._amm_add_liquidity.call_args.kwargs
        self.assertEqual(POOL, add["pool_address"])
        self.assertEqual(10.0, add["base_token_amount"])
        self.assertEqual(20.0, add["quote_token_amount"])
        self.assertEqual("amm", add["trading_type"])
        self.assertEqual(0.05, add["slippage_pct"])

        await self.run_to_limit_exit_and_close(executor)

        remove = self.connector._amm_remove_liquidity.call_args.kwargs
        self.assertEqual(100.0, remove["percentage"])
        self.assertIsNone(remove["lp_token_amount"])
        self.assertEqual(POOL, remove["pool_address"])
        self.assertEqual(0, remove["max_retries"])
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertIsNone(state.position_address)
        # 10 of 1010 LP of a pool now at 808 base and 2525 quote.
        self.assertEqual(Decimal("8"), state.base_amount)
        self.assertEqual(Decimal("25"), state.quote_amount)
        self.assertEqual(Decimal("0"), state.lp_token_residue)
        self.assertEqual(TX_REMOVE, state.close_tx_hash)
        # Deposited 10 base and 20 quote, took out 8 and 25: a SELL of 2 base for 5 quote.
        orders = [o for o in executor._held_position_orders if o["executed_amount_base"]]
        self.assertEqual(1, len(orders))
        self.assertEqual("SELL", orders[0]["trade_type"])
        self.assertEqual(2.0, orders[0]["executed_amount_base"])
        self.assertEqual(5.0, orders[0]["executed_amount_quote"])
        self.assertFalse(state.open_data_unavailable)
        self.assertFalse(state.close_data_unavailable)

    async def test_amounts_worked_out_from_readings_are_reported_as_estimates(self):
        executor = self.an_executor()
        await self.open(executor)

        info = executor.get_custom_info()
        self.assertTrue(info["open_amounts_estimated"])
        self.assertFalse(info["lp_token_amount_exact"])

        await self.run_to_limit_exit_and_close(executor)

        self.assertTrue(executor.get_custom_info()["close_amounts_estimated"])

    async def test_open_hold_limit_exit_and_close_are_booked_from_pending_data(self):
        executor = self.an_executor()
        # The amounts the PENDING answers stated, kept with each order; deliberately not what
        # the pool readings would give, so the test tells the two sources apart.
        self.pending["range-1"] = (TX_ADD, {
            "baseTokenAmountAdded": 9.5, "quoteTokenAmountAdded": 19, "lpTokenAmountAdded": 10, "fee": 0.0011})
        self.pending["range-2"] = (TX_REMOVE, {
            "baseTokenAmountRemoved": "7.9", "quoteTokenAmountRemoved": "24.5", "lpTokenAmountRemoved": "10",
            "fee": "0.0012"})

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(Decimal("9.5"), state.initial_base_amount)
        self.assertEqual(Decimal("19"), state.initial_quote_amount)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(Decimal("0.0011"), state.tx_fee)

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertEqual(Decimal("7.9"), state.base_amount)
        self.assertEqual(Decimal("24.5"), state.quote_amount)
        self.assertEqual(Decimal("0.0023"), state.tx_fee)
        orders = [o for o in executor._held_position_orders if o["executed_amount_base"]]
        self.assertEqual("SELL", orders[0]["trade_type"])
        self.assertAlmostEqual(1.6, orders[0]["executed_amount_base"])
        info = executor.get_custom_info()
        self.assertFalse(info["open_amounts_estimated"])
        self.assertFalse(info["close_amounts_estimated"])

    async def test_a_position_whose_lp_gateway_stated_closes_with_exactly_that_lp(self):
        executor = self.an_executor()
        self.gateway_states_amounts = True
        await self.open(executor)
        self.assertTrue(executor.get_custom_info()["lp_token_amount_exact"])

        await self.run_to_limit_exit_and_close(executor)

        remove = self.connector._amm_remove_liquidity.call_args.kwargs
        self.assertEqual(Decimal("10"), remove["lp_token_amount"])
        self.assertEqual(Decimal("0"), executor.lp_position_state.lp_token_residue)
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)

    async def test_an_lp_amount_gateway_cannot_read_exactly_is_removed_by_percentage_and_said_so(self):
        executor = self.an_executor(upper_limit_price=None)
        self.gateway_states_amounts = True
        await self.open(executor)
        # A whole number above 2^53-1 does not reach Gateway exactly as a JSON number.
        executor.lp_position_state.lp_token_amount = Decimal(2 ** 53)
        self.pool.wallet_lp, self.pool.supply = Decimal(2 ** 53), Decimal(2 ** 54)

        executor.early_stop(keep_position=True)
        await executor.control_task()

        remove = self.connector._amm_remove_liquidity.call_args.kwargs
        self.assertIsNone(remove["lp_token_amount"])
        self.assertEqual(100.0, remove["percentage"])
        self.assertTrue(any(r.levelname == "WARNING" and "by percentage instead" in r.getMessage()
                            for r in self.log_records))

    async def test_a_pending_data_key_that_is_missing_falls_back_to_the_reading_for_that_key(self):
        executor = self.an_executor()
        self.pending["range-1"] = (TX_ADD, {"lpTokenAmountAdded": 10, "quoteTokenAmountAdded": "not a number"})

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(Decimal("10"), state.initial_base_amount)
        self.assertEqual(Decimal("20"), state.initial_quote_amount)
        self.assertEqual(Decimal("0"), state.tx_fee)
        self.assertTrue(executor.get_custom_info()["open_amounts_estimated"])

    async def test_pending_data_kept_for_another_transaction_is_not_read(self):
        executor = self.an_executor()
        self.pending["range-1"] = ("tx-other", {
            "baseTokenAmountAdded": 1, "quoteTokenAmountAdded": 1, "lpTokenAmountAdded": 1})

        await self.open(executor)

        self.assertEqual(Decimal("10"), executor.lp_position_state.lp_token_amount)
        self.assertEqual(Decimal("10"), executor.lp_position_state.initial_base_amount)

    async def test_pending_data_naming_another_pool_is_not_read(self):
        executor = self.an_executor()
        self.pending["range-1"] = (TX_ADD, {
            "poolAddress": "pool-nft-2", "baseTokenAmountAdded": 1, "quoteTokenAmountAdded": 1, "lpTokenAmountAdded": 1})

        await self.open(executor)

        self.assertEqual(Decimal("10"), executor.lp_position_state.lp_token_amount)
        self.assertFalse(executor.get_custom_info()["lp_token_amount_exact"])

    async def test_pending_data_naming_the_pool_in_another_case_is_read(self):
        executor = self.an_executor(pool_address=POOL.upper())
        self.pending["range-1"] = (TX_ADD, {
            "poolAddress": POOL, "baseTokenAmountAdded": "9.5", "quoteTokenAmountAdded": "19",
            "lpTokenAmountAdded": "10"})

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(Decimal("9.5"), state.initial_base_amount)
        self.assertEqual(Decimal("19"), state.initial_quote_amount)
        self.assertTrue(executor.get_custom_info()["lp_token_amount_exact"])

    async def test_nothing_is_booked_while_the_add_has_not_returned_confirmed(self):
        """The connector returns the signature only after poll confirms the transaction."""
        executor = self.an_executor()
        booked_before_return = []

        async def add_seen_from_inside(**kwargs):
            booked_before_return.append(executor.lp_position_state.lp_token_amount)
            self.connector._trigger_add_liquidity_event.assert_not_called()
            return await self._add(**kwargs)
        self.connector._amm_add_liquidity.side_effect = add_seen_from_inside

        await self.open(executor)

        self.assertEqual([Decimal("0")], booked_before_return)
        self.connector._trigger_add_liquidity_event.assert_called_once()

    async def test_keep_position_false_swaps_the_net_back(self):
        executor = self.an_executor(keep_position=False)
        await self.open(executor)

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(CloseType.EARLY_STOP, executor.close_type)
        self.assertEqual(LPExecutorStates.SWAPPING, executor.lp_position_state.state)
        self.assertEqual(Decimal("-2"), executor._calculate_net_base_difference())
        self.assertEqual([], executor._held_position_orders)

    async def test_early_stop_keeping_the_position_records_the_net(self):
        executor = self.an_executor(upper_limit_price=None, keep_position=False)
        await self.open(executor)

        executor.early_stop(keep_position=True)
        await executor.control_task()

        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.assertEqual(Decimal("10"), executor.lp_position_state.base_amount)
        self.assertEqual(1, len(executor._held_position_orders))
        self.assertEqual(0.0, executor._held_position_orders[0]["executed_amount_base"])

    async def test_early_stop_while_the_add_is_in_flight_removes_it_once_it_lands(self):
        executor = self.an_executor()

        async def add_then_stop(**kwargs):
            executor.early_stop(keep_position=True)
            return await self._add(**kwargs)
        self.connector._amm_add_liquidity.side_effect = add_then_stop

        await self.open(executor)

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        await executor.control_task()
        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)

    async def test_an_early_stop_after_a_failed_reading_sends_no_add(self):
        executor = self.an_executor()
        self.connector.get_position_info_fresh.side_effect = RuntimeError("node unreachable")
        await self.open(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)

        executor.early_stop(keep_position=True)
        self.connector.get_position_info_fresh.side_effect = lambda **kwargs: self.pool.holding()
        await executor.control_task()
        await executor.control_task()

        self.connector._amm_add_liquidity.assert_not_awaited()
        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)

    async def test_an_early_stop_after_an_add_refused_for_slippage_sends_no_second_add(self):
        executor = self.an_executor()
        self.add_failure = recorded_reason(POOL_MOVED)
        await self.open(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)

        executor.early_stop(keep_position=False)
        self.add_failure = None
        await executor.control_task()
        await executor.control_task()

        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)

    async def test_the_lower_limit_price_closes_the_position_too(self):
        executor = self.an_executor(upper_limit_price=None, lower_limit_price=Decimal("1.5"))
        await self.open(executor)
        self.pool.move_to(Decimal("1200"), Decimal("1700"))

        await executor.control_task()
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)
        await self.next_refresh(executor)

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)


class TestAMMLimitExit(AMMPoolShareTestBase):

    async def test_a_price_past_the_limit_on_one_reading_only_does_not_close(self):
        executor = self.an_executor()
        await self.open(executor)
        await executor.control_task()

        self.pool.move_to(Decimal("808"), Decimal("2525"))  # pushed past the limit for one reading
        await self.next_refresh(executor)
        self.pool.move_to(Decimal("1010"), Decimal("2020"))  # and back
        await self.next_refresh(executor)
        self.pool.move_to(Decimal("808"), Decimal("2525"))  # past it again: a new first reading
        await self.next_refresh(executor)

        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)
        self.connector._amm_remove_liquidity.assert_not_awaited()

    async def test_two_readings_past_the_limit_less_than_a_refresh_interval_apart_do_not_close(self):
        executor = self.an_executor(position_refresh_interval=60.0)
        await self.open(executor)
        self.pool.move_to(Decimal("808"), Decimal("2525"))
        await executor.control_task()

        self.now += 30
        executor._last_position_refresh_timestamp = None  # a reading forced half an interval later
        await executor.control_task()
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)

        self.now += 30
        executor._last_position_refresh_timestamp = None  # a full interval after the first
        await executor.control_task()
        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)

    async def test_no_add_is_sent_when_the_pool_price_is_already_past_a_limit(self):
        executor = self.an_executor(upper_limit_price=Decimal("1.9"))

        await self.open(executor)

        self.connector._amm_add_liquidity.assert_not_awaited()
        self.assertEqual(LPExecutorStates.FAILED, executor.lp_position_state.state)
        self.assertIsNone(executor.lp_position_state.position_address)
        self.assertTrue(any("already above upper limit" in r.getMessage() for r in self.log_records))

    async def test_a_price_past_the_limit_for_one_block_does_not_close(self):
        executor = self.an_executor(position_refresh_interval=60.0)
        self.connector._block_time_seconds = 120.0
        await self.open(executor)
        self.pool.move_to(Decimal("808"), Decimal("2525"))  # pushed past the limit in one block
        await executor.control_task()
        await self.next_refresh(executor)  # a minute later: the same block, still past the limit
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)

        self.pool.move_to(Decimal("1010"), Decimal("2020"))  # the next block has it back
        await self.next_refresh(executor)

        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)
        self.connector._amm_remove_liquidity.assert_not_awaited()

    async def test_a_price_past_the_limit_on_readings_a_block_apart_closes(self):
        executor = self.an_executor(position_refresh_interval=60.0)
        self.connector._block_time_seconds = 120.0
        await self.open(executor)
        self.pool.move_to(Decimal("808"), Decimal("2525"))
        await executor.control_task()
        await self.next_refresh(executor)
        self.assertEqual(LPExecutorStates.IN_RANGE, executor.lp_position_state.state)

        await self.next_refresh(executor)  # one block time after the first reading, still past it

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)


class TestAMMWalletHoldingOtherLP(AMMPoolShareTestBase):

    def setUp(self) -> None:
        super().setUp()
        # Fifty LP of this pool were in the wallet before the executor's add: not its own.
        self.pool = FakePool(wallet_lp=Decimal("50"))

    async def test_the_position_is_only_the_lp_its_add_acquired(self):
        executor = self.an_executor()

        await self.open(executor)

        self.assertEqual(Decimal("10"), executor.lp_position_state.lp_token_amount)
        self.assertEqual(Decimal("10"), executor.lp_position_state.initial_base_amount)
        self.assertEqual(Decimal("20"), executor.lp_position_state.initial_quote_amount)

    async def test_monitoring_values_only_this_positions_share(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        self.pool.move_to(Decimal("1000"), Decimal("3000"))
        executor._last_position_refresh_timestamp = None

        await executor.control_task()

        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.IN_RANGE, state.state)
        self.assertAlmostEqual(Decimal("9.900990099"), state.base_amount, places=6)
        self.assertAlmostEqual(Decimal("29.70297030"), state.quote_amount, places=6)
        self.assertEqual(Decimal("0"), state.base_fee)
        self.assertEqual(Decimal("0"), state.quote_fee)

    async def test_the_close_of_a_position_whose_lp_gateway_stated_removes_only_that_lp(self):
        executor = self.an_executor()
        self.gateway_states_amounts = True
        await self.open(executor)

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(Decimal("10"), self.connector._amm_remove_liquidity.call_args.kwargs["lp_token_amount"])
        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertEqual(Decimal("0"), state.lp_token_residue)
        self.assertEqual(Decimal("50"), self.pool.wallet_lp)

    async def test_the_close_by_percentage_leaves_at_most_a_unit_of_its_own_lp(self):
        executor = self.an_executor()
        await self.open(executor)

        await self.run_to_limit_exit_and_close(executor)

        # 10 of the wallet's 60 LP, as a percentage rounded down to 8 decimal places.
        self.assertEqual(16.66666666, self.connector._amm_remove_liquidity.call_args.kwargs["percentage"])
        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        # 60 x 16.66666666% is 9.9999999996 LP, rounded down to 9 whole LP: one unit stays.
        self.assertEqual(Decimal("1"), state.lp_token_residue)
        self.assertEqual(Decimal("51"), self.pool.wallet_lp)
        self.assertEqual(Decimal("1"), executor.get_custom_info()["lp_token_residue"])
        # That unit cannot be told apart from LP another executor would acquire: the claim stays, and the log says so.
        await executor.control_task()
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])
        self.assertTrue(any(r.levelname == "WARNING" and "stays held until it is released by hand" in r.getMessage()
                            for r in self.log_records))


class TestAMMOtherMovementOfTheWalletsLP(AMMPoolShareTestBase):
    """Another add or remove on the same wallet and pool, not through this process, still in the
    mempool when this executor closes: confirmed LP and spendable LP differ."""

    async def test_an_exact_close_leaves_an_unconfirmed_deposit_of_the_wallet_alone(self):
        executor = self.an_executor(upper_limit_price=None)
        self.gateway_states_amounts = True
        await self.open(executor)
        self.pool.deposit_unconfirmed(Decimal("5"), Decimal("10"))  # 5 LP, spendable, not confirmed

        executor.early_stop(keep_position=True)
        await executor.control_task()

        self.assertEqual(Decimal("10"), self.connector._amm_remove_liquidity.call_args.kwargs["lp_token_amount"])
        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertEqual(Decimal("0"), state.lp_token_residue)
        self.assertEqual(Decimal("5"), self.pool.wallet_lp)  # the other deposit's LP is still the wallet's

    async def test_an_exact_close_takes_all_its_lp_while_another_remove_is_unconfirmed(self):
        self.pool = FakePool(wallet_lp=Decimal("5"))
        executor = self.an_executor(upper_limit_price=None)
        self.gateway_states_amounts = True
        await self.open(executor)
        self.pool.redeem_unconfirmed(Decimal("5"))  # 15 confirmed, 10 spendable

        executor.early_stop(keep_position=True)
        await executor.control_task()

        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertEqual(Decimal("0"), state.lp_token_residue)
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)  # nothing of this position's stays behind
        self.assertFalse(any("left the pool as well" in r.getMessage() for r in self.log_records))

    async def test_a_close_by_percentage_also_takes_an_unconfirmed_deposit_of_the_wallet(self):
        """Without the LP stated by Gateway the close can only go by percentage, and this is what
        can go wrong: Gateway applies 100% to the 15 LP the wallet can spend, against the 10
        confirmed LP read, so the other deposit's 5 LP go too, and the readings cannot see it."""
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        self.pool.deposit_unconfirmed(Decimal("5"), Decimal("10"))

        executor.early_stop(keep_position=True)
        await executor.control_task()

        info = executor.get_custom_info()
        self.assertEqual(100.0, self.connector._amm_remove_liquidity.call_args.kwargs["percentage"])
        self.assertEqual(Decimal("0"), self.pool.wallet_lp)
        self.assertEqual(Decimal("0"), executor.lp_position_state.lp_token_residue)
        self.assertFalse(info["lp_token_amount_exact"])
        self.assertTrue(info["close_amounts_estimated"])


class TestAMMOneExecutorPerPool(AMMPoolShareTestBase):

    async def test_a_second_amm_executor_on_the_same_pool_and_wallet_never_sends_anything(self):
        first = self.an_executor()
        await self.open(first)
        second = self.an_executor(id="amm-lp-2")

        await self.open(second)

        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.connector.get_position_info_fresh.assert_awaited()
        self.assertEqual(LPExecutorStates.FAILED, second.lp_position_state.state)
        self.assertEqual(CloseType.FAILED, second.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, second.status)
        self.assertTrue(any(r.levelname == "ERROR" and POOL in r.getMessage() and WALLET in r.getMessage()
                            and NETWORK in r.getMessage() for r in self.log_records))
        # The first keeps its LP and its pool.
        self.assertEqual(Decimal("10"), first.lp_position_state.lp_token_amount)
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_another_connector_name_for_the_same_wallet_cannot_take_the_pool(self):
        # The same chain, network and wallet under a second connector name.
        alias = self.a_connector()
        self.strategy.connectors = {NETWORK: self.connector, "ergodex/amm": alias}
        first = self.an_executor()
        await self.open(first)
        second = self.an_executor(id="amm-lp-2", connector_name="ergodex/amm")

        await self.open(second)

        alias._amm_add_liquidity.assert_not_awaited()
        self.assertEqual(CloseType.FAILED, second.close_type)
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_the_pool_id_in_another_case_is_the_same_pool(self):
        first = self.an_executor()
        await self.open(first)
        second = self.an_executor(id="amm-lp-2", pool_address=POOL.upper())

        await self.open(second)

        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(CloseType.FAILED, second.close_type)
        self.assertEqual({CLAIM: "amm-lp-1"}, LPExecutor._amm_pool_claims)

    async def test_a_second_amm_executor_refuses_to_start(self):
        first = self.an_executor()
        await self.open(first)
        second = self.an_executor(id="amm-lp-2")

        await second.on_start()

        self.assertEqual(CloseType.FAILED, second.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, second.status)

    async def test_the_pool_is_free_again_once_the_first_executor_has_closed(self):
        first = self.an_executor(upper_limit_price=None)
        await self.open(first)
        first.early_stop(keep_position=True)
        await first.control_task()  # removes
        await first.control_task()  # COMPLETE: stops
        self.assertEqual(RunnableStatus.TERMINATED, first.status)
        self.assertNotIn(CLAIM, LPExecutor._amm_pool_claims)

        second = self.an_executor(id="amm-lp-2")
        await self.open(second)

        self.assertEqual(Decimal("10"), second.lp_position_state.lp_token_amount)

    async def test_the_claim_is_kept_while_lp_of_its_own_may_be_left_unrecorded(self):
        first = self.an_executor()
        await self.open(first)

        first.stop()  # terminal, with its LP still in the pool and no marker

        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_each_hold_reason_under_which_lp_may_still_be_in_the_pool_keeps_the_claim(self):
        for reason in ("open_outcome_unknown", "open_unmeasured", "forced_stop_in_flight", "forced_stop_lp_in_pool",
                       "close_outcome_unknown", "pool_cannot_pay_in_full"):
            with self.subTest(reason=reason):
                LPExecutor._amm_pool_claims.clear()
                executor = self.an_executor()
                self.assertTrue(executor._amm_claim_pool())
                executor._hold_reason = reason
                executor._record_unclosed_position_marker()

                executor.stop()

                self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])
                self.assertEqual(reason, LPExecutor._amm_pool_claim_reasons[CLAIM])

    async def test_a_second_executor_is_told_the_hold_reason_and_how_to_release_the_claim(self):
        first = self.an_executor()
        self.add_failure = f"Transaction {SIGNATURE} not found after 720s of polling [code: TX_UNRESOLVED]"
        await self.open(first)
        for _ in range(LPExecutor.AMM_OPEN_REREADS):
            await self.next_refresh(first)
        self.assertEqual("open_outcome_unknown", first._hold_reason)
        self.add_failure = None

        second = self.an_executor(id="amm-lp-2")
        await self.open(second)

        self.assertEqual(CloseType.FAILED, second.close_type)
        self.assertTrue(any(
            r.levelname == "ERROR" and "hold_reason=open_outcome_unknown" in r.getMessage()
            and "LPExecutor.release_amm_pool_claim('ergo', 'mainnet', '9wallet', 'pool-nft-1')" in r.getMessage()
            for r in self.log_records))

    async def test_the_claim_can_be_released_by_hand_and_the_release_is_logged(self):
        first = self.an_executor()
        self.add_failure = "ClientPayloadError() [code: SUBMIT_OUTCOME_UNKNOWN]"
        await self.open(first)
        for _ in range(LPExecutor.AMM_OPEN_REREADS):
            await self.next_refresh(first)
        self.assertEqual(RunnableStatus.TERMINATED, first.status)
        self.assertIn(CLAIM, LPExecutor._amm_pool_claims)
        self.add_failure = None

        released = LPExecutor.release_amm_pool_claim("ergo", "mainnet", WALLET, POOL.upper())

        self.assertEqual("amm-lp-1", released)
        self.assertNotIn(CLAIM, LPExecutor._amm_pool_claims)
        self.assertTrue(any(
            r.levelname == "WARNING" and "Releasing the claim of AMM executor amm-lp-1" in r.getMessage()
            and "hold_reason=open_outcome_unknown" in r.getMessage() and POOL in r.getMessage()
            for r in self.log_records))
        self.assertIsNone(LPExecutor.release_amm_pool_claim("ergo", "mainnet", WALLET, POOL))
        second = self.an_executor(id="amm-lp-2")
        await self.open(second)
        self.assertEqual(Decimal("10"), second.lp_position_state.lp_token_amount)

    async def test_another_pool_or_another_wallet_is_not_refused(self):
        first = self.an_executor()
        await self.open(first)
        other_wallet = self.a_connector()
        other_wallet.address = "9other"
        self.strategy.connectors = {NETWORK: other_wallet}
        on_another_wallet = self.an_executor(id="amm-lp-2")
        self.strategy.connectors = {NETWORK: self.connector}
        on_another_pool = self.an_executor(id="amm-lp-3", pool_address="pool-nft-2")

        await self.open(on_another_wallet)
        await self.open(on_another_pool)

        self.assertNotEqual(LPExecutorStates.FAILED, on_another_wallet.lp_position_state.state)
        self.assertNotEqual(LPExecutorStates.FAILED, on_another_pool.lp_position_state.state)


class TestAMMOutcomeUnknown(AMMPoolShareTestBase):

    async def run_rereads(self, executor: LPExecutor):
        for _ in range(LPExecutor.AMM_OPEN_REREADS):
            await self.next_refresh(executor)

    async def test_an_add_of_unknown_outcome_whose_lp_is_never_seen_holds_with_the_marker(self):
        executor = self.an_executor()
        self.add_failure = f"Transaction {SIGNATURE} not found after 720s of polling [code: TX_UNRESOLVED]"

        await self.open(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        await self.run_rereads(executor)

        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertNotEqual(LPExecutorStates.FAILED, executor.lp_position_state.state)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        info = executor.get_custom_info()
        self.assertEqual("open_outcome_unknown", info["hold_reason"])
        self.assertIn("TX_UNRESOLVED", info["open_outcome_unknown"])
        marker = executor._held_position_orders[-1]
        self.assertTrue(marker["lp_unclosed_position"])
        self.assertEqual(POOL, marker["pool_address"])
        self.assertEqual(POOL, marker["position_address"])
        self.assertEqual("unmeasured", marker["lp_token_amount"])
        self.assertEqual(SIGNATURE, marker["open_tx_hash"])
        self.assertEqual("10", marker["offered_base_amount"])
        self.assertEqual("20", marker["offered_quote_amount"])
        self.assertEqual(0.0, marker["executed_amount_base"])
        # The add may still land, so the claim on the pool stays held until it is released by hand.
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])
        self.assertEqual("open_outcome_unknown", LPExecutor._amm_pool_claim_reasons[CLAIM])

    def an_unresolved_add_on_120s_blocks(self) -> LPExecutor:
        """An add whose poll ran out while its transaction was still pending, on 120-second blocks
        and one confirmation; Gateway's PENDING answer for it stated what it moves."""
        self.connector._block_time_seconds, self.connector._block_confirmations = 120.0, 1
        self.add_failure = f"Transaction {SIGNATURE} still unresolved after 720s of polling [code: TX_UNRESOLVED]"
        self.pending["range-1"] = (SIGNATURE, {
            "poolAddress": POOL, "baseTokenAmountAdded": "9.5", "quoteTokenAmountAdded": "19",
            "lpTokenAmountAdded": "10", "fee": "0.0011"})
        return self.an_executor(upper_limit_price=None, position_refresh_interval=30.0)

    async def test_an_unknown_add_that_confirms_after_300s_on_120s_blocks_is_booked_from_its_pending_data(self):
        executor = self.an_unresolved_add_on_120s_blocks()
        await self.open(executor)
        for _ in range(10):  # 300 seconds, its transaction still pending
            await self.next_refresh(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)

        self.pool.deposit(Decimal("10"), Decimal("20"))  # it confirms
        self.tx_status = {"txStatus": 1}
        await self.next_refresh(executor)

        state = executor.lp_position_state
        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(Decimal("9.5"), state.initial_base_amount)
        self.assertEqual(Decimal("19"), state.initial_quote_amount)
        self.assertEqual(Decimal("0.0011"), state.tx_fee)
        self.assertEqual(SIGNATURE, state.open_tx_hash)
        self.assertFalse(state.open_data_unavailable)
        info = executor.get_custom_info()
        self.assertTrue(info["lp_token_amount_exact"])
        self.assertFalse(info["open_amounts_estimated"])
        self.connector._get_gateway_instance.return_value.get_transaction_status.assert_awaited_with(
            "ergo", "mainnet", SIGNATURE, fail_silently=True)

    async def test_an_unknown_add_whose_transaction_failed_is_not_booked(self):
        executor = self.an_unresolved_add_on_120s_blocks()
        await self.open(executor)
        await self.next_refresh(executor)

        self.tx_status = {"txStatus": -1, "error": "inputs already spent"}
        await self.next_refresh(executor)
        await executor.control_task()

        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.FAILED, state.state)
        self.assertEqual(CloseType.FAILED, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertIsNone(state.position_address)
        self.assertEqual(Decimal("0"), state.lp_token_amount)
        self.connector._trigger_add_liquidity_event.assert_not_called()
        self.assertEqual([], executor._held_position_orders)
        # Nothing landed: the pool is free again.
        self.assertNotIn(CLAIM, LPExecutor._amm_pool_claims)

    async def test_an_unknown_add_without_a_verdict_is_followed_until_its_deadline_then_holds(self):
        executor = self.an_unresolved_add_on_120s_blocks()
        sent_at = self.now
        await self.open(executor)
        # Followed for 120s x (1 confirmation + 10 blocks) = 1320s after it was sent.
        while self.now + 30 < sent_at + 1320:
            await self.next_refresh(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        self.assertIsNone(executor.close_type)

        await self.next_refresh(executor)

        self.assertEqual(sent_at + 1320, self.now)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual("open_outcome_unknown", executor._hold_reason)
        marker = executor._held_position_orders[-1]
        self.assertTrue(marker["lp_unclosed_position"])
        self.assertEqual("unmeasured", marker["lp_token_amount"])
        self.assertEqual(SIGNATURE, marker["open_tx_hash"])
        self.connector._trigger_add_liquidity_event.assert_not_called()

    async def test_the_marker_of_an_unknown_add_carries_what_its_recovery_needs(self):
        self.pool = FakePool(wallet_lp=Decimal("50"))  # LP of this pool already in the wallet
        self.add_failure = f"Transaction {SIGNATURE} still unresolved after 720s of polling [code: TX_UNRESOLVED]"
        self.pending["range-1"] = (SIGNATURE, {"poolAddress": POOL, "lpTokenAmountAdded": "10"})
        executor = self.an_executor()
        await self.open(executor)
        await self.run_rereads(executor)

        marker = executor._held_position_orders[-1]
        self.assertEqual(Decimal("50"), Decimal(marker["lp_before_open"]))
        self.assertEqual("range-1", marker["open_order_id"])
        self.assertEqual(Decimal("10"), Decimal(marker["stated_lp_token_amount"]))
        self.assertEqual(SIGNATURE, marker["open_tx_hash"])
        self.assertNotIn("open_tx_note", marker)
        self.assertEqual(Decimal("50"), Decimal(executor.get_custom_info()["lp_before_open"]))

    async def test_the_marker_of_an_add_whose_reply_was_lost_says_its_transaction_id_is_unknown(self):
        self.pool = FakePool(wallet_lp=Decimal("50"))
        self.add_failure = "ClientPayloadError() [code: SUBMIT_OUTCOME_UNKNOWN]"
        executor = self.an_executor()
        await self.open(executor)
        await self.run_rereads(executor)

        marker = executor._held_position_orders[-1]
        self.assertEqual("open_outcome_unknown", marker["hold_reason"])
        self.assertIsNone(marker["open_tx_hash"])
        self.assertEqual(Decimal("50"), Decimal(marker["lp_before_open"]))
        self.assertEqual("range-1", marker["open_order_id"])
        self.assertIsNone(marker["stated_lp_token_amount"])
        self.assertEqual(
            "the add's reply was lost; its transaction id is unknown; check the wallet's recent transactions for "
            f"a deposit into pool {POOL}", marker["open_tx_note"])

    async def test_an_add_of_unknown_outcome_that_lands_later_is_adopted(self):
        executor = self.an_executor(upper_limit_price=None)
        self.add_failure = "ClientPayloadError() [code: SUBMIT_OUTCOME_UNKNOWN]"

        await self.open(executor)
        await self.next_refresh(executor)
        self.assertIsNone(executor.lp_position_state.position_address)
        self.pool.deposit(Decimal("10"), Decimal("20"))  # it confirms
        await self.next_refresh(executor)

        state = executor.lp_position_state
        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertTrue(state.open_data_unavailable)
        self.assertTrue(executor.get_custom_info()["open_amounts_estimated"])
        await self.next_refresh(executor)
        self.assertEqual(LPExecutorStates.IN_RANGE, state.state)

    async def test_an_add_of_unknown_outcome_that_has_landed_is_adopted(self):
        executor = self.an_executor()
        self.add_failure = "ClientPayloadError() [code: SUBMIT_OUTCOME_UNKNOWN]"
        reading = iter([self.pool.holding()])

        def landed_meanwhile(**kwargs):
            # The first reading is the one before the add; the add lands before the next.
            first = next(reading, None)
            if first is not None:
                self.pool.deposit(Decimal("10"), Decimal("20"))
                return first
            return self.pool.holding()
        self.connector.get_position_info_fresh.side_effect = landed_meanwhile

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertTrue(state.open_data_unavailable)

    async def test_a_dropped_add_is_unknown_only_where_not_found_proves_nothing(self):
        dropped = f"Transaction {SIGNATURE} dropped after 3 submissions [code: TRANSACTION_TIMEOUT]"
        executor = self.an_executor()
        self.add_failure = dropped
        await self.open(executor)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        self.assertIn("TRANSACTION_TIMEOUT", executor.get_custom_info()["open_outcome_unknown"])

        LPExecutor._amm_pool_claims.clear()
        self.connector.not_found_proves_dropped = True
        proven = self.an_executor(id="amm-lp-2")
        await self.open(proven)

        self.assertEqual(LPExecutorStates.FAILED, proven.lp_position_state.state)
        self.assertIsNone(proven.get_custom_info()["open_outcome_unknown"])

    async def test_a_remove_of_unknown_outcome_holds_with_the_unclosed_marker(self):
        executor = self.an_executor()
        await self.open(executor)
        self.remove_failure = "TimeoutError() [code: SUBMIT_OUTCOME_UNKNOWN]"

        await self.run_to_limit_exit_and_close(executor)
        await executor.control_task()

        self.assertEqual(1, self.connector._amm_remove_liquidity.await_count)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        info = executor.get_custom_info()
        self.assertEqual("close_outcome_unknown", info["hold_reason"])
        marker = executor._held_position_orders[-1]
        self.assertTrue(marker["lp_unclosed_position"])
        self.assertEqual(POOL, marker["position_address"])
        self.assertEqual(Decimal("10"), Decimal(marker["lp_token_amount"]))
        self.assertEqual(TX_ADD, marker["open_tx_hash"])
        self.assertEqual(0.0, marker["executed_amount_base"])
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_a_dropped_remove_is_retried_where_not_found_proves_it_dropped(self):
        executor = self.an_executor()
        self.connector.not_found_proves_dropped = True
        await self.open(executor)
        self.remove_failure = f"Transaction {SIGNATURE} dropped after 1 submissions [code: TRANSACTION_TIMEOUT]"

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        self.assertEqual(1, executor._current_retries)
        self.assertIsNone(executor._hold_reason)

    async def test_a_definitive_remove_failure_is_retried_like_a_clmm_close(self):
        executor = self.an_executor()
        await self.open(executor)
        self.remove_failure = "Gateway error: the pool moved (Conflict) [code: SIMULATION_FAILED]"

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        self.assertEqual(1, executor._current_retries)
        self.assertIsNone(executor.lp_position_state.active_close_order)

    async def test_gateways_pool_moved_refusal_of_the_add_widens_and_tries_again(self):
        executor = self.an_executor()
        self.add_failure = recorded_reason(POOL_MOVED)

        await self.open(executor)

        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        self.assertEqual(Decimal("0.25"), executor._slippage_pct)
        self.add_failure = None
        await executor.control_task()
        self.assertEqual(Decimal("0.25"), Decimal(str(self.connector._amm_add_liquidity.call_args.kwargs["slippage_pct"])))
        self.assertEqual(Decimal("10"), executor.lp_position_state.lp_token_amount)

    async def test_gateways_pool_moved_refusal_of_the_remove_widens(self):
        executor = self.an_executor()
        await self.open(executor)
        self.remove_failure = recorded_reason(POOL_MOVED)

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        self.assertEqual(Decimal("0.25"), executor._slippage_pct)

    async def test_any_other_add_failure_ends_failed_without_a_position(self):
        executor = self.an_executor()
        self.add_failure = "Gateway error: not enough funds (Bad Request)"

        await self.open(executor)

        self.assertEqual(LPExecutorStates.FAILED, executor.lp_position_state.state)
        self.assertIsNone(executor.get_custom_info()["open_outcome_unknown"])

    async def test_no_add_is_sent_without_the_reading_before_it(self):
        executor = self.an_executor()
        self.connector.get_position_info_fresh.side_effect = RuntimeError("node unreachable")

        await self.open(executor)

        self.connector._amm_add_liquidity.assert_not_awaited()
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        self.assertEqual(1, executor._current_retries)

    async def test_readings_that_failed_while_opening_leave_the_close_its_whole_retry_budget(self):
        executor = self.an_executor()
        self.connector.get_position_info_fresh.side_effect = RuntimeError("node unreachable")
        for _ in range(8):
            await executor.control_task()
        self.assertEqual(8, executor._current_retries)
        self.connector.get_position_info_fresh.side_effect = lambda **kwargs: self.pool.holding()
        await executor.control_task()
        self.assertEqual(POOL, executor.lp_position_state.position_address)
        self.assertEqual(0, executor._current_retries)

        self.remove_failure = "Gateway error: the node is unreachable (HttpError)"
        executor.early_stop(keep_position=True)
        for _ in range(3):
            executor._close_backoff_until = 0
            await executor.control_task()
            executor.evaluate_max_retries()

        self.assertEqual(3, executor._current_retries)
        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        self.assertIsNone(executor._hold_reason)

    async def test_a_confirmed_add_whose_lp_is_never_seen_holds_with_the_marker(self):
        executor = self.an_executor()

        async def confirmed_but_not_seen(**kwargs):
            return TX_ADD  # the wallet's LP of the pool does not change
        self.connector._amm_add_liquidity.side_effect = confirmed_but_not_seen

        await self.open(executor)
        self.assertIsNone(executor.lp_position_state.position_address)
        self.assertEqual(LPExecutorStates.OPENING, executor.lp_position_state.state)
        await self.run_rereads(executor)

        self.assertEqual(1, self.connector._amm_add_liquidity.await_count)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        self.assertEqual("open_unmeasured", executor._hold_reason)
        marker = executor._held_position_orders[-1]
        self.assertEqual("unmeasured", marker["lp_token_amount"])
        self.assertEqual(TX_ADD, marker["open_tx_hash"])
        self.connector._trigger_add_liquidity_event.assert_not_called()
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_a_confirmed_add_seen_only_on_a_later_reading_is_booked_from_that_reading(self):
        executor = self.an_executor(upper_limit_price=None)
        deposited = []

        async def confirmed_before_the_reading_shows_it(**kwargs):
            deposited.append(kwargs)
            return TX_ADD
        self.connector._amm_add_liquidity.side_effect = confirmed_before_the_reading_shows_it

        await self.open(executor)
        self.assertIsNone(executor.lp_position_state.position_address)
        self.pool.deposit(Decimal("10"), Decimal("20"))  # the next reading shows it
        await self.next_refresh(executor)

        state = executor.lp_position_state
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(TX_ADD, state.open_tx_hash)
        self.assertEqual(1, len(deposited))

    async def test_a_failed_reading_after_the_add_books_what_the_pool_took_not_the_offer(self):
        # Offered 10 base and 10 quote at 2 quote per base: the pool takes 5 base and 10 quote.
        executor = self.an_executor(upper_limit_price=None, quote_amount=Decimal("10"))
        readings = iter(["before", "fails"])

        def read(**kwargs):
            step = next(readings, None)
            if step == "fails":
                raise RuntimeError("node unreachable")
            return self.pool.holding()
        self.connector.get_position_info_fresh.side_effect = read

        await self.open(executor)
        state = executor.lp_position_state
        self.assertIsNone(state.position_address)
        self.assertEqual(Decimal("0"), state.initial_base_amount)
        self.connector._trigger_add_liquidity_event.assert_not_called()

        await self.next_refresh(executor)

        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("5"), state.lp_token_amount)
        self.assertEqual(Decimal("5"), state.initial_base_amount)
        self.assertEqual(Decimal("10"), state.initial_quote_amount)
        self.assertEqual(Decimal("0"), executor.get_net_pnl_quote())

    async def test_a_confirmed_add_whose_amounts_gateway_stated_is_booked_though_the_reading_fails(self):
        executor = self.an_executor(upper_limit_price=None)
        self.gateway_states_amounts = True
        readings = iter(["before", "fails"])

        def read(**kwargs):
            if next(readings, None) == "fails":
                raise RuntimeError("node unreachable")
            return self.pool.holding()
        self.connector.get_position_info_fresh.side_effect = read

        await self.open(executor)

        state = executor.lp_position_state
        self.assertEqual(POOL, state.position_address)
        self.assertEqual(Decimal("10"), state.lp_token_amount)
        self.assertEqual(Decimal("10"), state.initial_base_amount)
        self.assertFalse(executor.get_custom_info()["open_amounts_estimated"])

    async def test_a_confirmed_remove_whose_amounts_cannot_be_read_is_never_sent_again(self):
        executor = self.an_executor()
        await self.open(executor)
        await executor.control_task()
        executor.early_stop(keep_position=True)

        original = self.connector.get_position_info_fresh.side_effect
        calls = []

        def fail_after_the_remove(**kwargs):
            calls.append(1)
            if self.connector._amm_remove_liquidity.await_count:
                raise RuntimeError("node unreachable")
            return original(**kwargs)
        self.connector.get_position_info_fresh.side_effect = fail_after_the_remove

        await executor.control_task()

        state = executor.lp_position_state
        self.assertEqual(LPExecutorStates.COMPLETE, state.state)
        self.assertTrue(state.close_data_unavailable)
        self.assertEqual(Decimal("10"), state.base_amount)
        self.assertEqual(1, self.connector._amm_remove_liquidity.await_count)

    async def test_a_remove_the_pool_cannot_pay_in_full_holds_and_is_not_sent_again(self):
        executor = self.an_executor()
        await self.open(executor)
        self.remove_failure = (
            f"Gateway error: Pool {POOL}: the redeem would leave less than the pool's storage floor; at most "
            "7.5 LP tokens, 75.00000000% of the wallet's 10, can be removed now (HttpError)")

        await self.run_to_limit_exit_and_close(executor)
        await executor.control_task()

        self.assertEqual(1, self.connector._amm_remove_liquidity.await_count)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        info = executor.get_custom_info()
        self.assertEqual("pool_cannot_pay_in_full", info["hold_reason"])
        self.assertEqual(Decimal("7.5"), info["largest_removable_lp_token_amount"])
        self.assertTrue(executor._held_position_orders[-1]["lp_unclosed_position"])
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])

    async def test_a_remove_of_which_the_pool_can_pay_none_says_so(self):
        executor = self.an_executor()
        await self.open(executor)
        self.remove_failure = (
            f"Gateway error: Pool {POOL}: the redeem would empty a side; none of the wallet's LP tokens can be "
            "removed now (HttpError)")

        await self.run_to_limit_exit_and_close(executor)

        self.assertEqual("pool_cannot_pay_in_full", executor._hold_reason)
        self.assertEqual(Decimal("0"), executor.get_custom_info()["largest_removable_lp_token_amount"])

    async def test_exhausted_close_retries_record_the_lp_on_the_marker(self):
        executor = self.an_executor()
        await self.open(executor)
        executor._current_retries = executor._max_retries + 1

        executor.evaluate_max_retries()

        self.assertEqual("close_retries_exhausted", executor._hold_reason)
        marker = executor._held_position_orders[-1]
        self.assertEqual(Decimal("10"), Decimal(marker["lp_token_amount"]))
        self.assertEqual(POOL, marker["pool_address"])
        self.assertEqual(TX_ADD, marker["open_tx_hash"])


class TestAMMForcedStop(AMMPoolShareTestBase):
    """The orchestrator force-stops an executor still waiting on a transaction at shutdown."""

    async def test_a_forced_stop_while_the_add_is_polled_leaves_a_marker_with_its_transaction(self):
        executor = self.an_executor()
        polled, release = asyncio.Event(), asyncio.Event()

        async def add_still_polled(**kwargs):
            # Gateway answered PENDING: the connector keeps the transaction with the order and polls it.
            self.pending[kwargs["order_id"]] = (TX_ADD, {})
            polled.set()
            await release.wait()
            return await self._add(**kwargs)
        self.connector._amm_add_liquidity.side_effect = add_still_polled
        opening = asyncio.ensure_future(self.open(executor))
        await polled.wait()

        executor.force_stop_with_position_hold()

        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(RunnableStatus.TERMINATED, executor.status)
        marker = executor._held_position_orders[-1]
        self.assertTrue(marker["lp_unclosed_position"])
        self.assertEqual(POOL, marker["pool_address"])
        self.assertEqual("unmeasured", marker["lp_token_amount"])
        self.assertEqual(TX_ADD, marker["open_tx_hash"])
        self.assertEqual("forced_stop_in_flight", marker["hold_reason"])
        info = executor.get_custom_info()
        self.assertEqual(TX_ADD, info["open_tx_hash"])
        self.assertEqual("unmeasured", info["lp_token_amount"])
        self.assertEqual(POOL, info["pool_address"])
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])
        opening.cancel()
        with suppress(asyncio.CancelledError):
            await opening

    async def test_a_forced_stop_while_the_remove_is_polled_leaves_a_marker_with_its_transaction(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        polled, release = asyncio.Event(), asyncio.Event()

        async def remove_still_polled(**kwargs):
            self.pending[kwargs["order_id"]] = (TX_REMOVE, {})
            polled.set()
            await release.wait()
            return await self._remove(**kwargs)
        self.connector._amm_remove_liquidity.side_effect = remove_still_polled
        executor.early_stop(keep_position=True)
        closing = asyncio.ensure_future(executor.control_task())
        await polled.wait()

        executor.force_stop_with_position_hold()

        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.assertEqual(LPExecutorStates.CLOSING, executor.lp_position_state.state)
        marker = executor._held_position_orders[-1]
        self.assertTrue(marker["lp_unclosed_position"])
        self.assertEqual(POOL, marker["pool_address"])
        self.assertEqual(Decimal("10"), Decimal(marker["lp_token_amount"]))
        self.assertEqual(TX_ADD, marker["open_tx_hash"])
        self.assertEqual(TX_REMOVE, marker["close_tx_hash"])
        self.assertEqual("forced_stop_in_flight", marker["hold_reason"])
        self.assertEqual(TX_REMOVE, executor.get_custom_info()["close_tx_hash"])
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])
        closing.cancel()
        with suppress(asyncio.CancelledError):
            await closing

    async def test_a_forced_stop_with_the_lp_still_in_the_pool_leaves_a_marker(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)

        executor.force_stop_with_position_hold()

        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        marker = executor._held_position_orders[-1]
        self.assertEqual(Decimal("10"), Decimal(marker["lp_token_amount"]))
        self.assertEqual("forced_stop_lp_in_pool", marker["hold_reason"])
        self.assertEqual("amm-lp-1", LPExecutor._amm_pool_claims[CLAIM])


class TestAMMPositionGone(AMMPoolShareTestBase):

    async def test_lp_that_left_the_wallet_is_taken_as_removed_after_three_reads(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        self.pool.redeem(Decimal("10"))  # removed outside this executor

        for _ in range(3):
            executor._last_position_refresh_timestamp = None
            await executor.control_task()

        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.assertEqual(CloseType.POSITION_HOLD, executor.close_type)
        self.connector._amm_remove_liquidity.assert_not_awaited()

    async def test_a_close_that_finds_none_of_its_lp_never_sends_a_remove(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        executor.early_stop(keep_position=True)
        self.pool.redeem(Decimal("10"))

        for _ in range(3):
            await executor.control_task()

        self.assertEqual(LPExecutorStates.COMPLETE, executor.lp_position_state.state)
        self.connector._amm_remove_liquidity.assert_not_awaited()


class TestAMMVolume(AMMPoolShareTestBase):

    async def test_the_volume_is_the_fees_the_share_earned_over_the_pools_fee_rate(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)  # 10 base and 20 quote of a pool at 1010 and 2020
        # Swaps paid fees into the reserves: both sides grew by 1% at the same price.
        self.pool.move_to(Decimal("1020.1"), Decimal("2040.2"))
        await executor.control_task()

        # The share is worth 10.1 x 2 + 20.2 = 40.4 against 40 deposited: 0.4 quote of fees,
        # paid by 0.4 / 0.3% of volume.
        self.assertAlmostEqual(Decimal("133.3333"), executor.filled_amount_quote, places=3)

    async def test_a_price_move_alone_is_no_volume(self):
        executor = self.an_executor(upper_limit_price=None)
        await self.open(executor)
        self.pool.move_to(Decimal("1000"), Decimal("2040.2"))  # the same base x quote
        await executor.control_task()

        self.assertAlmostEqual(Decimal("0"), executor.filled_amount_quote, places=6)


class TestAMMConfigAndState(AMMPoolShareTestBase):

    def test_a_range_is_refused_for_an_amm_pool(self):
        with self.assertRaisesRegex(ValidationError, "must be left unset for an AMM pool"):
            self.a_config(lower_price=Decimal("1.5"), upper_price=Decimal("2.5"))
        with self.assertRaisesRegex(ValidationError, "must be left unset for an AMM pool"):
            self.a_config(upper_price=Decimal("2.5"))

    def test_an_amm_config_without_a_range_is_accepted(self):
        config = self.a_config(lower_limit_price=Decimal("1"))
        self.assertTrue(config.is_amm)
        self.assertEqual(Decimal("0"), config.lower_price)

    def test_an_amm_deposit_needs_both_sides(self):
        with self.assertRaisesRegex(ValidationError, "quote_amount"):
            self.a_config(quote_amount=Decimal("0"))

    def test_amm_limit_prices_must_be_in_order(self):
        with self.assertRaisesRegex(ValidationError, "lower_limit_price"):
            self.a_config(lower_limit_price=Decimal("3"), upper_limit_price=Decimal("2"))

    def test_a_clmm_config_still_needs_its_range(self):
        with self.assertRaisesRegex(ValidationError, "lower_price"):
            self.a_config(lp_provider="meteora/clmm", upper_limit_price=None)
        config = self.a_config(lp_provider="meteora/clmm", lower_price=Decimal("1"), upper_price=Decimal("2"))
        self.assertFalse(config.is_amm)

    def test_a_position_without_a_range_is_never_out_of_range(self):
        state = LPExecutorState(position_address=POOL, has_price_range=False)
        state.update_state(Decimal("1000"), 0)
        self.assertEqual(LPExecutorStates.IN_RANGE, state.state)
        ranged = LPExecutorState(position_address="pos", lower_price=Decimal("1"), upper_price=Decimal("2"))
        ranged.update_state(Decimal("1000"), 0)
        self.assertEqual(LPExecutorStates.OUT_OF_RANGE, ranged.state)

    async def test_custom_info_says_the_fees_are_in_the_reserves_and_names_the_position(self):
        executor = self.an_executor()
        await self.open(executor)

        info = executor.get_custom_info()

        self.assertEqual("amm", info["pool_type"])
        self.assertEqual(Decimal("10"), info["lp_token_amount"])
        self.assertEqual(POOL, info["pool_address"])
        self.assertEqual(TX_ADD, info["open_tx_hash"])
        self.assertIsNone(info["close_tx_hash"])
        self.assertEqual(0.0, info["base_fee"])
        self.assertEqual(0.0, info["quote_fee"])
        self.assertIn("reserves", info["fees_note"])

    def test_a_clmm_executor_reports_no_amm_fields(self):
        executor = self.an_executor(lp_provider="meteora/clmm", lower_price=Decimal("1"), upper_price=Decimal("2"),
                                    upper_limit_price=None)
        info = executor.get_custom_info()
        self.assertNotIn("pool_type", info)
        self.assertNotIn("lp_token_amount", info)
        self.assertNotIn("pool_address", info)
        self.assertTrue(executor.lp_position_state.has_price_range)
