import asyncio
from decimal import Decimal
from types import SimpleNamespace
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, MagicMock

from controllers.generic.ergo_pool_arb import ErgoPoolArb, ErgoPoolArbConfig
from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.trade_fee import TokenAmount
from hummingbot.strategy_v2.models.base import RunnableStatus
from hummingbot.strategy_v2.models.executor_actions import CreateExecutorAction
from hummingbot.strategy_v2.models.executors import CloseType
from hummingbot.strategy_v2.models.executors_info import ExecutorInfo
from hummingbot.strategy_v2.utils import outcome_hold

T0 = 1_700_000_000.0
WINDOW = 120.0 * (1 + 10)
POOL_A = "aa" * 32
POOL_B = "bb" * 32
ERG_ID = "0" * 64
SIGUSD_ID = "03" * 32
OTHER_ID = "4d" * 32


class ErgoPoolArbTest(IsolatedAsyncioWrapperTestCase, LoggerMixinForTest):
    def setUp(self):
        super().setUp()
        self.now = T0
        outcome_hold.reset_process_outcome_holds()
        self.addCleanup(outcome_hold.reset_process_outcome_holds)
        outcome_hold.reset_process_controller_halts()
        self.addCleanup(outcome_hold.reset_process_controller_halts)
        # pool -> (buy price, sell price)
        self.prices = {POOL_A: (Decimal("1.00"), Decimal("0.99")), POOL_B: (Decimal("1.06"), Decimal("1.05"))}
        self.answer_pool = {}

        self.ergo = MagicMock()
        self.ergo._block_time_seconds = 120.0
        self.ergo._block_confirmations = 1
        self.ergo._update_balances = AsyncMock()
        self.ergo.network_transaction_fee = TokenAmount("ERG", Decimal("0.002"))
        self.ergo.get_quote_price_and_pool = AsyncMock(side_effect=self._quote)
        self.ergo.get_quote_price_and_pool_fresh = AsyncMock(side_effect=self._quote)
        # pool -> (base token, quote token, price) as pool-info answers it
        self.pool_tokens = {POOL_A: (ERG_ID, SIGUSD_ID, 1.0), POOL_B: (ERG_ID, SIGUSD_ID, 1.05)}
        self.ergo.get_pool_info_by_address = AsyncMock(side_effect=self._pool_info)
        self.ergo.pair_token_addresses = MagicMock(return_value=(ERG_ID, SIGUSD_ID))
        self.ergo.swap_provider = "ergodex/amm"
        self.ergo._parse_dex_name = Gateway._parse_dex_name
        self.ergo._token_decimals = MagicMock(side_effect={"ERG": 9, "SigUSD": 2}.get)

        self.mdp = MagicMock()
        self.mdp.time.side_effect = lambda: self.now
        self.mdp.quantize_order_amount.side_effect = lambda connector, pair, amount: amount
        self.mdp.connectors = {"ergo-mainnet": self.ergo}
        self.controller = self.new_controller()
        self.set_loggers([self.controller.logger()])

    async def _quote(self, pair, is_buy, amount, pool_address=None):
        buy, sell = self.prices[pool_address]
        return (buy if is_buy else sell), self.answer_pool.get(pool_address, pool_address)

    async def _pool_info(self, pool, dex, trading_type):
        if self.pool_tokens.get(pool) is None:
            return None
        base, quote, price = self.pool_tokens[pool]
        return SimpleNamespace(address=pool, base_token_address=base, quote_token_address=quote, price=price)

    def new_controller(self, **overrides) -> ErgoPoolArb:
        fields = dict(id="epa-1", connector_name="ergo-mainnet", trading_pair="ERG-SigUSD", pool_a=POOL_A,
                      pool_b=POOL_B, order_amount=Decimal("10"), min_profitability=Decimal("0.02"))
        fields.update(overrides)
        return ErgoPoolArb(ErgoPoolArbConfig(**fields), market_data_provider=self.mdp, actions_queue=asyncio.Queue())

    def info(self, action: CreateExecutorAction, status: RunnableStatus, close_type=None,
             executed=Decimal("0"), custom=None) -> ExecutorInfo:
        config = action.executor_config
        custom_info = {"executed_amount_base": executed}
        custom_info.update(custom or {})
        done = status == RunnableStatus.TERMINATED
        return ExecutorInfo(
            id=config.id, timestamp=config.timestamp, type="order_executor", status=status, config=config,
            net_pnl_pct=Decimal("0"), net_pnl_quote=Decimal("0"), cum_fees_quote=Decimal("0"),
            filled_amount_quote=Decimal("0"), is_active=not done, is_trading=False, custom_info=custom_info,
            close_timestamp=self.now if done else None, close_type=close_type, controller_id="epa-1")

    def unknown(self, action):
        return {"outcome_unknown_order_id": f"order-{action.executor_config.id}",
                "outcome_unknown_connector": "ergo-mainnet", "outcome_unknown_close_timestamp": self.now}

    async def tick(self):
        await self.controller.update_processed_data()
        return self.controller.determine_executor_actions()

    async def bought(self):
        """Run the buy to a fill; return the buy and sell actions."""
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        self.assertEqual([], await self.tick())
        self.now += 300
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        [sell] = await self.tick()
        return buy, sell

    # ---- decisions ----

    async def test_no_trade_under_the_edge(self):
        self.prices[POOL_B] = (Decimal("1.03"), Decimal("1.015"))  # 1.5% gross, under 2%
        self.assertEqual([], await self.tick())
        self.assertEqual(4, self.ergo.get_quote_price_and_pool.await_count)

    async def test_network_fee_counts_against_the_edge(self):
        # 2.1% gross, 0.1% of it eaten by two fees of 0.05 ERG on 10 ERG.
        self.prices[POOL_B] = (Decimal("1.03"), Decimal("1.021"))
        self.ergo.network_transaction_fee = TokenAmount("ERG", Decimal("0.05"))
        self.assertEqual([], await self.tick())
        self.ergo.network_transaction_fee = TokenAmount("ERG", Decimal("0.002"))
        self.assertEqual(1, len(await self.tick()))

    async def test_no_trade_when_the_fee_cannot_be_priced(self):
        self.ergo.network_transaction_fee = None
        self.assertEqual([], await self.tick())

    async def test_buy_pinned_to_the_cheap_pool(self):
        [action] = await self.tick()
        config = action.executor_config
        self.assertEqual(TradeType.BUY, config.side)
        self.assertEqual(POOL_A, config.pool_address)
        self.assertEqual(Decimal("1.00"), config.price)
        self.assertEqual(Decimal("10"), config.amount)
        self.assertEqual(0, config.max_retries)
        self.assertEqual("epa-1", config.controller_id)
        # Each quote named its pool.
        pools = {(c.args[1], c.kwargs["pool_address"]) for c in self.ergo.get_quote_price_and_pool.await_args_list}
        self.assertEqual({(True, POOL_A), (False, POOL_B), (True, POOL_B), (False, POOL_A)}, pools)

    async def test_other_direction(self):
        self.prices = {POOL_A: (Decimal("1.06"), Decimal("1.05")), POOL_B: (Decimal("1.00"), Decimal("0.99"))}
        [action] = await self.tick()
        self.assertEqual(POOL_B, action.executor_config.pool_address)

    async def test_quote_from_another_pool_is_not_traded(self):
        self.answer_pool[POOL_B] = "cc" * 32
        self.assertEqual([], await self.tick())

    # ---- sequencing ----

    async def test_sell_only_after_the_buy_is_filled_on_the_other_pool(self):
        buy, sell = await self.bought()
        config = sell.executor_config
        self.assertEqual(TradeType.SELL, config.side)
        self.assertEqual(POOL_B, config.pool_address)
        self.assertEqual(Decimal("1.05"), config.price)
        self.assertEqual(Decimal("10"), config.amount)
        self.assertEqual(0, config.max_retries)

    async def test_one_arbitrage_in_flight(self):
        [buy] = await self.tick()
        quotes = self.ergo.get_quote_price_and_pool.await_count
        # Not yet listed, then running: no quotes, no second executor.
        self.assertEqual([], await self.tick())
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        for _ in range(3):
            self.now += 60
            self.assertEqual([], await self.tick())
        self.assertEqual(quotes, self.ergo.get_quote_price_and_pool.await_count)

    async def test_completed_arbitrage_resumes_after_the_delay(self):
        buy, sell = await self.bought()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.RUNNING)]
        self.assertEqual([], await self.tick())
        self.controller.executors_info[1] = self.info(sell, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD,
                                                      Decimal("10"))
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("INFO", "Arbitrage done"))
        self.now += 59
        self.assertEqual([], await self.tick())
        self.now += 1
        self.assertEqual(1, len(await self.tick()))

    async def test_definite_buy_failure_places_no_sell(self):
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.TERMINATED, CloseType.FAILED)]
        self.assertEqual([], await self.tick())
        self.assertIsNone(self.controller._halted)
        self.now += 60
        [again] = await self.tick()
        self.assertEqual(TradeType.BUY, again.executor_config.side)

    # ---- failures ----

    async def test_unknown_buy_holds_and_places_no_sell(self):
        [buy] = await self.tick()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.FAILED, custom=self.unknown(buy))]
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "may have been submitted; the sell is not placed"))
        quotes = self.ergo.get_quote_price_and_pool.await_count
        for self.now in (T0 + 61, T0 + 600, T0 + WINDOW - 1):
            self.assertEqual([], await self.tick())
        self.assertEqual(quotes, self.ergo.get_quote_price_and_pool.await_count)
        # The hold ends after its window and a balance refresh.
        self.now = T0 + WINDOW
        self.assertEqual(1, len(await self.tick()))
        self.ergo._update_balances.assert_awaited_once()

    async def test_unknown_sell_holds_and_halts(self):
        buy, sell = await self.bought()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED, custom=self.unknown(sell))]
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "stopped creating executors"))
        self.assertTrue(self.controller._outcome_hold.is_held(self.controller.executors_info, ["ergo-mainnet"]))
        self.now += WINDOW * 3
        self.assertEqual([], await self.tick())

    async def test_definite_sell_failure_stops_and_names_the_unhedged_position(self):
        self.controller = self.new_controller(sell_attempts=1)
        buy, sell = await self.bought()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED)]
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged(
            "ERROR", f"10 ERG bought on pool {POOL_A} at 1.00 (executor {buy.executor_config.id}) "
                     f"is not sold on pool {POOL_B}"))
        quotes = self.ergo.get_quote_price_and_pool.await_count
        for _ in range(3):
            self.now += 3600
            self.assertEqual([], await self.tick())
        self.assertEqual(quotes, self.ergo.get_quote_price_and_pool.await_count)
        self.assertIn("HALTED", "\n".join(self.controller.to_format_status()))

    async def test_sell_is_not_held_by_a_hold_from_elsewhere(self):
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        await self.tick()
        # Another controller's swap on this connector has an unknown outcome.
        outcome_hold._PROCESS_UNKNOWN_OUTCOMES["other"] = {
            "connector": "ergo-mainnet", "address": None, "controller_id": "x", "close_ts": self.now}
        self.prices[POOL_B] = (Decimal("1.05"), Decimal("1.04"))
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        [sell] = await self.tick()
        self.assertEqual(POOL_B, sell.executor_config.pool_address)
        # Placed at once, at a fresh quote, with a warning.
        self.assertEqual(Decimal("1.04"), sell.executor_config.price)
        self.assertTrue(self.is_partially_logged("WARNING", "holds new trades, but not this sell"))
        # New buys stay held.
        self.assertTrue(self.controller._is_held())

    # ---- config ----

    def test_config_refuses_bad_values(self):
        base = dict(id="c", pool_a=POOL_A, pool_b=POOL_B, order_amount=Decimal("1"),
                    min_profitability=Decimal("0.02"))
        ErgoPoolArbConfig(**base)
        for override in ({"pool_b": POOL_A}, {"pool_b": POOL_A.upper()}, {"pool_a": "zz" * 32},
                         {"pool_a": "aa" * 31}, {"order_amount": Decimal("0")},
                         {"min_profitability": Decimal("0")}, {"slippage_pct": Decimal("0")},
                         {"trading_pair": "ERG"}, {"sell_attempts": 0}, {"max_sell_loss_pct": Decimal("0")},
                         {"max_sell_loss_pct": Decimal("100")}):
            with self.assertRaises(ValueError, msg=str(override)):
                ErgoPoolArbConfig(**dict(base, **override))

    def test_config_without_pools_is_refused(self):
        with self.assertRaises(ValueError):
            ErgoPoolArbConfig(id="c")

    def test_pool_ids_are_lower_case(self):
        config = ErgoPoolArbConfig(id="c", pool_a=POOL_A.upper(), pool_b=POOL_B, order_amount=Decimal("10"))
        self.assertEqual(POOL_A, config.pool_a)

    def test_config_needs_an_order_amount(self):
        # No default: a 2-decimal quote token such as SigUSD refuses a small size.
        with self.assertRaises(ValueError):
            ErgoPoolArbConfig(id="c", pool_a=POOL_A, pool_b=POOL_B)

    # ---- review fixes ----

    def test_config_needs_an_edge_above_twice_the_slippage(self):
        # min_profitability is a fraction; slippage_pct is a percent.
        base = dict(id="c", pool_a=POOL_A, pool_b=POOL_B, order_amount=Decimal("10"))
        ErgoPoolArbConfig(**base, min_profitability=Decimal("0.0101"), slippage_pct=Decimal("0.5"))
        for min_profitability, slippage_pct in ((Decimal("0.01"), Decimal("0.5")), (Decimal("0.001"), Decimal("5"))):
            with self.assertRaises(ValueError, msg=f"{min_profitability} {slippage_pct}"):
                ErgoPoolArbConfig(**base, min_profitability=min_profitability, slippage_pct=slippage_pct)

    async def test_pool_of_another_pair_refuses_to_start(self):
        self.pool_tokens[POOL_B] = (ERG_ID, OTHER_ID, 300.0)
        self.prices[POOL_B] = (Decimal("301"), Decimal("300"))
        for _ in range(3):
            self.assertEqual([], await self.tick())
            self.now += 120
        self.assertTrue(self.is_partially_logged("ERROR", f"pool {POOL_B} holds tokens {ERG_ID} and {OTHER_ID}"))
        self.assertEqual(0, self.ergo.get_quote_price_and_pool.await_count)
        self.assertIn("REFUSED", "\n".join(self.controller.to_format_status()))
        self.assertIsNotNone(self.controller.get_custom_info()["refused"])

    async def test_pools_are_checked_once_and_either_order_is_the_pair(self):
        # A pool may list its tokens the other way round; its price is then base per quote.
        self.pool_tokens[POOL_B] = (SIGUSD_ID, ERG_ID, 1 / 1.05)
        [buy] = await self.tick()
        self.assertEqual(POOL_A, buy.executor_config.pool_address)
        self.assertEqual({POOL_A, POOL_B}, {c.args[0] for c in self.ergo.get_pool_info_by_address.await_args_list})
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        await self.tick()
        self.assertEqual(2, self.ergo.get_pool_info_by_address.await_count)

    async def test_pool_that_cannot_be_read_is_tried_again(self):
        self.pool_tokens[POOL_B] = None
        self.assertEqual([], await self.tick())
        self.assertEqual(0, self.ergo.get_quote_price_and_pool.await_count)
        self.assertIsNone(self.controller._refused)
        self.pool_tokens[POOL_B] = (ERG_ID, SIGUSD_ID, 1.05)
        self.assertEqual(1, len(await self.tick()))

    async def test_tolerance_that_rounds_to_nothing_is_refused(self):
        # 1 ERG at 0.52 SigUSD, 0.5%: 0.0026 SigUSD, under the 0.01 a 2-decimal token can carry.
        self.controller = self.new_controller(order_amount=Decimal("1"))
        self.pool_tokens = {POOL_A: (ERG_ID, SIGUSD_ID, 0.52), POOL_B: (ERG_ID, SIGUSD_ID, 0.55)}
        self.prices = {POOL_A: (Decimal("0.52"), Decimal("0.51")), POOL_B: (Decimal("0.56"), Decimal("0.55"))}
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "the price limit would bind no tolerance"))
        self.assertEqual(0, self.ergo.get_quote_price_and_pool.await_count)
        # At 10 ERG the tolerance is 0.026 SigUSD and it trades.
        self.controller = self.new_controller(order_amount=Decimal("10"))
        self.assertEqual(1, len(await self.tick()))

    async def test_sell_is_placed_at_a_fresh_quote_of_the_sell_pool(self):
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        await self.tick()
        # Pool B moved while the buy was mined.
        self.prices[POOL_B] = (Decimal("1.04"), Decimal("1.03"))
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        [sell] = await self.tick()
        self.assertEqual(Decimal("1.03"), sell.executor_config.price)
        self.assertEqual(POOL_B, sell.executor_config.pool_address)

    async def test_failed_sell_is_requoted_and_placed_again(self):
        buy, sell = await self.bought()
        self.prices[POOL_B] = (Decimal("1.03"), Decimal("1.02"))
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED)]
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("WARNING", "Sell attempt 1 of 3 failed"))
        self.now += 120
        [again] = await self.tick()
        self.assertEqual(TradeType.SELL, again.executor_config.side)
        self.assertEqual(POOL_B, again.executor_config.pool_address)
        self.assertEqual(Decimal("1.02"), again.executor_config.price)
        self.assertNotEqual(sell.executor_config.id, again.executor_config.id)
        self.assertIsNone(self.controller._halted)
        # The second sell fills: the arbitrage is done.
        self.controller.executors_info.append(
            self.info(again, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")))
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("INFO", "Arbitrage done"))

    async def test_sell_halts_once_its_attempts_are_used(self):
        buy, sell = await self.bought()
        infos = [self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        sells = [sell]
        for _ in range(2):
            infos.append(self.info(sells[-1], RunnableStatus.TERMINATED, CloseType.FAILED))
            self.controller.executors_info = list(infos)
            self.assertEqual([], await self.tick())
            self.now += 120
            [again] = await self.tick()
            sells.append(again)
        infos.append(self.info(sells[-1], RunnableStatus.TERMINATED, CloseType.FAILED))
        self.controller.executors_info = list(infos)
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "sell failed 3 times"))
        self.assertIsNotNone(self.controller._halted)
        self.assertEqual([], await self.tick())

    async def test_sell_halts_when_the_fresh_quote_is_under_the_loss_floor(self):
        buy, sell = await self.bought()
        # 10 ERG bought at 1.00: the floor is 9.90 SigUSD; 0.98 gives 9.80.
        self.prices[POOL_B] = (Decimal("0.99"), Decimal("0.98"))
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED)]
        self.assertEqual([], await self.tick())
        # The retry's quote, one block later, is under the floor: no halt yet, a re-quote one block on.
        self.now += 120
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("WARNING", "under the loss floor"))
        self.assertIsNone(self.controller._halted)
        self.now += 119
        self.assertEqual([], await self.tick())
        self.assertIsNone(self.controller._halted)
        # Still under the floor one block after the first breach: halt.
        self.now += 1
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "under 9.9"))
        self.assertIsNotNone(self.controller._halted)

    # ---- second review fixes ----

    async def _fail_sell(self, buy, sell, executed=Decimal("0")):
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED, executed)]
        return await self.tick()

    async def test_sell_retry_waits_one_block(self):
        self.ergo._block_time_seconds = 30.0
        buy, sell = await self.bought()
        failed_at = self.now
        self.assertEqual([], await self._fail_sell(buy, sell))
        quotes = self.ergo.get_quote_price_and_pool_fresh.await_count
        self.assertEqual([], await self.tick())
        self.now = failed_at + 29
        self.assertEqual([], await self.tick())
        # No quote is taken while waiting.
        self.assertEqual(quotes, self.ergo.get_quote_price_and_pool_fresh.await_count)
        self.now = failed_at + 30
        [again] = await self.tick()
        self.assertEqual(TradeType.SELL, again.executor_config.side)
        self.assertEqual(POOL_B, again.executor_config.pool_address)

    async def test_sell_retry_waits_120_seconds_without_a_block_time(self):
        self.ergo._block_time_seconds = None
        buy, sell = await self.bought()
        failed_at = self.now
        self.assertEqual([], await self._fail_sell(buy, sell))
        self.now = failed_at + 119
        self.assertEqual([], await self.tick())
        self.now = failed_at + 120
        [again] = await self.tick()
        self.assertEqual(TradeType.SELL, again.executor_config.side)

    async def test_sell_quotes_bypass_the_quote_cache(self):
        buy, sell = await self.bought()
        # The proposal's four quotes are cached ones; the sell's quote at the fill is fresh.
        self.assertEqual(4, self.ergo.get_quote_price_and_pool.await_count)
        self.ergo.get_quote_price_and_pool_fresh.assert_awaited_once_with(
            "ERG-SigUSD", False, Decimal("10"), pool_address=POOL_B)
        self.prices[POOL_B] = (Decimal("1.04"), Decimal("1.03"))
        await self._fail_sell(buy, sell)
        self.now += 120
        [again] = await self.tick()
        self.assertEqual(Decimal("1.03"), again.executor_config.price)
        self.assertEqual(4, self.ergo.get_quote_price_and_pool.await_count)
        self.assertEqual(2, self.ergo.get_quote_price_and_pool_fresh.await_count)

    async def test_one_block_floor_breach_that_recovers_sells(self):
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.RUNNING)]
        await self.tick()
        # When the buy fills, pool B is priced on an unconfirmed box: 0.98 gives 9.80, under the 9.90 floor.
        self.prices[POOL_B] = (Decimal("0.99"), Decimal("0.98"))
        self.now += 300
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        self.assertEqual([], await self.tick())
        self.assertIsNone(self.controller._halted)
        self.now += 60
        self.assertEqual([], await self.tick())
        # One block later the pool has settled back over the floor.
        self.prices[POOL_B] = (Decimal("1.04"), Decimal("1.03"))
        self.now += 60
        [sell] = await self.tick()
        self.assertEqual(TradeType.SELL, sell.executor_config.side)
        self.assertEqual(Decimal("1.03"), sell.executor_config.price)
        self.assertIsNone(self.controller._halted)

    async def test_failed_sell_with_a_fill_booked_halts(self):
        buy, sell = await self.bought()
        self.assertEqual([], await self._fail_sell(buy, sell, executed=Decimal("10")))
        self.assertIsNotNone(self.controller._halted)
        self.assertTrue(self.is_partially_logged("ERROR", "with 10 ERG booked"))
        self.now += 3600
        self.assertEqual([], await self.tick())
        [entry] = self.controller.get_custom_info()["possibly_unhedged"]
        self.assertEqual(f"sell ended {CloseType.FAILED} with 10 booked", entry["reason"])

    async def test_tolerance_is_measured_in_the_quote_token_when_the_base_is_coarser(self):
        # TOK (2 decimals) at 0.001 QQ (3 decimals): 0.5% of 100 TOK is 0.5 TOK but 0.0005 QQ, under the
        # 0.001 the price limit (in QQ) can carry.
        self.ergo._token_decimals = MagicMock(side_effect={"TOK": 2, "QQ": 3}.get)
        self.pool_tokens = {POOL_A: (ERG_ID, SIGUSD_ID, 0.001), POOL_B: (ERG_ID, SIGUSD_ID, 0.00106)}
        self.prices = {POOL_A: (Decimal("0.001"), Decimal("0.00099")), POOL_B: (Decimal("0.00106"), Decimal("0.00105"))}
        self.controller = self.new_controller(trading_pair="TOK-QQ", order_amount=Decimal("100"))
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "the price limit would bind no tolerance"))
        self.assertIsNotNone(self.controller._refused)
        self.assertEqual(0, self.ergo.get_quote_price_and_pool.await_count)
        # At 1000 TOK the tolerance is 0.005 QQ: not refused.
        self.controller = self.new_controller(trading_pair="TOK-QQ", order_amount=Decimal("1000"))
        await self.tick()
        self.assertIsNone(self.controller._refused)

    async def test_halt_does_not_freeze_a_sister_controller_sell(self):
        # Two configs of the same pools and pair in one process.
        self.controller = self.new_controller(sell_attempts=1)
        sister = self.new_controller(id="epa-2")
        await sister.update_processed_data()
        [sister_buy] = sister.determine_executor_actions()
        buy, sell = await self.bought()
        await self._fail_sell(buy, sell)
        self.assertIsNotNone(self.controller._halted)
        # The sister's buy fills: its sell is placed.
        self.assertIsNone(sister._halted)
        sister.executors_info = [
            self.info(sister_buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10"))]
        await sister.update_processed_data()
        [sister_sell] = sister.determine_executor_actions()
        self.assertEqual(TradeType.SELL, sister_sell.executor_config.side)
        self.assertEqual(POOL_B, sister_sell.executor_config.pool_address)

    async def test_halt_holds_whatever_the_case_of_the_pair(self):
        self.controller = self.new_controller(sell_attempts=1)
        buy, sell = await self.bought()
        await self._fail_sell(buy, sell)
        reason = self.controller._halted
        self.assertIsNotNone(reason)
        self.controller = self.new_controller(sell_attempts=1, trading_pair="ERG-SIGUSD")
        self.assertEqual(reason, self.controller._halted)
        self.now += 3600
        self.assertEqual([], await self.tick())

    async def test_halt_survives_a_new_controller_with_the_same_config(self):
        self.controller = self.new_controller(sell_attempts=1)
        buy, sell = await self.bought()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED)]
        await self.tick()
        reason = self.controller._halted
        self.assertIsNotNone(reason)
        # A strategy stop/start builds a new controller with the same config.
        self.controller = self.new_controller(sell_attempts=1)
        self.now += 3600
        self.assertEqual(reason, self.controller._halted)
        self.assertEqual([], await self.tick())
        self.assertIn("HALTED", "\n".join(self.controller.to_format_status()))
        # Other pools are another arbitrage.
        self.prices["cc" * 32] = self.prices[POOL_B]
        self.pool_tokens["cc" * 32] = self.pool_tokens[POOL_B]
        self.controller = self.new_controller(pool_b="cc" * 32)
        self.assertIsNone(self.controller._halted)
        self.assertEqual(1, len(await self.tick()))

    async def test_unknown_buy_is_listed_as_possibly_unhedged(self):
        [buy] = await self.tick()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.FAILED, custom=self.unknown(buy))]
        self.assertEqual([], await self.tick())
        entry = {"order_id": f"order-{buy.executor_config.id}", "amount": "10 ERG", "pool": POOL_A,
                 "price": "1.00", "reason": "buy outcome unknown"}
        self.assertEqual([entry], self.controller.get_custom_info()["possibly_unhedged"])
        status = "\n".join(self.controller.to_format_status())
        self.assertIn("Possibly unhedged buys", status)
        self.assertIn(f"order order-{buy.executor_config.id}: 10 ERG on pool {POOL_A} at 1.00", status)
        # Still listed after the hold has ended, and in a new controller of the same config.
        self.now = T0 + WINDOW
        await self.tick()
        self.controller = self.new_controller()
        self.assertEqual([entry], self.controller.get_custom_info()["possibly_unhedged"])

    async def test_halted_sell_lists_the_buy_as_possibly_unhedged(self):
        self.controller = self.new_controller(sell_attempts=1)
        buy, sell = await self.bought()
        self.controller.executors_info = [
            self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD, Decimal("10")),
            self.info(sell, RunnableStatus.TERMINATED, CloseType.FAILED)]
        await self.tick()
        [entry] = self.controller.get_custom_info()["possibly_unhedged"]
        self.assertEqual("10 ERG", entry["amount"])
        self.assertEqual(POOL_A, entry["pool"])
        self.assertEqual("sell failed", entry["reason"])

    async def test_buy_completed_with_nothing_booked_halts(self):
        [buy] = await self.tick()
        self.controller.executors_info = [self.info(buy, RunnableStatus.TERMINATED, CloseType.POSITION_HOLD)]
        self.assertEqual([], await self.tick())
        self.assertTrue(self.is_partially_logged("ERROR", "but no fill was booked; check the chain"))
        self.assertIsNotNone(self.controller._halted)
        self.now += 3600
        self.assertEqual([], await self.tick())
