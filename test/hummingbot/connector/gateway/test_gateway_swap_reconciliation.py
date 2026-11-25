import asyncio
import itertools
import random
import unittest
from decimal import ROUND_FLOOR, Decimal, localcontext
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.gateway import gateway_base
from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.connector.gateway.gateway_base import GatewayBase
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.event.event_logger import EventLogger
from hummingbot.core.event.events import MarketEvent
from hummingbot.core.gateway.gateway_error import GatewayError

TX_ID = "4f" * 32


class GatewaySwapTestBase(unittest.IsolatedAsyncioTestCase):
    """A Gateway connector on a network with two-minute blocks, talking to a mocked Gateway.

    The mocked execute-swap and poll answers are the ones a slow-block Gateway gives: the
    swap is returned PENDING with its transaction id, and poll moves it through PENDING,
    CONFIRMED, NOT_FOUND or FAILED.
    """

    PAIR = "RSN-ERG"

    def setUp(self) -> None:
        super().setUp()
        self.connector = Gateway(
            connector_name="ergo-mainnet",
            chain="ergo",
            network="mainnet",
            address="9-wallet",
            trading_pairs=[self.PAIR],
            trading_required=True,
        )
        self.connector._set_current_timestamp(1640000000.0)
        self.connector._native_currency = "ERG"
        self.connector._swap_provider = "ergodex/amm"
        self.connector._token_data = {"RSN": {"symbol": "RSN", "address": "8b" * 32, "decimals": 3}}
        self.connector._amount_quantum_dict = {"RSN": Decimal("0.001")}
        # The network's own config: blocks, confirmations, and the native coin's decimals
        # (the native coin is not in this network's token list). The block time is shrunk so
        # the test-sized poll window below governs; the rules are the same at any size.
        self.connector._read_network_timing({"blockTimeSeconds": 0.001, "confirmations": 1, "nativeDecimals": 9})
        self.connector.SIGNATURE_POLL_INTERVAL = 0.001
        self.connector.SIGNATURE_POLL_TIMEOUT = 0.05

        self.gateway = MagicMock()
        self.gateway.execute_swap = AsyncMock(return_value={"signature": TX_ID, "status": 0})
        self.poll_status = 0
        self.gateway.get_transaction_status = AsyncMock(side_effect=self._poll)
        patcher = patch.object(Gateway, "_get_gateway_instance", return_value=self.gateway)
        patcher.start()
        self.addCleanup(patcher.stop)

        self.filled_logger = EventLogger()
        self.failure_logger = EventLogger()
        self.tx_failure_logger = EventLogger()
        self.connector.add_listener(MarketEvent.OrderFilled, self.filled_logger)
        self.connector.add_listener(MarketEvent.OrderFailure, self.failure_logger)
        self.connector.add_listener(MarketEvent.TransactionFailure, self.tx_failure_logger)

    def tearDown(self) -> None:
        AllConnectorSettings.get_connector_settings().pop("ergo-mainnet", None)
        super().tearDown()

    async def _poll(self, chain, network, tx_hash, fail_silently=False):
        return {"signature": tx_hash, "txStatus": self.poll_status, "fee": 0.0011}

    async def _settle(self):
        # Order and trade updates are processed in tasks of their own.
        for _ in range(10):
            await asyncio.sleep(0)

    async def _place_sell(self, order_id: str = "sell-1", **kwargs):
        await self.connector._create_order(
            TradeType.SELL, order_id, self.PAIR, Decimal("100"), Decimal("0.0234"), **kwargs
        )
        await self._settle()
        return self.connector.get_order(order_id)

    async def _run_status_loop(self, poll_status: int):
        self.poll_status = poll_status
        await self.connector.update_order_status(self.connector.gateway_orders)
        await self._settle()


class GatewayUnresolvedSwapTest(GatewaySwapTestBase):
    """A swap still unresolved when polling stops is kept open on its hash, never failed."""

    async def test_unresolved_swap_stays_open_with_its_hash(self):
        order = await self._place_sell()

        self.assertIsNotNone(order)
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(TX_ID, order.exchange_order_id)
        self.assertEqual(0, len(self.failure_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_unresolved_swap_is_filled_once_the_status_loop_sees_it_confirmed(self):
        order = await self._place_sell()

        await self._run_status_loop(0)  # still pending: nothing changes
        self.assertTrue(order.is_open)
        await self._run_status_loop(1)

        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))
        self.assertEqual(Decimal("100"), self.filled_logger.event_log[0].amount)
        self.assertEqual(0, len(self.failure_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_unresolved_swap_fails_only_on_a_failed_status(self):
        order = await self._place_sell()

        await self._run_status_loop(-1)

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertEqual(1, len(self.failure_logger.event_log))
        self.assertEqual(1, len(self.tx_failure_logger.event_log))
        self.assertEqual(0, len(self.filled_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_not_found_never_fails_the_order_and_a_later_confirmation_fills_it_once(self):
        # Real two-minute blocks: the swap is polled for 720s before it is handed to the
        # status loop, so the order is already older than any not-found deadline then. The
        # wall clock the poll reads advances a minute per reading, so the 720s pass at once.
        self.connector._read_network_timing({"blockTimeSeconds": 120, "confirmations": 1, "nativeDecimals": 9})
        self.connector.SIGNATURE_POLL_TIMEOUT = GatewayBase.SIGNATURE_POLL_TIMEOUT
        self.assertEqual(720.0, self.connector.signature_poll_timeout)
        clock = MagicMock()
        clock.time.side_effect = itertools.count(start=0, step=60)
        self.poll_status = -2
        with patch.object(gateway_base, "time", clock):
            order = await self._place_sell()

        self.assertEqual(1, self.gateway.execute_swap.await_count)
        self.assertTrue(order.is_open)
        self.assertEqual(TX_ID, order.exchange_order_id)

        self.connector._set_current_timestamp(order.creation_timestamp + 10000)
        with patch.object(self.connector.logger(), "network") as network_log:
            for _ in range(self.connector._order_tracker.lost_order_count_limit + 1):
                await self._run_status_loop(-2)
        self.assertEqual(OrderState.OPEN, order.current_state)
        self.assertEqual(1, network_log.call_count)

        await self._run_status_loop(1)
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))
        self.assertEqual(0, len(self.failure_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_the_hash_is_saved_with_the_order_while_the_swap_is_polled(self):
        # A client restarted during the poll restores the order with its hash, so the
        # restarted status loop can still count its fill.
        saved_states = []

        async def poll(chain, network, tx_hash, fail_silently=False):
            saved_states.append(self.connector.tracking_states)
            return {"signature": tx_hash, "txStatus": 0, "fee": 0.0011}

        self.gateway.get_transaction_status = AsyncMock(side_effect=poll)
        await self._place_sell()
        self.assertEqual(TX_ID, saved_states[0]["sell-1"]["exchange_order_id"])

        restarted = Gateway(
            connector_name="ergo-mainnet", chain="ergo", network="mainnet", address="9-wallet",
            trading_pairs=[self.PAIR], trading_required=True,
        )
        restarted._native_currency = "ERG"
        restarted._read_network_timing({"blockTimeSeconds": 120, "confirmations": 1, "nativeDecimals": 9})
        restarted.restore_tracking_states(saved_states[0])
        self.gateway.get_transaction_status = AsyncMock(side_effect=self._poll)
        self.poll_status = 1
        with patch("hummingbot.connector.gateway.gateway_in_flight_order.GET_GATEWAY_EX_ORDER_ID_TIMEOUT", 0.01):
            await restarted.update_order_status(restarted.gateway_orders)
        await self._settle()
        self.assertEqual(OrderState.FILLED, restarted.get_order("sell-1").current_state)

    async def test_a_market_order_is_never_filled_at_a_nan_price(self):
        self.poll_status = 0
        await self.connector._create_order(TradeType.SELL, "sell-market", self.PAIR, Decimal("100"), Decimal("NaN"))
        await self._settle()
        await self._run_status_loop(1)

        self.assertEqual(1, len(self.filled_logger.event_log))
        fill = self.filled_logger.event_log[0]
        self.assertEqual(Decimal("0"), fill.price)
        self.assertEqual(Decimal("100"), fill.amount)

    async def test_cancel_leaves_the_order_open_and_does_not_raise(self):
        order = await self._place_sell()

        self.assertEqual("sell-1", self.connector.cancel(self.PAIR, "sell-1"))
        await self._settle()
        self.assertTrue(order.is_open)
        self.assertEqual(TX_ID, order.exchange_order_id)


class GatewaySwapAnswersTest(GatewaySwapTestBase):
    """No Gateway answer leads the client to post the same swap a second time."""

    async def test_retry_answer_fails_the_order_and_sends_nothing_more(self):
        # 409: the pool moved outside the bound, and nothing that can still execute was sent.
        self.gateway.execute_swap = AsyncMock(side_effect=GatewayError(
            "The pool moved: ... outside the bound. Nothing that can still execute was submitted; "
            "retry with a fresh quote.", status=409,
        ))
        order = await self._place_sell()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertIsNone(order.exchange_order_id)
        self.assertEqual(1, self.gateway.execute_swap.await_count)
        self.gateway.get_transaction_status.assert_not_awaited()

    def _unknown_outcome(self) -> GatewayError:
        return GatewayError(
            f"Transaction {TX_ID} pending: the node's answer to the submit was lost (socket hang up). "
            "Poll /chains/ergo/poll with this signature until it confirms or fails before any new swap; "
            "do not resubmit.",
            status=504, code="TRANSACTION_TIMEOUT",
        )

    async def test_unknown_submit_outcome_still_pending_keeps_the_order_open_on_its_hash(self):
        self.gateway.execute_swap = AsyncMock(side_effect=self._unknown_outcome())
        order = await self._place_sell()

        self.assertTrue(order.is_open)
        self.assertEqual(TX_ID, order.exchange_order_id)
        self.assertEqual(1, self.gateway.execute_swap.await_count)

        await self._run_status_loop(1)
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_unknown_submit_outcome_then_confirmed_is_counted_once(self):
        self.poll_status = 1
        self.gateway.execute_swap = AsyncMock(side_effect=self._unknown_outcome())
        order = await self._place_sell()

        self.assertEqual(TX_ID, order.exchange_order_id)
        self.assertEqual(1, self.gateway.execute_swap.await_count)
        await self._run_status_loop(1)
        await self._run_status_loop(1)
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))

    async def test_unknown_submit_outcome_then_not_found_is_not_resubmitted(self):
        self.poll_status = -2
        self.gateway.execute_swap = AsyncMock(side_effect=self._unknown_outcome())
        order = await self._place_sell()

        self.assertTrue(order.is_open)
        self.assertEqual(TX_ID, order.exchange_order_id)
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_a_lost_reply_to_the_submit_fails_the_order_as_possibly_submitted(self):
        # The connection to Gateway dropped before its reply arrived: Gateway may have
        # submitted the swap, and nothing names its transaction.
        self.gateway.execute_swap = AsyncMock(side_effect=aiohttp.ServerDisconnectedError())
        order = await self._place_sell()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertIn("[code: SUBMIT_OUTCOME_UNKNOWN]", self.connector.order_failure_reason("sell-1"))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_a_lost_reply_to_the_submit_on_solana_rules_fails_the_order_as_before(self):
        self.connector._read_network_timing({"nativeDecimals": 9})
        self.gateway.execute_swap = AsyncMock(side_effect=asyncio.TimeoutError())
        order = await self._place_sell()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertNotIn("SUBMIT_OUTCOME_UNKNOWN", self.connector.order_failure_reason("sell-1"))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_pending_then_confirmed_while_polling_is_counted_once(self):
        self.poll_status = 1
        order = await self._place_sell()

        self.assertEqual(TX_ID, order.exchange_order_id)
        await self._run_status_loop(1)
        await self._run_status_loop(1)
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))
        self.assertEqual(1, self.gateway.execute_swap.await_count)

    async def test_a_refused_connection_fails_the_order_as_never_sent(self):
        # Gateway is not listening: the connection was refused before any request was sent,
        # so the swap was not submitted and may be placed again.
        self.gateway.execute_swap = AsyncMock(side_effect=aiohttp.ClientConnectorError(
            MagicMock(), ConnectionRefusedError(111, "Connection refused")))
        order = await self._place_sell()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertIn("Connection refused", self.connector.order_failure_reason("sell-1"))
        self.assertNotIn("SUBMIT_OUTCOME_UNKNOWN", self.connector.order_failure_reason("sell-1"))
        self.assertEqual(1, self.gateway.execute_swap.await_count)


class GatewaySwapStatusLoopWhilePollingTest(GatewaySwapTestBase):
    """The status loop and the swap's own poll run at the same time on the same transaction."""

    async def _poll(self, chain, network, tx_hash, fail_silently=False):
        # The swap's own poll asks with fail_silently; the status loop does not.
        status = self.inner_status if fail_silently else self.poll_status
        return {"signature": tx_hash, "txStatus": status, "fee": 0.0011}

    async def _start_sell(self):
        """Start a SELL and return once its hash is attached, while its own poll still runs."""
        self.connector.SIGNATURE_POLL_TIMEOUT = 5.0
        self.inner_status = 0
        task = asyncio.ensure_future(self.connector._create_order(
            TradeType.SELL, "sell-1", self.PAIR, Decimal("100"), Decimal("0.0234")))
        for _ in range(1000):
            await asyncio.sleep(0.001)
            order = self.connector.get_order("sell-1")
            if order is not None and order.exchange_order_id is not None:
                return task, order
        self.fail("the swap's hash was never attached")

    async def test_a_late_failure_from_the_swaps_own_poll_leaves_a_filled_order_filled(self):
        task, order = await self._start_sell()
        await self._run_status_loop(1)
        self.assertEqual(OrderState.FILLED, order.current_state)

        self.inner_status = -1
        await asyncio.wait_for(task, timeout=5)
        await self._settle()

        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual(1, len(self.filled_logger.event_log))
        self.assertEqual(0, len(self.failure_logger.event_log))
        self.assertIsNone(self.connector.order_failure_reason("sell-1"))

    async def test_a_failure_from_the_swaps_own_poll_still_fails_an_open_order(self):
        task, order = await self._start_sell()

        self.inner_status = -1
        await asyncio.wait_for(task, timeout=5)
        await self._settle()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertIn("TX_NOT_CONFIRMED", self.connector.order_failure_reason("sell-1"))


class GatewaySwapNetworkConfigRecoveryTest(GatewaySwapTestBase):
    """A Solana connector whose network config could not be read at start reads it again on a swap."""

    def setUp(self) -> None:
        super().setUp()
        self.connector = Gateway(
            connector_name="solana-mainnet-beta", chain="solana", network="mainnet-beta",
            address="sol-wallet", trading_pairs=["SOL-USDC"], trading_required=True,
        )
        self.connector._set_current_timestamp(1640000000.0)
        self.connector._native_currency = "SOL"
        self.connector._swap_provider = "jupiter/router"
        self.connector.SIGNATURE_POLL_INTERVAL = 0.001
        self.connector.SIGNATURE_POLL_TIMEOUT = 0.05
        self.gateway.get_configuration = AsyncMock(return_value={
            "swapProvider": "jupiter/router", "nativeCurrencySymbol": "SOL",
        })

    def tearDown(self) -> None:
        AllConnectorSettings.get_connector_settings().pop("solana-mainnet-beta", None)
        super().tearDown()

    async def test_a_dropped_swap_is_re_submitted_once_after_the_config_is_read(self):
        self.assertFalse(self.connector.not_found_proves_dropped)
        self.gateway.execute_swap = AsyncMock(side_effect=[
            {"signature": "sig-1", "status": 0},
            {"signature": "sig-2", "status": 1, "data": {"amountIn": 1, "amountOut": 150}},
        ])
        self.poll_status = -2
        await self.connector._create_order(TradeType.SELL, "sell-1", "SOL-USDC", Decimal("1"), Decimal("150"))
        await self._settle()

        self.gateway.get_configuration.assert_awaited_once_with("solana-mainnet-beta")
        self.assertTrue(self.connector.not_found_proves_dropped)
        self.assertEqual(2, self.gateway.execute_swap.await_count)
        order = self.connector.get_order("sell-1")
        self.assertEqual(OrderState.FILLED, order.current_state)
        self.assertEqual("sig-2", order.exchange_order_id)

    async def test_until_the_config_is_read_a_dropped_swap_is_not_re_submitted(self):
        self.gateway.get_configuration = AsyncMock(side_effect=asyncio.TimeoutError())
        self.poll_status = -2
        await self.connector._create_order(TradeType.SELL, "sell-1", "SOL-USDC", Decimal("1"), Decimal("150"))
        await self._settle()

        self.assertFalse(self.connector.not_found_proves_dropped)
        self.assertEqual(1, self.gateway.execute_swap.await_count)
        self.assertTrue(self.connector.get_order("sell-1").is_open)


class GatewaySwapQuotedBookingPriceTest(GatewaySwapTestBase):
    """An order placed without a usable price is booked at Gateway's quote, when one comes."""

    async def test_a_market_order_confirmed_by_the_status_loop_is_booked_at_the_quote(self):
        self.gateway.quote_swap = AsyncMock(return_value={"price": 0.0235})
        self.poll_status = 0
        await self.connector._create_order(TradeType.SELL, "sell-market", self.PAIR, Decimal("100"), Decimal("NaN"))
        await self._settle()
        await self._run_status_loop(1)

        self.gateway.quote_swap.assert_awaited_once()
        self.assertEqual(TradeType.SELL, self.gateway.quote_swap.await_args.kwargs["side"])
        self.assertEqual(Decimal("100"), self.gateway.quote_swap.await_args.kwargs["amount"])
        fill = self.filled_logger.event_log[0]
        self.assertEqual(Decimal("0.0235"), fill.price)
        self.assertEqual(Decimal("100"), fill.amount)
        self.assertNotIn("min_amount_out", self.gateway.execute_swap.await_args.kwargs)

    async def test_an_order_with_a_usable_price_asks_for_no_quote(self):
        self.gateway.quote_swap = AsyncMock(return_value={"price": 0.0235})
        await self._place_sell()
        await self._run_status_loop(1)

        self.gateway.quote_swap.assert_not_awaited()
        self.assertEqual(Decimal("0.0234"), self.filled_logger.event_log[0].price)

    async def test_without_a_quote_the_fill_is_booked_at_price_0(self):
        self.gateway.quote_swap = AsyncMock(side_effect=GatewayError("no route", status=500))
        self.poll_status = 0
        await self.connector._create_order(TradeType.SELL, "sell-market", self.PAIR, Decimal("100"), Decimal("NaN"))
        await self._settle()
        await self._run_status_loop(1)

        self.assertEqual(1, self.gateway.execute_swap.await_count)
        self.assertEqual(Decimal("0"), self.filled_logger.event_log[0].price)

    async def test_the_submission_never_waits_for_the_quote(self):
        released = asyncio.Event()

        async def slow_quote(**kwargs):
            await released.wait()
            return {"price": 0.0235}

        self.gateway.quote_swap = AsyncMock(side_effect=slow_quote)
        self.poll_status = 1
        await self.connector._create_order(TradeType.BUY, "buy-market", self.PAIR, Decimal("100"), Decimal("0"))

        self.assertEqual(1, self.gateway.execute_swap.await_count)
        self.gateway.quote_swap.assert_awaited_once()
        released.set()
        await self._settle()


class GatewaySwapPriceLimitTest(GatewaySwapTestBase):
    """An AMM swap carries the order's price as a limit Gateway can bind the swap to."""

    def _limit(self, trade_type: TradeType, pair: str, amount: str, price: str, slippage=None):
        return self.connector._swap_price_limit(trade_type, pair, Decimal(amount), Decimal(price), slippage)

    async def test_sell_sends_min_amount_out_in_quote_token(self):
        self.poll_status = 1
        await self._place_sell(slippage_pct=1.0)

        kwargs = self.gateway.execute_swap.await_args.kwargs
        # 100 RSN x 0.0234 ERG x (1 - 1%) = 2.3166 ERG
        self.assertEqual(Decimal("2.316600000"), kwargs["min_amount_out"])
        self.assertNotIn("max_amount_in", kwargs)

    async def test_buy_sends_max_amount_in_in_quote_token(self):
        self.poll_status = 1
        await self.connector._create_order(
            TradeType.BUY, "buy-1", self.PAIR, Decimal("100"), Decimal("0.0234"), slippage_pct=1.0
        )

        kwargs = self.gateway.execute_swap.await_args.kwargs
        # 100 RSN x 0.0234 ERG x (1 + 1%) = 2.3634 ERG
        self.assertEqual(Decimal("2.363400000"), kwargs["max_amount_in"])
        self.assertNotIn("min_amount_out", kwargs)

    async def test_the_same_limit_goes_with_every_submission(self):
        # Solana rules (no block timing): a dropped transaction is re-submitted once, and the
        # re-submission is bound by the same limit as the first.
        self.connector._read_network_timing({"nativeDecimals": 9})
        self.gateway.execute_swap = AsyncMock(side_effect=[
            {"signature": "sig-1", "status": 0},
            {"signature": "sig-2", "status": 1, "data": {"amountIn": 100, "amountOut": 2.33}},
        ])
        self.poll_status = -2
        order = await self._place_sell(slippage_pct=1.0)

        self.assertEqual(2, self.gateway.execute_swap.await_count)
        limits = [call.kwargs["min_amount_out"] for call in self.gateway.execute_swap.await_args_list]
        self.assertEqual([Decimal("2.316600000")] * 2, limits)
        # The order carries the hash of the submission that landed, not of the first one.
        self.assertEqual("sig-2", order.exchange_order_id)

    async def test_no_limit_for_a_market_order_or_on_other_routes(self):
        self.poll_status = 1
        await self.connector._create_order(TradeType.SELL, "sell-nan", self.PAIR, Decimal("100"), Decimal("NaN"))
        self.assertNotIn("min_amount_out", self.gateway.execute_swap.await_args.kwargs)

        self.connector._swap_provider = "jupiter/router"
        await self.connector._create_order(TradeType.SELL, "sell-router", self.PAIR, Decimal("100"), Decimal("0.0234"))
        self.assertNotIn("min_amount_out", self.gateway.execute_swap.await_args.kwargs)

    async def test_no_limit_on_an_amm_connector_that_does_not_bind_it(self):
        # A Solana AMM swap of a token traded by mint address, whose decimals the client
        # does not know: no limit is computed or sent, and the swap goes out as before.
        self.connector._read_network_timing({"nativeCurrencySymbol": "SOL"})
        self.connector._swap_provider = "raydium/amm"
        self.poll_status = 1
        mint = "Mint" + "1" * 40
        await self.connector._create_order(
            TradeType.SELL, "sell-raydium", f"{mint}-SOL", Decimal("100"), Decimal("0.0234"), slippage_pct=1.0
        )
        await self._settle()

        kwargs = self.gateway.execute_swap.await_args.kwargs
        self.assertNotIn("min_amount_out", kwargs)
        self.assertNotIn("max_amount_in", kwargs)
        self.assertNotEqual(OrderState.FAILED, self.connector.get_order("sell-raydium").current_state)

    def test_without_a_slippage_the_order_price_is_the_limit(self):
        self.assertEqual({"min_amount_out": Decimal("2.340000000")}, self._limit(TradeType.SELL, self.PAIR, "100", "0.0234"))
        self.assertEqual({"max_amount_in": Decimal("2.340000000")}, self._limit(TradeType.BUY, self.PAIR, "100", "0.0234"))

    def test_rounding_tightens_at_the_quote_tokens_decimals(self):
        self.connector._token_data["SigUSD"] = {"symbol": "SigUSD", "decimals": 2}
        self.connector._token_data["SigRSV"] = {"symbol": "SigRSV", "decimals": 0}
        # 1.5 x 1.237 = 1.8555; less 0.5% = 1.8462225, up to 1.85; plus 0.5% = 1.8647775, down to 1.86
        self.assertEqual({"min_amount_out": Decimal("1.85")}, self._limit(TradeType.SELL, "ERG-SigUSD", "1.5", "1.237", 0.5))
        self.assertEqual({"max_amount_in": Decimal("1.86")}, self._limit(TradeType.BUY, "ERG-SigUSD", "1.5", "1.237", 0.5))
        # A token with no decimals: 2 x 1234.5678 x 0.99 = 2444.444244, up to 2445
        self.assertEqual({"min_amount_out": Decimal("2445")}, self._limit(TradeType.SELL, "ERG-SigRSV", "2", "1234.5678", 1))
        # Just under a whole unit at nine decimals: 3 x 0.333333333333 = 0.999999999999
        self.assertEqual({"min_amount_out": Decimal("1.000000000")}, self._limit(TradeType.SELL, self.PAIR, "3", "0.333333333333"))
        self.assertEqual({"max_amount_in": Decimal("0.999999999")}, self._limit(TradeType.BUY, self.PAIR, "3", "0.333333333333"))

    def test_a_limit_is_cut_to_what_a_json_number_holds_exactly(self):
        # 1,000,000 RSN x 12.3456789012345678 = 12345678.9012345678 ERG: 17 significant digits
        # at nine decimals, more than a JSON number carries exactly. Rounded to the nearest
        # 15 significant digits, both ways: within half a unit in the 15th digit.
        sell = self._limit(TradeType.SELL, self.PAIR, "1000000", "12.3456789012345678")["min_amount_out"]
        buy = self._limit(TradeType.BUY, self.PAIR, "1000000", "12.3456789012345678")["max_amount_in"]
        self.assertEqual(Decimal("12345678.9012346"), sell)
        self.assertEqual(Decimal("12345678.9012346"), buy)
        for limit in (sell, buy):
            self.assertEqual(limit, Decimal(repr(float(limit))))

    def test_native_coin_decimals_come_from_the_network_config(self):
        # ERG is not in this network's token list; the network config gives its 9 decimals.
        self.assertNotIn("ERG", self.connector._token_data)
        limit = self._limit(TradeType.SELL, self.PAIR, "7", "0.0123456789012")["min_amount_out"]
        self.assertEqual(Decimal("0.086419753"), limit)

    def test_unknown_decimals_refuse_the_swap(self):
        self.connector._native_decimals = None
        for trade_type in (TradeType.SELL, TradeType.BUY):
            with self.assertRaises(ValueError) as ctx:
                self._limit(trade_type, self.PAIR, "100", "0.0234")
            self.assertIn("decimals of ERG are unknown", str(ctx.exception))

    async def test_a_swap_whose_limit_cannot_be_bound_fails_before_anything_is_sent(self):
        self.connector._native_decimals = None
        order = await self._place_sell()

        self.assertEqual(OrderState.FAILED, order.current_state)
        self.assertIn("decimals of ERG are unknown", self.connector.order_failure_reason("sell-1"))
        self.gateway.execute_swap.assert_not_awaited()

    def test_decimals_are_found_whatever_the_case_of_the_symbol(self):
        # Gateway trades "ERG-SIGUSD" as it trades "ERG-SigUSD", and SigUSD has 2 decimals.
        self.connector._token_data["SigUSD"] = {"symbol": "SigUSD", "address": "03" * 32, "decimals": 2}
        # 10 x 1.237 x 0.99 = 12.2463, up to 12.25; 10 x 1.237 x 1.01 = 12.4937, down to 12.49
        self.assertEqual({"min_amount_out": Decimal("12.25")}, self._limit(TradeType.SELL, "ERG-SIGUSD", "10", "1.237", 1))
        self.assertEqual({"max_amount_in": Decimal("12.49")}, self._limit(TradeType.BUY, "ERG-SIGUSD", "10", "1.237", 1))

    def test_decimals_are_found_by_token_id(self):
        token_id = "03" * 32
        self.connector._token_data["SigUSD"] = {"symbol": "SigUSD", "address": token_id, "decimals": 2}
        self.assertEqual({"min_amount_out": Decimal("12.25")}, self._limit(TradeType.SELL, f"ERG-{token_id}", "10", "1.237", 1))

    def test_a_symbol_that_matches_two_tokens_but_for_case_is_unknown(self):
        self.connector._token_data["Abc"] = {"symbol": "Abc", "address": "0a" * 32, "decimals": 2}
        self.connector._token_data["ABc"] = {"symbol": "ABc", "address": "0b" * 32, "decimals": 6}
        with self.assertRaises(ValueError):
            self._limit(TradeType.SELL, "ERG-ABC", "10", "1.237")

    def test_a_price_read_from_a_float_is_bound_to_what_the_pool_pays(self):
        # The quote's price reaches the client as a float, and the order's price is that
        # float floored to 1e-15. At zero slippage the limit must still be the pool's own
        # output, not one raw unit above it.
        rng = random.Random(11)
        # (pair, quote decimals, amount, range of the pool's output in raw quote units)
        cases = (("rsBTC-ERG", 9, "0.01", 5 * 10 ** 11, 2 * 10 ** 12),   # about 1e5 ERG per rsBTC
                 ("ERG-RSN", 3, "7", 2 * 10 ** 5, 4 * 10 ** 5),            # about 42.7 RSN per ERG
                 ("ERG-SigUSD", 2, "10.5", 10 ** 3, 2 * 10 ** 3),          # about 1.2 SigUSD per ERG
                 ("RSN-ERG", 9, "100", 2 * 10 ** 9, 3 * 10 ** 9))          # about 0.0234 ERG per RSN
        self.connector._token_data["SigUSD"] = {"symbol": "SigUSD", "decimals": 2}
        for pair, decimals, amount, low, high in cases:
            for _ in range(300):
                raw_out = rng.randint(low, high)
                exact_out = Decimal(raw_out).scaleb(-decimals)
                with localcontext() as context:
                    context.prec = 100
                    # Gateway floors the ratio to 18 decimals and sends it as a double.
                    quoted = float((exact_out / Decimal(amount)).quantize(Decimal("1e-18"), rounding=ROUND_FLOOR))
                price = self.connector.quantize_order_price(pair, Decimal(quoted))
                limit = self._limit(TradeType.SELL, pair, amount, str(price))["min_amount_out"]
                self.assertEqual(exact_out, limit, (pair, raw_out, quoted))

    def test_a_limit_below_what_gateway_can_read_refuses_the_swap(self):
        # 1000 units at 4e-10 ERG is 4e-7 ERG: as a JSON number that reads "4e-7" in Gateway.
        with self.assertRaises(ValueError):
            self._limit(TradeType.SELL, self.PAIR, "1000", "0.0000000004")
        self.assertEqual({"min_amount_out": Decimal("0.000001000")},
                         self._limit(TradeType.SELL, self.PAIR, "1000", "0.000000001"))

    def test_no_limit_without_a_usable_price(self):
        for price in ("NaN", "0", "-1"):
            self.assertEqual({}, self._limit(TradeType.SELL, self.PAIR, "100", price), price)

    def test_a_limit_is_never_looser_than_the_order_and_never_finer_than_the_token(self):
        rng = random.Random(7)
        for _ in range(2000):
            decimals = rng.randint(0, 18)
            self.connector._token_data["Q"] = {"symbol": "Q", "decimals": decimals}
            amount = Decimal(rng.randint(1, 10 ** 12)).scaleb(-rng.randint(0, 9))
            price = Decimal(rng.randint(1, 10 ** 15)).scaleb(-rng.randint(0, 15))
            slippage = Decimal(rng.randint(0, 500)) / Decimal("100")
            for trade_type, key, sign in ((TradeType.SELL, "min_amount_out", -1), (TradeType.BUY, "max_amount_in", 1)):
                with localcontext() as context:
                    context.prec = 100
                    exact = amount * price * (Decimal("100") + sign * slippage) / Decimal("100")
                    # Half a unit in the 15th significant digit: the most float noise removed.
                    tolerance = Decimal(1).scaleb(exact.adjusted() - 14) / 2
                try:
                    limit = self.connector._swap_price_limit(trade_type, "B-Q", amount, price, slippage)[key]
                except ValueError:
                    # Refused, and nothing sent, only when the limit is too small or too large
                    # to send, or when a BUY's limit is less than one unit of the quote token.
                    self.assertTrue(
                        exact < Decimal("1e-6") or exact >= Decimal("1e15") - 1
                        or key == "max_amount_in" and exact < Decimal(1).scaleb(-decimals),
                        (exact, key, decimals),
                    )
                    continue
                if trade_type == TradeType.SELL:
                    self.assertGreaterEqual(limit, exact - tolerance)
                else:
                    self.assertLessEqual(limit, exact + tolerance)
                self.assertLessEqual(-limit.normalize().as_tuple().exponent, decimals)
                self.assertEqual(limit, Decimal(repr(float(limit))))
                # Gateway can read its JSON text: no exponent, at most 15 significant digits.
                self.assertTrue(
                    limit == 0 and key == "min_amount_out" or Decimal("1e-6") <= limit < Decimal("1e15"), limit
                )

    def test_a_limit_of_1e15_or_more_refuses_the_swap(self):
        # 1e9 RSN x 1234567.89012345678 ERG is about 1.23e15 ERG: its JSON number reads with
        # 16 digits in Gateway, more than it accepts.
        for trade_type in (TradeType.SELL, TradeType.BUY):
            with self.assertRaises(ValueError) as ctx:
                self._limit(trade_type, self.PAIR, "1000000000", "1234567.89012345678")
            self.assertIn("1e15 or more", str(ctx.exception))
        with self.assertRaises(ValueError):
            self._limit(TradeType.SELL, self.PAIR, "1000000", "1000000000")
        self.assertEqual({"min_amount_out": Decimal("999999999999999")},
                         self._limit(TradeType.SELL, self.PAIR, "1", "999999999999999"))

    def test_a_buy_whose_limit_rounds_to_0_refuses_the_swap(self):
        self.connector._token_data["SigUSD"] = {"symbol": "SigUSD", "decimals": 2}
        # 3 x 0.001 = 0.003 SigUSD: less than one cent, down to 0.00. No swap could pass it.
        with self.assertRaises(ValueError) as ctx:
            self._limit(TradeType.BUY, "ERG-SigUSD", "3", "0.001")
        self.assertIn("rounds to 0", str(ctx.exception))
        # A SELL's minimum rounds up, to 0.01.
        self.assertEqual({"min_amount_out": Decimal("0.01")}, self._limit(TradeType.SELL, "ERG-SigUSD", "3", "0.001"))

    async def test_a_swap_whose_limit_cannot_be_sent_fails_before_anything_is_sent(self):
        await self.connector._create_order(
            TradeType.SELL, "sell-1", self.PAIR, Decimal("1000000000"), Decimal("1234567.89012345678"))
        await self._settle()

        self.assertEqual(OrderState.FAILED, self.connector.get_order("sell-1").current_state)
        self.assertIn("1e15 or more", self.connector.order_failure_reason("sell-1"))
        self.gateway.execute_swap.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
