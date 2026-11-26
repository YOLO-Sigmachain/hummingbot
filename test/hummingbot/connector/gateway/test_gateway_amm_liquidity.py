"""AMM liquidity requests on a network whose transactions never expire.

Gateway answers an AMM add or remove PENDING, with the transaction id and, when it states
them, the amounts the transaction fixes in `pendingData`. The connector keeps those with the
open order and returns the transaction only once poll has confirmed it.
"""
import asyncio
import json
import unittest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp

from hummingbot.client.settings import AllConnectorSettings
from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.gateway.gateway_error import GatewayError
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient, exact_json_number
from hummingbot.strategy_v2.executors.gateway_utils import is_slippage_failure

TX_ID = "6a" * 32
PENDING_ADD = {"baseTokenAmountAdded": 10, "quoteTokenAmountAdded": 20, "lpTokenAmountAdded": 10, "fee": 0.0011}


class GatewayAMMLiquidityTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.connector = Gateway(
            connector_name="ergo-mainnet",
            chain="ergo",
            network="mainnet",
            address="9-wallet",
            trading_pairs=["ERG-TOK"],
            trading_required=True,
        )
        self.connector._set_current_timestamp(1640000000.0)
        self.connector._native_currency = "ERG"
        self.connector._read_network_timing({"blockTimeSeconds": 0.001, "confirmations": 1, "nativeDecimals": 9})
        self.connector.SIGNATURE_POLL_INTERVAL = 0.001
        self.connector.SIGNATURE_POLL_TIMEOUT = 0.05
        self.connector.get_pool_address = AsyncMock(return_value="listed-pool")

        self.gateway = MagicMock()
        self.gateway.amm_add_liquidity = AsyncMock(
            return_value={"signature": TX_ID, "status": 0, "pendingData": PENDING_ADD})
        self.gateway.amm_remove_liquidity = AsyncMock(return_value={"signature": TX_ID, "status": 0})
        self.poll_statuses = [0, 1]
        self.gateway.get_transaction_status = AsyncMock(side_effect=self._poll)
        patcher = patch.object(Gateway, "_get_gateway_instance", return_value=self.gateway)
        patcher.start()
        self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        AllConnectorSettings.get_connector_settings().pop("ergo-mainnet", None)
        super().tearDown()

    async def _poll(self, chain, network, tx_hash, fail_silently=False):
        status = self.poll_statuses.pop(0) if len(self.poll_statuses) > 1 else self.poll_statuses[0]
        return {"signature": tx_hash, "txStatus": status, "fee": 0.0011}

    async def _settle(self):
        # Order updates are processed in tasks of their own.
        for _ in range(10):
            await asyncio.sleep(0)

    async def _add(self, order_id="range-1", **kwargs):
        signature = await self.connector._amm_add_liquidity(
            TradeType.RANGE, order_id, "ERG-TOK", 2.0, 10.0, 20.0, dex_name="ergodex",
            pool_address="pool-nft-1", **kwargs)
        await self._settle()
        return signature

    async def test_a_pending_add_returns_only_once_poll_confirms_and_keeps_its_pending_data(self):
        signature = await self._add()

        self.assertEqual(TX_ID, signature)
        self.assertEqual(2, self.gateway.get_transaction_status.await_count)
        order = self.connector.get_order("range-1")
        self.assertEqual((TX_ID, PENDING_ADD), order.pending_swap_data)
        self.assertEqual(TX_ID, order.exchange_order_id)
        self.assertEqual(1, self.gateway.amm_add_liquidity.await_count)

    async def test_the_given_pool_is_used_without_looking_one_up(self):
        await self._add()

        self.assertEqual("pool-nft-1", self.gateway.amm_add_liquidity.call_args.kwargs["pool_address"])
        self.connector.get_pool_address.assert_not_awaited()

    async def test_without_a_pool_the_pairs_listed_pool_is_used_as_before(self):
        await self.connector._amm_add_liquidity(TradeType.RANGE, "range-1", "ERG-TOK", 2.0, 10.0, 20.0, dex_name="ergodex")

        self.assertEqual("listed-pool", self.gateway.amm_add_liquidity.call_args.kwargs["pool_address"])

    async def test_an_add_still_unresolved_when_polling_stops_returns_nothing_and_says_so(self):
        self.poll_statuses = [0]

        signature = await self._add()

        self.assertIsNone(signature)
        self.assertIn("TX_UNRESOLVED", self.connector.order_failure_reason("range-1"))
        self.assertEqual(1, self.gateway.amm_add_liquidity.await_count)

    async def test_a_lost_reply_marks_the_add_as_possibly_submitted(self):
        self.gateway.amm_add_liquidity = AsyncMock(side_effect=aiohttp.ServerDisconnectedError())

        signature = await self._add()

        self.assertIsNone(signature)
        self.assertIn("[code: SUBMIT_OUTCOME_UNKNOWN]", self.connector.order_failure_reason("range-1"))
        self.assertEqual(OrderState.FAILED, self.connector.get_order("range-1").current_state)

    async def test_a_refused_connection_is_not_marked_as_possibly_submitted(self):
        self.gateway.amm_remove_liquidity = AsyncMock(
            side_effect=aiohttp.ClientConnectorError(MagicMock(), OSError("refused")))

        signature = await self.connector._amm_remove_liquidity(
            TradeType.RANGE, "range-2", "ERG-TOK", 50.0, dex_name="ergodex", pool_address="pool-nft-1")

        self.assertIsNone(signature)
        self.assertNotIn("SUBMIT_OUTCOME_UNKNOWN", self.connector.order_failure_reason("range-2"))

    async def test_a_remove_sends_its_pool_and_slippage_and_keeps_its_pending_data(self):
        pending = {"baseTokenAmountRemoved": 8, "quoteTokenAmountRemoved": 25, "lpTokenAmountRemoved": 10}
        self.gateway.amm_remove_liquidity = AsyncMock(
            return_value={"signature": TX_ID, "status": 0, "pendingData": pending})

        signature = await self.connector._amm_remove_liquidity(
            TradeType.RANGE, "range-2", "ERG-TOK", 16.66666666, dex_name="ergodex",
            pool_address="pool-nft-1", slippage_pct=0.25, max_retries=0)

        self.assertEqual(TX_ID, signature)
        sent = self.gateway.amm_remove_liquidity.call_args.kwargs
        self.assertEqual("pool-nft-1", sent["pool_address"])
        self.assertEqual(0.25, sent["slippage_pct"])
        self.assertEqual(16.66666666, sent["percentage"])
        self.assertEqual((TX_ID, pending), self.connector.get_order("range-2").pending_swap_data)

    async def test_a_remove_given_its_lp_tokens_sends_them(self):
        await self.connector._amm_remove_liquidity(
            TradeType.RANGE, "range-2", "ERG-TOK", 100.0, dex_name="ergodex", pool_address="pool-nft-1",
            max_retries=0, lp_token_amount=Decimal("9.5"))

        self.assertEqual(Decimal("9.5"), self.gateway.amm_remove_liquidity.call_args.kwargs["lp_token_amount"])

    async def test_a_pending_answer_without_pending_data_keeps_its_signature_while_it_is_polled(self):
        self.gateway.amm_add_liquidity = AsyncMock(return_value={"signature": TX_ID, "status": 0})

        await self._add()

        self.assertEqual((TX_ID, {}), self.connector.get_order("range-1").pending_swap_data)

    async def test_a_confirmed_answer_keeps_its_data_with_the_order(self):
        """The shape a provider that answers CONFIRMED gives: the amounts and fee in `data`."""
        data = {"fee": 0.01, "baseTokenAmountAdded": 9.5, "quoteTokenAmountAdded": 19.0}
        self.gateway.amm_add_liquidity = AsyncMock(return_value={"signature": "sig1", "status": 1, "data": data})

        signature = await self._add()

        self.assertEqual("sig1", signature)
        self.assertEqual(("sig1", data), self.connector.get_order("range-1").pending_swap_data)

    async def test_gateways_pool_moved_refusal_is_recorded_as_a_slippage_failure(self):
        refusal = ("The pool moved: the pool price is now 2.1 TOK per ERG, more than 0.05% from 2 on pool "
                   "pool-nft-1. Nothing that can still execute was submitted; retry with a fresh quote.")
        self.gateway.amm_add_liquidity = AsyncMock(side_effect=GatewayError(refusal, status=409, http_error="HttpError"))

        self.assertIsNone(await self._add())

        reason = self.connector.order_failure_reason("range-1")
        self.assertIn(refusal, reason)
        self.assertTrue(is_slippage_failure(Exception(reason)))
        self.assertEqual(1, self.gateway.amm_add_liquidity.await_count)

    async def test_no_other_refusal_is_recorded_as_a_slippage_failure(self):
        moved = ("The pool moved: x. Nothing that can still execute was submitted; retry with a fresh quote.")
        others = [
            GatewayError("Pool p changed under 3 attempt(s); nothing that can still execute was submitted. Retry.",
                         status=409, http_error="HttpError"),
            GatewayError(moved, status=400, http_error="HttpError"),
            GatewayError("Two boxes at pool trees hold LP token t", status=409, http_error="HttpError"),
        ]
        for i, error in enumerate(others):
            self.gateway.amm_add_liquidity = AsyncMock(side_effect=error)
            await self._add(order_id=f"range-{i + 10}")
            self.assertFalse(is_slippage_failure(Exception(self.connector.order_failure_reason(f"range-{i + 10}"))))

    async def test_a_confirmed_add_is_not_booked_from_its_pending_data_by_the_status_loop(self):
        """A liquidity order's fill stays booked as before; the executor reads the amounts."""
        await self._add()
        order = self.connector.get_order("range-1")

        self.assertIsNone(self.connector._trade_update_from_pending_data(order, Decimal("0.0011")))


class GatewayHttpClientAMMRemoveTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_remove_sends_slippage_only_when_given(self):
        client = GatewayHttpClient.get_instance()
        with patch.object(client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as request:
            await client.amm_remove_liquidity(
                network="ergo-mainnet", wallet_address="9-wallet", pool_address="POOL",
                percentage=16.66666666, dex="ergodex", slippage_pct=0.25)
            await client.amm_remove_liquidity(
                network="ergo-mainnet", wallet_address="9-wallet", pool_address="POOL",
                percentage=100.0, dex="ergodex")

        with_slippage, without = (call.args[2] for call in request.call_args_list)
        self.assertEqual(0.25, with_slippage["slippagePct"])
        self.assertEqual(16.66666666, with_slippage["percentageToRemove"])
        self.assertNotIn("slippagePct", without)
        self.assertNotIn("lpTokenAmount", without)

    async def test_the_remove_sends_lp_tokens_instead_of_a_percentage_when_given(self):
        client = GatewayHttpClient.get_instance()
        with patch.object(client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as request:
            await client.amm_remove_liquidity(
                network="ergo-mainnet", wallet_address="9-wallet", pool_address="POOL",
                percentage=100.0, dex="ergodex", lp_token_amount=Decimal("12.345678901"))

        sent = request.call_args.args[2]
        self.assertEqual(12.345678901, sent["lpTokenAmount"])
        self.assertNotIn("percentageToRemove", sent)
        self.assertEqual("12.345678901", json.dumps(sent["lpTokenAmount"]))

    def test_an_lp_amount_is_sent_only_as_a_number_gateway_reads_exactly(self):
        self.assertEqual(10, exact_json_number(Decimal("10.0")))
        self.assertIsInstance(exact_json_number(Decimal("10.0")), int)
        self.assertEqual(2 ** 53 - 1, exact_json_number(Decimal(2 ** 53 - 1)))
        self.assertIsNone(exact_json_number(Decimal(2 ** 53)))
        self.assertEqual("123456.789012345", json.dumps(exact_json_number(Decimal("123456.789012345"))))
        self.assertIsNone(exact_json_number(Decimal("123456.7890123456")))  # 16 significant digits
        self.assertIsNone(exact_json_number(Decimal("0.00001")))  # a float writes it as 1e-05
        self.assertIsNone(exact_json_number(Decimal("0")))

    async def test_an_lp_amount_without_an_exact_json_number_is_refused_before_sending(self):
        client = GatewayHttpClient.get_instance()
        with patch.object(client, "api_request", new=AsyncMock()) as request:
            with self.assertRaises(ValueError):
                await client.amm_remove_liquidity(
                    network="ergo-mainnet", wallet_address="9-wallet", pool_address="POOL",
                    percentage=100.0, dex="ergodex", lp_token_amount=Decimal(2 ** 53))
        request.assert_not_awaited()
