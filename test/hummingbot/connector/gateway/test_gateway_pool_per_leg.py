"""A pool named per swap (poolAddress), kept with its order, and the pool a quote came from.

With no pool named, every request is the one the client sent before.
"""
import json
from decimal import Decimal
from test.hummingbot.connector.gateway.test_gateway_swap_reconciliation import TX_ID, GatewaySwapTestBase
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from test.logger_mixin_for_test import LoggerMixinForTest
from unittest.mock import AsyncMock, patch

from hummingbot.connector.gateway.gateway import Gateway
from hummingbot.connector.gateway.gateway_in_flight_order import GatewayInFlightOrder
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient

POOL_A = "1a" * 32
POOL_B = "2b" * 32
ERG_ID = "0" * 64
RSN_ID = "8b" * 32
OTHER_ID = "4d" * 32


class HttpClientPoolFieldTest(IsolatedAsyncioWrapperTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.client = GatewayHttpClient.get_instance()

    async def _quote(self, trading_type="amm", **kwargs):
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"price": "1"})) as mock_req:
            await self.client.quote_swap(
                network="mainnet", chain="ergo", dex="ergodex", trading_type=trading_type,
                base_asset="ERG", quote_asset="SigUSD", amount=Decimal("2"), side=TradeType.BUY, **kwargs,
            )
        return mock_req

    async def _execute(self, trading_type="amm", **kwargs):
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as mock_req:
            await self.client.execute_swap(
                network="mainnet", chain="ergo", dex="ergodex", trading_type=trading_type,
                base_asset="ERG", quote_asset="SigUSD", amount=Decimal("2"), side=TradeType.SELL,
                wallet_address="WALLET", **kwargs,
            )
        return mock_req

    async def test_quote_without_pool_is_the_old_request(self):
        mock_req = await self._quote()
        self.assertEqual(
            ("get", "trading/amm/quote-swap",
             {"chainNetwork": "ergo-mainnet", "connector": "ergodex", "baseToken": "ERG",
              "quoteToken": "SigUSD", "amount": "2", "side": "BUY"}),
            mock_req.call_args.args[:3])

    async def test_quote_with_pool_sends_pool_address(self):
        payload = (await self._quote(pool_address=POOL_A)).call_args.args[2]
        self.assertEqual(POOL_A, payload["poolAddress"])

    async def test_quote_with_pool_on_clmm_route(self):
        payload = (await self._quote(trading_type="clmm", pool_address=POOL_A)).call_args.args[2]
        self.assertEqual(POOL_A, payload["poolAddress"])

    async def test_quote_with_pool_on_router_route_is_refused_before_sending(self):
        with patch.object(self.client, "api_request", new=AsyncMock()) as mock_req:
            with self.assertRaises(ValueError):
                await self.client.quote_swap(
                    network="mainnet-beta", chain="solana", dex="jupiter", trading_type="router",
                    base_asset="SOL", quote_asset="USDC", amount=Decimal("1"), side=TradeType.BUY,
                    pool_address=POOL_A,
                )
        mock_req.assert_not_called()

    async def test_execute_without_pool_is_the_old_body(self):
        mock_req = await self._execute()
        self.assertEqual(
            ("post", "trading/amm/execute-swap",
             {"chainNetwork": "ergo-mainnet", "connector": "ergodex", "baseToken": "ERG",
              "quoteToken": "SigUSD", "amount": 2.0, "side": "SELL", "walletAddress": "WALLET"}),
            mock_req.call_args.args[:3])

    async def test_execute_with_pool_sends_pool_address(self):
        body = (await self._execute(pool_address=POOL_B, min_amount_out=Decimal("1.5"))).call_args.args[2]
        self.assertEqual(POOL_B, body["poolAddress"])
        self.assertEqual(1.5, body["minAmountOut"])

    async def test_execute_with_pool_on_router_route_is_refused_before_sending(self):
        with patch.object(self.client, "api_request", new=AsyncMock()) as mock_req:
            with self.assertRaises(ValueError):
                await self.client.execute_swap(
                    network="mainnet-beta", chain="solana", dex="jupiter", trading_type="router",
                    base_asset="SOL", quote_asset="USDC", amount=Decimal("1"), side=TradeType.BUY,
                    wallet_address="WALLET", pool_address=POOL_A,
                )
        mock_req.assert_not_called()

    async def test_blank_pool_is_refused(self):
        with self.assertRaises(ValueError):
            await self._execute(pool_address="  ")


class ConnectorPoolPerLegTest(GatewaySwapTestBase, LoggerMixinForTest):
    def setUp(self) -> None:
        super().setUp()
        self.set_loggers([self.connector.logger()])
        # Gateway's quote answer; tokenIn/tokenOut follow the side, as ergodex answers them.
        self.answer = {"price": "0.0234", "poolAddress": POOL_A}
        self.gateway.quote_swap = AsyncMock(side_effect=self._quote_answer)
        # The quote cache is shared by every connector in the process.
        for name in ("get_quote_price", "get_quote_price_and_pool"):
            method = getattr(Gateway, name, None)
            if method is not None:
                method.cache_clear()
                self.addCleanup(method.cache_clear)

    async def _quote_answer(self, **kwargs):
        if kwargs["side"] == TradeType.BUY:
            tokens = {"tokenIn": ERG_ID, "tokenOut": RSN_ID}
        else:
            tokens = {"tokenIn": RSN_ID, "tokenOut": ERG_ID}
        return dict(tokens, **self.answer)

    async def test_quote_without_pool_sends_the_old_arguments(self):
        price = await self.connector.get_quote_price(self.PAIR, True, Decimal("100"))
        self.assertEqual(Decimal("0.0234"), price)
        self.assertEqual(
            {"network", "chain", "dex", "trading_type", "base_asset", "quote_asset", "amount", "side",
             "slippage_pct"},
            set(self.gateway.quote_swap.await_args.kwargs))

    async def test_quote_with_pool_sends_it(self):
        await self.connector.get_quote_price(self.PAIR, True, Decimal("100"), pool_address=POOL_A)
        self.assertEqual(POOL_A, self.gateway.quote_swap.await_args.kwargs["pool_address"])

    async def test_quote_and_pool_returns_the_answered_pool(self):
        self.assertEqual((Decimal("0.0234"), POOL_A),
                         await self.connector.get_quote_price_and_pool(self.PAIR, False, Decimal("100")))
        self.assertNotIn("pool_address", self.gateway.quote_swap.await_args.kwargs)

    async def test_quote_from_another_pool_than_named_gives_no_price(self):
        self.answer = {"price": "0.0234", "poolAddress": POOL_B}
        self.assertEqual((None, POOL_B), await self.connector.get_quote_price_and_pool(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))
        self.assertIsNone(await self.connector.get_quote_price(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))
        self.assertTrue(self.is_partially_logged("ERROR", f"came from pool {POOL_B}"))

    async def test_fresh_quote_bypasses_the_quote_cache(self):
        first = await self.connector.get_quote_price_and_pool(self.PAIR, False, Decimal("100"), pool_address=POOL_A)
        self.assertEqual((Decimal("0.0234"), POOL_A), first)
        # Gateway's answer changes: the cached quote serves the old one within its 5 s, the fresh one asks again.
        self.answer = {"price": "0.0250", "poolAddress": POOL_A}
        self.assertEqual(first, await self.connector.get_quote_price_and_pool(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.assertEqual(1, self.gateway.quote_swap.await_count)
        self.assertEqual((Decimal("0.0250"), POOL_A), await self.connector.get_quote_price_and_pool_fresh(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.answer = {"price": "0.0260", "poolAddress": POOL_A}
        self.assertEqual((Decimal("0.0260"), POOL_A), await self.connector.get_quote_price_and_pool_fresh(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.assertEqual(3, self.gateway.quote_swap.await_count)
        self.assertEqual(POOL_A, self.gateway.quote_swap.await_args.kwargs["pool_address"])

    async def test_fresh_quote_checks_the_named_pool_too(self):
        self.answer = {"price": "0.0234", "poolAddress": POOL_B}
        self.assertEqual((None, POOL_B), await self.connector.get_quote_price_and_pool_fresh(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))

    async def test_quote_answer_without_pool(self):
        self.answer = {"price": "0.0234"}
        self.assertEqual((Decimal("0.0234"), None),
                         await self.connector.get_quote_price_and_pool(self.PAIR, True, Decimal("100")))

    async def test_named_pool_quote_of_the_pair_gives_its_price(self):
        for is_buy in (True, False):
            self.assertEqual((Decimal("0.0234"), POOL_A), await self.connector.get_quote_price_and_pool(
                self.PAIR, is_buy, Decimal("100"), pool_address=POOL_A))

    async def test_named_pool_of_another_pair_gives_no_price(self):
        # The pool holds ERG and another token: Gateway prices it as it is.
        self.answer = {"price": "300", "poolAddress": POOL_A, "tokenIn": OTHER_ID, "tokenOut": ERG_ID}
        self.assertEqual((None, POOL_A), await self.connector.get_quote_price_and_pool(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.assertIsNone(await self.connector.get_quote_price(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.assertTrue(self.is_partially_logged("ERROR", f"the answer trades {OTHER_ID} for {ERG_ID}"))

    async def test_named_pool_quote_in_the_wrong_direction_gives_no_price(self):
        # A SELL of RSN must pay RSN in; this answer pays ERG in.
        self.answer = {"price": "0.0234", "poolAddress": POOL_A, "tokenIn": ERG_ID, "tokenOut": RSN_ID}
        self.assertEqual((None, POOL_A), await self.connector.get_quote_price_and_pool(
            self.PAIR, False, Decimal("100"), pool_address=POOL_A))
        self.assertTrue(self.is_partially_logged("ERROR", "ignoring it"))

    async def test_named_pool_quote_without_tokens_gives_no_price(self):
        self.gateway.quote_swap = AsyncMock(return_value={"price": "0.0234", "poolAddress": POOL_A})
        self.assertEqual((None, POOL_A), await self.connector.get_quote_price_and_pool(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))
        self.assertTrue(self.is_partially_logged("ERROR", "names no tokenIn/tokenOut"))

    async def test_named_pool_of_a_pair_not_in_the_token_list_gives_no_price(self):
        self.connector._token_data = {}
        self.assertEqual((None, POOL_A), await self.connector.get_quote_price_and_pool(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))
        self.assertTrue(self.is_partially_logged("ERROR", "are not in the token list"))

    async def test_quote_without_pool_does_not_check_tokens(self):
        self.answer = {"price": "0.0234", "poolAddress": POOL_A, "tokenIn": OTHER_ID, "tokenOut": ERG_ID}
        self.assertEqual((Decimal("0.0234"), POOL_A),
                         await self.connector.get_quote_price_and_pool(self.PAIR, True, Decimal("100")))

    def test_pair_token_addresses(self):
        # ERG is not in this token list: the native coin is the all-zero id.
        self.assertEqual((RSN_ID, ERG_ID), self.connector.pair_token_addresses(self.PAIR))
        self.assertEqual((RSN_ID, ERG_ID), self.connector.pair_token_addresses("rsn-ERG"))
        self.assertIsNone(self.connector.pair_token_addresses("XYZ-ERG"))

    async def test_quote_error_gives_none_and_none(self):
        self.gateway.quote_swap.side_effect = Exception("down")
        self.assertEqual((None, None), await self.connector.get_quote_price_and_pool(
            self.PAIR, True, Decimal("100"), pool_address=POOL_A))

    async def test_swap_without_pool_sends_the_old_arguments(self):
        await self._place_sell()
        self.assertEqual(
            {"dex", "trading_type", "base_asset", "quote_asset", "side", "amount", "network", "chain",
             "wallet_address", "slippage_pct", "min_amount_out"},
            set(self.gateway.execute_swap.await_args.kwargs))

    async def test_swap_without_pool_keeps_no_pool(self):
        order = await self._place_sell()
        self.assertIsNone(order.pool_address)
        self.assertNotIn("pool_address", order.to_json())
        self.assertTrue(self.is_partially_logged("INFO", "on the pool Gateway chooses"))

    async def test_swap_with_pool_sends_it_and_keeps_it_with_the_order(self):
        order = await self._place_sell(pool_address=POOL_B)
        kwargs = self.gateway.execute_swap.await_args.kwargs
        self.assertEqual(POOL_B, kwargs["pool_address"])
        # The price limit is still sent, computed as before.
        self.assertIn("min_amount_out", kwargs)
        self.assertEqual(POOL_B, order.pool_address)
        self.assertTrue(self.is_partially_logged("INFO", f"on pool {POOL_B}."))

        restored = GatewayInFlightOrder.from_json(json.loads(json.dumps(order.to_json())))
        self.assertEqual(POOL_B, restored.pool_address)

    async def test_second_submit_of_a_tracked_order_keeps_its_pool(self):
        await self._place_sell(pool_address=POOL_B)
        await self._place_sell()
        self.assertEqual(POOL_B, self.gateway.execute_swap.await_args.kwargs["pool_address"])

    async def test_requote_for_an_order_with_no_price_uses_its_pool(self):
        await self.connector._create_order(
            TradeType.BUY, "buy-1", self.PAIR, Decimal("100"), Decimal("NaN"), pool_address=POOL_A)
        await self._settle()
        self.assertEqual(POOL_A, self.gateway.quote_swap.await_args.kwargs["pool_address"])

    async def test_requote_for_an_order_without_pool_sends_none(self):
        await self.connector._create_order(TradeType.BUY, "buy-1", self.PAIR, Decimal("100"), Decimal("NaN"))
        await self._settle()
        self.assertNotIn("pool_address", self.gateway.quote_swap.await_args.kwargs)

    async def test_confirmed_answer_naming_the_pool_is_logged(self):
        self.gateway.execute_swap.return_value = {
            "signature": TX_ID, "status": 1,
            "data": {"amountIn": 100, "amountOut": 2.34, "fee": 0.0011, "poolAddress": POOL_B}}
        await self._place_sell(pool_address=POOL_B)
        self.assertTrue(self.is_partially_logged("INFO", f"traded on pool {POOL_B}"))

    async def test_confirmed_answer_naming_another_pool_is_an_error(self):
        self.gateway.execute_swap.return_value = {
            "signature": TX_ID, "status": 1,
            "data": {"amountIn": 100, "amountOut": 2.34, "fee": 0.0011, "poolAddress": POOL_A}}
        await self._place_sell(pool_address=POOL_B)
        self.assertTrue(self.is_partially_logged("ERROR", f"Gateway names pool {POOL_A}"))

    def test_buy_and_sell_pass_the_pool_to_the_order(self):
        with patch.object(Gateway, "_create_order", new=AsyncMock()) as create:
            with patch("hummingbot.connector.gateway.gateway.safe_ensure_future", side_effect=lambda c: c.close()):
                self.connector.buy(self.PAIR, Decimal("1"), OrderType.LIMIT, Decimal("1"), pool_address=POOL_A)
        self.assertEqual(POOL_A, create.call_args.kwargs["pool_address"])


class InFlightOrderPoolJsonTest(IsolatedAsyncioWrapperTestCase):
    def test_order_saved_without_pool_restores_with_none(self):
        order = GatewayInFlightOrder(client_order_id="o", trading_pair="ERG-SigUSD", order_type=OrderType.AMM_SWAP,
                                     trade_type=TradeType.BUY, creation_timestamp=1.0)
        data = order.to_json()
        self.assertNotIn("pool_address", data)
        self.assertIsNone(GatewayInFlightOrder.from_json(data).pool_address)
