import unittest
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, patch

from hummingbot.core.data_type.common import TradeType
from hummingbot.core.gateway.gateway_http_client import GatewayHttpClient


class GatewayHttpClientSwapRouteTest(IsolatedAsyncioWrapperTestCase):
    """The swap methods must hit /trading/{router,clmm,amm}/*-swap.

    The trading type selects the route and `connector` is the bare name; it is not a
    'connector/type' string, and not the older /connectors/{dex}/{type}/*-swap path."""

    def setUp(self) -> None:
        super().setUp()
        self.client = GatewayHttpClient.get_instance()

    def test_to_chain_network_combines_short_network_with_chain(self):
        self.assertEqual("solana-mainnet-beta", self.client._to_chain_network("mainnet-beta", "solana"))

    def test_to_chain_network_leaves_full_network_untouched(self):
        self.assertEqual("solana-mainnet-beta", self.client._to_chain_network("solana-mainnet-beta", "solana"))
        self.assertEqual("solana-mainnet-beta", self.client._to_chain_network("solana-mainnet-beta", None))

    async def test_quote_swap_uses_unified_route_and_keys(self):
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"price": "1"})) as mock_req:
            await self.client.quote_swap(
                network="mainnet-beta", chain="solana", dex="jupiter", trading_type="router",
                base_asset="SOL", quote_asset="USDC", amount=Decimal("0.01"), side=TradeType.SELL,
            )
        method, path = mock_req.call_args.args[0], mock_req.call_args.args[1]
        payload = mock_req.call_args.args[2]
        self.assertEqual("get", method)
        self.assertEqual("trading/router/quote-swap", path)
        self.assertEqual("solana-mainnet-beta", payload["chainNetwork"])
        self.assertEqual("jupiter", payload["connector"])
        self.assertEqual("SOL", payload["baseToken"])
        self.assertEqual("USDC", payload["quoteToken"])
        self.assertEqual("SELL", payload["side"])
        self.assertNotIn("network", payload)  # legacy key must be gone

    async def test_execute_swap_uses_unified_route_and_keys(self):
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as mock_req:
            await self.client.execute_swap(
                network="mainnet-beta", chain="solana", dex="jupiter", trading_type="router",
                base_asset="SOL", quote_asset="USDC", amount=Decimal("0.02"), side=TradeType.SELL,
                wallet_address="WALLET",
            )
        method, path = mock_req.call_args.args[0], mock_req.call_args.args[1]
        payload = mock_req.call_args.args[2]
        self.assertEqual("post", method)
        self.assertEqual("trading/router/execute-swap", path)
        self.assertEqual("solana-mainnet-beta", payload["chainNetwork"])
        self.assertEqual("jupiter", payload["connector"])
        self.assertEqual("WALLET", payload["walletAddress"])
        self.assertNotIn("network", payload)

    async def _amm_execute(self, **kwargs):
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as mock_req:
            await self.client.execute_swap(
                network="mainnet", chain="ergo", dex="ergodex", trading_type="amm",
                base_asset="RSN", quote_asset="ERG", amount=Decimal("100"), wallet_address="WALLET", **kwargs,
            )
        return mock_req

    async def test_execute_swap_sends_the_sell_limit_on_the_amm_route(self):
        mock_req = await self._amm_execute(side=TradeType.SELL, min_amount_out=Decimal("2.3166"))
        payload = mock_req.call_args.args[2]
        self.assertEqual("trading/amm/execute-swap", mock_req.call_args.args[1])
        # A JSON number, like every other amount the client sends.
        self.assertEqual(2.3166, payload["minAmountOut"])
        self.assertIsInstance(payload["minAmountOut"], float)
        self.assertNotIn("maxAmountIn", payload)

    async def test_execute_swap_sends_the_buy_limit_on_the_amm_route(self):
        mock_req = await self._amm_execute(side=TradeType.BUY, max_amount_in=Decimal("2.3634"))
        payload = mock_req.call_args.args[2]
        self.assertEqual(2.3634, payload["maxAmountIn"])
        self.assertNotIn("minAmountOut", payload)

    async def test_execute_swap_without_limits_sends_neither_field(self):
        mock_req = await self._amm_execute(side=TradeType.SELL)
        payload = mock_req.call_args.args[2]
        self.assertNotIn("minAmountOut", payload)
        self.assertNotIn("maxAmountIn", payload)

    async def test_execute_swap_never_sends_a_limit_on_the_router_route(self):
        # The router route forbids undeclared fields; a limit there would fail every swap.
        with patch.object(self.client, "api_request", new=AsyncMock(return_value={"signature": "sig"})) as mock_req:
            with self.assertRaises(ValueError):
                await self.client.execute_swap(
                    network="mainnet-beta", chain="solana", dex="jupiter", trading_type="router",
                    base_asset="SOL", quote_asset="USDC", amount=Decimal("1"), side=TradeType.SELL,
                    wallet_address="WALLET", min_amount_out=Decimal("150"),
                )
        mock_req.assert_not_called()


if __name__ == "__main__":
    unittest.main()
