import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock

from hummingbot.client.command.gateway_command import GatewayCommand
from hummingbot.client.settings import GATEWAY_CHAINS
from hummingbot.connector.gateway.common_types import known_chains


class KnownChainsTest(unittest.TestCase):
    """The client knows every chain the running Gateway reports, not only the built-in ones."""

    def setUp(self) -> None:
        super().setUp()
        self._saved = list(GATEWAY_CHAINS)

    def tearDown(self) -> None:
        GATEWAY_CHAINS[:] = self._saved
        super().tearDown()

    def test_built_in_chains_before_gateway_is_reached(self):
        GATEWAY_CHAINS.clear()
        self.assertEqual(("ethereum", "solana", "ergo"), known_chains())

    def test_chains_gateway_reports_are_added_after_the_built_in_ones(self):
        GATEWAY_CHAINS[:] = ["solana", "examplechain", "Ethereum", "ergo"]
        self.assertEqual(("ethereum", "solana", "ergo", "examplechain"), known_chains())

    def test_an_empty_chain_name_is_ignored(self):
        GATEWAY_CHAINS[:] = ["", "examplechain"]
        self.assertEqual(("ethereum", "solana", "ergo", "examplechain"), known_chains())


class GatewayBalanceChainsTest(unittest.TestCase):
    """`gateway balance` with no chain lists the chains the running Gateway serves."""

    def setUp(self) -> None:
        super().setUp()
        self._saved = list(GATEWAY_CHAINS)

    def tearDown(self) -> None:
        GATEWAY_CHAINS[:] = self._saved
        super().tearDown()

    def _chains_listed(self):
        command = MagicMock()
        command.client_config_map.commands_timeout.other_commands_timeout = 1
        gateway = MagicMock()
        # No default network: each chain is reported and skipped, nothing else is asked.
        gateway.get_default_network_for_chain = AsyncMock(return_value=None)
        command._get_gateway_instance.return_value = gateway
        asyncio.run(GatewayCommand._get_balances(command))
        return [call.args[0] for call in gateway.get_default_network_for_chain.await_args_list]

    def test_a_stock_gateway_gets_the_stock_chains(self):
        GATEWAY_CHAINS[:] = ["ethereum", "solana"]
        self.assertEqual(["ethereum", "solana"], self._chains_listed())

    def test_a_gateway_that_serves_more_chains_gets_them_all(self):
        GATEWAY_CHAINS[:] = ["solana", "ergo", "Ethereum", "examplechain"]
        self.assertEqual(["ethereum", "solana", "ergo", "examplechain"], self._chains_listed())


if __name__ == "__main__":
    unittest.main()
