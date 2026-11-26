import unittest.mock
from decimal import Decimal
from pathlib import Path
from test.hummingbot.strategy import assign_config_default
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import hummingbot.strategy.amm_arb.start as amm_arb_start
from hummingbot.strategy.amm_arb.amm_arb import AmmArbStrategy
from hummingbot.strategy.amm_arb.amm_arb_config_map import amm_arb_config_map


class AMMArbStartTest(IsolatedAsyncioWrapperTestCase):

    def setUp(self) -> None:
        super().setUp()
        self.strategy: AmmArbStrategy = None
        self.markets = {"binance": None, "balancer": None}
        self.notifications = []
        self.log_errors = []
        assign_config_default(amm_arb_config_map)
        amm_arb_config_map.get("strategy").value = "amm_arb"
        amm_arb_config_map.get("connector_1").value = "binance"
        amm_arb_config_map.get("market_1").value = "ETH-USDT"
        amm_arb_config_map.get("connector_2").value = "balancer"
        amm_arb_config_map.get("market_2").value = "ETH-USDT"
        amm_arb_config_map.get("order_amount").value = Decimal("1")
        amm_arb_config_map.get("min_profitability").value = Decimal("10")
        amm_arb_config_map.get("market_1_slippage_buffer").value = Decimal("1")
        amm_arb_config_map.get("market_2_slippage_buffer").value = Decimal("0")

    def _initialize_market_assets(self, market, trading_pairs):
        pass

    async def initialize_markets(self, market_names):
        pass

    def _notify(self, message):
        self.notifications.append(message)

    def logger(self):
        return self

    def error(self, message, exc_info):
        self.log_errors.append(message)

    @unittest.mock.patch('hummingbot.strategy.amm_arb.amm_arb.AmmArbStrategy.add_markets')
    async def test_amm_arb_strategy_creation(self, mock):
        await amm_arb_start.start(self)
        self.assertEqual(self.strategy._order_amount, Decimal(1))
        self.assertEqual(self.strategy._min_profitability, Decimal("10") / Decimal("100"))
        self.assertEqual(30.0, self.strategy._cex_leg_timeout)

    def test_cex_leg_timeout_is_configurable_with_a_30_second_default(self):
        # MP-H2. A runner that mounts the stock config map and template has no such key.
        if "cex_leg_timeout" not in amm_arb_config_map:
            self.skipTest("stock amm_arb_config_map mounted (no cex_leg_timeout)")
        config = amm_arb_config_map["cex_leg_timeout"]
        self.assertEqual(Decimal("30"), config.default)
        self.assertFalse(config.prompt_on_new)
        template = Path(amm_arb_start.__file__).parents[2] / "templates" / "conf_amm_arb_strategy_TEMPLATE.yml"
        self.assertIn("\ncex_leg_timeout: 30\n", template.read_text())

    @unittest.mock.patch('hummingbot.strategy.amm_arb.amm_arb.AmmArbStrategy.add_markets')
    async def test_cex_leg_timeout_is_passed_to_the_strategy(self, mock):
        if "cex_leg_timeout" not in amm_arb_config_map:
            self.skipTest("stock amm_arb_config_map mounted (no cex_leg_timeout)")
        amm_arb_config_map.get("cex_leg_timeout").value = Decimal("45")
        await amm_arb_start.start(self)
        self.assertEqual(45.0, self.strategy._cex_leg_timeout)
