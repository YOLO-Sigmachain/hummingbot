from decimal import Decimal
from unittest import TestCase

from pydantic import ValidationError

from controllers.generic.lp_rebalancer.lp_rebalancer import LPRebalancerConfig


class TestLPRebalancerRefusesAMM(TestCase):
    """The rebalancer places and moves price ranges, which an AMM pool share does not have."""

    def a_config(self, lp_provider: str) -> LPRebalancerConfig:
        return LPRebalancerConfig(
            id="lp-rebalancer-1",
            connector_name="ergo-mainnet",
            lp_provider=lp_provider,
            trading_pair="ERG-TOK",
            pool_address="pool-nft-1",
            total_amount_quote=Decimal("50"),
        )

    def test_an_amm_provider_is_refused_with_the_reason(self):
        with self.assertRaisesRegex(ValidationError, "is an AMM pool, which has no price ranges"):
            self.a_config("ergodex/amm")

    def test_a_clmm_provider_is_accepted(self):
        self.assertEqual("meteora/clmm", self.a_config("meteora/clmm").lp_provider)
