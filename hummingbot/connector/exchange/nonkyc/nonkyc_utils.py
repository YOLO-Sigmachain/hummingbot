import datetime
from decimal import Decimal
from typing import Any, Dict, Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "ERG-USDT"

# NonKYC base rate is 0.2% maker and taker (nonkyc.io/fees, tier 0).
# The spec says the trade fee is in the quote asset, so a buy pays it on top of the cost.
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.002"),
    taker_percent_fee_decimal=Decimal("0.002"),
    buy_percent_fee_deducted_from_returns=False,
)


def is_market_active(exchange_info: Dict[str, Any]) -> bool:
    """
    True when the market is listed as active. Pause and API flags are checked
    separately, before each order, so a paused market fails an order locally.
    """
    return isinstance(exchange_info, dict) and exchange_info.get("isActive", False) is True


def to_decimal(value: Any) -> Optional[Decimal]:
    """Decimal from a JSON number or string, through str so a float keeps its printed digits."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        result = Decimal(str(value))
    except Exception:
        return None
    return result if result.is_finite() else None


def to_milliseconds(value: Any) -> Optional[int]:
    """Unix ms from a number, a numeric string or an ISO-8601 UTC string."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if text.isdigit():
            return int(text)
        try:
            parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            # The spec says string times are UTC
            parsed = parsed.replace(tzinfo=datetime.timezone.utc)
        return int(parsed.timestamp() * 1000)
    return None


class NonkycConfigMap(BaseConnectorConfigMap):
    connector: str = "nonkyc"
    nonkyc_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your NonKYC API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    nonkyc_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your NonKYC API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="nonkyc")


KEYS = NonkycConfigMap.model_construct()
