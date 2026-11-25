from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional, Tuple, TypedDict


class Chain(Enum):
    ETHEREUM = ('ethereum', 'ETH')
    SOLANA = ('solana', 'SOL')
    ERGO = ('ergo', 'ERG')

    def __init__(self, chain: str, native_currency: str):
        self.chain = chain
        self.native_currency = native_currency


def known_chains() -> Tuple[str, ...]:
    """Names of the chains Gateway can serve: the Chain members, then every other chain
    the running Gateway reports in /config/chains.

    The Chain members are always included, so this never knows fewer chains than the
    enum, including before Gateway has been reached. A chain added to Gateway by config
    is known as soon as the Gateway status monitor has read it, without a client change.
    Native currencies are not part of this: they differ per network, and each network's
    Gateway config states its own `nativeCurrencySymbol`.
    """
    # Imported here: hummingbot.client.settings imports far more than this module needs.
    from hummingbot.client.settings import GATEWAY_CHAINS

    names = [chain.chain for chain in Chain]
    for name in GATEWAY_CHAINS:
        name = name.lower()
        if name and name not in names:
            names.append(name)
    return tuple(names)


class Connector(Enum):
    def __int__(self, chain: Chain, connector: str):
        self.chain = chain
        self.connector = connector


class ConnectorType(Enum):
    SWAP = "SWAP"
    CLMM = "CLMM"
    AMM = "AMM"


class TransactionStatus(Enum):
    """Transaction status constants for gateway operations."""
    CONFIRMED = 1
    PENDING = 0
    FAILED = -1
    # Unknown to the chain: never received or dropped. On Solana this is terminal
    # once the transaction's blockhash expires (~90s after submission).
    NOT_FOUND = -2


class Token(TypedDict):
    """Token information from gateway."""
    symbol: str
    address: str
    decimals: int
    name: str


def get_connector_type(connector_name: str) -> ConnectorType:
    if "/clmm" in connector_name:
        return ConnectorType.CLMM
    elif "/amm" in connector_name:
        return ConnectorType.AMM
    return ConnectorType.SWAP


@dataclass
class PlaceOrderResult:
    update_timestamp: float
    client_order_id: str
    exchange_order_id: Optional[str]
    trading_pair: str
    misc_updates: Dict[str, Any] = field(default_factory=lambda: {})
    exception: Optional[Exception] = None


@dataclass
class CancelOrderResult:
    client_order_id: str
    trading_pair: str
    misc_updates: Dict[str, Any] = field(default_factory=lambda: {})
    not_found: bool = False
    exception: Optional[Exception] = None
