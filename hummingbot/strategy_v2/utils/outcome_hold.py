"""Hold after a swap whose outcome is unknown (FORK-NOTES section 2.2).

The Gateway connector marks a swap whose submit answer was lost with SUBMIT_OUTCOME_UNKNOWN: it
may still land. Classic amm_arb and the v2 arbitrage and XEMM controllers start no new trade on
that connector for blockTime x (confirmations + OUTCOME_HOLD_FOLLOW_BLOCKS), and not until a
balance refresh of that connector has succeeded after the window.
"""
import asyncio
import logging
import math
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple

# The Gateway connector puts this on a swap whose submit answer was lost: it may still land.
SUBMIT_OUTCOME_UNKNOWN = "[code: SUBMIT_OUTCOME_UNKNOWN]"
# Hold after such a failure for blockTime x (confirmations + this), the LP executor's window.
OUTCOME_HOLD_FOLLOW_BLOCKS = 10
# Hold for this long when the connector does not know its block time.
OUTCOME_HOLD_DEFAULT_SECONDS = 1800.0
# A failed balance refresh after the window is tried again at most this often.
OUTCOME_HOLD_REFRESH_RETRY_SECONDS = 10.0
# A failed balance refresh is logged at most this often.
OUTCOME_HOLD_REFRESH_LOG_SECONDS = 60.0

# Keys an executor puts in its custom info when one of its orders failed with the marker.
OUTCOME_UNKNOWN_ORDER_ID = "outcome_unknown_order_id"
OUTCOME_UNKNOWN_CONNECTOR = "outcome_unknown_connector"
OUTCOME_UNKNOWN_CLOSE_TIMESTAMP = "outcome_unknown_close_timestamp"
# Set as well when the other leg also failed with the marker: both swaps may land.
OUTCOME_UNKNOWN_OTHER_ORDER_ID = "outcome_unknown_other_order_id"
OUTCOME_UNKNOWN_OTHER_CONNECTOR = "outcome_unknown_other_connector"
OUTCOME_UNKNOWN_WAITING_SINCE = "outcome_unknown_waiting_since"
# A flagged executor still waiting for its other leg after the hold window logs an error this often.
OUTCOME_UNKNOWN_WAITING_LOG_SECONDS = 600.0


def outcome_hold_seconds(connector: Any) -> float:
    """How long to hold after an unknown outcome on this connector.

    blockTime x (confirmations + OUTCOME_HOLD_FOLLOW_BLOCKS) when the connector read the
    network's block time from Gateway, else OUTCOME_HOLD_DEFAULT_SECONDS.
    """
    block_time = getattr(connector, "_block_time_seconds", None)
    if (not isinstance(block_time, (int, float)) or isinstance(block_time, bool)
            or not math.isfinite(block_time) or block_time <= 0):
        return OUTCOME_HOLD_DEFAULT_SECONDS
    confirmations = getattr(connector, "_block_confirmations", 1)
    if not isinstance(confirmations, int) or isinstance(confirmations, bool) or confirmations < 1:
        confirmations = 1
    return float(block_time) * (confirmations + OUTCOME_HOLD_FOLLOW_BLOCKS)


# order id -> {"connector", "address", "controller_id", "close_ts"} for each order of unknown outcome
# that an executor of any controller in this process reported. close_ts None: the executor still
# runs. The hold lives in the process, not in the controller, so it survives the closed-executor
# buffer trim and a strategy stop/start, and holds every controller on that connector. A process
# restart forgets it.
_PROCESS_UNKNOWN_OUTCOMES: Dict[str, Dict[str, Any]] = {}
# Order ids whose hold has ended, so that an executor still listed does not hold again.
_PROCESS_CLEARED_OUTCOMES: Set[str] = set()


def reset_process_outcome_holds():
    """Forget every controller hold of this process (for tests)."""
    _PROCESS_UNKNOWN_OUTCOMES.clear()
    _PROCESS_CLEARED_OUTCOMES.clear()


class ControllerOutcomeHold:
    """The hold for a v2 controller, read from its executors' custom info into a process registry.

    An executor that reported an order of unknown outcome holds every controller on that
    connector while it runs, then for outcome_hold_seconds() after its close timestamp, then until
    one balance refresh of that connector succeeds. A held controller creates no executor.
    """

    def __init__(self, logger: logging.Logger, controller_id: Optional[str] = None):
        self._logger = logger
        self._controller_id = controller_id
        self._next_refresh_at: Dict[str, float] = {}
        self._last_refresh_warning = 0.0

    @staticmethod
    def _unknown_outcomes(executors_info: Iterable[Any]) -> Iterator[Tuple[Any, str, str]]:
        for executor in executors_info:
            info = getattr(executor, "custom_info", None) or {}
            order_id = info.get(OUTCOME_UNKNOWN_ORDER_ID)
            connector_name = info.get(OUTCOME_UNKNOWN_CONNECTOR)
            if order_id and connector_name:
                yield executor, order_id, connector_name
                # Both legs failed with the marker: the other leg holds its own connector too.
                other_id = info.get(OUTCOME_UNKNOWN_OTHER_ORDER_ID)
                other_connector = info.get(OUTCOME_UNKNOWN_OTHER_CONNECTOR)
                if other_id and other_connector:
                    yield executor, other_id, other_connector

    @staticmethod
    def _close_timestamp(executor: Any) -> Optional[float]:
        if not executor.is_done:
            return None
        close_ts = executor.custom_info.get(OUTCOME_UNKNOWN_CLOSE_TIMESTAMP) or executor.close_timestamp
        return close_ts if isinstance(close_ts, (int, float)) and not isinstance(close_ts, bool) else None

    def observe(self, executors_info: Iterable[Any], now: Optional[float] = None,
                connectors: Optional[Dict[str, Any]] = None):
        """Record this controller's executors' orders of unknown outcome in the process registry.

        With `now`, an order of this controller that is still running in the registry but no longer
        listed (the executor was stored, or the strategy was stopped) is taken as closed at `now`.
        """
        executors_info = list(executors_info)
        listed: Set[str] = set()
        for executor, order_id, connector_name in self._unknown_outcomes(executors_info):
            listed.add(order_id)
            if order_id in _PROCESS_CLEARED_OUTCOMES:
                continue
            entry = _PROCESS_UNKNOWN_OUTCOMES.setdefault(order_id, {
                "connector": connector_name,
                "address": None,
                "controller_id": getattr(executor, "controller_id", None),
                "close_ts": None,
            })
            if entry["close_ts"] is None:
                entry["close_ts"] = self._close_timestamp(executor)
        for order_id, entry in _PROCESS_UNKNOWN_OUTCOMES.items():
            if connectors is not None and entry["address"] is None and entry["connector"] in connectors:
                address = getattr(connectors[entry["connector"]], "address", None)
                entry["address"] = address if isinstance(address, str) else None
            if (now is not None and entry["close_ts"] is None and order_id not in listed
                    and entry["controller_id"] == self._controller_id):
                entry["close_ts"] = now

    def is_held(self, executors_info: Iterable[Any], connector_names: Iterable[str]) -> bool:
        """True while any order of unknown outcome in this process on these connectors is not cleared."""
        self.observe(executors_info)
        names = set(connector_names)
        return any(entry["connector"] in names for entry in _PROCESS_UNKNOWN_OUTCOMES.values())

    async def refresh_due(self, executors_info: Iterable[Any], connectors: Dict[str, Any], now: float):
        """Refresh the balances of each connector whose hold window has passed; clear its holds on success."""
        self.observe(executors_info, now, connectors)
        due: Dict[str, List[str]] = {}
        for order_id, entry in _PROCESS_UNKNOWN_OUTCOMES.items():
            connector_name = entry["connector"]
            if connector_name not in connectors:
                continue
            close_ts = entry["close_ts"]
            if close_ts is None or now < close_ts + outcome_hold_seconds(connectors.get(connector_name)):
                continue
            due.setdefault(connector_name, []).append(order_id)
        for connector_name, order_ids in due.items():
            if now < self._next_refresh_at.get(connector_name, 0.0):
                continue
            try:
                await connectors[connector_name]._update_balances()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._next_refresh_at[connector_name] = now + OUTCOME_HOLD_REFRESH_RETRY_SECONDS
                if now - self._last_refresh_warning >= OUTCOME_HOLD_REFRESH_LOG_SECONDS:
                    self._last_refresh_warning = now
                    self._logger.warning(f"Hold kept: balance refresh of {connector_name} after the hold "
                                         f"window failed: {e!r}")
                continue
            for order_id in order_ids:
                _PROCESS_UNKNOWN_OUTCOMES.pop(order_id, None)
                _PROCESS_CLEARED_OUTCOMES.add(order_id)
                self._logger.info(f"Hold for order {order_id} on {connector_name} ended: window passed and "
                                  "balances refreshed.")
