from typing import Any, Dict, List, Optional

from hummingbot.connector.exchange.nonkyc.nonkyc_constants import UNSEQUENCED_SNAPSHOT_ID
from hummingbot.connector.exchange.nonkyc.nonkyc_utils import to_milliseconds
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType


def book_side(levels: Any) -> List[List[str]]:
    """[[price, quantity], ...] as strings, from either list or dict levels."""
    result = []
    for level in levels or []:
        if isinstance(level, dict):
            price, quantity = level.get("price"), level.get("quantity")
        elif isinstance(level, (list, tuple)) and len(level) >= 2:
            price, quantity = level[0], level[1]
        else:
            continue
        if price is None or quantity is None:
            continue
        result.append([str(price), str(quantity)])
    return result


def book_update_id(data: Dict[str, Any]) -> Optional[int]:
    """
    The id that orders websocket snapshots and diffs: the websocket book sequence.
    The REST /market/orderbook 'sequence' is the trade counter, not this one, so it
    is never read as a book id (see NonkycOrderBook.rest_snapshot_message_from_exchange).
    """
    sequence = data.get("sequence")
    if sequence is not None and not isinstance(sequence, bool):
        try:
            return int(str(sequence))
        except ValueError:
            return None
    return None


class NonkycOrderBook(OrderBook):
    """
    A websocket snapshot carries the book sequence; diffs after it are applied in
    order. A REST snapshot has no book sequence (UNSEQUENCED_SNAPSHOT_ID): no diff is
    replayed or applied on it, and the next websocket snapshot replaces it.
    """

    def restore_from_snapshot_and_diffs(self, snapshot: OrderBookMessage, diffs: List[OrderBookMessage]):
        # The base version bisects with OrderBookMessage.__lt__, which puts any message
        # with an update id first, so it replays every saved diff. Here only the run of
        # diffs that follows the snapshot without a gap is replayed (none on a REST
        # snapshot), so diffs from before a sequence reset never come back.
        self.apply_snapshot(snapshot.bids, snapshot.asks, snapshot.update_id)
        if snapshot.update_id <= UNSEQUENCED_SNAPSHOT_ID:
            return
        by_id = {}
        for diff in diffs:
            by_id.setdefault(diff.update_id, []).append(diff)
        next_id = snapshot.update_id + 1
        while next_id in by_id:
            for diff in by_id[next_id]:
                self.apply_diffs(diff.bids, diff.asks, diff.update_id)
            next_id += 1

    def apply_diffs(self, bids, asks, update_id: int):
        # On a REST snapshot (no sequence) no diff can be placed: wait for a websocket snapshot
        if self.snapshot_uid <= UNSEQUENCED_SNAPSHOT_ID:
            return
        # A diff older than the snapshot is already in it; replaying it would undo newer levels
        if update_id < self.snapshot_uid:
            return
        super().apply_diffs(bids, asks, update_id)

    @classmethod
    def rest_snapshot_message_from_exchange(cls,
                                            msg: Dict[str, Any],
                                            timestamp: float,
                                            metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Snapshot from REST /market/orderbook. Its 'sequence' is the trade counter, so
        the snapshot gets UNSEQUENCED_SNAPSHOT_ID instead.
        """
        if metadata:
            msg.update(metadata)
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": UNSEQUENCED_SNAPSHOT_ID,
            "bids": book_side(msg.get("bids")),
            "asks": book_side(msg.get("asks")),
        }, timestamp=timestamp)

    @classmethod
    def snapshot_message_from_exchange(cls,
                                       msg: Dict[str, Any],
                                       timestamp: float,
                                       metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Snapshot from the WS snapshotOrderbook params, which carry the book sequence.
        """
        if metadata:
            msg.update(metadata)
        update_id = book_update_id(msg)
        if update_id is None:
            raise ValueError(f"NonKYC order book snapshot has no sequence: {list(msg.keys())}")
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": book_side(msg.get("bids")),
            "asks": book_side(msg.get("asks")),
        }, timestamp=timestamp)

    @classmethod
    def diff_message_from_exchange(cls,
                                   msg: Dict[str, Any],
                                   timestamp: Optional[float] = None,
                                   metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        Diff from the WS updateOrderbook params. Quantities are the new level size; 0 removes it.
        """
        if metadata:
            msg.update(metadata)
        update_id = book_update_id(msg)
        if update_id is None:
            raise ValueError(f"NonKYC order book diff has no sequence: {list(msg.keys())}")
        return OrderBookMessage(OrderBookMessageType.DIFF, {
            "trading_pair": msg["trading_pair"],
            "update_id": update_id,
            "bids": book_side(msg.get("bids")),
            "asks": book_side(msg.get("asks")),
        }, timestamp=timestamp)

    @classmethod
    def trade_message_from_exchange(cls, trade: Dict[str, Any], metadata: Optional[Dict] = None) -> OrderBookMessage:
        """
        One public trade (one element of updateTrades params.data).
        """
        if metadata:
            trade.update(metadata)
        ts_ms = to_milliseconds(trade.get("timestampms", trade.get("timestamp")))
        if ts_ms is None:
            raise ValueError("NonKYC public trade has no timestamp")
        return OrderBookMessage(OrderBookMessageType.TRADE, {
            "trading_pair": trade["trading_pair"],
            "trade_type": float(TradeType.SELL.value) if trade["side"] == "sell" else float(TradeType.BUY.value),
            "trade_id": str(trade["id"]),
            "update_id": ts_ms,
            "price": str(trade["price"]),
            "amount": str(trade["quantity"]),
        }, timestamp=ts_ms * 1e-3)
