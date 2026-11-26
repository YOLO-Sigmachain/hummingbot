"""
Messages for the keyless NonKYC replay harness.

Recorded: the ERG/USDT market record and the order book shape come from public
GETs of https://api.nonkyc.io/api/v2 (/market/getlist, /market/orderbook) on
2026-11-26, trimmed to the fields the connector reads.

Handmade: private replies (createorder, getorder, account/trades, balances,
cancelorder, errors) follow the Order, Trade2, Balance and Error schemas of
https://api.nonkyc.io/openapi.json v1.3.0. No key was used. Websocket report
shapes are not documented; they are guesses and the connector only uses them
to decide which order to re-read over REST.
"""
import copy
import json
from decimal import Decimal
from typing import Any, Dict, List, Optional

SYMBOL = "ERG/USDT"
TRADING_PAIR = "ERG-USDT"
EXCHANGE_ORDER_ID = "6745a1f0c1d2e3f4a5b6c7d8"

ERG_USDT_MARKET = {
    "_id": "651213de4db82952da233722",
    "symbol": "ERG/USDT",
    "primaryName": "Ergo",
    "primaryTicker": "ERG",
    "lastPrice": "0.1888",
    "priceDecimals": 6,
    "quantityDecimals": 4,
    "isActive": True,
    "isPaused": False,
    "minimumQuantity": 0,
    "maxAllowedPrice": "",
    "minAllowedPrice": "",
    "pauseBuys": False,
    "pauseSells": False,
    "minQuote": 1,
    "minQuoteActive": True,
    "isMinQuoteActive": True,
    "apiExcluded": False,
    "id": "651213de4db82952da233722",
}

# Other markets, built from shapes seen in the live list
LTC_BTC_MARKET = {**ERG_USDT_MARKET, "symbol": "LTC/BTC", "primaryTicker": "LTC", "priceDecimals": 8,
                  "quantityDecimals": 3, "minimumQuantity": 0.008, "minQuote": 1e-05}
PAUSED_MARKET = {**ERG_USDT_MARKET, "symbol": "XPE/USDT", "primaryTicker": "XPE", "isPaused": True}
BUYS_PAUSED_MARKET = {**ERG_USDT_MARKET, "symbol": "ERG/BTC", "primaryTicker": "ERG", "pauseBuys": True,
                      "minQuote": 1e-05}
API_EXCLUDED_MARKET = {**ERG_USDT_MARKET, "symbol": "ERG/XMR", "primaryTicker": "ERG", "apiExcluded": True}
INACTIVE_MARKET = {**ERG_USDT_MARKET, "symbol": "OLD/USDT", "primaryTicker": "OLD", "isActive": False}

MARKETS = [ERG_USDT_MARKET, LTC_BTC_MARKET, PAUSED_MARKET, BUYS_PAUSED_MARKET, API_EXCLUDED_MARKET, INACTIVE_MARKET]

ORDER_BOOK_SNAPSHOT = {
    "marketid": "651213de4db82952da233722",
    "symbol": "ERG/USDT",
    "timestamp": 1795727619232,
    "sequence": "5337",
    "bids": [{"price": "0.188714", "quantity": "0.2015"}, {"price": "0.188046", "quantity": "0.2034"}],
    "asks": [{"price": "0.1888", "quantity": "19.3953"}, {"price": "0.189661", "quantity": "0.2011"}],
}


def ws_book_message(method: str, sequence: int, bids=None, asks=None) -> Dict[str, Any]:
    return {"method": method, "params": {
        "symbol": SYMBOL, "sequence": str(sequence), "timestamp": "2026-11-26T15:13:39.232Z",
        "bids": bids or [], "asks": asks or []}}


def order_record(client_order_id: str, quantity: str = "10", executed: str = "0", status: str = "Active",
                 is_active: bool = True, side: str = "buy", price: str = "0.188",
                 exchange_order_id: str = EXCHANGE_ORDER_ID) -> Dict[str, Any]:
    remain = str(max(float(quantity) - float(executed), 0))
    return {
        "id": exchange_order_id,
        "userProvidedId": client_order_id,
        "market": {"id": "651213de4db82952da233722", "symbol": SYMBOL},
        "side": side,
        "type": "limit",
        "price": price,
        "quantity": quantity,
        "executedQuantity": executed,
        "remainQuantity": remain,
        "remainTotal": "0",
        "remainTotalWithFee": "0",
        "lastTradeAt": 1795727700000,
        "status": status,
        "isActive": is_active,
        "createdAt": 1795727600000,
        "updatedAt": 1795727700000,
    }


def account_order_record(client_order_id: str, quantity: str = "10", executed: str = "0", status: str = "Active",
                         is_active: bool = True, side: str = "buy", price: str = "0.188",
                         exchange_order_id: str = EXCHANGE_ORDER_ID) -> Dict[str, Any]:
    """One element of GET /account/orders (the spec's Order2)."""
    return {
        "id": exchange_order_id,
        "userProvidedId": client_order_id,
        "symbol": SYMBOL,
        "side": side,
        "type": "limit",
        "price": price,
        "quantity": quantity,
        "executedQuantity": executed,
        "status": status,
        "isActive": is_active,
        "timestamp": 1795727600000,
        "updatedAt": 1795727700000,
    }


def trade_record(trade_id: str, quantity: str, price: str = "0.1880", fee: str = "0.000752",
                 side: str = "buy", exchange_order_id: str = EXCHANGE_ORDER_ID, timestamp: int = 1795727700000,
                 alternate_fee: Optional[str] = None, alternate_asset: Optional[str] = None,
                 total_with_fee: Optional[str] = None) -> Dict[str, Any]:
    # Handmade: totalWithFee as the spec reads (fee in quote): value + fee on a buy, value - fee on a sell
    if total_with_fee is None:
        value = Decimal(quantity) * Decimal(price)
        total_with_fee = str(value + Decimal(fee) if side == "buy" else value - Decimal(fee))
    trade = {
        "id": trade_id,
        "market": {"id": "651213de4db82952da233722", "symbol": SYMBOL},
        "orderid": exchange_order_id,
        "side": side,
        "triggeredBy": "sell" if side == "buy" else "buy",
        "price": price,
        "quantity": quantity,
        "fee": fee,
        "totalWithFee": total_with_fee,
        "timestamp": timestamp,
    }
    if alternate_fee is not None:
        trade["alternateFee"] = alternate_fee
        trade["alternateFeeAsset"] = alternate_asset
    return trade


def ws_report(client_order_id: str, executed: str, status: str = "Partly Filled", report_type: str = "trade",
              trade_id: Optional[str] = None, trade_quantity: Optional[str] = None) -> Dict[str, Any]:
    params = order_record(client_order_id, executed=executed, status=status)
    params["reportType"] = report_type
    if trade_id is not None:
        params.update({"tradeId": trade_id, "tradeQuantity": trade_quantity, "tradePrice": "0.1880",
                       "tradeFee": "0.000752"})
    return {"jsonrpc": "2.0", "method": "report", "params": params}


def error_body(code: int, message: str, description: str = "") -> str:
    return json.dumps({"error": {"code": code, "message": message, "description": description}})


NOT_FOUND_BODY = error_body(20002, "Order not found",
                            "Attempt to get active order that  not existing, filled, canceled or expired.")
DUPLICATE_ID_BODY = error_body(20008, "Duplicate clientOrderId")
INSUFFICIENT_FUNDS_BODY = error_body(20001, "Insufficient funds")
GATEWAY_TIMEOUT_BODY = error_body(504, "Gateway Timeout", "Check the result of your request later")
SERVICE_UNAVAILABLE_BODY = error_body(503, "Service Unavailable", "Try it again later")
RATE_LIMIT_BODY = error_body(429, "Too many requests")

BALANCES = [
    {"asset": "ERG", "name": "Ergo", "available": "100.5", "pending": "3", "held": "10", "assetid": "a1"},
    {"asset": "USDT", "name": "Tether", "available": "25.25", "pending": "0", "held": "1.88", "assetid": "a2"},
]


def copy_of(value: Any) -> Any:
    return copy.deepcopy(value)


def trades_page(trades: List[Dict[str, Any]]) -> str:
    return json.dumps(trades)
