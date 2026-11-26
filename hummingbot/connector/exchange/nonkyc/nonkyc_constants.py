from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

DEFAULT_DOMAIN = "com"
EXCHANGE_NAME = "nonkyc"

# Base urls (from https://api.nonkyc.io/openapi.json, v1.3.0)
REST_URL = "https://api.nonkyc.io/api/v2"
WS_URL = "wss://api.nonkyc.io"

HBOT_ORDER_ID_PREFIX = "HBOT-"
MAX_ORDER_ID_LEN = 32

# Public endpoints
SERVER_TIME_PATH_URL = "/time"
TICKER_INFO_PATH_URL = "/ticker"
TICKER_BOOK_PATH_URL = "/tickers"
MARKETS_INFO_PATH_URL = "/market/getlist"
ORDERBOOK_SNAPSHOT_PATH_URL = "/market/orderbook"

# Private endpoints
USER_BALANCES_PATH_URL = "/balances"
USER_TRADES_PATH_URL = "/account/trades"
CREATE_ORDER_PATH_URL = "/createorder"
CANCEL_ORDER_PATH_URL = "/cancelorder"
ORDER_INFO_PATH_URL = "/getorder"
ACCOUNT_ORDERS_PATH_URL = "/account/orders"

# Order params. The spec enum is lower case.
SIDE_BUY = "buy"
SIDE_SELL = "sell"
ORDER_TYPE_LIMIT = "limit"

# Paging of /account/trades. The spec caps limit and skip at 500.
TRADES_PAGE_LIMIT = 500
TRADES_MAX_SKIP = 500
# One page of /account/orders (the spec caps limit at 500)
ACCOUNT_ORDERS_PAGE_LIMIT = 500
# The spec gives /account/orders a `status` ('active', 'filled', 'cancelled') and no
# default, so the unknown-outcome lookup and sweep ask without it, then for each
# finished status, and merge the records by id.
ACCOUNT_ORDERS_STATUS_QUERIES = (None, "filled", "cancelled")
# Trade queries look this far back before the cursor, to cover clock skew.
TRADES_OVERLAP_MS = 60_000

# Order book depth for both the REST snapshot and the WS subscription
ORDER_BOOK_DEPTH = 100

# Websocket methods
WS_METHOD_LOGIN = "login"
WS_METHOD_SUBSCRIBE_ORDERBOOK = "subscribeOrderbook"
WS_METHOD_UNSUBSCRIBE_ORDERBOOK = "unsubscribeOrderbook"
WS_METHOD_SUBSCRIBE_TRADES = "subscribeTrades"
WS_METHOD_UNSUBSCRIBE_TRADES = "unsubscribeTrades"
WS_METHOD_SUBSCRIBE_REPORTS = "subscribeReports"

# Websocket notifications
SNAPSHOT_EVENT_TYPE = "snapshotOrderbook"
DIFF_EVENT_TYPE = "updateOrderbook"
TRADE_EVENT_TYPE = "updateTrades"
TRADE_SNAPSHOT_EVENT_TYPE = "snapshotTrades"
REPORT_EVENT_TYPE = "report"
ACTIVE_ORDERS_EVENT_TYPE = "activeOrders"

WS_HEARTBEAT_TIME_INTERVAL = 30
WS_LOGIN_TIMEOUT = 10.0

# Request timeouts in seconds. A createorder that times out is not failed at once:
# it is looked up for UNCONFIRMED_CREATE_GRACE_SECONDS (see below).
CREATE_ORDER_TIMEOUT = 10.0
REQUEST_TIMEOUT = 30.0
# Pause after a 429 before the next request
RATE_LIMIT_BACKOFF_SECONDS = 10.0

# A createorder whose outcome is unknown (timeout, 5xx, 504, no id) is looked up by
# its client id for this long. 'Not found' answers do not count in this time.
UNCONFIRMED_CREATE_GRACE_SECONDS = 300.0
# Still not found after the grace period: the order fails with this marker, the same
# text as hummingbot/strategy_v2/utils/outcome_hold.py SUBMIT_OUTCOME_UNKNOWN, so a
# strategy that knows the marker holds instead of trading on.
SUBMIT_OUTCOME_UNKNOWN = "[code: SUBMIT_OUTCOME_UNKNOWN]"
# Such an order's client id stays on a sweep list this long; any listed order that
# shows up in /account/orders is cancelled and its fills are booked.
UNKNOWN_OUTCOME_SWEEP_TTL_SECONDS = 24 * 60 * 60.0
UNKNOWN_OUTCOME_SWEEP_INTERVAL = 60.0

# Websocket login refused or unanswered: wait before the next try, doubling up to the cap
WS_LOGIN_BACKOFF_INITIAL_SECONDS = 5.0
WS_LOGIN_BACKOFF_MAX_SECONDS = 300.0

# Order book sequencing (websocket only; the REST book 'sequence' is the trade counter)
# A diff up to this many sequence numbers ahead is held for the missing ones ...
BOOK_REORDER_MAX_GAP = 3
# ... for at most this many seconds, then it is a gap.
BOOK_REORDER_WINDOW_SECONDS = 1.0
# No websocket book message for a pair in this long: resubscribe it.
BOOK_STALE_SECONDS = 60.0
BOOK_WATCHDOG_INTERVAL = 10.0
# A pair added while running is resubscribed this long after, so its websocket
# snapshot reaches the book the tracker builds for it
RESYNC_AFTER_ADD_SECONDS = 5.0
# A diff without a sequence keeps the connector not ready for this long.
UNSEQUENCED_DIFF_NOT_READY_SECONDS = 60.0
# Id given to a REST snapshot: it carries no book sequence, replays no diff and is
# replaced by the next websocket snapshot.
UNSEQUENCED_SNAPSHOT_ID = 0

# Order statuses from the Order schema. Anything else is looked at with isActive.
ORDER_STATE = {
    "New": OrderState.OPEN,
    "Active": OrderState.OPEN,
    "Partly Filled": OrderState.PARTIALLY_FILLED,
    "Filled": OrderState.FILLED,
    "Cancelled": OrderState.CANCELED,
}

# Error codes from the spec
ORDER_NOT_FOUND_ERROR_CODE = 20002
DUPLICATE_CLIENT_ORDER_ID_ERROR_CODE = 20008
# Codes that mean the request was looked at and refused, so no order exists
DEFINITE_REJECT_ERROR_CODES = {
    402,    # method not found
    403,    # action forbidden for account
    1001,   # authorization required
    1002,   # authorization failed
    1003,   # forbidden for this API key
    1004,   # unsupported authorisation method
    2001,   # symbol not found
    2002,   # currency not found
    10001,  # validation error
    20001,  # insufficient funds
}

# Rate limits. NonKYC publishes no numbers, only error 429. These are kept low on
# purpose. Placement and status queries use separate pools, so polling never
# spends the order budget.
PUBLIC_POOL = "PUBLIC"
PRIVATE_ORDER_POOL = "PRIVATE_ORDERS"
PRIVATE_QUERY_POOL = "PRIVATE_QUERIES"

ONE_SECOND = 1
MAX_REQUEST = 1000

PUBLIC_POOL_LIMIT = 10
PRIVATE_ORDER_POOL_LIMIT = 5
PRIVATE_QUERY_POOL_LIMIT = 5


def _public(limit_id: str) -> RateLimit:
    return RateLimit(limit_id=limit_id, limit=MAX_REQUEST, time_interval=ONE_SECOND,
                     linked_limits=[LinkedLimitWeightPair(PUBLIC_POOL, 1)])


def _private_order(limit_id: str) -> RateLimit:
    return RateLimit(limit_id=limit_id, limit=MAX_REQUEST, time_interval=ONE_SECOND,
                     linked_limits=[LinkedLimitWeightPair(PRIVATE_ORDER_POOL, 1)])


def _private_query(limit_id: str) -> RateLimit:
    return RateLimit(limit_id=limit_id, limit=MAX_REQUEST, time_interval=ONE_SECOND,
                     linked_limits=[LinkedLimitWeightPair(PRIVATE_QUERY_POOL, 1)])


RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_POOL, limit=PUBLIC_POOL_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=PRIVATE_ORDER_POOL, limit=PRIVATE_ORDER_POOL_LIMIT, time_interval=ONE_SECOND),
    RateLimit(limit_id=PRIVATE_QUERY_POOL, limit=PRIVATE_QUERY_POOL_LIMIT, time_interval=ONE_SECOND),
    _public(SERVER_TIME_PATH_URL),
    _public(TICKER_INFO_PATH_URL),
    _public(TICKER_BOOK_PATH_URL),
    _public(MARKETS_INFO_PATH_URL),
    _public(ORDERBOOK_SNAPSHOT_PATH_URL),
    _private_order(CREATE_ORDER_PATH_URL),
    _private_order(CANCEL_ORDER_PATH_URL),
    _private_query(ORDER_INFO_PATH_URL),
    _private_query(ACCOUNT_ORDERS_PATH_URL),
    _private_query(USER_TRADES_PATH_URL),
    _private_query(USER_BALANCES_PATH_URL),
]
