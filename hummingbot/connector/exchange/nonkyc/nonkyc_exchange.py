import asyncio
import math
from decimal import Decimal
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_0, s_decimal_NaN
from hummingbot.connector.exchange.nonkyc import (
    nonkyc_constants as CONSTANTS,
    nonkyc_utils,
    nonkyc_web_utils as web_utils,
)
from hummingbot.connector.exchange.nonkyc.nonkyc_api_order_book_data_source import NonkycAPIOrderBookDataSource
from hummingbot.connector.exchange.nonkyc.nonkyc_api_user_stream_data_source import NonkycAPIUserStreamDataSource
from hummingbot.connector.exchange.nonkyc.nonkyc_auth import NonkycAuth
from hummingbot.connector.exchange.nonkyc.nonkyc_utils import to_decimal, to_milliseconds
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import TradeFillOrderDetails, combine_to_hb_trading_pair, split_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.event.events import MarketEvent, OrderFilledEvent
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

TERMINAL_STATES = {OrderState.CANCELED, OrderState.FILLED, OrderState.FAILED}


class NonkycAmbiguousOrderError(Exception):
    """The createorder request may or may not have placed the order."""


class NonkycResponseError(IOError):
    """NonKYC answered with an unexpected shape. This is not a 'not found' answer."""


class NonkycExchange(ExchangePyBase):
    # Polls of a terminal status that waits for its fills, before it is applied anyway
    MAX_TERMINAL_DEFERRALS = 6
    # Pause and API flags come with the trading rules (public getlist): keep them fresh
    TRADING_RULES_INTERVAL = 60.0
    # An order of unknown outcome is checked for a failure reason within a tick or two
    MAX_REMEMBERED_FAILURES = 100
    # totalWithFee is compared with quantity x price to this tolerance
    FEE_ASSET_TOLERANCE = Decimal("1e-8")

    web_utils = web_utils

    def __init__(self,
                 nonkyc_api_key: str,
                 nonkyc_api_secret: str,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        # kept private; never logged
        self._api_key = nonkyc_api_key
        self._secret_key = nonkyc_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        # per pair: pause and API flags and price bounds from the market list
        self._market_flags: Dict[str, Dict[str, Any]] = {}
        # per pair: newest trade time seen, in ms
        self._trade_cursor_ms: Dict[str, int] = {}
        self._last_trades_poll_timestamp: float = 0.0
        # client id -> (deferrals counted, time of the last one)
        self._terminal_deferrals: Dict[str, Tuple[int, float]] = {}
        self._rate_limited_until: float = 0.0
        # Signed requests one at a time, so nonces reach NonKYC in the order they were made
        self._signed_request_lock = asyncio.Lock()
        # client ids whose createorder request has not returned yet
        self._creates_in_flight: Set[str] = set()
        # client id -> time the createorder outcome became unknown (start of the grace period)
        self._unconfirmed_creates: Dict[str, float] = {}
        # client id -> (trading pair, creation time) of orders failed with an unknown outcome
        self._unknown_outcome_sweep: Dict[str, Tuple[str, float]] = {}
        self._last_sweep_timestamp: float = 0.0
        self._order_failure_reasons: Dict[str, str] = {}
        # client id -> time its createorder request went out (the newest MAX_REMEMBERED_FAILURES)
        self._creates_sent: Dict[str, float] = {}
        self._fee_asset_undecided_logged = False
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # no balance stream is documented; balances come from REST polls
        self.real_time_balance_update = False

    @staticmethod
    def nonkyc_order_type(order_type: OrderType) -> str:
        if order_type is OrderType.LIMIT:
            return CONSTANTS.ORDER_TYPE_LIMIT
        raise ValueError(f"NonKYC connector does not place {order_type.name} orders")

    @property
    def authenticator(self):
        return NonkycAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # Fills before the cancel are fetched in _place_cancel before CANCELED is set
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        # NonKYC has no post-only flag, and market orders take any price on a thin book
        return [OrderType.LIMIT]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        return await self._api_get(path_url=CONSTANTS.TICKER_BOOK_PATH_URL)

    @property
    def status_dict(self) -> Dict[str, bool]:
        status = super().status_dict
        # Diffs without a sequence cannot be ordered: the book would freeze or drift
        data_source = self.order_book_tracker.data_source
        books_sequenced = getattr(data_source, "books_sequenced", None)
        status["order_book_sequenced"] = books_sequenced() if callable(books_sequenced) else True
        return status

    def check_order_locally(self, trading_pair: str, is_buy: bool, amount: Decimal,
                            price: Decimal) -> Optional[str]:
        """
        Why NonKYC would refuse this LIMIT order, or None if the market rules allow it.
        Sends nothing. Amount and price are rounded down to the market steps first, as
        an order placement would do.
        """
        try:
            quantized_amount = self.quantize_order_amount(trading_pair, amount)
            quantized_price = self.quantize_order_price(trading_pair, price)
            self._check_order_locally(trading_pair, TradeType.BUY if is_buy else TradeType.SELL,
                                      OrderType.LIMIT, quantized_amount, quantized_price)
        except (ValueError, KeyError, ArithmeticError) as error:
            return str(error) or type(error).__name__
        return None

    def order_failure_reason(self, order_id: str) -> Optional[str]:
        """Why an order failed, when the connector knows more than FAILED (see SUBMIT_OUTCOME_UNKNOWN)."""
        return self._order_failure_reasons.get(order_id)

    def create_request_sent(self, order_id: str) -> bool:
        """
        True once the createorder request of this order has gone out (after the wait for the
        signed-request lock and any 429 pause; the throttler's own wait of at most about a
        second comes after). A strategy can start a fill timer from here rather than from
        placement. False for an order whose request has not gone out, or that is not known.
        """
        return order_id in self._creates_sent

    def _note_create_sent(self, order_id: str):
        self._creates_sent[order_id] = self._current_time()
        while len(self._creates_sent) > self.MAX_REMEMBERED_FAILURES:
            self._creates_sent.pop(next(iter(self._creates_sent)))

    def _remember_failure_reason(self, order_id: str, reason: str):
        self._order_failure_reasons[order_id] = reason
        while len(self._order_failure_reasons) > self.MAX_REMEMBERED_FAILURES:
            self._order_failure_reasons.pop(next(iter(self._order_failure_reasons)))

    # === error classification ===

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        # NonKYC documents no time error, so a request is never sent twice for it
        return False

    @staticmethod
    def _is_not_found_error(error: BaseException) -> bool:
        status, code = web_utils.parse_request_error(error)
        return status is not None and code == CONSTANTS.ORDER_NOT_FOUND_ERROR_CODE

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_not_found_error(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_not_found_error(cancelation_exception)

    @staticmethod
    def _is_definite_create_reject(error: BaseException) -> bool:
        """
        True only when NonKYC answered and refused the order. A timeout, a dropped
        connection, a 5xx, a 504 or a duplicate id may hide a live order.
        """
        status, code = web_utils.parse_request_error(error)
        if status is None:
            return False
        if code == CONSTANTS.DUPLICATE_CLIENT_ORDER_ID_ERROR_CODE:
            return False
        if status == 429:
            return True
        if status >= 500 or status == 408:
            return False
        if code in CONSTANTS.DEFINITE_REJECT_ERROR_CODES:
            return True
        return status in (401, 403)

    # === factories ===

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return NonkycAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return NonkycAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    # === fees ===

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        if is_maker is None:
            is_maker = order_type is OrderType.LIMIT_MAKER
        return TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=order_side,
            percent=self.estimate_fee_pct(is_maker))

    def _fee_from_trade(self, trade_type: TradeType, trading_pair: str, trade: Dict[str, Any]) -> TradeFeeBase:
        """
        Fee as the trade record states it. A fee paid in an alternate asset is booked
        alone, in that asset. Otherwise 'fee' is in the quote asset (the spec), except
        on a buy whose totalWithFee shows it was taken from the base received (the FAQ).
        """
        base_asset, quote_asset = split_hb_trading_pair(trading_pair)
        alternate_fee = to_decimal(trade.get("alternateFee"))
        alternate_asset = trade.get("alternateFeeAsset")
        if alternate_fee is not None and alternate_fee > s_decimal_0 and alternate_asset:
            return TradeFeeBase.new_spot_fee(
                fee_schema=self.trade_fee_schema(),
                trade_type=trade_type,
                flat_fees=[TokenAmount(token=str(alternate_asset), amount=alternate_fee)])
        flat_fees = []
        fee = to_decimal(trade.get("fee"))
        if fee is not None and fee > s_decimal_0:
            if trade_type is TradeType.BUY and self._buy_fee_is_in_base(trade, fee):
                return DeductedFromReturnsTradeFee(flat_fees=[TokenAmount(token=base_asset, amount=fee)])
            flat_fees.append(TokenAmount(token=quote_asset, amount=fee))
        return TradeFeeBase.new_spot_fee(
            fee_schema=self.trade_fee_schema(),
            trade_type=trade_type,
            flat_fees=flat_fees)

    def _buy_fee_is_in_base(self, trade: Dict[str, Any], fee: Decimal) -> bool:
        """
        totalWithFee == quantity x price + fee: the fee was paid in quote on top.
        totalWithFee == quantity x price: the fee was taken from the base received.
        Anything else cannot be told apart: the quote default is kept, logged once.
        """
        total = to_decimal(trade.get("totalWithFee"))
        quantity = to_decimal(trade.get("quantity"))
        price = to_decimal(trade.get("price"))
        if total is not None and quantity is not None and price is not None:
            value = quantity * price
            in_quote = abs(total - (value + fee)) <= self.FEE_ASSET_TOLERANCE
            in_base = abs(total - value) <= self.FEE_ASSET_TOLERANCE
            if in_quote != in_base:
                return in_base
        if not self._fee_asset_undecided_logged:
            self._fee_asset_undecided_logged = True
            self.logger().warning(
                f"NonKYC trade {trade.get('id')}: totalWithFee {trade.get('totalWithFee')!r} does not show which "
                f"asset the fee was paid in; booking it in the quote asset. Logged once.")
        return False

    async def _update_trading_fees(self):
        # NonKYC has no fee endpoint; set the account tier in conf_fee_overrides.yml
        pass

    # === requests ===

    async def _api_request(
            self,
            path_url,
            overwrite_url: Optional[str] = None,
            method: RESTMethod = RESTMethod.GET,
            params: Optional[Dict[str, Any]] = None,
            data: Optional[Dict[str, Any]] = None,
            is_auth_required: bool = False,
            return_err: bool = False,
            limit_id: Optional[str] = None,
            headers: Optional[Dict[str, Any]] = None,
            timeout: Optional[float] = None,
            on_send: Optional[Callable[[], None]] = None,
            **kwargs,
    ) -> Any:
        request = dict(path_url=path_url, overwrite_url=overwrite_url, method=method, params=params, data=data,
                       is_auth_required=is_auth_required, return_err=return_err, limit_id=limit_id,
                       headers=headers, timeout=timeout, on_send=on_send)
        if is_auth_required:
            # The nonce is taken when the request is signed. One signed request at a
            # time, from signing to reply, so a later nonce never arrives first.
            async with self._signed_request_lock:
                return await self._api_request_once(**request)
        return await self._api_request_once(**request)

    async def _api_request_once(
            self,
            path_url,
            overwrite_url: Optional[str],
            method: RESTMethod,
            params: Optional[Dict[str, Any]],
            data: Optional[Dict[str, Any]],
            is_auth_required: bool,
            return_err: bool,
            limit_id: Optional[str],
            headers: Optional[Dict[str, Any]],
            timeout: Optional[float],
            on_send: Optional[Callable[[], None]] = None,
    ) -> Any:
        # Sent once: no retry, so an order request is never repeated here
        delay = self._rate_limited_until - self._time()
        if delay > 0:
            await self._sleep(delay)
        rest_assistant = await self._web_assistants_factory.get_rest_assistant()
        url = overwrite_url or await self._api_request_url(path_url=path_url, is_auth_required=is_auth_required)
        if on_send is not None:
            # after the signed-request lock and any 429 pause: the request goes out now
            on_send()
        try:
            return await rest_assistant.execute_request(
                url=url,
                params=params,
                data=data,
                method=method,
                is_auth_required=is_auth_required,
                return_err=return_err,
                throttler_limit_id=limit_id if limit_id else path_url,
                headers=headers,
                timeout=timeout if timeout is not None else CONSTANTS.REQUEST_TIMEOUT,
            )
        except IOError as request_error:
            status, _ = web_utils.parse_request_error(request_error)
            if status == 429:
                self._rate_limited_until = self._time() + CONSTANTS.RATE_LIMIT_BACKOFF_SECONDS
                self.logger().warning(
                    f"NonKYC rate limit hit; pausing requests for {CONSTANTS.RATE_LIMIT_BACKOFF_SECONDS} s.")
            raise

    # === orders ===

    def _check_order_locally(self, trading_pair: str, trade_type: TradeType, order_type: OrderType,
                             amount: Decimal, price: Decimal):
        """Refuse before sending anything when the market rules say NonKYC would refuse."""
        self.nonkyc_order_type(order_type)
        if price is None or not price.is_finite() or price <= s_decimal_0:
            raise ValueError(f"A NonKYC LIMIT order needs a positive price, got {price}")
        if amount is None or not amount.is_finite() or amount <= s_decimal_0:
            raise ValueError(f"A NonKYC order needs a positive amount, got {amount}")
        flags = self._market_flags.get(trading_pair)
        if flags is None:
            raise ValueError(f"No NonKYC market rules loaded for {trading_pair}")
        if flags["is_paused"]:
            raise ValueError(f"NonKYC market {trading_pair} is paused")
        if flags["api_excluded"]:
            raise ValueError(f"NonKYC market {trading_pair} is closed to API trading")
        if trade_type is TradeType.BUY and flags["pause_buys"]:
            raise ValueError(f"NonKYC market {trading_pair} has buys paused")
        if trade_type is TradeType.SELL and flags["pause_sells"]:
            raise ValueError(f"NonKYC market {trading_pair} has sells paused")
        if flags["min_price"] is not None and price < flags["min_price"]:
            raise ValueError(f"Price {price} is below the NonKYC minimum {flags['min_price']} for {trading_pair}")
        if flags["max_price"] is not None and price > flags["max_price"]:
            raise ValueError(f"Price {price} is above the NonKYC maximum {flags['max_price']} for {trading_pair}")
        rule = self._trading_rules.get(trading_pair)
        if rule is None:
            raise ValueError(f"No NonKYC trading rule loaded for {trading_pair}")
        if amount < rule.min_order_size:
            raise ValueError(f"Amount {amount} is below the NonKYC minimum {rule.min_order_size} for {trading_pair}")
        if price * amount < rule.min_notional_size:
            raise ValueError(
                f"Order value {price * amount} is below the NonKYC minimum {rule.min_notional_size} for {trading_pair}")
        if amount % rule.min_base_amount_increment != s_decimal_0:
            raise ValueError(f"Amount {amount} is not a multiple of {rule.min_base_amount_increment}")
        if price % rule.min_price_increment != s_decimal_0:
            raise ValueError(f"Price {price} is not a multiple of {rule.min_price_increment}")

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        self._check_order_locally(trading_pair, trade_type, order_type, amount, price)
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {
            "userProvidedId": order_id,
            "symbol": symbol,
            "side": CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL,
            "type": self.nonkyc_order_type(order_type),
            "quantity": f"{amount:f}",
            "price": f"{price:f}",
            # refuse instead of silently truncating
            "strictValidate": True,
        }
        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.CREATE_ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True,
                limit_id=CONSTANTS.CREATE_ORDER_PATH_URL,
                timeout=CONSTANTS.CREATE_ORDER_TIMEOUT,
                on_send=lambda: self._note_create_sent(order_id))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._is_definite_create_reject(error):
                raise
            raise NonkycAmbiguousOrderError(f"{type(error).__name__}: {str(error)[:300]}") from error

        if not isinstance(order_result, dict) or not order_result.get("id"):
            raise NonkycAmbiguousOrderError("createorder reply has no order id")
        echoed_id = order_result.get("userProvidedId")
        if echoed_id is not None and str(echoed_id) != order_id:
            raise NonkycAmbiguousOrderError(f"createorder reply is for client id {echoed_id}")
        created_ms = to_milliseconds(order_result.get("createdAt"))
        transact_time = created_ms * 1e-3 if created_ms is not None else self.current_timestamp
        return str(order_result["id"]), transact_time

    async def _place_order_and_process_update(self, order: InFlightOrder, **kwargs) -> Optional[str]:
        client_order_id = order.client_order_id
        # While the request is out, NonKYC may not have stored the order yet: a
        # 'not found' answer then means nothing and is not counted.
        self._creates_in_flight.add(client_order_id)
        try:
            exchange_order_id, update_timestamp = await self._place_order(
                order_id=client_order_id,
                trading_pair=order.trading_pair,
                amount=order.amount,
                trade_type=order.trade_type,
                order_type=order.order_type,
                price=order.price,
                **kwargs,
            )
        except NonkycAmbiguousOrderError as error:
            # Not FAILED: the order may be live. It stays pending and is looked up by
            # its client id (getorder and /account/orders) for the grace period;
            # 'not found' answers in that time are not counted.
            self._unconfirmed_creates[client_order_id] = self._current_time()
            self._order_tracker._order_not_found_records.pop(client_order_id, None)
            self.logger().warning(
                f"The outcome of order {client_order_id} is unknown ({error}). It stays pending and is "
                f"looked up by its client id for {CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS:.0f} s.")
            self._poll_notifier.set()
            return None
        finally:
            self._creates_in_flight.discard(client_order_id)

        if order.exchange_order_id is None:
            order.update_exchange_order_id(exchange_order_id)
        # A status poll or report may have moved the order on already
        if order.is_pending_create:
            self._order_tracker.process_order_update(OrderUpdate(
                client_order_id=order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=order.trading_pair,
                update_timestamp=update_timestamp,
                new_state=OrderState.OPEN,
            ))
        return exchange_order_id

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        try:
            return await self._place_cancel_once(order_id, tracked_order)
        except asyncio.TimeoutError as error:
            # The base class counts a cancel timeout as 'not found'; it is not proof of anything
            raise NonkycResponseError(f"NonKYC cancel of {order_id} timed out; not counted as not found") from error

    async def _place_cancel_once(self, order_id: str, tracked_order: InFlightOrder):
        if tracked_order.exchange_order_id is None:
            # The create reply may have been lost: learn the exchange id first.
            # A 'not found' answer raises and is counted by the base class, except
            # while the create is in flight or its outcome is still being looked up.
            try:
                order_update = await self._request_order_status(tracked_order)
            except Exception as error:
                if self._create_outcome_pending(tracked_order.client_order_id) and self._is_not_found_error(error):
                    raise NonkycResponseError(
                        f"Order {order_id} is not found yet while its create outcome is looked up; "
                        f"cancel not sent and not counted as not found") from error
                raise
            self._order_tracker.process_order_update(order_update)
            if order_update.new_state in TERMINAL_STATES:
                return order_update.new_state == OrderState.CANCELED

        cancel_result = await self._api_post(
            path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
            data={"id": tracked_order.exchange_order_id},
            is_auth_required=True,
            limit_id=CONSTANTS.CANCEL_ORDER_PATH_URL)
        if not (isinstance(cancel_result, dict) and cancel_result.get("success") is True):
            raise NonkycResponseError(f"NonKYC did not confirm the cancel of {order_id}: {cancel_result}")

        # Book fills that landed before the cancel, before the order is marked CANCELED
        trades_fetched = True
        try:
            await self._fetch_and_process_order_trades(tracked_order)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            trades_fetched = False
            self.logger().warning(f"Could not fetch trades of cancelled order {order_id}: {error}")
        if tracked_order.is_filled:
            return False
        # The trade list can lag the cancel. Re-read the order by NonKYC's id: if it says more
        # was executed than the trades booked, CANCELED is not applied here; the status poll
        # applies CANCELED or FILLED once the trades are listed (or after the deferrals).
        try:
            order_data = await self._get_order_record(tracked_order)
            order_update = await self._order_update_from_record(tracked_order, order_data)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(
                f"Could not re-read cancelled order {order_id}: {error}. "
                f"Deferring cancellation to status poll.")
            return False
        executed = to_decimal(order_data.get("executedQuantity")) or s_decimal_0
        if executed > tracked_order.executed_amount_base:
            self.logger().warning(
                f"NonKYC confirmed the cancel of {order_id}, but its record shows {executed} executed and only "
                f"{tracked_order.executed_amount_base} is booked from trades. Not marked cancelled until the "
                f"trades are listed.")
            self._order_tracker.process_order_update(order_update)
            return False
        if not trades_fetched:
            self.logger().warning(
                f"NonKYC confirmed the cancel of {order_id}, but its trades could not be fetched. "
                f"Deferring cancellation to status poll.")
            return False
        return not tracked_order.is_filled

    # === trading rules and symbols ===

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        if not isinstance(exchange_info_dict, list):
            raise NonkycResponseError("Unexpected NonKYC market list reply")
        rules = []
        flags = {}
        for market in filter(nonkyc_utils.is_market_active, exchange_info_dict):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=market.get("symbol"))
            except KeyError:
                continue
            try:
                price_step = Decimal(1).scaleb(-int(market["priceDecimals"]))
                size_step = Decimal(1).scaleb(-int(market["quantityDecimals"]))
                min_quantity = to_decimal(market.get("minimumQuantity")) or s_decimal_0
                min_notional = s_decimal_0
                if market.get("isMinQuoteActive") is True or market.get("minQuoteActive") is True:
                    min_notional = to_decimal(market.get("minQuote")) or s_decimal_0
                rules.append(TradingRule(
                    trading_pair,
                    min_order_size=max(size_step, min_quantity),
                    min_price_increment=price_step,
                    min_base_amount_increment=size_step,
                    min_notional_size=min_notional,
                    supports_market_orders=False,
                ))
                flags[trading_pair] = {
                    "is_paused": market.get("isPaused") is True,
                    "pause_buys": market.get("pauseBuys") is True,
                    "pause_sells": market.get("pauseSells") is True,
                    "api_excluded": market.get("apiExcluded") is True,
                    "min_price": self._positive_or_none(market.get("minAllowedPrice")),
                    "max_price": self._positive_or_none(market.get("maxAllowedPrice")),
                }
            except Exception:
                self.logger().exception(f"Error parsing the NonKYC market rule for {market.get('symbol')}. Skipping.")
        self._market_flags = flags
        return rules

    @staticmethod
    def _positive_or_none(value: Any) -> Optional[Decimal]:
        result = to_decimal(value)
        return result if result is not None and result > s_decimal_0 else None

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        mapping = bidict()
        markets = exchange_info if isinstance(exchange_info, list) else []
        for market in filter(nonkyc_utils.is_market_active, markets):
            symbol = market.get("symbol")
            if not isinstance(symbol, str) or "/" not in symbol:
                continue
            symbol_base, quote = symbol.split("/", 1)
            trading_pair = combine_to_hb_trading_pair(base=market.get("primaryTicker") or symbol_base, quote=quote)
            if symbol in mapping or trading_pair in mapping.inverse:
                self.logger().warning(f"Skipping NonKYC market {symbol}: its pair {trading_pair} is already mapped.")
                continue
            mapping[symbol] = trading_pair
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=f"{CONSTANTS.TICKER_INFO_PATH_URL}/{symbol}",
            limit_id=CONSTANTS.TICKER_INFO_PATH_URL
        )
        price = to_decimal(resp_json.get("last_price")) if isinstance(resp_json, dict) else None
        if price is None:
            raise NonkycResponseError(f"Unexpected NonKYC ticker reply for {trading_pair}")
        return float(price)

    # === polling ===

    def _get_poll_interval(self, timestamp: float) -> float:
        # Websocket pings keep last_recv_time fresh even when no report arrives,
        # so poll fast whenever an order can still change or fill.
        if len(self._order_tracker.all_fillable_orders) > 0:
            return self.SHORT_POLL_INTERVAL
        return super()._get_poll_interval(timestamp)

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        # A failed time sync must not stop balance and order updates
        try:
            await self._time_synchronizer.update_server_time_offset_with_time_provider(
                time_provider=self.web_utils.get_current_server_time(throttler=self._throttler, domain=self.domain))
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Could not sync time with NonKYC ({error}); keeping the last offset.")

    async def _update_order_status(self):
        # Fills first, then status, so a terminal status finds its fills booked
        await self._update_trades_for_pairs()
        await self._update_orders()
        await self._sweep_unknown_outcomes()

    async def _cancel_lost_orders(self):
        # Book the fills of lost orders before the cancel: a cancel answered 'not found'
        # drops a lost order, and a filled order must not be dropped with its fills unread
        lost_orders = list(self._order_tracker.lost_orders.values())
        await self._update_orders_fills(lost_orders)
        for lost_order in lost_orders:
            if lost_order.executed_amount_base >= lost_order.amount:
                continue  # filled in full: its completion is already processed from the trades
            if lost_order.client_order_id in self._order_tracker.lost_orders:
                await self._execute_order_cancel(order=lost_order)

    async def _update_orders_fills(self, orders: List[InFlightOrder]):
        for order in orders:
            try:
                await self._fetch_and_process_order_trades(order)
            except asyncio.CancelledError:
                raise
            except Exception as request_error:
                self.logger().warning(
                    f"Failed to fetch trade updates for order {order.client_order_id}. Error: {request_error}")

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        # Only NonKYC's own 'order not found' answer counts toward lost.
        # 429, 5xx, timeouts and odd replies are logged and retried next poll.
        client_order_id = order.client_order_id
        if client_order_id in self._creates_in_flight:
            self.logger().debug(f"Order {client_order_id} not found while its create is in flight; not counted.")
            return
        if client_order_id in self._unconfirmed_creates:
            await self._resolve_unconfirmed_create(order)
            return
        if not self._is_order_not_found_during_status_update_error(error):
            self.logger().warning(
                f"Error fetching status of order {client_order_id}: {error}. Not counted as not found.")
            return
        if order.exchange_order_id is not None:
            # 20002 also covers a filled order: read its trades before counting
            try:
                await self._fetch_and_process_order_trades(order)
            except asyncio.CancelledError:
                raise
            except Exception as trades_error:
                self.logger().warning(
                    f"Order {client_order_id} not found and its trades could not be read ({trades_error}); "
                    f"not counted as not found.")
                return
            if order.is_filled or order.executed_amount_base >= order.amount:
                return
        await self._order_tracker.process_order_not_found(client_order_id)

    def _create_outcome_pending(self, client_order_id: str) -> bool:
        return client_order_id in self._creates_in_flight or client_order_id in self._unconfirmed_creates

    async def _account_orders(self, trading_pair: str, since_ms: Optional[int],
                              status: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        One page of /account/orders for the pair, newest first. Each record carries
        userProvidedId. `status` ('active', 'filled', 'cancelled') is sent only when given.
        """
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params: Dict[str, Any] = {"symbol": symbol}
        if status is not None:
            params["status"] = status
        if since_ms is not None:
            params["since"] = max(int(since_ms), 0)
        params["limit"] = CONSTANTS.ACCOUNT_ORDERS_PAGE_LIMIT
        page = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_ORDERS_PATH_URL,
            params=params,
            is_auth_required=True,
            limit_id=CONSTANTS.ACCOUNT_ORDERS_PATH_URL)
        if not isinstance(page, list):
            raise NonkycResponseError(f"Unexpected NonKYC account orders reply for {symbol}")
        if len(page) >= CONSTANTS.ACCOUNT_ORDERS_PAGE_LIMIT:
            # The order looked for may be older than this page: say so where the operator sees it
            self.logger().network(
                f"NonKYC account orders for {symbol} (status {status or 'not sent'}) filled a whole page "
                f"({len(page)}); older orders in the window are not read, and an order of unknown outcome "
                f"may be missed.",
                app_warning_msg=f"NonKYC account orders for {symbol} hit the page cap; an order of unknown "
                                f"outcome may be missed.")
        return [record for record in page if isinstance(record, dict) and record.get("id")]

    async def _account_orders_any_status(
            self, trading_pair: str, since_ms: Optional[int],
            found: Optional[Callable[[Dict[str, Any]], bool]] = None) -> List[Dict[str, Any]]:
        """
        /account/orders without `status`, then with status=filled and status=cancelled, merged
        by NonKYC id: the spec gives no default for `status`, so the default may list active
        orders only. With `found`, stops at the first query that lists a matching record.
        One query failing does not stop the others; all failing raises the last error.
        """
        merged: Dict[str, Dict[str, Any]] = {}
        last_error: Optional[Exception] = None
        answered = False
        for status in CONSTANTS.ACCOUNT_ORDERS_STATUS_QUERIES:
            try:
                records = await self._account_orders(trading_pair, since_ms, status=status)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                last_error = error
                self.logger().warning(
                    f"NonKYC account orders for {trading_pair} (status {status or 'not sent'}) failed: {error}")
                continue
            answered = True
            for record in records:
                merged[str(record["id"])] = record
            if found is not None and any(found(record) for record in records):
                break
        if not answered and last_error is not None:
            raise last_error
        return list(merged.values())

    @staticmethod
    def _since_creation_ms(created: Optional[float]) -> Optional[int]:
        if created and created > 0:
            return int(created * 1e3) - CONSTANTS.TRADES_OVERLAP_MS
        return None

    async def _resolve_unconfirmed_create(self, order: InFlightOrder):
        """
        getorder did not find an order whose create outcome is unknown. Look for it by
        client id in /account/orders: without `status`, then filled, then cancelled.
        Not found by the end of the grace period: fail it as of unknown outcome.
        """
        client_order_id = order.client_order_id
        started = self._unconfirmed_creates.get(client_order_id)
        if started is None:
            return
        record = None

        def is_this_order(r: Dict[str, Any]) -> bool:
            return str(r.get("userProvidedId")) == client_order_id
        try:
            records = await self._account_orders_any_status(
                order.trading_pair, self._since_creation_ms(order.creation_timestamp), found=is_this_order)
            record = next((r for r in records if is_this_order(r)), None)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self.logger().warning(f"Could not look up order {client_order_id} in NonKYC account orders: {error}")
        if record is not None:
            self._unconfirmed_creates.pop(client_order_id, None)
            self._order_tracker._order_not_found_records.pop(client_order_id, None)
            self.logger().info(f"Order {client_order_id} of unknown outcome found in NonKYC account orders "
                               f"as {record.get('status')} ({record.get('id')}).")
            order_update = await self._order_update_from_record(order, record)
            self._order_tracker.process_order_update(order_update)
            return
        if self._current_time() - started >= CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS:
            self._fail_unknown_outcome(order)

    def _fail_unknown_outcome(self, order: InFlightOrder):
        client_order_id = order.client_order_id
        now = self._current_time()
        self._unconfirmed_creates.pop(client_order_id, None)
        reason = (f"NonKYC createorder outcome unknown: order {client_order_id} was not found by getorder or "
                  f"/account/orders within {CONSTANTS.UNCONFIRMED_CREATE_GRACE_SECONDS:.0f} s and may still appear "
                  f"{CONSTANTS.SUBMIT_OUTCOME_UNKNOWN}")
        self._remember_failure_reason(client_order_id, reason)
        created = order.creation_timestamp if order.creation_timestamp and order.creation_timestamp > 0 else now
        self._unknown_outcome_sweep[client_order_id] = (order.trading_pair, created)
        # the first sweep comes one interval later, not in the same poll
        self._last_sweep_timestamp = now
        self.logger().error(
            f"{reason}. Failing it; if NonKYC lists it later it is cancelled and its fills are booked.")
        self._update_order_after_failure(client_order_id, order.trading_pair,
                                         exception=NonkycAmbiguousOrderError(reason))

    async def _sweep_unknown_outcomes(self):
        """
        Orders failed with an unknown outcome stay on a list for a day. Any of them that
        NonKYC lists is cancelled if open, and its fills are booked.
        """
        if not self._unknown_outcome_sweep:
            return
        now = self._current_time()
        for client_order_id, (trading_pair, created) in list(self._unknown_outcome_sweep.items()):
            if now - created > CONSTANTS.UNKNOWN_OUTCOME_SWEEP_TTL_SECONDS:
                self._unknown_outcome_sweep.pop(client_order_id, None)
                self.logger().warning(
                    f"Order {client_order_id} of unknown outcome was never listed by NonKYC in "
                    f"{CONSTANTS.UNKNOWN_OUTCOME_SWEEP_TTL_SECONDS / 3600:.0f} h; no longer checked.")
        if not self._unknown_outcome_sweep or now - self._last_sweep_timestamp < CONSTANTS.UNKNOWN_OUTCOME_SWEEP_INTERVAL:
            return
        self._last_sweep_timestamp = now
        pairs: Dict[str, float] = {}
        for trading_pair, created in self._unknown_outcome_sweep.values():
            pairs[trading_pair] = min(created, pairs.get(trading_pair, created))
        for trading_pair, oldest in sorted(pairs.items()):
            try:
                records = await self._account_orders_any_status(trading_pair, self._since_creation_ms(oldest))
                for record in records:
                    client_order_id = str(record.get("userProvidedId"))
                    entry = self._unknown_outcome_sweep.get(client_order_id)
                    if entry is not None and entry[0] == trading_pair:
                        await self._settle_swept_order(client_order_id, trading_pair, entry[1], record)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger().network(
                    f"Error checking NonKYC account orders for orders of unknown outcome on {trading_pair}: {error}",
                    app_warning_msg=f"Failed to check {trading_pair} orders of unknown outcome.")

    async def _settle_swept_order(self, client_order_id: str, trading_pair: str, created: float,
                                  record: Dict[str, Any]):
        exchange_order_id = str(record["id"])
        # lets the fill recovery book this order's trades under its client id
        self._exchange_order_ids[exchange_order_id] = client_order_id
        self.logger().error(
            f"Order {client_order_id}, failed as of unknown outcome, is listed by NonKYC as {record.get('status')} "
            f"(id {exchange_order_id}, executed {record.get('executedQuantity')}). Cancelling it if open and "
            f"booking its fills.")
        done = record.get("isActive") is False
        if not done:
            try:
                result = await self._api_post(
                    path_url=CONSTANTS.CANCEL_ORDER_PATH_URL,
                    data={"id": exchange_order_id},
                    is_auth_required=True,
                    limit_id=CONSTANTS.CANCEL_ORDER_PATH_URL)
                done = isinstance(result, dict) and result.get("success") is True
                if not done:
                    self.logger().warning(f"NonKYC did not confirm the cancel of {client_order_id}: {result}")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                done = self._is_not_found_error(error)
                if not done:
                    self.logger().warning(f"Could not cancel order {client_order_id} of unknown outcome: {error}")
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        trades = await self._fetch_account_trades(symbol=symbol, since_ms=self._since_creation_ms(created))
        for trade in trades:
            if str(trade.get("orderid")) == exchange_order_id:
                self._recover_untracked_fill(trading_pair, trade)
        if done:
            self._unknown_outcome_sweep.pop(client_order_id, None)

    def _current_time(self) -> float:
        now = self.current_timestamp
        return now if now is not None and not math.isnan(now) and now > 0 else self._time()

    async def _update_trades_for_pairs(self):
        fillable = self._order_tracker.all_fillable_orders
        now = self._current_time()
        periodic_due = int(now / self.LONG_POLL_INTERVAL) > int(self._last_trades_poll_timestamp / self.LONG_POLL_INTERVAL)
        pairs = {order.trading_pair for order in fillable.values()}
        if periodic_due:
            pairs.update(self._trading_pairs)
        if not pairs:
            return
        self._last_trades_poll_timestamp = now
        by_exchange_id = {order.exchange_order_id: order for order in fillable.values()
                          if order.exchange_order_id is not None}
        for trading_pair in sorted(pairs):
            try:
                await self._update_trades_for_pair(trading_pair, fillable.values(), by_exchange_id)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # one bad pair or reply never stops the status and balance updates
                self.logger().network(
                    f"Error fetching NonKYC trades for {trading_pair}: {error}",
                    app_warning_msg=f"Failed to fetch trade updates for {trading_pair}.")

    async def _update_trades_for_pair(self, trading_pair: str, orders: Iterable[InFlightOrder],
                                      by_exchange_id: Dict[str, InFlightOrder]):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        cursor = self._trade_cursor_ms.get(trading_pair)
        if cursor is None:
            created = [o.creation_timestamp for o in orders
                       if o.trading_pair == trading_pair and o.creation_timestamp and o.creation_timestamp > 0]
            cursor = int(min(created) * 1e3) if created else int(self._time_synchronizer.time() * 1e3)
        trades = await self._fetch_account_trades(symbol=symbol, since_ms=cursor - CONSTANTS.TRADES_OVERLAP_MS)
        newest = cursor
        for trade in trades:
            trade_ms = self._trade_time_ms(trade)
            if trade_ms is not None:
                newest = max(newest, trade_ms)
            order = by_exchange_id.get(str(trade.get("orderid")))
            if order is not None and order.trading_pair == trading_pair:
                trade_update = self._trade_update_from_trade(order, trade)
                if trade_update is not None:
                    self._process_trade_updates(order, [trade_update])
            else:
                self._recover_untracked_fill(trading_pair, trade)
        # moves only after a good reply, and only to trade times NonKYC gave. If the
        # paging cap was hit (warned in _fetch_account_trades), trades older than the
        # newest 1000 in the window were not read, and the cursor still moves past them.
        self._trade_cursor_ms[trading_pair] = newest

    async def _fetch_account_trades(self, symbol: str, since_ms: Optional[int]) -> List[Dict[str, Any]]:
        trades: List[Dict[str, Any]] = []
        skip = 0
        while True:
            params: Dict[str, Any] = {"symbol": symbol}
            if since_ms is not None:
                params["since"] = max(int(since_ms), 0)
            params["limit"] = CONSTANTS.TRADES_PAGE_LIMIT
            if skip:
                params["skip"] = skip
            page = await self._api_get(
                path_url=CONSTANTS.USER_TRADES_PATH_URL,
                params=params,
                is_auth_required=True,
                limit_id=CONSTANTS.USER_TRADES_PATH_URL)
            if not isinstance(page, list):
                raise NonkycResponseError(f"Unexpected NonKYC trades reply for {symbol}")
            trades.extend(trade for trade in page if isinstance(trade, dict))
            if len(page) < CONSTANTS.TRADES_PAGE_LIMIT:
                break
            if skip + CONSTANTS.TRADES_PAGE_LIMIT > CONSTANTS.TRADES_MAX_SKIP:
                # The spec caps skip at 500 and has no 'until', so older trades in the
                # window cannot be reached: say so where the operator sees it
                self.logger().network(
                    f"NonKYC trade list for {symbol} hit the paging cap ({len(trades)} trades read); older trades "
                    f"in this window are not read and their fills may be missing.",
                    app_warning_msg=f"NonKYC trade list for {symbol} hit the paging cap; some fills may be missing.")
                break
            skip += CONSTANTS.TRADES_PAGE_LIMIT
        return trades

    @staticmethod
    def _trade_time_ms(trade: Dict[str, Any]) -> Optional[int]:
        for key in ("timestamp", "createdAt", "updatedAt"):
            value = to_milliseconds(trade.get(key))
            if value is not None:
                return value
        return None

    def _trade_update_from_trade(self, order: InFlightOrder, trade: Dict[str, Any]) -> Optional[TradeUpdate]:
        """The one place a NonKYC trade record becomes a fill. Keyed by the trade's own id."""
        trade_id = trade.get("id")
        quantity = to_decimal(trade.get("quantity"))
        price = to_decimal(trade.get("price"))
        if not trade_id or quantity is None or price is None or quantity <= s_decimal_0 or price <= s_decimal_0:
            self.logger().warning(f"Skipping a malformed NonKYC trade for order {order.client_order_id}.")
            return None
        side = trade.get("side")
        expected_side = CONSTANTS.SIDE_BUY if order.trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        if side is not None and side != expected_side:
            # orderid already ties the trade to the order; the side field is unverified, so only warn
            self.logger().warning(
                f"NonKYC trade {trade_id} says {side} but order {order.client_order_id} is a {expected_side}.")
        trade_ms = self._trade_time_ms(trade)
        triggered_by = trade.get("triggeredBy")
        return TradeUpdate(
            trade_id=str(trade_id),
            client_order_id=order.client_order_id,
            exchange_order_id=str(trade.get("orderid") or order.exchange_order_id),
            trading_pair=order.trading_pair,
            fill_timestamp=trade_ms * 1e-3 if trade_ms is not None else self._current_time(),
            fill_price=price,
            fill_base_amount=quantity,
            fill_quote_amount=quantity * price,
            fee=self._fee_from_trade(order.trade_type, order.trading_pair, trade),
            is_taker=(triggered_by == side) if (triggered_by and side) else True,
        )

    def _process_trade_updates(self, order: InFlightOrder, trade_updates: List[TradeUpdate]):
        for trade_update in trade_updates:
            if trade_update.trade_id in order.order_fills:
                continue
            if order.executed_amount_base + trade_update.fill_base_amount > order.amount:
                self.logger().error(
                    f"NonKYC trade {trade_update.trade_id} would take order {order.client_order_id} to "
                    f"{order.executed_amount_base + trade_update.fill_base_amount} of {order.amount}; not booked.")
                continue
            self._order_tracker.process_trade_update(trade_update)
        is_lost = order.client_order_id in self._order_tracker.lost_orders
        if order.executed_amount_base >= order.amount and (is_lost or order.current_state not in TERMINAL_STATES):
            # complete from the summed trades, not from any order record
            self._order_tracker.process_order_update(OrderUpdate(
                client_order_id=order.client_order_id,
                exchange_order_id=order.exchange_order_id,
                trading_pair=order.trading_pair,
                update_timestamp=self._current_time(),
                new_state=OrderState.FILLED,
            ))

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        if order.exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        since_ms = None
        if order.creation_timestamp and order.creation_timestamp > 0:
            since_ms = int(order.creation_timestamp * 1e3) - CONSTANTS.TRADES_OVERLAP_MS
        trades = await self._fetch_account_trades(symbol=symbol, since_ms=since_ms)
        updates = []
        for trade in trades:
            if str(trade.get("orderid")) != order.exchange_order_id:
                continue
            trade_update = self._trade_update_from_trade(order, trade)
            if trade_update is not None:
                updates.append(trade_update)
        return updates

    async def _fetch_and_process_order_trades(self, order: InFlightOrder):
        self._process_trade_updates(order, await self._all_trade_updates_for_order(order))

    def _recover_untracked_fill(self, trading_pair: str, trade: Dict[str, Any]):
        """Fill of an order known to the DB but no longer tracked (for example after a restart)."""
        trade_id = str(trade.get("id"))
        exchange_order_id = str(trade.get("orderid"))
        if not self.is_confirmed_new_order_filled_event(trade_id, exchange_order_id, trading_pair):
            return
        quantity = to_decimal(trade.get("quantity"))
        price = to_decimal(trade.get("price"))
        if quantity is None or price is None or quantity <= s_decimal_0 or price <= s_decimal_0:
            return
        trade_type = TradeType.BUY if trade.get("side") == CONSTANTS.SIDE_BUY else TradeType.SELL
        trade_ms = self._trade_time_ms(trade)
        self._current_trade_fills.add(TradeFillOrderDetails(
            market=self.display_name, exchange_trade_id=trade_id, symbol=trading_pair))
        self.trigger_event(
            MarketEvent.OrderFilled,
            OrderFilledEvent(
                timestamp=trade_ms * 1e-3 if trade_ms is not None else self._current_time(),
                order_id=self._exchange_order_ids.get(exchange_order_id, None),
                trading_pair=trading_pair,
                trade_type=trade_type,
                order_type=OrderType.LIMIT,
                price=price,
                amount=quantity,
                trade_fee=self._fee_from_trade(trade_type, trading_pair, trade),
                exchange_trade_id=trade_id,
                exchange_order_id=exchange_order_id,
            ))
        self.logger().info(f"Recreating missing NonKYC trade {trade_id} in TradeFill.")

    # === order status ===

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        order_data = await self._get_order_record(tracked_order)
        return await self._order_update_from_record(tracked_order, order_data)

    async def _get_order_record(self, tracked_order: InFlightOrder) -> Dict[str, Any]:
        # getorder takes NonKYC's id or the client id (userProvidedId). NonKYC's id once
        # known; the client id before, so an order whose create reply was lost is found
        order_key = tracked_order.exchange_order_id or tracked_order.client_order_id
        order_data = await self._api_get(
            path_url=f"{CONSTANTS.ORDER_INFO_PATH_URL}/{order_key}",
            is_auth_required=True,
            limit_id=CONSTANTS.ORDER_INFO_PATH_URL)
        if not isinstance(order_data, dict) or not order_data.get("id"):
            raise NonkycResponseError(f"Unexpected NonKYC getorder reply for {tracked_order.client_order_id}")
        echoed_id = order_data.get("userProvidedId")
        if echoed_id is not None and str(echoed_id) != tracked_order.client_order_id:
            raise NonkycResponseError(f"NonKYC getorder for {tracked_order.client_order_id} returned {echoed_id}")
        if tracked_order.exchange_order_id is not None and str(order_data["id"]) != tracked_order.exchange_order_id:
            raise NonkycResponseError(
                f"NonKYC getorder for {tracked_order.client_order_id} returned order {order_data['id']}, "
                f"expected {tracked_order.exchange_order_id}")
        # an answer, so earlier 'not found' answers no longer count, and the create outcome is known
        self._order_tracker._order_not_found_records.pop(tracked_order.client_order_id, None)
        self._unconfirmed_creates.pop(tracked_order.client_order_id, None)
        return order_data

    def _order_state_from_record(self, order_data: Dict[str, Any]) -> Optional[OrderState]:
        status = order_data.get("status")
        state = CONSTANTS.ORDER_STATE.get(status)
        is_active = order_data.get("isActive")
        quantity = to_decimal(order_data.get("quantity"))
        executed = to_decimal(order_data.get("executedQuantity")) or s_decimal_0
        if is_active is False and state in (None, OrderState.OPEN, OrderState.PARTIALLY_FILLED):
            # inactive and not filled in full means it was cancelled with what it had
            state = (OrderState.FILLED if quantity is not None and quantity > s_decimal_0 and executed >= quantity
                     else OrderState.CANCELED)
        elif state is None and is_active is True:
            state = OrderState.PARTIALLY_FILLED if executed > s_decimal_0 else OrderState.OPEN
        if state is None:
            self.logger().warning(f"Unknown NonKYC order status {status!r}; state left as is.")
        return state

    async def _order_update_from_record(self, order: InFlightOrder, order_data: Dict[str, Any]) -> OrderUpdate:
        exchange_order_id = str(order_data["id"])
        if order.current_state is OrderState.PENDING_CREATE:
            # A pending order found already finished fires no created event, so the
            # recorder never learns its id; without it a later fill could not be booked
            self._exchange_order_ids[exchange_order_id] = order.client_order_id
        if order.exchange_order_id is None:
            order.update_exchange_order_id(exchange_order_id)

        # The record's executedQuantity is cumulative. It is never booked; it only
        # says whether trades are missing, and those come from the trade list.
        executed = to_decimal(order_data.get("executedQuantity")) or s_decimal_0
        if executed > order.executed_amount_base:
            try:
                await self._fetch_and_process_order_trades(order)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger().warning(f"Could not fetch trades of order {order.client_order_id}: {error}")

        state = self._order_state_from_record(order_data)
        if state is None:
            state = OrderState.OPEN if order.current_state is OrderState.PENDING_CREATE else order.current_state

        if state in (OrderState.FILLED, OrderState.CANCELED) and executed > order.executed_amount_base:
            # One deferral per order per short poll interval at most, so a burst of
            # reports cannot use up the wait for the trades
            deferrals, last_counted = self._terminal_deferrals.get(order.client_order_id, (0, None))
            now = self._current_time()
            if last_counted is None or now - last_counted >= self.SHORT_POLL_INTERVAL:
                deferrals += 1
                self._terminal_deferrals[order.client_order_id] = (deferrals, now)
            if deferrals <= self.MAX_TERMINAL_DEFERRALS:
                self.logger().warning(
                    f"NonKYC reports order {order.client_order_id} {state.name} with {executed} executed, but only "
                    f"{order.executed_amount_base} is booked from trades. Waiting for the trades.")
                state = OrderState.PARTIALLY_FILLED if order.executed_amount_base > s_decimal_0 else OrderState.OPEN
            else:
                self.logger().error(
                    f"Trades for order {order.client_order_id} still missing after {deferrals - 1} polls; "
                    f"applying {state.name} with {order.executed_amount_base} of {executed} booked.")
        else:
            self._terminal_deferrals.pop(order.client_order_id, None)

        updated_ms = to_milliseconds(order_data.get("updatedAt"))
        return OrderUpdate(
            client_order_id=order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=order.trading_pair,
            update_timestamp=updated_ms * 1e-3 if updated_ms is not None else self._current_time(),
            new_state=state,
        )

    # === user stream ===

    async def _user_stream_event_listener(self):
        """
        A report only says an order changed. Its state and fills are read back over
        REST (getorder and the trade list), so each fill is booked once, keyed by
        NonKYC's trade id, whichever path sees it first.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                if not isinstance(event_message, dict):
                    continue
                method = event_message.get("method")
                params = event_message.get("params")
                if method == CONSTANTS.REPORT_EVENT_TYPE:
                    await self._refresh_order_from_report(params, event_message)
                elif method == CONSTANTS.ACTIVE_ORDERS_EVENT_TYPE and isinstance(params, list):
                    for item in params:
                        await self._refresh_order_from_report(item, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error in user stream listener loop.")

    def _order_for_report(self, params: Dict[str, Any], event_message: Optional[Dict[str, Any]]) -> Optional[InFlightOrder]:
        orders = self._order_tracker.all_fillable_orders
        client_order_id = params.get("userProvidedId") or (event_message or {}).get("userProvidedId")
        order = orders.get(str(client_order_id)) if client_order_id else None
        if order is None and params.get("id") is not None:
            exchange_order_id = str(params["id"])
            order = next((o for o in orders.values() if o.exchange_order_id == exchange_order_id), None)
        return order

    async def _refresh_order_from_report(self, params: Any, event_message: Optional[Dict[str, Any]]):
        if not isinstance(params, dict):
            return
        order = self._order_for_report(params, event_message)
        if order is None:
            return
        if order.client_order_id in self._order_tracker.all_updatable_orders:
            try:
                order_update = await self._request_order_status(order)
                self._order_tracker.process_order_update(order_update)
                return
            except asyncio.CancelledError:
                raise
            except Exception as error:
                self.logger().warning(f"Could not refresh order {order.client_order_id} after a report: {error}")
        await self._fetch_and_process_order_trades(order)

    # === balances ===

    async def _update_balances(self):
        balances = await self._api_get(
            path_url=CONSTANTS.USER_BALANCES_PATH_URL,
            is_auth_required=True,
            limit_id=CONSTANTS.USER_BALANCES_PATH_URL)
        if not isinstance(balances, list):
            raise NonkycResponseError("Unexpected NonKYC balances reply")
        available_balances: Dict[str, Decimal] = {}
        total_balances: Dict[str, Decimal] = {}
        for entry in balances:
            if not isinstance(entry, dict):
                continue
            asset = entry.get("asset")
            available = to_decimal(entry.get("available"))
            held = to_decimal(entry.get("held")) or s_decimal_0
            if not asset or available is None:
                continue
            # pending deposits are not spendable and are left out
            available_balances[asset] = available
            total_balances[asset] = available + held
        # replace in one step; assets no longer listed are dropped
        self._account_available_balances.clear()
        self._account_available_balances.update(available_balances)
        self._account_balances.clear()
        self._account_balances.update(total_balances)
