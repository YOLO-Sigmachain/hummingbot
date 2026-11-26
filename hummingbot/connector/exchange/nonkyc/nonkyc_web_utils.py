import json
import re
from typing import Any, Callable, Optional, Tuple

import hummingbot.connector.exchange.nonkyc.nonkyc_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

_HTTP_STATUS_RE = re.compile(r"HTTP status is (\d{3})")
_ERROR_CODE_RE = re.compile(r'"code"\s*:\s*(\d+)')


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    return CONSTANTS.REST_URL + path_url


def parse_request_error(error: BaseException) -> Tuple[Optional[int], Optional[int]]:
    """
    (HTTP status, NonKYC error code) from an error raised by the REST assistant.
    Either is None when the error carries no such value (a timeout, a dropped connection).
    """
    text = str(error)
    status_match = _HTTP_STATUS_RE.search(text)
    status = int(status_match.group(1)) if status_match else None
    code = None
    marker = "Error: "
    if marker in text:
        try:
            body = json.loads(text.split(marker, 1)[1])
            if isinstance(body, dict):
                inner = body.get("error", body)
                if isinstance(inner, dict) and inner.get("code") is not None:
                    code = int(inner["code"])
        except (ValueError, TypeError):
            code = None
    if code is None and status is not None:
        code_match = _ERROR_CODE_RE.search(text)
        code = int(code_match.group(1)) if code_match else None
    return status, code


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        time_synchronizer: Optional[TimeSynchronizer] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
        time_provider: Optional[Callable] = None,
        auth: Optional[AuthBase] = None, ) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = time_provider or (lambda: get_current_server_time(
        throttler=throttler,
        domain=domain,
    ))
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    return WebAssistantsFactory(throttler=throttler)


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(
        throttler: Optional[AsyncThrottler] = None,
        domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> float:
    """Server time in ms from the documented GET /time."""
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    response: Any = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL, domain=domain),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
        timeout=CONSTANTS.REQUEST_TIMEOUT,
    )
    server_time = response.get("serverTime") if isinstance(response, dict) else None
    if isinstance(server_time, bool) or not isinstance(server_time, (int, float)):
        raise IOError(f"Unexpected NonKYC server time reply: {response}")
    return float(server_time)
