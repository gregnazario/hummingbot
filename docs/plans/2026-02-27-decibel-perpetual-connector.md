# Decibel Perpetual Connector Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Integrate Decibel (an Aptos-based perpetuals DEX) as a derivative connector in Hummingbot, following the HyperLiquid perpetual connector pattern with an additional Aptos client module.

**Architecture:** Clone the `hyperliquid_perpetual` connector structure into `decibel_perpetual`, replacing EIP-712 signing with Aptos Ed25519 transactions, HyperLiquid's single POST `/info`+`/exchange` API with Decibel's REST GET endpoints + on-chain writes, and adapting WebSocket subscriptions to Decibel's topic format. A dedicated `DecibelAptosClient` encapsulates all Aptos SDK interactions.

**Tech Stack:** Python 3.10+, `aptos-sdk` (PyPI), `pydantic`, `aiohttp`, Hummingbot's `PerpetualDerivativePyBase` framework.

**Reference Design:** `docs/plans/2026-02-27-decibel-perpetual-connector-design.md`

**Reference Connector:** `hummingbot/connector/derivative/hyperliquid_perpetual/`

---

## Task 1: Constants & Configuration Foundation

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/__init__.py`
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_constants.py`
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_utils.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_utils.py`

**Step 1: Create the directory structure and __init__.py**

```bash
mkdir -p hummingbot/connector/derivative/decibel_perpetual
mkdir -p test/hummingbot/connector/derivative/decibel_perpetual
touch hummingbot/connector/derivative/decibel_perpetual/__init__.py
touch test/hummingbot/connector/derivative/decibel_perpetual/__init__.py
```

**Step 2: Write decibel_perpetual_constants.py**

This file defines all URLs, endpoints, rate limits, contract addresses, and order state mappings. Unlike HyperLiquid which uses POST to `/info` and `/exchange` for everything, Decibel has distinct REST GET endpoints under `/api/v1/` and on-chain writes via Aptos transactions.

```python
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.in_flight_order import OrderState

EXCHANGE_NAME = "decibel_perpetual"
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = None

MARKET_ORDER_SLIPPAGE = 0.05

DOMAIN = EXCHANGE_NAME
TESTNET_DOMAIN = "decibel_perpetual_testnet"

# REST API base URLs
PERPETUAL_BASE_URL = "https://api.mainnet.aptoslabs.com/decibel"
TESTNET_BASE_URL = "https://api.testnet.aptoslabs.com/decibel"

# WebSocket URLs
PERPETUAL_WS_URL = "wss://api.mainnet.aptoslabs.com/decibel/ws"
TESTNET_WS_URL = "wss://api.testnet.aptoslabs.com/decibel/ws"

# Aptos node URLs (for submitting transactions)
APTOS_NODE_URL = "https://fullnode.mainnet.aptoslabs.com/v1"
APTOS_TESTNET_NODE_URL = "https://fullnode.testnet.aptoslabs.com/v1"

# Decibel package addresses on Aptos
MAINNET_PACKAGE_ADDRESS = "0x50ead22afd6ffd9769e3b3d6e0e64a2a350d68e8b102c4e72e33d0b8cfdfdb06"
TESTNET_PACKAGE_ADDRESS = "0x952535c3049e52f195f26798c2f1340d7dd5100edbe0f464e520a974d16fbe9f"

# Move module path
DEX_ACCOUNTS_MODULE = "dex_accounts"
PLACE_ORDER_FUNCTION = "place_order_to_subaccount"
CANCEL_ORDER_FUNCTION = "cancel_order_to_subaccount"

# Price/size use 9 decimal precision on-chain
ON_CHAIN_DECIMAL_PLACES = 9

FUNDING_RATE_UPDATE_INTERNAL_SECOND = 60

CURRENCY = "USDC"

# REST API endpoints (GET requests with query params)
MARKETS_URL = "/api/v1/markets"
PRICES_URL = "/api/v1/prices"
ACCOUNT_OVERVIEW_URL = "/api/v1/account_overviews"
ACCOUNT_POSITIONS_URL = "/api/v1/account_positions"
OPEN_ORDERS_URL = "/api/v1/open_orders"
ORDER_HISTORY_URL = "/api/v1/order_history"
TRADE_HISTORY_URL = "/api/v1/trade_history"
SUBACCOUNTS_URL = "/api/v1/subaccounts"
CANDLESTICK_URL = "/api/v1/candlestick"
TRADES_URL = "/api/v1/trades"
FUNDING_RATE_HISTORY_URL = "/api/v1/funding_rate_history"

# WebSocket topic patterns (Decibel uses "topic" not "channel")
WS_DEPTH_TOPIC = "depth"           # depth:{marketAddr}
WS_TRADES_TOPIC = "trades"         # trades:{marketAddr}
WS_MARKET_PRICE_TOPIC = "market_price"  # market_price:{marketAddr}
WS_ORDER_UPDATES_TOPIC = "order_updates"     # order_updates:{accountAddr}
WS_USER_TRADES_TOPIC = "user_trades"         # user_trades:{accountAddr}
WS_ACCOUNT_POSITIONS_TOPIC = "account_positions"  # account_positions:{accountAddr}
WS_ACCOUNT_OVERVIEW_TOPIC = "account_overview"    # account_overview:{accountAddr}

# Order states mapping from Decibel API statuses
ORDER_STATE = {
    "open": OrderState.OPEN,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "rejected": OrderState.FAILED,
    "expired": OrderState.CANCELED,
}

# Time in Force values for on-chain order placement
TIF_GTC = 0  # GoodTillCanceled
TIF_POST_ONLY = 1  # PostOnly
TIF_IOC = 2  # ImmediateOrCancel

HEARTBEAT_TIME_INTERVAL = 30.0
WS_SESSION_TIMEOUT = 3600  # 1 hour max session
WS_MAX_SUBSCRIPTIONS = 100

MAX_REQUEST = 600
ALL_ENDPOINTS_LIMIT = "All"

RATE_LIMITS = [
    RateLimit(ALL_ENDPOINTS_LIMIT, limit=MAX_REQUEST, time_interval=60),
    RateLimit(limit_id=MARKETS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=PRICES_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_OVERVIEW_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ACCOUNT_POSITIONS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=OPEN_ORDERS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=ORDER_HISTORY_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=TRADE_HISTORY_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
    RateLimit(limit_id=SUBACCOUNTS_URL, limit=MAX_REQUEST, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(ALL_ENDPOINTS_LIMIT)]),
]

ORDER_NOT_EXIST_MESSAGE = "order"
UNKNOWN_ORDER_MESSAGE = "Order not found"
```

**Step 3: Write decibel_perpetual_utils.py**

Config maps for mainnet and testnet, plus registration exports. Unlike HyperLiquid (which has wallet mode selection), Decibel always uses Aptos private key + Geomi API key.

```python
from decimal import Decimal
from typing import Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00011"),  # 0.011% maker
    taker_percent_fee_decimal=Decimal("0.00034"),  # 0.034% taker
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = False  # On-chain DEX
EXAMPLE_PAIR = "BTC-PERP"
BROKER_ID = "HBOT"


class DecibelPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "decibel_perpetual"
    decibel_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Geomi API key (Bearer token for REST/WS)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos private key (hex, for signing on-chain transactions)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_trading_account: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your trading account (subaccount) address (leave blank to auto-discover)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="decibel_perpetual")


KEYS = DecibelPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["decibel_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"decibel_perpetual_testnet": "decibel_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"decibel_perpetual_testnet": "BTC-PERP"}
OTHER_DOMAINS_DEFAULT_FEES = {"decibel_perpetual_testnet": [0.011, 0.034]}


class DecibelPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "decibel_perpetual_testnet"
    decibel_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Geomi API key (Bearer token for REST/WS)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_testnet_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos private key (hex, for signing on-chain transactions)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_testnet_trading_account: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your trading account (subaccount) address (leave blank to auto-discover)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="decibel_perpetual_testnet")


OTHER_DOMAINS_KEYS = {
    "decibel_perpetual_testnet": DecibelPerpetualTestnetConfigMap.model_construct()
}
```

**Step 4: Write test for utils config**

```python
# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_utils.py
import unittest
from decimal import Decimal

from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_utils import (
    CENTRALIZED,
    DEFAULT_FEES,
    EXAMPLE_PAIR,
    KEYS,
    OTHER_DOMAINS,
    OTHER_DOMAINS_KEYS,
    DecibelPerpetualConfigMap,
    DecibelPerpetualTestnetConfigMap,
)


class DecibelPerpetualUtilsTest(unittest.TestCase):
    def test_default_fees(self):
        self.assertEqual(DEFAULT_FEES.maker_percent_fee_decimal, Decimal("0.00011"))
        self.assertEqual(DEFAULT_FEES.taker_percent_fee_decimal, Decimal("0.00034"))

    def test_centralized_is_false(self):
        self.assertFalse(CENTRALIZED)

    def test_example_pair(self):
        self.assertEqual(EXAMPLE_PAIR, "BTC-PERP")

    def test_keys_config(self):
        self.assertIsInstance(KEYS, DecibelPerpetualConfigMap)
        self.assertEqual(KEYS.connector, "decibel_perpetual")

    def test_other_domains(self):
        self.assertIn("decibel_perpetual_testnet", OTHER_DOMAINS)

    def test_testnet_keys_config(self):
        testnet_keys = OTHER_DOMAINS_KEYS["decibel_perpetual_testnet"]
        self.assertIsInstance(testnet_keys, DecibelPerpetualTestnetConfigMap)
        self.assertEqual(testnet_keys.connector, "decibel_perpetual_testnet")
```

**Step 5: Run test to verify it passes**

```bash
python -m pytest test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_utils.py -v
```

Expected: PASS

**Step 6: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/__init__.py \
       hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_constants.py \
       hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_utils.py \
       test/hummingbot/connector/derivative/decibel_perpetual/__init__.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_utils.py
git commit -m "feat(decibel): add constants, config maps, and registration for decibel_perpetual connector"
```

---

## Task 2: Web Utils & REST Pre-processor

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_web_utils.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_web_utils.py`

**Step 1: Write decibel_perpetual_web_utils.py**

Unlike HyperLiquid (all POST with JSON body), Decibel uses GET endpoints with `Authorization: Bearer <key>` headers. The pre-processor adds auth headers to every request.

```python
import time
from typing import Any, Dict, Optional

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest
from hummingbot.core.web_assistant.rest_pre_processors import RESTPreProcessorBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class DecibelPerpetualRESTPreProcessor(RESTPreProcessorBase):
    async def pre_process(self, request: RESTRequest) -> RESTRequest:
        if request.headers is None:
            request.headers = {}
        request.headers["Content-Type"] = "application/json"
        return request


def private_rest_url(*args, **kwargs) -> str:
    return rest_url(*args, **kwargs)


def public_rest_url(*args, **kwargs) -> str:
    return rest_url(*args, **kwargs)


def rest_url(path_url: str, domain: str = CONSTANTS.DOMAIN):
    base_url = CONSTANTS.PERPETUAL_BASE_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_BASE_URL
    return base_url + path_url


def wss_url(domain: str = CONSTANTS.DOMAIN):
    return CONSTANTS.PERPETUAL_WS_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_WS_URL


def build_api_factory(
        throttler: Optional[AsyncThrottler] = None,
        auth: Optional[AuthBase] = None) -> WebAssistantsFactory:
    throttler = throttler or create_throttler()
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[DecibelPerpetualRESTPreProcessor()],
        auth=auth)
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    api_factory = WebAssistantsFactory(
        throttler=throttler,
        rest_pre_processors=[DecibelPerpetualRESTPreProcessor()])
    return api_factory


def create_throttler() -> AsyncThrottler:
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(throttler, domain) -> float:
    return time.time()


def is_exchange_information_valid(rule: Dict[str, Any]) -> bool:
    """Check if a market is valid for trading (mode must be 'Open')."""
    return rule.get("mode", "Open") == "Open"
```

**Step 2: Write test**

```python
# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_web_utils.py
import unittest

from hummingbot.connector.derivative.decibel_perpetual import decibel_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_web_utils import (
    is_exchange_information_valid,
    rest_url,
    wss_url,
)


class DecibelPerpetualWebUtilsTest(unittest.TestCase):
    def test_rest_url_mainnet(self):
        url = rest_url("/api/v1/markets", domain=CONSTANTS.DOMAIN)
        self.assertEqual(url, f"{CONSTANTS.PERPETUAL_BASE_URL}/api/v1/markets")

    def test_rest_url_testnet(self):
        url = rest_url("/api/v1/markets", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(url, f"{CONSTANTS.TESTNET_BASE_URL}/api/v1/markets")

    def test_wss_url_mainnet(self):
        url = wss_url(domain=CONSTANTS.DOMAIN)
        self.assertEqual(url, CONSTANTS.PERPETUAL_WS_URL)

    def test_wss_url_testnet(self):
        url = wss_url(domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(url, CONSTANTS.TESTNET_WS_URL)

    def test_is_exchange_information_valid_open(self):
        self.assertTrue(is_exchange_information_valid({"mode": "Open"}))

    def test_is_exchange_information_valid_reduce_only(self):
        self.assertFalse(is_exchange_information_valid({"mode": "ReduceOnly"}))

    def test_is_exchange_information_valid_no_mode(self):
        # Defaults to "Open" if missing
        self.assertTrue(is_exchange_information_valid({}))
```

**Step 3: Run test**

```bash
python -m pytest test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_web_utils.py -v
```

Expected: PASS

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_web_utils.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_web_utils.py
git commit -m "feat(decibel): add web utils with REST pre-processor and URL builders"
```

---

## Task 3: Authentication Module

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_auth.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_auth.py`

**Step 1: Write decibel_perpetual_auth.py**

Key difference from HyperLiquid: no EIP-712 signing. Instead, this auth module adds Bearer token headers to REST/WS requests. On-chain signing is handled by the Aptos client (Task 4).

```python
from typing import Any, Dict, Optional

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class DecibelPerpetualAuth(AuthBase):
    """
    Authentication for Decibel REST API and WebSocket.

    REST: Authorization: Bearer <api_key>
    WebSocket: Sec-Websocket-Protocol: decibel, <api_key>

    On-chain transaction signing is handled by DecibelAptosClient, not this class.
    """

    def __init__(self, api_key: str, secret_key: str, trading_account: Optional[str] = None):
        self._api_key = api_key
        self._secret_key = secret_key
        self._trading_account = trading_account

    @property
    def api_key(self) -> str:
        return self._api_key

    @property
    def secret_key(self) -> str:
        return self._secret_key

    @property
    def trading_account(self) -> Optional[str]:
        return self._trading_account

    @trading_account.setter
    def trading_account(self, value: str):
        self._trading_account = value

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """Add Bearer token to REST API requests."""
        if request.headers is None:
            request.headers = {}
        request.headers["Authorization"] = f"Bearer {self._api_key}"
        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        WebSocket authentication is handled via Sec-Websocket-Protocol header
        during connection, not per-message. This is a no-op since the header
        is set during connection creation.
        """
        return request

    def get_ws_auth_headers(self) -> Dict[str, Any]:
        """Returns headers needed for authenticated WebSocket connection."""
        return {
            "Sec-Websocket-Protocol": f"decibel, {self._api_key}"
        }
```

**Step 2: Write test**

```python
# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_auth.py
import asyncio
import unittest

from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest


class DecibelPerpetualAuthTest(unittest.TestCase):
    def setUp(self):
        self._api_key = "test_api_key_12345"
        self._secret_key = "0xaabbccdd"
        self._trading_account = "0x1234567890abcdef"
        self.auth = DecibelPerpetualAuth(
            api_key=self._api_key,
            secret_key=self._secret_key,
            trading_account=self._trading_account,
        )

    def test_rest_authenticate_adds_bearer_token(self):
        request = RESTRequest(method=RESTMethod.GET, url="https://example.com/api/v1/markets")
        result = asyncio.get_event_loop().run_until_complete(self.auth.rest_authenticate(request))
        self.assertIn("Authorization", result.headers)
        self.assertEqual(result.headers["Authorization"], f"Bearer {self._api_key}")

    def test_rest_authenticate_preserves_existing_headers(self):
        request = RESTRequest(
            method=RESTMethod.GET,
            url="https://example.com",
            headers={"Content-Type": "application/json"}
        )
        result = asyncio.get_event_loop().run_until_complete(self.auth.rest_authenticate(request))
        self.assertEqual(result.headers["Content-Type"], "application/json")
        self.assertEqual(result.headers["Authorization"], f"Bearer {self._api_key}")

    def test_ws_auth_headers(self):
        headers = self.auth.get_ws_auth_headers()
        self.assertEqual(headers["Sec-Websocket-Protocol"], f"decibel, {self._api_key}")

    def test_trading_account_property(self):
        self.assertEqual(self.auth.trading_account, self._trading_account)
        self.auth.trading_account = "0xnewaddress"
        self.assertEqual(self.auth.trading_account, "0xnewaddress")

    def test_trading_account_defaults_none(self):
        auth = DecibelPerpetualAuth(api_key="key", secret_key="secret")
        self.assertIsNone(auth.trading_account)
```

**Step 3: Run test**

```bash
python -m pytest test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_auth.py -v
```

Expected: PASS

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_auth.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_auth.py
git commit -m "feat(decibel): add auth module with Bearer token for REST and WS protocol header"
```

---

## Task 4: Aptos Client Module

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_aptos_client.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_aptos_client.py`

**Step 1: Write decibel_perpetual_aptos_client.py**

This is the core novel piece — no equivalent exists in HyperLiquid. It wraps the `aptos-sdk` to build, sign, and submit Decibel transactions to the Aptos blockchain.

Key points:
- Prices and sizes use 9-decimal u64 encoding: `5.67` -> `5670000000`
- Market addresses are derived from package address + market name seed
- The `place_order_to_subaccount` function takes: signer, subaccount, market, price, size, is_buy, tif, reduce_only, and optional client_order_id/stop params
- The `cancel_order_to_subaccount` function takes: signer, subaccount, order_id, market

```python
import hashlib
import logging
from decimal import Decimal
from typing import List, Optional

from aptos_sdk.account import Account
from aptos_sdk.account_address import AccountAddress
from aptos_sdk.async_client import RestClient
from aptos_sdk.bcs import Serializer
from aptos_sdk.transactions import EntryFunction, TransactionArgument, TransactionPayload
from aptos_sdk.type_tag import TypeTag

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS

logger = logging.getLogger(__name__)


class DecibelAptosClient:
    """
    Wraps the Aptos SDK to build, sign, and submit Decibel on-chain transactions.

    Handles:
    - Order placement via place_order_to_subaccount
    - Order cancellation via cancel_order_to_subaccount
    - Price/size formatting to 9-decimal u64 integers
    - Market address derivation
    """

    def __init__(self, private_key: str, domain: str = CONSTANTS.DOMAIN):
        self._domain = domain
        self._account = Account.load_key(private_key)
        node_url = CONSTANTS.APTOS_NODE_URL if domain == CONSTANTS.DOMAIN else CONSTANTS.APTOS_TESTNET_NODE_URL
        self._client = RestClient(node_url)
        self._package_address = AccountAddress.from_str(
            CONSTANTS.MAINNET_PACKAGE_ADDRESS if domain == CONSTANTS.DOMAIN else CONSTANTS.TESTNET_PACKAGE_ADDRESS
        )

    @property
    def account_address(self) -> str:
        return str(self._account.address())

    async def place_order(
        self,
        subaccount: str,
        market_addr: str,
        price: Decimal,
        size: Decimal,
        is_buy: bool,
        tif: int = CONSTANTS.TIF_GTC,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> str:
        """
        Place an order on Decibel via Aptos on-chain transaction.

        Returns the transaction hash.
        """
        price_u64 = self.format_value(price)
        size_u64 = self.format_value(size)

        # Build optional args - client_order_id, stop_price, tp_trigger, tp_limit, sl_trigger, sl_limit,
        # builder_addr, builder_fee_bps
        # For standard orders, most optional params are 0 / empty
        cloid_u128 = 0
        if client_order_id:
            # Convert client order ID string to a u128 via hash
            md5 = hashlib.md5()
            md5.update(client_order_id.encode("utf-8"))
            cloid_u128 = int(md5.hexdigest(), 16)

        function = EntryFunction.natural(
            f"{self._package_address}::{CONSTANTS.DEX_ACCOUNTS_MODULE}",
            CONSTANTS.PLACE_ORDER_FUNCTION,
            [],  # type_args
            [
                TransactionArgument(AccountAddress.from_str(subaccount), Serializer.struct),
                TransactionArgument(AccountAddress.from_str(market_addr), Serializer.struct),
                TransactionArgument(price_u64, Serializer.u64),
                TransactionArgument(size_u64, Serializer.u64),
                TransactionArgument(is_buy, Serializer.bool),
                TransactionArgument(tif, Serializer.u8),
                TransactionArgument(reduce_only, Serializer.bool),
                TransactionArgument(cloid_u128, Serializer.u128),
                TransactionArgument(0, Serializer.u64),  # stop_price
                TransactionArgument(0, Serializer.u64),  # tp_trigger_price
                TransactionArgument(0, Serializer.u64),  # tp_limit_price
                TransactionArgument(0, Serializer.u64),  # sl_trigger_price
                TransactionArgument(0, Serializer.u64),  # sl_limit_price
                TransactionArgument(AccountAddress.from_str("0x0"), Serializer.struct),  # builder_addr
                TransactionArgument(0, Serializer.u64),  # builder_fee_bps
            ],
        )
        payload = TransactionPayload(function)
        signed_txn = await self._client.create_bcs_signed_transaction(self._account, payload)
        tx_hash = await self._client.submit_bcs_transaction(signed_txn)
        await self._client.wait_for_transaction(tx_hash)
        return tx_hash

    async def cancel_order(
        self,
        subaccount: str,
        market_addr: str,
        order_id: int,
    ) -> str:
        """
        Cancel an order on Decibel via Aptos on-chain transaction.

        Returns the transaction hash.
        """
        function = EntryFunction.natural(
            f"{self._package_address}::{CONSTANTS.DEX_ACCOUNTS_MODULE}",
            CONSTANTS.CANCEL_ORDER_FUNCTION,
            [],
            [
                TransactionArgument(AccountAddress.from_str(subaccount), Serializer.struct),
                TransactionArgument(order_id, Serializer.u128),
                TransactionArgument(AccountAddress.from_str(market_addr), Serializer.struct),
            ],
        )
        payload = TransactionPayload(function)
        signed_txn = await self._client.create_bcs_signed_transaction(self._account, payload)
        tx_hash = await self._client.submit_bcs_transaction(signed_txn)
        await self._client.wait_for_transaction(tx_hash)
        return tx_hash

    @staticmethod
    def format_value(value: Decimal) -> int:
        """Convert a decimal value to 9-decimal u64 integer for on-chain use."""
        return int(value * Decimal(10 ** CONSTANTS.ON_CHAIN_DECIMAL_PLACES))

    @staticmethod
    def parse_value(raw: int) -> Decimal:
        """Convert a 9-decimal u64 integer back to Decimal."""
        return Decimal(raw) / Decimal(10 ** CONSTANTS.ON_CHAIN_DECIMAL_PLACES)

    @staticmethod
    def derive_market_address(package_address: str, market_name: str) -> str:
        """
        Derive a market's object address from the package address and market name.
        Uses the same derivation as Decibel's TypeScript SDK.
        """
        # This derives the perp engine global address, then the market address
        # Implementation matches the helper from Decibel docs
        perp_engine_seed = b"GlobalPerpEngine"
        # Use Aptos object address derivation
        # For now, markets should be fetched from the API and cached
        raise NotImplementedError("Use GET /api/v1/markets to fetch market addresses instead of deriving them")

    async def close(self):
        """Clean up the REST client."""
        await self._client.close()
```

**Step 2: Write test (with mocked Aptos SDK)**

```python
# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_aptos_client.py
import unittest
from decimal import Decimal

from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_aptos_client import DecibelAptosClient
from hummingbot.connector.derivative.decibel_perpetual import decibel_perpetual_constants as CONSTANTS


class DecibelAptosClientStaticTest(unittest.TestCase):
    """Test static utility methods that don't require network access."""

    def test_format_value_integer(self):
        result = DecibelAptosClient.format_value(Decimal("5"))
        self.assertEqual(result, 5_000_000_000)

    def test_format_value_decimal(self):
        result = DecibelAptosClient.format_value(Decimal("5.67"))
        self.assertEqual(result, 5_670_000_000)

    def test_format_value_small(self):
        result = DecibelAptosClient.format_value(Decimal("0.001"))
        self.assertEqual(result, 1_000_000)

    def test_format_value_zero(self):
        result = DecibelAptosClient.format_value(Decimal("0"))
        self.assertEqual(result, 0)

    def test_parse_value(self):
        result = DecibelAptosClient.parse_value(5_670_000_000)
        self.assertEqual(result, Decimal("5.67"))

    def test_parse_value_roundtrip(self):
        original = Decimal("123.456789")
        encoded = DecibelAptosClient.format_value(original)
        decoded = DecibelAptosClient.parse_value(encoded)
        self.assertEqual(decoded, original)
```

**Step 3: Run test**

```bash
python -m pytest test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_aptos_client.py -v
```

Expected: PASS (only static methods tested; on-chain tests need mocking in integration tests)

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_aptos_client.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_aptos_client.py
git commit -m "feat(decibel): add Aptos client for on-chain order placement and cancellation"
```

---

## Task 5: Order Book Data Source

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_api_order_book_data_source.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_api_order_book_data_source.py`

**Step 1: Write decibel_perpetual_api_order_book_data_source.py**

Key differences from HyperLiquid:
- WebSocket topics use `depth:{marketAddr}` instead of subscribing with `{"type": "l2Book", "coin": symbol}`
- REST snapshot uses GET `/api/v1/markets` for depth instead of POST `/info` with `{"type": "l2Book"}`
- Decibel WS subscribes with `{"method": "subscribe", "topic": "depth:0xaddr"}` (simpler format)
- Funding info from `market_price:{marketAddr}` WS topic or REST `/api/v1/prices`

This is the largest file and should be modeled closely on `hyperliquid_perpetual_api_order_book_data_source.py` with Decibel-specific WS topic formats and REST endpoints.

The implementer should:
1. Copy `hyperliquid_perpetual_api_order_book_data_source.py` as a starting point
2. Replace all WS subscription logic to use `{"method": "subscribe", "topic": "depth:{market_addr}"}`
3. Replace REST snapshot to use GET `/api/v1/markets` endpoint
4. Replace funding info to use GET `/api/v1/prices` endpoint
5. Update message parsing to match Decibel's response format
6. Replace the connector type hint to `DecibelPerpetualDerivative`

The WS subscribe format is:
```json
{"method": "subscribe", "topic": "depth:{marketAddr}"}
{"method": "subscribe", "topic": "trades:{marketAddr}"}
{"method": "subscribe", "topic": "market_price:{marketAddr}"}
```

**Step 2: Write skeleton test modeled on hyperliquid test**

The test should mock REST and WS responses with Decibel-format data.

**Step 3: Run test, iterate**

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_api_order_book_data_source.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_api_order_book_data_source.py
git commit -m "feat(decibel): add order book data source with WS depth, trades, and funding info"
```

---

## Task 6: User Stream Data Source

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_user_stream_data_source.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_user_stream_data_source.py`

**Step 1: Write decibel_perpetual_user_stream_data_source.py**

Key differences from HyperLiquid:
- HyperLiquid subscribes to `orderUpdates` and `user` channels
- Decibel subscribes to `order_updates:{accountAddr}` and `user_trades:{accountAddr}`
- WebSocket auth via `Sec-Websocket-Protocol: decibel, <api_key>` header on connection
- Message format differs (Decibel uses `topic` field, HyperLiquid uses `channel`)

The implementer should:
1. Copy `hyperliquid_perpetual_user_stream_data_source.py` as a starting point
2. Replace WS subscription to use `{"method": "subscribe", "topic": "order_updates:{account_addr}"}`
3. Replace auth header setup to use `Sec-Websocket-Protocol`
4. Update heartbeat to respond to server ping frames (30s interval)
5. Replace the connector type hint

WS subscribe format:
```json
{"method": "subscribe", "topic": "order_updates:{accountAddr}"}
{"method": "subscribe", "topic": "user_trades:{accountAddr}"}
```

**Step 2: Write test**

**Step 3: Run test, iterate**

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_user_stream_data_source.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_user_stream_data_source.py
git commit -m "feat(decibel): add user stream data source for order updates and trade fills"
```

---

## Task 7: Main Derivative Connector

**Files:**
- Create: `hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_derivative.py`
- Test: `test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_derivative.py`

**Step 1: Write decibel_perpetual_derivative.py**

This is the central connector class. Key differences from HyperLiquid:

1. **Constructor**: Takes `api_key`, `secret_key`, `trading_account` instead of `address`, `secret_key`, `use_vault`, `mode`
2. **Authenticator**: Creates `DecibelPerpetualAuth` instead of `HyperliquidPerpetualAuth`
3. **Order placement**: Uses `DecibelAptosClient.place_order()` (on-chain Aptos transaction) instead of POST to `/exchange`
4. **Order cancellation**: Uses `DecibelAptosClient.cancel_order()` instead of POST to `/exchange`
5. **Balance tracking**: GET `/api/v1/account_overviews` instead of POST `/info` with `clearinghouseState`
6. **Position tracking**: GET `/api/v1/account_positions` instead of POST `/info` with `clearinghouseState`
7. **Trading rules**: GET `/api/v1/markets` returns `market_addr`, `market_name`, `sz_decimals`, `px_decimals`, `tick_size`, `min_size`, `lot_size`, `max_leverage`, `mode`
8. **Trading pair mapping**: Uses `market_name` (e.g., `"BTC-PERP"`) mapped to `market_addr`
9. **Order ID**: Decibel returns `order_id` (u128) from the on-chain event after transaction confirmation
10. **Subaccount discovery**: On startup, fetch subaccounts from GET `/api/v1/subaccounts?account={addr}` if not provided in config

The implementer should:
1. Copy `hyperliquid_perpetual_derivative.py` as a starting point
2. Replace the constructor to accept Decibel-specific params and initialize `DecibelAptosClient`
3. Replace `_place_order` to use `self._aptos_client.place_order()`
4. Replace `_place_cancel` to use `self._aptos_client.cancel_order()`
5. Replace `_update_balances` to use GET account overview
6. Replace `_update_positions` to use GET account positions
7. Replace `_format_trading_rules` to parse Decibel market format
8. Replace `_initialize_trading_pair_symbols_from_exchange_info` for Decibel's market name format
9. Add subaccount auto-discovery in `start()` or `_initialize_trading_pair_symbol_map()`
10. Build `market_name_to_addr` mapping from markets endpoint
11. Replace all HyperLiquid-specific `coin_to_asset` logic with `market_name_to_addr` mapping

Key mapping differences:
- HyperLiquid: `coin_to_asset["BTC"] = 0` (integer index)
- Decibel: `market_name_to_addr["BTC-PERP"] = "0xabc..."` (Aptos address)

**Step 2: Write test modeled on hyperliquid test**

The test file should extend `AbstractExchangeConnectorTests.ExchangeConnectorTests` following the same pattern as `test_hyperliquid_perpetual_derivative.py`, with mocked REST responses matching Decibel's API format.

**Step 3: Run test, iterate until core methods pass**

**Step 4: Commit**

```bash
git add hummingbot/connector/derivative/decibel_perpetual/decibel_perpetual_derivative.py \
       test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_derivative.py
git commit -m "feat(decibel): add main derivative connector with on-chain order execution"
```

---

## Task 8: Integration Testing & Verification

**Files:**
- All files from Tasks 1-7

**Step 1: Run full test suite**

```bash
python -m pytest test/hummingbot/connector/derivative/decibel_perpetual/ -v --tb=short
```

Expected: All tests PASS

**Step 2: Verify connector registration**

```bash
python -c "
from hummingbot.client.settings import AllConnectorSettings
settings = AllConnectorSettings.get_connector_settings()
print('decibel_perpetual' in settings)
print('decibel_perpetual_testnet' in settings)
print(settings.get('decibel_perpetual'))
"
```

Expected: `True`, `True`, and connector setting details

**Step 3: Verify aptos-sdk dependency is available**

Check if `aptos-sdk` needs to be added to `setup.py` or `requirements.txt`. Add it if missing:

```bash
grep -r "aptos" setup.py pyproject.toml requirements*.txt
```

If not found, add `aptos-sdk` to the project's dependencies.

**Step 4: Run linting**

```bash
flake8 hummingbot/connector/derivative/decibel_perpetual/ --max-line-length=130
```

Fix any linting issues.

**Step 5: Final commit**

```bash
git add -A
git commit -m "feat(decibel): finalize decibel_perpetual connector integration and fix linting"
```

---

## Task Order & Dependencies

```
Task 1 (constants + config) ─────────────┐
Task 2 (web utils) ──────────────────────┤
Task 3 (auth) ───────────────────────────┤──> Task 7 (main connector)──> Task 8 (integration)
Task 4 (aptos client) ──────────────────┤
Task 5 (order book data source) ─────────┤
Task 6 (user stream data source) ────────┘
```

Tasks 1-4 are independent and can be parallelized. Tasks 5-6 depend on 1-2. Task 7 depends on all of 1-6. Task 8 depends on 7.

---

## Key Reference Files

When implementing each task, refer to the HyperLiquid perpetual connector equivalent:

| Decibel File | HyperLiquid Reference |
|---|---|
| `decibel_perpetual_constants.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_constants.py` |
| `decibel_perpetual_utils.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_utils.py` |
| `decibel_perpetual_web_utils.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_web_utils.py` |
| `decibel_perpetual_auth.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_auth.py` |
| `decibel_perpetual_aptos_client.py` | (no equivalent - novel module) |
| `decibel_perpetual_api_order_book_data_source.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_api_order_book_data_source.py` |
| `decibel_perpetual_user_stream_data_source.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_user_stream_data_source.py` |
| `decibel_perpetual_derivative.py` | `hummingbot/connector/derivative/hyperliquid_perpetual/hyperliquid_perpetual_derivative.py` |

## Decibel API Quick Reference

| Operation | Method | Endpoint / Mechanism |
|---|---|---|
| List markets | GET | `/api/v1/markets` |
| Market prices | GET | `/api/v1/prices` |
| Account overview | GET | `/api/v1/account_overviews?account={addr}` |
| Account positions | GET | `/api/v1/account_positions?account={addr}` |
| Open orders | GET | `/api/v1/open_orders?account={addr}` |
| Trade history | GET | `/api/v1/trade_history?account={addr}` |
| Order history | GET | `/api/v1/order_history?account={addr}` |
| Subaccounts | GET | `/api/v1/subaccounts?account={addr}` |
| Place order | On-chain | `dex_accounts::place_order_to_subaccount` |
| Cancel order | On-chain | `dex_accounts::cancel_order_to_subaccount` |
| Orderbook WS | WebSocket | `depth:{marketAddr}` |
| Trades WS | WebSocket | `trades:{marketAddr}` |
| Price WS | WebSocket | `market_price:{marketAddr}` |
| Order updates WS | WebSocket | `order_updates:{accountAddr}` |
| User trades WS | WebSocket | `user_trades:{accountAddr}` |
| Positions WS | WebSocket | `account_positions:{accountAddr}` |
