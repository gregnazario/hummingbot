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
