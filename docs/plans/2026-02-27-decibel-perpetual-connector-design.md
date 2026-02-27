# Decibel Perpetual Connector Design

## Overview

Integrate Decibel (https://decibel.trade) as a perpetual derivative connector in Hummingbot. Decibel is an on-chain perpetuals exchange built on Aptos, using a central-limit order book implemented in Move smart contracts.

The connector follows the HyperLiquid Perpetual connector pattern (Approach B: Hybrid REST/On-Chain) with an additional Aptos client module for clean separation of on-chain transaction logic.

## Key Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Market type | Perpetual only | Decibel is a perps-focused DEX |
| Aptos SDK | `aptos-sdk` (PyPI) | Official package, mirrors Decibel's TS SDK pattern |
| Credentials | Geomi API key + Aptos private key | Dual auth: Bearer token for reads, Ed25519 for writes |
| Testnet | Yes (OTHER_DOMAINS pattern) | Mainnet + testnet as separate connector variants |
| Feature scope | Standard perp features | Limit/market orders, cancel, positions, balances, orderbook |
| Subaccount | Auto-discover with override | Fetch from API, allow config override |
| Default fees | Tier 0: 0.034% taker / 0.011% maker | Most conservative default |

## File Structure

### Connector

```
hummingbot/connector/derivative/decibel_perpetual/
├── __init__.py
├── decibel_perpetual_derivative.py            # Main connector (PerpetualDerivativePyBase)
├── decibel_perpetual_auth.py                  # Dual auth: Bearer + Aptos signing delegation
├── decibel_perpetual_aptos_client.py          # Aptos SDK wrapper for on-chain txns
├── decibel_perpetual_constants.py             # URLs, endpoints, rate limits, contract addrs
├── decibel_perpetual_web_utils.py             # REST + WebAssistantsFactory setup
├── decibel_perpetual_api_order_book_data_source.py  # WS orderbook + trades
├── decibel_perpetual_user_stream_data_source.py     # WS order/position updates
└── decibel_perpetual_utils.py                 # Config maps, registration exports
```

### Tests

```
test/hummingbot/connector/derivative/decibel_perpetual/
├── test_decibel_perpetual_derivative.py
├── test_decibel_perpetual_auth.py
├── test_decibel_perpetual_aptos_client.py
├── test_decibel_perpetual_api_order_book_data_source.py
├── test_decibel_perpetual_user_stream_data_source.py
└── test_decibel_perpetual_utils.py
```

## Authentication

### REST API (reads)

All GET endpoints require:
```
Authorization: Bearer <geomi_api_key>
```

### WebSocket

Connection requires:
```
Sec-Websocket-Protocol: decibel, <geomi_api_key>
```

### On-Chain (writes)

Order placement and cancellation are Aptos transactions:
1. Build transaction payload targeting Decibel's Move functions
2. Sign with Ed25519 private key via `aptos_sdk.Account`
3. Submit to Aptos blockchain
4. Parse transaction events for confirmation

## Aptos Client (`decibel_perpetual_aptos_client.py`)

Encapsulates all Aptos-specific logic:

```python
class DecibelAptosClient:
    def __init__(self, private_key: str, network: str, package_address: str):
        # Initialize aptos_sdk.Account, RestClient

    async def place_order(self, subaccount, market_addr, price, size,
                          is_buy, tif, reduce_only, client_order_id=None) -> str:
        # Build place_order_to_subaccount txn, sign, submit, return order_id

    async def cancel_order(self, subaccount, market_addr, order_id) -> bool:
        # Build cancel_order_to_subaccount txn, sign, submit

    async def get_subaccounts(self, account_addr) -> List[str]:
        # Fetch subaccounts via REST API

    @staticmethod
    def format_price(price: Decimal, decimals: int) -> int:
        # Convert decimal price to 9-decimal u64 integer

    @staticmethod
    def format_size(size: Decimal, decimals: int) -> int:
        # Convert decimal size to 9-decimal u64 integer
```

### Price/Size Formatting

Decibel uses 9-decimal u64 integers: `5.67` -> `5670000000`

Markets provide `px_decimals` and `sz_decimals` from `GET /api/v1/markets`.

### Contract Addresses

| Network | Package Address |
|---|---|
| Mainnet | `0x50ead22afd6ffd9769e3b3d6e0e64a2a350d68e8b102c4e72e33d0b8cfdfdb06` |
| Testnet | `0x952535c3049e52f195f26798c2f1340d7dd5100edbe0f464e520a974d16fbe9f` |

### Move Functions

- `{package}::dex_accounts::place_order_to_subaccount` — Place order
- `{package}::dex_accounts::cancel_order_to_subaccount` — Cancel order

## Data Flow

### Market Data (read path via WebSocket)

```
wss://api.{network}.aptoslabs.com/decibel/ws
  ├── depth:{marketAddr}          -> OrderBookTrackerDataSource -> OrderBook
  ├── trades:{marketAddr}         -> OrderBookTrackerDataSource -> TradeMessages
  └── market_price:{marketAddr}   -> Ticker data (mark_px, funding_rate)
```

### User Data (read path via WebSocket + REST polling)

```
WebSocket:
  ├── order_updates:{accountAddr}    -> Order status changes
  ├── user_trades:{accountAddr}      -> Fill events
  └── account_positions:{accountAddr} -> Position updates

REST polling fallback:
  ├── GET /api/v1/account_overviews  -> Balance/margin info
  ├── GET /api/v1/open_orders        -> Open orders
  └── GET /api/v1/account_positions  -> Positions
```

### Order Execution (write path via Aptos transactions)

```
place_order() -> DecibelAptosClient.place_order()
    -> Build Aptos transaction (place_order_to_subaccount)
    -> Sign with Ed25519 private key
    -> Submit to Aptos blockchain
    -> Parse transaction events for order_id
    -> Return exchange_order_id

cancel_order() -> DecibelAptosClient.cancel_order()
    -> Build Aptos transaction (cancel_order_to_subaccount)
    -> Sign, submit, confirm
```

## Trading Pair & Market Mapping

On startup:
1. Fetch all markets from `GET /api/v1/markets`
2. Build maps: `trading_pair <-> market_addr`
3. Store per-market: `sz_decimals`, `px_decimals`, `tick_size`, `min_size`, `lot_size`, `max_leverage`
4. Hummingbot trading pair format: use Decibel's `market_name` directly (e.g., `BTC-PERP`)

## Order Type Mapping

| Hummingbot Type | Decibel TIF | Value | Description |
|---|---|---|---|
| `LIMIT` | GoodTillCanceled | `0` | Standard limit order |
| `LIMIT_MAKER` | PostOnly | `1` | Add liquidity only |
| `MARKET` | ImmediateOrCancel | `2` | Market order via IOC |

## Balance & Position Tracking

### Balances

- Poll `GET /api/v1/account_overviews?account={addr}`
- `perp_equity_balance` -> `_account_balances["USDC"]`
- `usdc_cross_withdrawable_balance` -> `_account_available_balances["USDC"]`

### Positions

- WebSocket `account_positions:{accountAddr}` for real-time
- Fallback: `GET /api/v1/account_positions?account={addr}`
- Fields: `size`, `entry_price`, `unrealized_funding`, `estimated_liquidation_price`, `is_isolated`, `user_leverage`

## Configuration

### Config Map (`decibel_perpetual_utils.py`)

```python
class DecibelPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "decibel_perpetual"
    decibel_perpetual_api_key: SecretStr       # Geomi Bearer token
    decibel_perpetual_secret_key: SecretStr     # Aptos private key (hex)
    decibel_perpetual_trading_account: Optional[str]  # Subaccount address override
```

### Registration Exports

```python
DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00011"),   # 0.011%
    taker_percent_fee_decimal=Decimal("0.00034"),   # 0.034%
    buy_percent_fee_deducted_from_returns=True
)
CENTRALIZED = False
EXAMPLE_PAIR = "BTC-PERP"
OTHER_DOMAINS = ["decibel_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"decibel_perpetual_testnet": "decibel_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"decibel_perpetual_testnet": "BTC-PERP"}
OTHER_DOMAINS_DEFAULT_FEES = {"decibel_perpetual_testnet": [0.011, 0.034]}
OTHER_DOMAINS_KEYS = {"decibel_perpetual_testnet": DecibelPerpetualTestnetConfigMap.model_construct()}
```

## URLs & Rate Limits

| Domain | REST Base | WebSocket |
|---|---|---|
| Mainnet | `https://api.mainnet.aptoslabs.com/decibel` | `wss://api.mainnet.aptoslabs.com/decibel/ws` |
| Testnet | `https://api.testnet.aptoslabs.com/decibel` | `wss://api.testnet.aptoslabs.com/decibel/ws` |

Rate limits: Conservative defaults via `AsyncThrottler` (Decibel docs don't specify explicit limits). WebSocket has 100 subscription limit per connection and 1-hour session timeout.

## WebSocket Protocol

### Subscribe

```json
{"method": "subscribe", "topic": "depth:0xmarketAddr"}
```

### Heartbeat

Server sends ping frames every 30 seconds. Client must respond with pong.

### Reconnection

1-hour session timeout requires reconnection logic with exponential backoff.

## Dependencies

- `aptos-sdk` (PyPI) — Aptos transaction building and signing
- Existing Hummingbot dependencies (aiohttp, pydantic, etc.)
