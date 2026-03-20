import asyncio
import hashlib
import time
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from bidict import bidict

# Patch split_hb_trading_pair before any connector imports so that multi-hyphen
# trading pairs like "BTC-PERP-USDC" are handled correctly (rsplit on last "-").
import hummingbot.connector.utils as _conn_utils

_original_split = _conn_utils.split_hb_trading_pair


def _safe_split(trading_pair: str) -> Tuple[str, str]:
    parts = trading_pair.rsplit("-", 1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return _original_split(trading_pair)


_conn_utils.split_hb_trading_pair = _safe_split

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_web_utils as web_utils
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_api_order_book_data_source import (
    DecibelPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_derivative import (
    DecibelPerpetualDerivative,
)
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount

API_KEY = "test_api_key"
SECRET_KEY = "0xdeadbeef"
TRADING_ACCOUNT = "0xsubaccount1234"
TRADING_PAIR = "BTC-PERP-USDC"
EXCHANGE_SYMBOL = "BTC-PERP"
MARKET_ADDR = "0xabc123def456"
DOMAIN = CONSTANTS.TESTNET_DOMAIN

MARKETS_RESPONSE = [
    {
        "market_name": "BTC-PERP",
        "market_addr": MARKET_ADDR,
        "sz_decimals": 4,
        "px_decimals": 2,
        "tick_size": "0.01",
        "min_size": "0.0001",
        "lot_size": "0.0001",
        "max_leverage": "50",
        "mode": "Open",
    }
]


class TestDecibelPerpetualDerivative(IsolatedAsyncioWrapperTestCase):
    level = 0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "BTC-PERP"
        cls.quote_asset = "USDC"
        cls.trading_pair = TRADING_PAIR
        cls.exchange_symbol = EXCHANGE_SYMBOL

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.exchange = DecibelPerpetualDerivative(
            decibel_perpetual_api_key=API_KEY,
            decibel_perpetual_secret_key=SECRET_KEY,
            decibel_perpetual_trading_account=TRADING_ACCOUNT,
            trading_pairs=[self.trading_pair],
            trading_required=True,
            domain=DOMAIN,
        )

        # Mock the Aptos client so we never import aptos-sdk
        self.mock_aptos_client = AsyncMock()
        self.mock_aptos_client.place_order = AsyncMock(return_value="0xfaketxhash")
        self.mock_aptos_client.cancel_order = AsyncMock(return_value="0xfakecancelhash")
        self.mock_aptos_client.account_address = "0xwallet123"
        self.exchange._aptos_client = self.mock_aptos_client
        self.exchange._get_aptos_client = MagicMock(return_value=self.mock_aptos_client)

        # Set up the symbol map as if _initialize_trading_pair_symbol_map already ran
        self.exchange._set_trading_pair_symbol_map(bidict({EXCHANGE_SYMBOL: TRADING_PAIR}))
        self.exchange.market_name_to_addr = {EXCHANGE_SYMBOL: MARKET_ADDR}
        self.exchange.addr_to_market_name = {MARKET_ADDR: EXCHANGE_SYMBOL}

        # Set up a trading rule
        self.exchange._trading_rules[TRADING_PAIR] = TradingRule(
            trading_pair=TRADING_PAIR,
            min_order_size=Decimal("0.0001"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.0001"),
            buy_order_collateral_token="USDC",
            sell_order_collateral_token="USDC",
        )

        self.exchange._set_current_timestamp(1640780000)
        self.exchange.logger().setLevel(1)
        self.exchange.logger().addHandler(self)
        self.exchange._order_tracker.logger().setLevel(1)
        self.exchange._order_tracker.logger().addHandler(self)

    def tearDown(self) -> None:
        super().tearDown()

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and message in record.getMessage()
            for record in self.log_records
        )

    # ==================================================================
    # Properties
    # ==================================================================

    def test_name_returns_domain(self):
        self.assertEqual(DOMAIN, self.exchange.name)

    def test_authenticator_returns_auth_when_trading_required(self):
        auth = self.exchange.authenticator
        self.assertIsNotNone(auth)
        self.assertEqual(API_KEY, auth.api_key)

    def test_authenticator_returns_none_when_not_trading(self):
        exchange = DecibelPerpetualDerivative(
            decibel_perpetual_api_key=API_KEY,
            decibel_perpetual_secret_key=SECRET_KEY,
            trading_pairs=[TRADING_PAIR],
            trading_required=False,
            domain=DOMAIN,
        )
        self.assertIsNone(exchange.authenticator)

    def test_supported_order_types(self):
        types = self.exchange.supported_order_types()
        self.assertIn(OrderType.LIMIT, types)
        self.assertIn(OrderType.LIMIT_MAKER, types)
        self.assertIn(OrderType.MARKET, types)

    def test_supported_position_modes(self):
        modes = self.exchange.supported_position_modes()
        self.assertEqual([PositionMode.ONEWAY], modes)

    def test_is_cancel_request_synchronous(self):
        self.assertTrue(self.exchange.is_cancel_request_in_exchange_synchronous)

    # ==================================================================
    # Symbol map initialization
    # ==================================================================

    def test_initialize_trading_pair_symbols_from_exchange_info_list(self):
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(MARKETS_RESPONSE)
        self.assertIn(EXCHANGE_SYMBOL, self.exchange.market_name_to_addr)
        self.assertIn(MARKET_ADDR, self.exchange.addr_to_market_name)

    def test_initialize_trading_pair_symbols_from_exchange_info_dict(self):
        """Response wrapped in a dict with 'markets' key."""
        self.exchange._initialize_trading_pair_symbols_from_exchange_info({"markets": MARKETS_RESPONSE})
        self.assertIn(EXCHANGE_SYMBOL, self.exchange.market_name_to_addr)

    def test_initialize_trading_pair_symbols_skips_close_only(self):
        markets = [
            {**MARKETS_RESPONSE[0], "mode": "CloseOnly"},
        ]
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)
        self.assertEqual({}, self.exchange.market_name_to_addr)

    def test_initialize_trading_pair_symbols_skips_empty_market_name(self):
        markets = [{"market_name": "", "market_addr": "0xfoo", "mode": "Open"}]
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)
        self.assertEqual({}, self.exchange.market_name_to_addr)

    def test_initialize_trading_pair_symbols_handles_non_list(self):
        """Non-list, non-dict input raises AttributeError."""
        with self.assertRaises(AttributeError):
            self.exchange._initialize_trading_pair_symbols_from_exchange_info("invalid")

    # ==================================================================
    # Trading rules
    # ==================================================================

    async def test_format_trading_rules(self):
        rules = await self.exchange._format_trading_rules(MARKETS_RESPONSE)
        self.assertEqual(1, len(rules))
        rule = rules[0]
        self.assertEqual(TRADING_PAIR, rule.trading_pair)
        self.assertEqual(Decimal("0.0001"), rule.min_order_size)
        self.assertEqual(Decimal("0.01"), rule.min_price_increment)

    async def test_format_trading_rules_error_handling(self):
        """Invalid market data should be skipped."""
        bad_markets = [{"market_name": "INVALID", "market_addr": "0xbad", "mode": "Open"}]
        # The exchange_symbol_associated_to_pair will raise KeyError for INVALID
        with patch.object(self.exchange, "trading_pair_associated_to_exchange_symbol", side_effect=KeyError):
            rules = await self.exchange._format_trading_rules(bad_markets)
        self.assertEqual(0, len(rules))

    async def test_format_trading_rules_defaults(self):
        """When sz_decimals/px_decimals are missing, defaults are used."""
        markets = [{
            "market_name": EXCHANGE_SYMBOL,
            "market_addr": MARKET_ADDR,
            "mode": "Open",
        }]
        rules = await self.exchange._format_trading_rules(markets)
        self.assertEqual(1, len(rules))

    def test_quantize_order_price(self):
        result = self.exchange.quantize_order_price(TRADING_PAIR, Decimal("12345.6789"))
        self.assertIsInstance(result, Decimal)

    # ==================================================================
    # Auto-discover subaccount
    # ==================================================================

    async def test_auto_discover_trading_account_already_set(self):
        """No REST call when trading_account is already set."""
        self.exchange._api_get = AsyncMock()
        await self.exchange._auto_discover_trading_account()
        self.exchange._api_get.assert_not_awaited()

    async def test_auto_discover_trading_account_list_string(self):
        """List of string addresses — use first."""
        self.exchange._auth.trading_account = None
        self.exchange._trading_account = None
        self.exchange._api_get = AsyncMock(return_value=["0xdiscovered1", "0xdiscovered2"])
        await self.exchange._auto_discover_trading_account()
        self.assertEqual("0xdiscovered1", self.exchange._auth.trading_account)

    async def test_auto_discover_trading_account_list_dict(self):
        """List of dicts with 'address' key."""
        self.exchange._auth.trading_account = None
        self.exchange._trading_account = None
        self.exchange._api_get = AsyncMock(return_value=[{"address": "0xdictaddr"}])
        await self.exchange._auto_discover_trading_account()
        self.assertEqual("0xdictaddr", self.exchange._auth.trading_account)

    async def test_auto_discover_trading_account_empty_list(self):
        """Empty list logs warning."""
        self.exchange._auth.trading_account = None
        self.exchange._trading_account = None
        self.exchange._api_get = AsyncMock(return_value=[])
        await self.exchange._auto_discover_trading_account()
        self.assertTrue(self._is_logged("WARNING", "No subaccounts found"))

    async def test_auto_discover_trading_account_not_trading(self):
        """When not trading, skip discovery."""
        exchange = DecibelPerpetualDerivative(
            decibel_perpetual_api_key=API_KEY,
            decibel_perpetual_secret_key=SECRET_KEY,
            trading_pairs=[TRADING_PAIR],
            trading_required=False,
            domain=DOMAIN,
        )
        exchange._api_get = AsyncMock()
        await exchange._auto_discover_trading_account()
        exchange._api_get.assert_not_awaited()

    # ==================================================================
    # Buy / Sell (order IDs)
    # ==================================================================

    def test_buy_returns_hex_order_id(self):
        with patch.object(self.exchange, "_create_order", new_callable=AsyncMock):
            order_id = self.exchange.buy(TRADING_PAIR, Decimal("1.0"), OrderType.LIMIT, Decimal("50000"))
        self.assertTrue(order_id.startswith("0x"))
        self.assertEqual(34, len(order_id))  # "0x" + 32 hex chars

    def test_sell_returns_hex_order_id(self):
        with patch.object(self.exchange, "_create_order", new_callable=AsyncMock):
            order_id = self.exchange.sell(TRADING_PAIR, Decimal("1.0"), OrderType.LIMIT, Decimal("50000"))
        self.assertTrue(order_id.startswith("0x"))

    def test_buy_market_order_applies_slippage(self):
        """Market buy should quantize price with positive slippage."""
        with patch.object(self.exchange, "get_mid_price", return_value=Decimal("50000")):
            with patch.object(self.exchange, "_create_order", new_callable=AsyncMock) as mock_create:
                self.exchange.buy(TRADING_PAIR, Decimal("1.0"), OrderType.MARKET, Decimal("NaN"))
                # _create_order should be called with price > 50000
                call_kwargs = mock_create.call_args[1]
                self.assertGreater(call_kwargs["price"], Decimal("50000"))

    def test_sell_market_order_applies_slippage(self):
        """Market sell should quantize price with negative slippage."""
        with patch.object(self.exchange, "get_mid_price", return_value=Decimal("50000")):
            with patch.object(self.exchange, "_create_order", new_callable=AsyncMock) as mock_create:
                self.exchange.sell(TRADING_PAIR, Decimal("1.0"), OrderType.MARKET, Decimal("NaN"))
                call_kwargs = mock_create.call_args[1]
                self.assertLess(call_kwargs["price"], Decimal("50000"))

    def test_buy_with_explicit_price_for_market(self):
        """Market order with explicit price should use it as reference."""
        with patch.object(self.exchange, "_create_order", new_callable=AsyncMock) as mock_create:
            self.exchange.buy(TRADING_PAIR, Decimal("1.0"), OrderType.MARKET, Decimal("48000"))
            call_kwargs = mock_create.call_args[1]
            self.assertGreater(call_kwargs["price"], Decimal("48000"))

    def test_sell_limit_passes_price_through(self):
        """LIMIT order passes price through without slippage."""
        with patch.object(self.exchange, "_create_order", new_callable=AsyncMock) as mock_create:
            self.exchange.sell(TRADING_PAIR, Decimal("1.0"), OrderType.LIMIT, Decimal("55000"))
            call_kwargs = mock_create.call_args[1]
            self.assertEqual(Decimal("55000"), call_kwargs["price"])

    # ==================================================================
    # Place order (on-chain)
    # ==================================================================

    async def test_place_order_limit(self):
        exchange_id, ts = await self.exchange._place_order(
            order_id="0x1234",
            trading_pair=TRADING_PAIR,
            amount=Decimal("1.0"),
            trade_type=TradeType.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
        )
        self.assertEqual("0xfaketxhash", exchange_id)
        self.mock_aptos_client.place_order.assert_awaited_once()
        call_kwargs = self.mock_aptos_client.place_order.call_args[1]
        self.assertEqual(CONSTANTS.TIF_GTC, call_kwargs["tif"])
        self.assertTrue(call_kwargs["is_buy"])
        self.assertFalse(call_kwargs["reduce_only"])

    async def test_place_order_limit_maker(self):
        await self.exchange._place_order(
            order_id="0x1234",
            trading_pair=TRADING_PAIR,
            amount=Decimal("0.5"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT_MAKER,
            price=Decimal("60000"),
        )
        call_kwargs = self.mock_aptos_client.place_order.call_args[1]
        self.assertEqual(CONSTANTS.TIF_POST_ONLY, call_kwargs["tif"])
        self.assertFalse(call_kwargs["is_buy"])

    async def test_place_order_market(self):
        await self.exchange._place_order(
            order_id="0x1234",
            trading_pair=TRADING_PAIR,
            amount=Decimal("0.1"),
            trade_type=TradeType.BUY,
            order_type=OrderType.MARKET,
            price=Decimal("52000"),
        )
        call_kwargs = self.mock_aptos_client.place_order.call_args[1]
        self.assertEqual(CONSTANTS.TIF_IOC, call_kwargs["tif"])

    async def test_place_order_reduce_only(self):
        await self.exchange._place_order(
            order_id="0x1234",
            trading_pair=TRADING_PAIR,
            amount=Decimal("1.0"),
            trade_type=TradeType.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal("50000"),
            position_action=PositionAction.CLOSE,
        )
        call_kwargs = self.mock_aptos_client.place_order.call_args[1]
        self.assertTrue(call_kwargs["reduce_only"])

    async def test_place_order_no_market_addr_raises(self):
        self.exchange.market_name_to_addr = {}
        with self.assertRaises(ValueError):
            await self.exchange._place_order(
                order_id="0x1234",
                trading_pair=TRADING_PAIR,
                amount=Decimal("1.0"),
                trade_type=TradeType.BUY,
                order_type=OrderType.LIMIT,
                price=Decimal("50000"),
            )

    # ==================================================================
    # Place cancel (on-chain)
    # ==================================================================

    async def test_place_cancel_success(self):
        tracked = InFlightOrder(
            client_order_id="0xabc",
            exchange_order_id="12345",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
        )
        result = await self.exchange._place_cancel("0xabc", tracked)
        self.assertTrue(result)
        self.mock_aptos_client.cancel_order.assert_awaited_once()
        call_kwargs = self.mock_aptos_client.cancel_order.call_args[1]
        self.assertEqual(12345, call_kwargs["order_id"])

    async def test_place_cancel_non_integer_exchange_id(self):
        """If exchange_order_id is a tx hash (not integer), should raise IOError."""
        tracked = InFlightOrder(
            client_order_id="0xabc",
            exchange_order_id="0xfaketxhash",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
        )
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("0xabc", tracked)

    async def test_place_cancel_timeout_waiting_for_id(self):
        """When get_exchange_order_id times out, raise IOError."""
        tracked = MagicMock(spec=InFlightOrder)
        tracked.exchange_order_id = None
        tracked.trading_pair = TRADING_PAIR
        tracked.get_exchange_order_id = AsyncMock(side_effect=asyncio.TimeoutError)
        with self.assertRaises(IOError):
            await self.exchange._place_cancel("0xabc", tracked)

    async def test_place_cancel_unknown_order(self):
        """When the exchange says order not found, should raise."""
        self.mock_aptos_client.cancel_order = AsyncMock(
            side_effect=Exception(CONSTANTS.UNKNOWN_ORDER_MESSAGE)
        )
        tracked = InFlightOrder(
            client_order_id="0xabc",
            exchange_order_id="99999",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
        )
        with self.assertRaises(Exception):
            await self.exchange._place_cancel("0xabc", tracked)

    async def test_place_cancel_no_market_addr_raises(self):
        self.exchange.market_name_to_addr = {}
        tracked = InFlightOrder(
            client_order_id="0xabc",
            exchange_order_id="12345",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
        )
        with self.assertRaises(ValueError):
            await self.exchange._place_cancel("0xabc", tracked)

    # ==================================================================
    # User stream event listener
    # ==================================================================

    async def test_user_stream_order_update(self):
        """Order update from WS is processed correctly."""
        order = InFlightOrder(
            client_order_id="0xclient1",
            exchange_order_id="67890",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient1"] = order

        order_msg = {
            "order_id": "67890",
            "client_order_id": "0xclient1",
            "status": "filled",
            "unix_ms": 1640780001000,
        }
        self.exchange._process_order_message(order_msg)
        # The order should now be in FILLED state after processing
        self.assertEqual("67890", order.exchange_order_id)

    async def test_user_stream_order_update_by_exchange_id(self):
        """Fallback matching by exchange_order_id when client_order_id doesn't match."""
        order = InFlightOrder(
            client_order_id="0xclient2",
            exchange_order_id="77777",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient2"] = order

        order_msg = {
            "order_id": "77777",
            "client_order_id": "unknown",
            "status": "canceled",
            "unix_ms": 1640780002000,
        }
        self.exchange._process_order_message(order_msg)

    async def test_user_stream_order_update_unknown_ignored(self):
        """Unknown client_order_id is silently ignored."""
        order_msg = {
            "order_id": "99999",
            "client_order_id": "0xunknown",
            "status": "open",
        }
        self.exchange._process_order_message(order_msg)
        self.assertTrue(self._is_logged("DEBUG", "Ignoring order message"))

    async def test_user_stream_trade_message(self):
        """Trade message from WS is processed correctly."""
        order = InFlightOrder(
            client_order_id="0xclient3",
            exchange_order_id="88888",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient3"] = order

        trade_msg = {
            "trade_id": "t123",
            "order_id": "88888",
            "client_order_id": "0xclient3",
            "price": "50000",
            "size": "0.5",
            "fee_amount": "0.01",
            "is_buy": True,
            "unix_ms": 1640780003000,
        }
        await self.exchange._process_trade_message(trade_msg)

    async def test_user_stream_trade_message_by_client_id(self):
        """Trade matched by client_order_id when exchange_order_id doesn't match."""
        order = InFlightOrder(
            client_order_id="0xclient4",
            exchange_order_id=None,
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient4"] = order

        trade_msg = {
            "trade_id": "t124",
            "order_id": "unknown_oid",
            "client_order_id": "0xclient4",
            "price": "49000",
            "size": "0.3",
            "fee_amount": "0.005",
            "unix_ms": 1640780004000,
        }
        await self.exchange._process_trade_message(trade_msg)

    async def test_user_stream_trade_message_not_found(self):
        """Trade for unknown order is ignored."""
        trade_msg = {
            "trade_id": "t999",
            "order_id": "nonexistent",
            "client_order_id": "0xnobody",
            "price": "50000",
            "size": "1.0",
        }
        # Should not raise
        await self.exchange._process_trade_message(trade_msg)

    async def test_user_stream_event_listener_processes_order_topic(self):
        """Full event listener integration for order_updates topic."""
        order = InFlightOrder(
            client_order_id="0xclient5",
            exchange_order_id="55555",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient5"] = order

        event = {
            "topic": f"{CONSTANTS.WS_ORDER_UPDATES_TOPIC}:{TRADING_ACCOUNT}",
            "data": {
                "order_id": "55555",
                "client_order_id": "0xclient5",
                "status": "open",
                "unix_ms": 1640780005000,
            },
        }
        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=[event, asyncio.CancelledError()])
        self.exchange._user_stream_tracker = MagicMock()
        self.exchange._user_stream_tracker.user_stream = mock_queue

        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._user_stream_event_listener()

    async def test_user_stream_event_listener_processes_trade_topic(self):
        """Full event listener integration for user_trades topic."""
        order = InFlightOrder(
            client_order_id="0xclient6",
            exchange_order_id="66666",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient6"] = order

        event = {
            "topic": f"{CONSTANTS.WS_USER_TRADES_TOPIC}:{TRADING_ACCOUNT}",
            "data": {
                "trade_id": "t200",
                "order_id": "66666",
                "client_order_id": "0xclient6",
                "price": "50000",
                "size": "0.1",
                "fee_amount": "0.001",
                "unix_ms": 1640780006000,
            },
        }
        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=[event, asyncio.CancelledError()])
        self.exchange._user_stream_tracker = MagicMock()
        self.exchange._user_stream_tracker.user_stream = mock_queue

        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._user_stream_event_listener()

    async def test_user_stream_event_listener_data_list(self):
        """Data as a list of order updates."""
        order = InFlightOrder(
            client_order_id="0xclient7",
            exchange_order_id="77700",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient7"] = order

        event = {
            "topic": f"{CONSTANTS.WS_ORDER_UPDATES_TOPIC}:{TRADING_ACCOUNT}",
            "data": [
                {
                    "order_id": "77700",
                    "client_order_id": "0xclient7",
                    "status": "partially_filled",
                    "unix_ms": 1640780007000,
                }
            ],
        }
        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=[event, asyncio.CancelledError()])
        self.exchange._user_stream_tracker = MagicMock()
        self.exchange._user_stream_tracker.user_stream = mock_queue

        with self.assertRaises(asyncio.CancelledError):
            await self.exchange._user_stream_event_listener()

    # ==================================================================
    # _process_trade_rs_event_message (REST trade fill)
    # ==================================================================

    def test_process_trade_rs_event_message_by_exchange_id(self):
        order = InFlightOrder(
            client_order_id="0xclient8",
            exchange_order_id="88800",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient8"] = order
        all_fillable = self.exchange._order_tracker.all_fillable_orders_by_exchange_order_id

        fill = {
            "trade_id": "tfill1",
            "order_id": "88800",
            "price": "49999",
            "size": "0.5",
            "fee_amount": "0.01",
            "unix_ms": 1640780008000,
        }
        self.exchange._process_trade_rs_event_message(fill, all_fillable)

    def test_process_trade_rs_event_message_by_client_order_id(self):
        order = InFlightOrder(
            client_order_id="0xclient9",
            exchange_order_id="99900",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._order_tracker._in_flight_orders["0xclient9"] = order

        fill = {
            "trade_id": "tfill2",
            "order_id": "wrong_exchange_id",
            "client_order_id": "0xclient9",
            "price": "50100",
            "size": "0.2",
            "fee_amount": "0.005",
            "unix_ms": 1640780009000,
        }
        all_fillable = self.exchange._order_tracker.all_fillable_orders_by_exchange_order_id
        self.exchange._process_trade_rs_event_message(fill, all_fillable)

    def test_process_trade_rs_event_message_unknown_order(self):
        """Fill for unknown order is silently ignored."""
        fill = {
            "trade_id": "tfill3",
            "order_id": "nonexistent",
            "price": "50000",
            "size": "1.0",
        }
        self.exchange._process_trade_rs_event_message(fill, {})

    async def test_all_trade_updates_for_order_returns_empty(self):
        """_all_trade_updates_for_order returns empty list."""
        order = MagicMock()
        result = await self.exchange._all_trade_updates_for_order(order)
        self.assertEqual([], result)

    # ==================================================================
    # Balance update
    # ==================================================================

    async def test_update_balances_dict_response(self):
        self.exchange._api_get = AsyncMock(return_value={
            "perp_equity_balance": "10000.50",
            "usdc_cross_withdrawable_balance": "9500.25",
        })
        await self.exchange._update_balances()
        self.assertEqual(Decimal("10000.50"), self.exchange._account_balances["USDC"])
        self.assertEqual(Decimal("9500.25"), self.exchange._account_available_balances["USDC"])

    async def test_update_balances_list_response(self):
        self.exchange._api_get = AsyncMock(return_value=[{
            "perp_equity_balance": "5000",
            "usdc_cross_withdrawable_balance": "4500",
        }])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("5000"), self.exchange._account_balances["USDC"])

    async def test_update_balances_empty_list(self):
        self.exchange._api_get = AsyncMock(return_value=[])
        await self.exchange._update_balances()
        self.assertEqual(Decimal("0"), self.exchange._account_balances.get("USDC", Decimal("0")))

    async def test_update_balances_no_trading_account(self):
        """When no trading account, skip balance update."""
        self.exchange._auth.trading_account = None
        self.exchange._trading_account = None
        self.exchange._api_get = AsyncMock()
        await self.exchange._update_balances()
        self.exchange._api_get.assert_not_awaited()

    # ==================================================================
    # Position update
    # ==================================================================

    async def test_update_positions_long(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {
                "market_name": EXCHANGE_SYMBOL,
                "market_addr": MARKET_ADDR,
                "size": "1.5",
                "entry_price": "49000",
                "unrealized_pnl": "1500",
                "user_leverage": "10",
            }
        ])
        await self.exchange._update_positions()
        positions = self.exchange._perpetual_trading.account_positions
        self.assertTrue(len(positions) > 0)

    async def test_update_positions_short(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {
                "market_name": EXCHANGE_SYMBOL,
                "market_addr": MARKET_ADDR,
                "size": "-2.0",
                "entry_price": "51000",
                "unrealized_pnl": "-500",
                "user_leverage": "5",
            }
        ])
        await self.exchange._update_positions()
        positions = self.exchange._perpetual_trading.account_positions
        self.assertTrue(len(positions) > 0)

    async def test_update_positions_zero_size_removed(self):
        """Position with size=0 should be removed."""
        self.exchange._api_get = AsyncMock(return_value=[
            {
                "market_name": EXCHANGE_SYMBOL,
                "market_addr": MARKET_ADDR,
                "size": "0",
                "entry_price": "50000",
                "unrealized_pnl": "0",
                "user_leverage": "1",
            }
        ])
        await self.exchange._update_positions()

    async def test_update_positions_empty_clears_all(self):
        """Empty position list clears all tracked positions."""
        self.exchange._api_get = AsyncMock(return_value=[])
        await self.exchange._update_positions()
        positions = self.exchange._perpetual_trading.account_positions
        self.assertEqual(0, len(positions))

    async def test_update_positions_no_trading_account(self):
        """Skip when no trading account."""
        self.exchange._auth.trading_account = None
        self.exchange._trading_account = None
        self.exchange._api_get = AsyncMock()
        await self.exchange._update_positions()
        self.exchange._api_get.assert_not_awaited()

    # ==================================================================
    # Order status request
    # ==================================================================

    async def test_request_order_status_found_in_open_orders(self):
        tracked = InFlightOrder(
            client_order_id="0xclientA",
            exchange_order_id="11111",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._api_get = AsyncMock(return_value=[
            {"order_id": "11111", "client_order_id": "0xclientA", "status": "open", "unix_ms": 1640780010000}
        ])
        result = await self.exchange._request_order_status(tracked)
        self.assertIsInstance(result, OrderUpdate)
        self.assertEqual("0xclientA", result.client_order_id)

    async def test_request_order_status_fallback_to_history(self):
        tracked = InFlightOrder(
            client_order_id="0xclientB",
            exchange_order_id="22222",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        # First call (open_orders) returns nothing, second (order_history) returns the order
        self.exchange._api_get = AsyncMock(side_effect=[
            [],
            [{"order_id": "22222", "client_order_id": "0xclientB", "status": "filled", "unix_ms": 1640780011000}],
        ])
        result = await self.exchange._request_order_status(tracked)
        self.assertEqual("22222", result.exchange_order_id)

    async def test_request_order_status_not_found_raises(self):
        tracked = InFlightOrder(
            client_order_id="0xclientC",
            exchange_order_id="33333",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._api_get = AsyncMock(return_value=[])
        with self.assertRaises(IOError):
            await self.exchange._request_order_status(tracked)

    async def test_request_order_status_no_exchange_id_timeout(self):
        """When exchange_order_id is None and times out, still searches by client_order_id."""
        tracked = InFlightOrder(
            client_order_id="0xclientD",
            trading_pair=TRADING_PAIR,
            order_type=OrderType.LIMIT,
            trade_type=TradeType.BUY,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
            creation_timestamp=1640780000,
            initial_state=OrderState.OPEN,
        )
        self.exchange._api_get = AsyncMock(return_value=[
            {"order_id": "44444", "client_order_id": "0xclientD", "status": "open", "unix_ms": 1640780012000}
        ])
        result = await self.exchange._request_order_status(tracked)
        self.assertEqual("0xclientD", result.client_order_id)

    # ==================================================================
    # Funding
    # ==================================================================

    async def test_fetch_last_fee_payment_success(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {
                "unix_ms": 1640780013000,
                "funding_rate": "0.0001",
                "payment_amount": "1.5",
            }
        ])
        ts, rate, payment = await self.exchange._fetch_last_fee_payment(TRADING_PAIR)
        self.assertGreater(ts, 0)
        self.assertEqual(Decimal("0.0001"), rate)
        self.assertEqual(Decimal("1.5"), payment)

    async def test_fetch_last_fee_payment_empty(self):
        self.exchange._api_get = AsyncMock(return_value=[])
        ts, rate, payment = await self.exchange._fetch_last_fee_payment(TRADING_PAIR)
        self.assertEqual(0, ts)
        self.assertEqual(Decimal("-1"), rate)

    async def test_fetch_last_fee_payment_zero_payment(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {"unix_ms": 1640780014000, "funding_rate": "0.0002", "payment_amount": "0"}
        ])
        ts, rate, payment = await self.exchange._fetch_last_fee_payment(TRADING_PAIR)
        self.assertEqual(0, ts)
        self.assertEqual(Decimal("-1"), rate)

    async def test_fetch_last_fee_payment_error(self):
        self.exchange._api_get = AsyncMock(side_effect=Exception("network error"))
        ts, rate, payment = await self.exchange._fetch_last_fee_payment(TRADING_PAIR)
        self.assertEqual(0, ts)

    # ==================================================================
    # Last traded price
    # ==================================================================

    async def test_get_last_traded_price_from_market_addr(self):
        self.exchange._api_get = AsyncMock(return_value=[{"mark_px": "50500.5"}])
        price = await self.exchange._get_last_traded_price(TRADING_PAIR)
        self.assertEqual(50500.5, price)

    async def test_get_last_traded_price_dict_response(self):
        self.exchange._api_get = AsyncMock(return_value={"mark_px": "49000"})
        price = await self.exchange._get_last_traded_price(TRADING_PAIR)
        self.assertEqual(49000.0, price)

    async def test_get_last_traded_price_fallback_all_prices(self):
        """When first call fails, falls back to all prices."""
        call_count = 0

        async def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("first call fails")
            return [{"market_addr": MARKET_ADDR, "mark_px": "48000"}]

        self.exchange._api_get = AsyncMock(side_effect=side_effect)
        price = await self.exchange._get_last_traded_price(TRADING_PAIR)
        self.assertEqual(48000.0, price)

    async def test_get_last_traded_price_not_found_raises(self):
        self.exchange._api_get = AsyncMock(side_effect=Exception("fail"))
        with self.assertRaises(RuntimeError):
            await self.exchange._get_last_traded_price(TRADING_PAIR)

    async def test_get_last_traded_price_unknown_pair(self):
        """When trading pair not in symbol map, tries fallback."""
        self.exchange._set_trading_pair_symbol_map(bidict())
        self.exchange._api_get = AsyncMock(side_effect=Exception("fail"))
        with self.assertRaises(RuntimeError):
            await self.exchange._get_last_traded_price(TRADING_PAIR)

    # ==================================================================
    # Fee calculation
    # ==================================================================

    def test_get_fee(self):
        fee = self.exchange._get_fee(
            base_currency="BTC-PERP",
            quote_currency="USDC",
            order_type=OrderType.LIMIT,
            order_side=TradeType.BUY,
            position_action=PositionAction.OPEN,
            amount=Decimal("1.0"),
            price=Decimal("50000"),
        )
        self.assertIsNotNone(fee)

    # ==================================================================
    # Position mode / leverage
    # ==================================================================

    async def test_get_position_mode(self):
        mode = await self.exchange._get_position_mode()
        self.assertEqual(PositionMode.ONEWAY, mode)

    async def test_trading_pair_position_mode_set_oneway(self):
        success, msg = await self.exchange._trading_pair_position_mode_set(PositionMode.ONEWAY, TRADING_PAIR)
        self.assertTrue(success)
        self.assertEqual("", msg)

    async def test_trading_pair_position_mode_set_hedge_fails(self):
        success, msg = await self.exchange._trading_pair_position_mode_set(PositionMode.HEDGE, TRADING_PAIR)
        self.assertFalse(success)
        self.assertIn("ONEWAY", msg)

    async def test_set_trading_pair_leverage(self):
        success, msg = await self.exchange._set_trading_pair_leverage(TRADING_PAIR, 10)
        self.assertTrue(success)

    # ==================================================================
    # Collateral tokens
    # ==================================================================

    def test_get_buy_collateral_token(self):
        token = self.exchange.get_buy_collateral_token(TRADING_PAIR)
        self.assertEqual("USDC", token)

    def test_get_sell_collateral_token(self):
        token = self.exchange.get_sell_collateral_token(TRADING_PAIR)
        self.assertEqual("USDC", token)

    # ==================================================================
    # Miscellaneous
    # ==================================================================

    def test_is_request_exception_related_to_time_synchronizer(self):
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(Exception()))

    def test_is_order_not_found_during_status_update(self):
        exc = Exception(f"Some error about {CONSTANTS.ORDER_NOT_EXIST_MESSAGE}")
        self.assertTrue(self.exchange._is_order_not_found_during_status_update_error(exc))

    def test_is_order_not_found_during_cancellation(self):
        exc = Exception(CONSTANTS.UNKNOWN_ORDER_MESSAGE)
        self.assertTrue(self.exchange._is_order_not_found_during_cancelation_error(exc))

    async def test_get_all_pairs_prices(self):
        self.exchange._api_get = AsyncMock(return_value=[
            {"market_addr": MARKET_ADDR, "mark_px": "50000"},
        ])
        result = await self.exchange.get_all_pairs_prices()
        self.assertEqual(1, len(result))
        self.assertEqual(EXCHANGE_SYMBOL, result[0]["symbol"])
        self.assertEqual("50000", result[0]["price"])

    async def test_get_all_pairs_prices_error(self):
        self.exchange._api_get = AsyncMock(side_effect=Exception("fail"))
        result = await self.exchange.get_all_pairs_prices()
        self.assertEqual([], result)

    def test_funding_fee_poll_interval(self):
        self.assertEqual(120, self.exchange.funding_fee_poll_interval)

    def test_client_order_id_max_length(self):
        self.assertIsNone(self.exchange.client_order_id_max_length)

    def test_client_order_id_prefix(self):
        self.assertEqual(CONSTANTS.BROKER_ID, self.exchange.client_order_id_prefix)

    async def test_make_network_check_request(self):
        self.exchange._api_get = AsyncMock(return_value={})
        await self.exchange._make_network_check_request()
        self.exchange._api_get.assert_awaited_once()

    async def test_update_trading_fees_noop(self):
        """_update_trading_fees is a no-op."""
        await self.exchange._update_trading_fees()

    def test_trading_rules_request_path(self):
        self.assertEqual(CONSTANTS.MARKETS_URL, self.exchange.trading_rules_request_path)

    def test_trading_pairs_request_path(self):
        self.assertEqual(CONSTANTS.MARKETS_URL, self.exchange.trading_pairs_request_path)

    def test_check_network_request_path(self):
        self.assertEqual(CONSTANTS.MARKETS_URL, self.exchange.check_network_request_path)
