import asyncio
import time
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_api_order_book_data_source import (
    DecibelPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType

API_KEY = "test_api_key"
SECRET_KEY = "0xdeadbeef"
TRADING_PAIR = "BTC-PERP-USDC"
EXCHANGE_SYMBOL = "BTC-PERP"
MARKET_ADDR = "0xabc123def456"
TRADING_ACCOUNT = "0xsubaccount1234"
DOMAIN = CONSTANTS.TESTNET_DOMAIN


class TestDecibelPerpetualAPIOrderBookDataSource(IsolatedAsyncioWrapperTestCase):
    level = 0

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.mock_connector = MagicMock()
        self.mock_connector._auth = DecibelPerpetualAuth(
            api_key=API_KEY,
            secret_key=SECRET_KEY,
            trading_account=TRADING_ACCOUNT,
        )
        self.mock_connector.market_name_to_addr = {EXCHANGE_SYMBOL: MARKET_ADDR}
        self.mock_connector.addr_to_market_name = {MARKET_ADDR: EXCHANGE_SYMBOL}
        self.mock_connector.exchange_symbol_associated_to_pair = AsyncMock(return_value=EXCHANGE_SYMBOL)
        self.mock_connector.trading_pair_associated_to_exchange_symbol = AsyncMock(return_value=TRADING_PAIR)

        self.mock_api_factory = MagicMock()

        self.data_source = DecibelPerpetualAPIOrderBookDataSource(
            trading_pairs=[TRADING_PAIR],
            connector=self.mock_connector,
            api_factory=self.mock_api_factory,
            domain=DOMAIN,
        )

        self.data_source.logger().setLevel(1)
        self.data_source.logger().addHandler(self)

    def handle(self, record):
        self.log_records.append(record)

    def _is_logged(self, log_level: str, message: str) -> bool:
        return any(
            record.levelname == log_level and message in record.getMessage()
            for record in self.log_records
        )

    # ------------------------------------------------------------------
    # get_last_traded_prices
    # ------------------------------------------------------------------

    async def test_get_last_traded_prices_delegates_to_connector(self):
        expected = {TRADING_PAIR: 50000.0}
        self.mock_connector.get_last_traded_prices = AsyncMock(return_value=expected)
        result = await self.data_source.get_last_traded_prices([TRADING_PAIR])
        self.assertEqual(expected, result)

    # ------------------------------------------------------------------
    # REST order book snapshot
    # ------------------------------------------------------------------

    async def test_request_order_book_snapshot_from_prices_dict(self):
        """Prices endpoint returns a dict — should build synthetic snapshot."""
        self.mock_connector._api_get = AsyncMock(return_value={"mark_px": "50000.5"})
        # Remove _request_orderbook_snapshot so fallback is used
        if hasattr(self.mock_connector, "_request_orderbook_snapshot"):
            delattr(self.mock_connector, "_request_orderbook_snapshot")

        result = await self.data_source._request_order_book_snapshot(TRADING_PAIR)
        self.assertEqual(TRADING_PAIR, result["trading_pair"])
        self.assertEqual([[50000.5, 0.0]], result["bids"])
        self.assertEqual([[50000.5, 0.0]], result["asks"])

    async def test_request_order_book_snapshot_from_prices_list(self):
        """Prices endpoint returns a list with one element."""
        self.mock_connector._api_get = AsyncMock(return_value=[{"mark_px": "42000.0"}])
        if hasattr(self.mock_connector, "_request_orderbook_snapshot"):
            delattr(self.mock_connector, "_request_orderbook_snapshot")

        result = await self.data_source._request_order_book_snapshot(TRADING_PAIR)
        self.assertEqual(42000.0, result["bids"][0][0])

    async def test_request_order_book_snapshot_empty_list(self):
        """Prices endpoint returns an empty list — empty bids/asks."""
        self.mock_connector._api_get = AsyncMock(return_value=[])
        if hasattr(self.mock_connector, "_request_orderbook_snapshot"):
            delattr(self.mock_connector, "_request_orderbook_snapshot")

        result = await self.data_source._request_order_book_snapshot(TRADING_PAIR)
        self.assertEqual([], result["bids"])
        self.assertEqual([], result["asks"])

    async def test_request_order_book_snapshot_uses_connector_helper(self):
        """If connector has _request_orderbook_snapshot, use it."""
        expected = {"trading_pair": TRADING_PAIR, "bids": [[1, 2]], "asks": [[3, 4]], "timestamp": 100}
        self.mock_connector._request_orderbook_snapshot = AsyncMock(return_value=expected)

        result = await self.data_source._request_order_book_snapshot(TRADING_PAIR)
        self.assertEqual(expected, result)

    async def test_order_book_snapshot_returns_message(self):
        self.mock_connector._api_get = AsyncMock(return_value={"mark_px": "50000"})
        if hasattr(self.mock_connector, "_request_orderbook_snapshot"):
            delattr(self.mock_connector, "_request_orderbook_snapshot")

        msg = await self.data_source._order_book_snapshot(TRADING_PAIR)
        self.assertIsInstance(msg, OrderBookMessage)
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)

    # ------------------------------------------------------------------
    # WebSocket connection
    # ------------------------------------------------------------------

    async def test_connected_websocket_assistant_with_auth(self):
        mock_ws = AsyncMock()
        self.mock_api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)

        ws = await self.data_source._connected_websocket_assistant()
        self.assertIs(ws, mock_ws)
        call_kwargs = mock_ws.connect.call_args[1]
        self.assertIn("wss://", call_kwargs["ws_url"])
        self.assertIn(API_KEY, call_kwargs["ws_headers"].get("Sec-Websocket-Protocol", ""))

    async def test_connected_websocket_assistant_without_auth(self):
        self.mock_connector._auth = None
        mock_ws = AsyncMock()
        self.mock_api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)

        ws = await self.data_source._connected_websocket_assistant()
        call_kwargs = mock_ws.connect.call_args[1]
        self.assertEqual({}, call_kwargs["ws_headers"])

    # ------------------------------------------------------------------
    # _subscribe_channels
    # ------------------------------------------------------------------

    async def test_subscribe_channels_sends_depth_trades_market_price(self):
        mock_ws = AsyncMock()
        sent_payloads = []
        mock_ws.send = AsyncMock(side_effect=lambda req: sent_payloads.append(req.payload))

        await self.data_source._subscribe_channels(mock_ws)

        # 3 subscriptions per trading pair
        self.assertEqual(3, len(sent_payloads))
        topics = [p["topic"] for p in sent_payloads]
        self.assertTrue(any(t.startswith(CONSTANTS.WS_DEPTH_TOPIC) for t in topics))
        self.assertTrue(any(t.startswith(CONSTANTS.WS_TRADES_TOPIC) for t in topics))
        self.assertTrue(any(t.startswith(CONSTANTS.WS_MARKET_PRICE_TOPIC) for t in topics))
        for t in topics:
            self.assertIn(MARKET_ADDR, t)

    async def test_subscribe_channels_raises_on_error(self):
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(side_effect=RuntimeError("ws error"))
        with self.assertRaises(RuntimeError):
            await self.data_source._subscribe_channels(mock_ws)

    # ------------------------------------------------------------------
    # _channel_originating_message
    # ------------------------------------------------------------------

    def test_channel_routing_depth(self):
        msg = {"topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{MARKET_ADDR}"}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._snapshot_messages_queue_key, channel)

    def test_channel_routing_trades(self):
        msg = {"topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{MARKET_ADDR}"}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._trade_messages_queue_key, channel)

    def test_channel_routing_market_price(self):
        msg = {"topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{MARKET_ADDR}"}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual(self.data_source._funding_info_messages_queue_key, channel)

    def test_channel_routing_unknown_returns_empty(self):
        msg = {"topic": "unknown:0x123"}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual("", channel)

    def test_channel_routing_empty_topic_returns_empty(self):
        msg = {}
        channel = self.data_source._channel_originating_message(msg)
        self.assertEqual("", channel)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def test_extract_market_addr_from_topic(self):
        topic = f"depth:{MARKET_ADDR}"
        result = self.data_source._extract_market_addr_from_topic(topic)
        self.assertEqual(MARKET_ADDR, result)

    def test_extract_market_addr_from_topic_no_colon(self):
        result = self.data_source._extract_market_addr_from_topic("depth")
        self.assertEqual("", result)

    async def test_market_addr_to_trading_pair(self):
        result = await self.data_source._market_addr_to_trading_pair(MARKET_ADDR)
        self.assertEqual(TRADING_PAIR, result)

    async def test_trading_pair_to_market_addr(self):
        result = await self.data_source._trading_pair_to_market_addr(TRADING_PAIR)
        self.assertEqual(MARKET_ADDR, result)

    async def test_trading_pair_to_market_addr_fallback(self):
        """When market_name_to_addr doesn't have the symbol, return symbol directly."""
        self.mock_connector.market_name_to_addr = {}
        result = await self.data_source._trading_pair_to_market_addr(TRADING_PAIR)
        self.assertEqual(EXCHANGE_SYMBOL, result)

    # ------------------------------------------------------------------
    # _parse_order_book_diff_message
    # ------------------------------------------------------------------

    async def test_parse_order_book_diff_message(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{MARKET_ADDR}",
            "data": {
                "bids": [{"price": "50000.5", "size": "1.2"}],
                "asks": [{"price": "50001.0", "size": "0.8"}],
                "timestamp": 1700000000000,
            },
        }
        await self.data_source._parse_order_book_diff_message(raw, queue)
        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.DIFF, msg.type)
        self.assertEqual(TRADING_PAIR, msg.trading_pair)
        self.assertEqual(1, len(msg.bids))
        self.assertAlmostEqual(50000.5, msg.bids[0].price)
        self.assertAlmostEqual(1.2, msg.bids[0].amount)
        self.assertEqual(1, len(msg.asks))
        self.assertAlmostEqual(50001.0, msg.asks[0].price)
        self.assertAlmostEqual(0.8, msg.asks[0].amount)

    async def test_parse_order_book_diff_empty_levels(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{MARKET_ADDR}",
            "data": {"timestamp": 1700000000000},
        }
        await self.data_source._parse_order_book_diff_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual([], msg.bids)
        self.assertEqual([], msg.asks)

    # ------------------------------------------------------------------
    # _parse_order_book_snapshot_message
    # ------------------------------------------------------------------

    async def test_parse_order_book_snapshot_message(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{MARKET_ADDR}",
            "data": {
                "bids": [{"price": "49000", "size": "2.0"}],
                "asks": [{"price": "51000", "size": "3.0"}],
                "timestamp": 1700000000000,
            },
        }
        await self.data_source._parse_order_book_snapshot_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.SNAPSHOT, msg.type)

    # ------------------------------------------------------------------
    # _parse_trade_message
    # ------------------------------------------------------------------

    async def test_parse_trade_message_list(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{MARKET_ADDR}",
            "data": [
                {"price": "50000", "size": "0.1", "side": "buy", "timestamp": 1700000000000, "trade_id": "t1"},
                {"price": "49999", "size": "0.2", "side": "sell", "timestamp": 1700000000001, "trade_id": "t2"},
            ],
        }
        await self.data_source._parse_trade_message(raw, queue)
        self.assertEqual(2, queue.qsize())

        msg1: OrderBookMessage = queue.get_nowait()
        self.assertEqual(OrderBookMessageType.TRADE, msg1.type)
        self.assertEqual("t1", msg1.trade_id)

    async def test_parse_trade_message_single_dict(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{MARKET_ADDR}",
            "data": {"price": "50000", "size": "0.5", "side": "sell", "timestamp": 1700000000000, "trade_id": "t3"},
        }
        await self.data_source._parse_trade_message(raw, queue)
        self.assertEqual(1, queue.qsize())
        msg: OrderBookMessage = queue.get_nowait()
        self.assertEqual("t3", msg.trade_id)

    async def test_parse_trade_message_fallback_trade_id(self):
        """When trade_id missing, falls back to timestamp string."""
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{MARKET_ADDR}",
            "data": [{"price": "10", "size": "1", "side": "buy", "timestamp": 9999}],
        }
        await self.data_source._parse_trade_message(raw, queue)
        msg = queue.get_nowait()
        self.assertEqual("9999", msg.trade_id)

    # ------------------------------------------------------------------
    # _parse_funding_info_message
    # ------------------------------------------------------------------

    async def test_parse_funding_info_message(self):
        queue = asyncio.Queue()
        raw = {
            "topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{MARKET_ADDR}",
            "data": {
                "oracle_px": "50000.0",
                "mark_px": "50010.0",
                "funding_rate_bps": "1.5",
                "timestamp": 1700000000000,
            },
        }
        await self.data_source._parse_funding_info_message(raw, queue)
        info: FundingInfoUpdate = queue.get_nowait()
        self.assertEqual(TRADING_PAIR, info.trading_pair)
        self.assertEqual(Decimal("50000.0"), info.index_price)
        self.assertEqual(Decimal("50010.0"), info.mark_price)
        # 1.5 bps / 10000 = 0.00015
        self.assertEqual(Decimal("1.5") / Decimal("10000"), info.rate)

    async def test_parse_funding_info_message_skips_untracked_pair(self):
        """If the trading pair is not in _trading_pairs, the message is ignored."""
        queue = asyncio.Queue()
        # Make the market_addr_to_trading_pair return a different pair
        self.mock_connector.trading_pair_associated_to_exchange_symbol = AsyncMock(
            return_value="ETH-PERP-USDC"
        )
        raw = {
            "topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{MARKET_ADDR}",
            "data": {"oracle_px": "3000", "mark_px": "3001", "funding_rate_bps": "0.5"},
        }
        await self.data_source._parse_funding_info_message(raw, queue)
        self.assertTrue(queue.empty())

    # ------------------------------------------------------------------
    # get_funding_info (REST)
    # ------------------------------------------------------------------

    async def test_get_funding_info_returns_info(self):
        self.mock_connector._api_get = AsyncMock(return_value={
            "oracle_px": "50000",
            "mark_px": "50100",
            "funding_rate_bps": "2.0",
        })

        info = await self.data_source.get_funding_info(TRADING_PAIR)
        self.assertIsInstance(info, FundingInfo)
        self.assertEqual(Decimal("50000"), info.index_price)
        self.assertEqual(Decimal("50100"), info.mark_price)
        self.assertEqual(Decimal("2.0") / Decimal("10000"), info.rate)

    async def test_get_funding_info_returns_zero_on_none(self):
        """If _request_complete_funding_info returns None, get zero fallback."""
        self.mock_connector._api_get = AsyncMock(side_effect=Exception("network error"))

        info = await self.data_source.get_funding_info(TRADING_PAIR)
        self.assertEqual(Decimal("0"), info.index_price)

    # ------------------------------------------------------------------
    # _request_complete_funding_info
    # ------------------------------------------------------------------

    async def test_request_complete_funding_info_list_response(self):
        self.mock_connector._api_get = AsyncMock(return_value=[
            {"market_addr": MARKET_ADDR, "oracle_px": "50000", "mark_px": "50100", "funding_rate_bps": "1"},
            {"market_addr": "0xother", "oracle_px": "3000", "mark_px": "3001", "funding_rate_bps": "0"},
        ])
        result = await self.data_source._request_complete_funding_info(TRADING_PAIR)
        self.assertEqual(MARKET_ADDR, result["market_addr"])

    async def test_request_complete_funding_info_dict_response(self):
        expected = {"oracle_px": "50000", "mark_px": "50100"}
        self.mock_connector._api_get = AsyncMock(return_value=expected)
        result = await self.data_source._request_complete_funding_info(TRADING_PAIR)
        self.assertEqual(expected, result)

    # ------------------------------------------------------------------
    # _next_funding_time
    # ------------------------------------------------------------------

    def test_next_funding_time_is_next_hour(self):
        now = time.time()
        next_hour = int(((now // 3600) + 1) * 3600)
        result = self.data_source._next_funding_time()
        self.assertEqual(next_hour, result)

    # ------------------------------------------------------------------
    # Dynamic subscribe / unsubscribe
    # ------------------------------------------------------------------

    async def test_subscribe_to_trading_pair_no_ws(self):
        self.data_source._ws_assistant = None
        result = await self.data_source.subscribe_to_trading_pair(TRADING_PAIR)
        self.assertFalse(result)

    async def test_subscribe_to_trading_pair_success(self):
        mock_ws = AsyncMock()
        sent_payloads = []
        mock_ws.send = AsyncMock(side_effect=lambda req: sent_payloads.append(req.payload))
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(TRADING_PAIR)
        self.assertTrue(result)
        self.assertEqual(3, len(sent_payloads))

    async def test_unsubscribe_from_trading_pair_no_ws(self):
        self.data_source._ws_assistant = None
        result = await self.data_source.unsubscribe_from_trading_pair(TRADING_PAIR)
        self.assertFalse(result)

    async def test_unsubscribe_from_trading_pair_success(self):
        mock_ws = AsyncMock()
        sent_payloads = []
        mock_ws.send = AsyncMock(side_effect=lambda req: sent_payloads.append(req.payload))
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.unsubscribe_from_trading_pair(TRADING_PAIR)
        self.assertTrue(result)
        self.assertEqual(3, len(sent_payloads))
        for p in sent_payloads:
            self.assertEqual("unsubscribe", p["method"])

    async def test_subscribe_to_trading_pair_error(self):
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(side_effect=RuntimeError("fail"))
        self.data_source._ws_assistant = mock_ws

        result = await self.data_source.subscribe_to_trading_pair(TRADING_PAIR)
        self.assertFalse(result)
