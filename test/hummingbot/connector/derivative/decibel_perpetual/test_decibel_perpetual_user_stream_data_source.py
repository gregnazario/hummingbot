import asyncio
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_user_stream_data_source import (
    DecibelPerpetualUserStreamDataSource,
)
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest

API_KEY = "test_api_key"
SECRET_KEY = "0xdeadbeef"
TRADING_ACCOUNT = "0xsubaccount1234"
TRADING_PAIR = "BTC-USDC"
DOMAIN = CONSTANTS.TESTNET_DOMAIN


class TestDecibelPerpetualUserStreamDataSource(IsolatedAsyncioWrapperTestCase):
    level = 0

    def setUp(self) -> None:
        super().setUp()
        self.log_records = []

        self.auth = DecibelPerpetualAuth(
            api_key=API_KEY,
            secret_key=SECRET_KEY,
            trading_account=TRADING_ACCOUNT,
        )
        self.mock_connector = MagicMock()
        self.mock_api_factory = MagicMock()

        self.data_source = DecibelPerpetualUserStreamDataSource(
            auth=self.auth,
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
    # last_recv_time property
    # ------------------------------------------------------------------

    def test_last_recv_time_returns_zero_when_no_ws(self):
        self.assertEqual(0, self.data_source.last_recv_time)

    def test_last_recv_time_returns_ws_value(self):
        mock_ws = MagicMock()
        mock_ws.last_recv_time = 12345.0
        self.data_source._ws_assistant = mock_ws
        self.assertEqual(12345.0, self.data_source.last_recv_time)

    # ------------------------------------------------------------------
    # _get_ws_assistant
    # ------------------------------------------------------------------

    async def test_get_ws_assistant_creates_once(self):
        mock_ws = AsyncMock()
        self.mock_api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)

        ws1 = await self.data_source._get_ws_assistant()
        ws2 = await self.data_source._get_ws_assistant()

        self.assertIs(ws1, ws2)
        self.mock_api_factory.get_ws_assistant.assert_awaited_once()

    # ------------------------------------------------------------------
    # _connected_websocket_assistant
    # ------------------------------------------------------------------

    async def test_connected_websocket_assistant_connects_with_auth_headers(self):
        mock_ws = AsyncMock()
        self.mock_api_factory.get_ws_assistant = AsyncMock(return_value=mock_ws)

        ws = await self.data_source._connected_websocket_assistant()

        self.assertIs(ws, mock_ws)
        mock_ws.connect.assert_awaited_once()
        call_kwargs = mock_ws.connect.call_args[1]
        self.assertIn("wss://", call_kwargs["ws_url"])
        self.assertEqual(CONSTANTS.HEARTBEAT_TIME_INTERVAL, call_kwargs["ping_timeout"])
        # Auth headers should contain the API key
        ws_headers = call_kwargs["ws_headers"]
        self.assertIn(API_KEY, ws_headers.get("Sec-Websocket-Protocol", ""))

    # ------------------------------------------------------------------
    # _subscribe_channels
    # ------------------------------------------------------------------

    async def test_subscribe_channels_sends_order_and_trade_topics(self):
        mock_ws = AsyncMock()
        sent_payloads = []
        mock_ws.send = AsyncMock(side_effect=lambda req: sent_payloads.append(req.payload))

        await self.data_source._subscribe_channels(mock_ws)

        self.assertEqual(2, len(sent_payloads))

        order_payload = sent_payloads[0]
        self.assertEqual("subscribe", order_payload["method"])
        self.assertTrue(order_payload["topic"].startswith(CONSTANTS.WS_ORDER_UPDATES_TOPIC))
        self.assertIn(TRADING_ACCOUNT, order_payload["topic"])

        trades_payload = sent_payloads[1]
        self.assertEqual("subscribe", trades_payload["method"])
        self.assertTrue(trades_payload["topic"].startswith(CONSTANTS.WS_USER_TRADES_TOPIC))
        self.assertIn(TRADING_ACCOUNT, trades_payload["topic"])

        self.assertTrue(self._is_logged("INFO", "Subscribed to private order updates"))

    async def test_subscribe_channels_raises_on_exception(self):
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(side_effect=RuntimeError("connection lost"))

        with self.assertRaises(RuntimeError):
            await self.data_source._subscribe_channels(mock_ws)

    async def test_subscribe_channels_re_raises_cancelled_error(self):
        mock_ws = AsyncMock()
        mock_ws.send = AsyncMock(side_effect=asyncio.CancelledError)

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source._subscribe_channels(mock_ws)

    # ------------------------------------------------------------------
    # _process_event_message
    # ------------------------------------------------------------------

    async def test_process_event_message_enqueues_order_updates(self):
        queue = asyncio.Queue()
        msg = {
            "topic": f"{CONSTANTS.WS_ORDER_UPDATES_TOPIC}:{TRADING_ACCOUNT}",
            "data": {"order_id": "123", "status": "open"},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertFalse(queue.empty())
        self.assertEqual(msg, queue.get_nowait())

    async def test_process_event_message_enqueues_user_trades(self):
        queue = asyncio.Queue()
        msg = {
            "topic": f"{CONSTANTS.WS_USER_TRADES_TOPIC}:{TRADING_ACCOUNT}",
            "data": {"trade_id": "t1", "price": "50000"},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertFalse(queue.empty())
        self.assertEqual(msg, queue.get_nowait())

    async def test_process_event_message_ignores_unrelated_topics(self):
        queue = asyncio.Queue()
        msg = {
            "topic": "depth:0xmarket123",
            "data": {"bids": [], "asks": []},
        }
        await self.data_source._process_event_message(msg, queue)
        self.assertTrue(queue.empty())

    async def test_process_event_message_raises_on_error(self):
        queue = asyncio.Queue()
        msg = {"error": {"message": "auth failed"}}

        with self.assertRaises(IOError):
            await self.data_source._process_event_message(msg, queue)

    async def test_process_event_message_raises_on_string_error(self):
        queue = asyncio.Queue()
        msg = {"error": "something went wrong"}

        with self.assertRaises(IOError):
            await self.data_source._process_event_message(msg, queue)

    # ------------------------------------------------------------------
    # _process_websocket_messages (ping on timeout)
    # ------------------------------------------------------------------

    async def test_process_websocket_messages_sends_ping_on_timeout(self):
        mock_ws = AsyncMock()

        call_count = 0

        async def fake_super_process(self_arg, websocket_assistant, queue):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise asyncio.TimeoutError
            else:
                raise asyncio.CancelledError

        with patch.object(
            DecibelPerpetualUserStreamDataSource.__bases__[0],
            "_process_websocket_messages",
            new=fake_super_process,
        ):
            queue = asyncio.Queue()
            with self.assertRaises(asyncio.CancelledError):
                await self.data_source._process_websocket_messages(mock_ws, queue)

            mock_ws.send.assert_awaited_once()
            sent_req = mock_ws.send.call_args[0][0]
            self.assertEqual({"method": "ping"}, sent_req.payload)
