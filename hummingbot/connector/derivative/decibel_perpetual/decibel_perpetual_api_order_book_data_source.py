import asyncio
import time
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_web_utils as web_utils
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_derivative import (
        DecibelPerpetualDerivative,
    )


class DecibelPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    _bpobds_logger: Optional[HummingbotLogger] = None
    _trading_pair_symbol_map: Dict[str, Dict[str, str]] = {}
    _mapping_initialization_lock = asyncio.Lock()

    def __init__(
            self,
            trading_pairs: List[str],
            connector: 'DecibelPerpetualDerivative',
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN
    ):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._domain = domain
        self._trading_pairs: List[str] = trading_pairs
        self._message_queue: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self._funding_info_messages_queue_key = "funding_info"
        self._snapshot_messages_queue_key = "order_book_snapshot"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def get_last_traded_prices(
            self,
            trading_pairs: List[str],
            domain: Optional[str] = None
    ) -> Dict[str, float]:
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        """
        Fetch current funding info for a trading pair from the REST API prices endpoint.

        The Decibel ``GET /api/v1/prices`` endpoint returns an array of market price
        objects, each containing ``oracle_px``, ``mark_px``, and ``funding_rate_bps``
        keyed by market address.
        """
        funding_data = await self._request_complete_funding_info(trading_pair)

        if funding_data is not None:
            return FundingInfo(
                trading_pair=trading_pair,
                index_price=Decimal(str(funding_data.get("oracle_px", "0"))),
                mark_price=Decimal(str(funding_data.get("mark_px", "0"))),
                next_funding_utc_timestamp=self._next_funding_time(),
                rate=Decimal(str(funding_data.get("funding_rate_bps", "0"))) / Decimal("10000"),
            )

        # Fallback: return zero-value placeholder
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=Decimal("0"),
            mark_price=Decimal("0"),
            next_funding_utc_timestamp=self._next_funding_time(),
            rate=Decimal("0"),
        )

    async def listen_for_funding_info(self, output: asyncio.Queue):
        """
        Reads the funding info events from the WebSocket queue and publishes
        ``FundingInfoUpdate`` objects to the output queue.
        """
        message_queue = self._message_queue[self._funding_info_messages_queue_key]
        while True:
            try:
                funding_info_event = await message_queue.get()
                await self._parse_funding_info_message(funding_info_event, output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception(
                    "Unexpected error when processing public funding info updates from exchange"
                )
                await self._sleep(5)

    # ------------------------------------------------------------------
    # REST order book snapshot
    # ------------------------------------------------------------------

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Request an order book snapshot via REST.

        Decibel provides ``GET /api/v1/trades?market_addr=<addr>`` for recent trades
        but has no dedicated REST orderbook endpoint.  We use the
        ``GET /api/v1/prices?market_addr=<addr>`` endpoint to seed a minimal book
        from the last traded / mark prices.  The full depth will be populated once
        the WebSocket ``depth`` subscription delivers the initial snapshot.

        If the connector exposes a ``_request_orderbook_snapshot`` helper we prefer
        that; otherwise we fall back to the prices endpoint.
        """
        market_addr = await self._trading_pair_to_market_addr(trading_pair)

        # Try connector-level REST orderbook if available (may be added by the
        # derivative connector for convenience).
        if hasattr(self._connector, "_request_orderbook_snapshot"):
            return await self._connector._request_orderbook_snapshot(market_addr)

        # Fallback: build a minimal snapshot from the prices endpoint so that
        # ``get_new_order_book`` can return without waiting for the WS.
        params = {"market_addr": market_addr}
        data = await self._connector._api_get(
            path_url=CONSTANTS.PRICES_URL,
            params=params,
        )

        # The prices response is expected to be a dict (or list with one element)
        # containing at least ``mark_px``.
        if isinstance(data, list):
            data = data[0] if data else {}

        mark_px = float(data.get("mark_px", 0))
        timestamp = time.time()

        # Return a synthetic snapshot with empty levels -- the WS depth channel
        # will send a proper full snapshot shortly after connection.
        return {
            "trading_pair": trading_pair,
            "timestamp": timestamp,
            "bids": [[mark_px, 0.0]] if mark_px else [],
            "asks": [[mark_px, 0.0]] if mark_px else [],
        }

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot_response: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp = snapshot_response.get("timestamp", time.time())
        update_id = int(snapshot_timestamp * 1e3)

        snapshot_msg: OrderBookMessage = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": snapshot_response.get("trading_pair", trading_pair),
                "update_id": update_id,
                "bids": snapshot_response.get("bids", []),
                "asks": snapshot_response.get("asks", []),
            },
            timestamp=snapshot_timestamp,
        )
        return snapshot_msg

    # ------------------------------------------------------------------
    # WebSocket connection & subscriptions
    # ------------------------------------------------------------------

    async def _connected_websocket_assistant(self) -> WSAssistant:
        url = web_utils.wss_url(self._domain)
        ws: WSAssistant = await self._api_factory.get_ws_assistant()

        # Decibel WS requires authentication via Sec-Websocket-Protocol header.
        ws_headers = {}
        if hasattr(self._connector, "_auth") and self._connector._auth is not None:
            ws_headers = self._connector._auth.get_ws_auth_headers()

        await ws.connect(
            ws_url=url,
            ping_timeout=CONSTANTS.HEARTBEAT_TIME_INTERVAL,
            ws_headers=ws_headers,
        )
        return ws

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribe to depth, trades, and market_price channels for every
        tracked trading pair.

        Decibel subscription format::

            {"method": "subscribe", "topic": "depth:<marketAddr>"}
            {"method": "subscribe", "topic": "trades:<marketAddr>"}
            {"method": "subscribe", "topic": "market_price:<marketAddr>"}
        """
        try:
            for trading_pair in self._trading_pairs:
                market_addr = await self._trading_pair_to_market_addr(trading_pair)

                # Depth (order book)
                depth_payload = {
                    "method": "subscribe",
                    "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{market_addr}",
                }
                await ws.send(WSJSONRequest(payload=depth_payload))

                # Trades
                trades_payload = {
                    "method": "subscribe",
                    "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{market_addr}",
                }
                await ws.send(WSJSONRequest(payload=trades_payload))

                # Market price / funding info
                market_price_payload = {
                    "method": "subscribe",
                    "topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{market_addr}",
                }
                await ws.send(WSJSONRequest(payload=market_price_payload))

                self.logger().info(
                    f"Subscribed to public order book, trade, and funding info channels "
                    f"for {trading_pair}..."
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book data streams."
            )
            raise

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        """
        Map an incoming WS event to the internal message queue key.

        Decibel WS messages contain a ``topic`` field with format
        ``<channel>:<marketAddr>``, e.g. ``depth:0xabc...``.
        """
        channel = ""
        topic: str = event_message.get("topic", "")

        if not topic:
            # Possibly a subscription confirmation or heartbeat -- ignore.
            return channel

        if topic.startswith(CONSTANTS.WS_DEPTH_TOPIC + ":"):
            channel = self._snapshot_messages_queue_key
        elif topic.startswith(CONSTANTS.WS_TRADES_TOPIC + ":"):
            channel = self._trade_messages_queue_key
        elif topic.startswith(CONSTANTS.WS_MARKET_PRICE_TOPIC + ":"):
            channel = self._funding_info_messages_queue_key

        return channel

    # ------------------------------------------------------------------
    # Message parsing helpers
    # ------------------------------------------------------------------

    def _extract_market_addr_from_topic(self, topic: str) -> str:
        """Extract the market address portion from a ``channel:addr`` topic string."""
        parts = topic.split(":", 1)
        return parts[1] if len(parts) > 1 else ""

    async def _market_addr_to_trading_pair(self, market_addr: str) -> str:
        """
        Resolve a market address back to a Hummingbot trading pair.

        The connector is expected to maintain an ``addr_to_market_name`` mapping
        (the inverse of ``market_name_to_addr``).
        """
        if hasattr(self._connector, "addr_to_market_name"):
            market_name = self._connector.addr_to_market_name.get(market_addr)
            if market_name is not None:
                return await self._connector.trading_pair_associated_to_exchange_symbol(market_name)

        # Fallback: try looking up via exchange_symbol which may be the address itself
        return await self._connector.trading_pair_associated_to_exchange_symbol(market_addr)

    async def _trading_pair_to_market_addr(self, trading_pair: str) -> str:
        """
        Resolve a Hummingbot trading pair to its Decibel market address.

        The connector is expected to maintain a ``market_name_to_addr`` dict
        populated during ``_update_trading_rules``.
        """
        ex_symbol = await self._connector.exchange_symbol_associated_to_pair(
            trading_pair=trading_pair
        )
        if hasattr(self._connector, "market_name_to_addr"):
            addr = self._connector.market_name_to_addr.get(ex_symbol)
            if addr is not None:
                return addr
        # If the exchange symbol IS the market address already, return it directly.
        return ex_symbol

    # ------------------------------------------------------------------
    # Order book parsing
    # ------------------------------------------------------------------

    async def _parse_order_book_diff_message(
            self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """
        Parse a depth update (diff) from the Decibel WS and enqueue an
        ``OrderBookMessage`` of type DIFF.

        Expected ``raw_message`` structure::

            {
                "topic": "depth:0xabc...",
                "data": {
                    "bids": [{"price": "50000.5", "size": "1.2"}, ...],
                    "asks": [{"price": "50001.0", "size": "0.8"}, ...],
                    "timestamp": 1700000000000
                }
            }
        """
        topic = raw_message.get("topic", "")
        market_addr = self._extract_market_addr_from_topic(topic)
        trading_pair = await self._market_addr_to_trading_pair(market_addr)

        data = raw_message.get("data", {})
        timestamp_ms = data.get("timestamp", int(time.time() * 1e3))
        timestamp_s = float(timestamp_ms) * 1e-3

        bids = [
            [float(level["price"]), float(level["size"])]
            for level in data.get("bids", [])
        ]
        asks = [
            [float(level["price"]), float(level["size"])]
            for level in data.get("asks", [])
        ]

        order_book_message = OrderBookMessage(
            OrderBookMessageType.DIFF,
            {
                "trading_pair": trading_pair,
                "update_id": timestamp_ms,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp_s,
        )
        message_queue.put_nowait(order_book_message)

    async def _parse_order_book_snapshot_message(
            self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """
        Parse a depth snapshot from the Decibel WS and enqueue an
        ``OrderBookMessage`` of type SNAPSHOT.

        The message format is the same as diff -- the first message received
        on the depth channel after subscription is treated as a snapshot.
        """
        topic = raw_message.get("topic", "")
        market_addr = self._extract_market_addr_from_topic(topic)
        trading_pair = await self._market_addr_to_trading_pair(market_addr)

        data = raw_message.get("data", {})
        timestamp_ms = data.get("timestamp", int(time.time() * 1e3))
        timestamp_s = float(timestamp_ms) * 1e-3

        bids = [
            [float(level["price"]), float(level["size"])]
            for level in data.get("bids", [])
        ]
        asks = [
            [float(level["price"]), float(level["size"])]
            for level in data.get("asks", [])
        ]

        order_book_message = OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": timestamp_ms,
                "bids": bids,
                "asks": asks,
            },
            timestamp=timestamp_s,
        )
        message_queue.put_nowait(order_book_message)

    async def _parse_trade_message(
            self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """
        Parse a trade event from the Decibel WS.

        Expected ``raw_message`` structure::

            {
                "topic": "trades:0xabc...",
                "data": [
                    {
                        "price": "50000.5",
                        "size": "0.1",
                        "side": "buy",
                        "timestamp": 1700000000000,
                        "trade_id": "abc123"
                    },
                    ...
                ]
            }

        Or a single trade (not in a list)::

            {
                "topic": "trades:0xabc...",
                "data": { ... single trade ... }
            }
        """
        topic = raw_message.get("topic", "")
        market_addr = self._extract_market_addr_from_topic(topic)
        trading_pair = await self._market_addr_to_trading_pair(market_addr)

        data = raw_message.get("data", [])

        # Normalise to a list of trades
        if isinstance(data, dict):
            data = [data]

        for trade_data in data:
            side = trade_data.get("side", "").lower()
            trade_type = (
                float(TradeType.BUY.value) if side == "buy"
                else float(TradeType.SELL.value)
            )

            timestamp_ms = trade_data.get("timestamp", int(time.time() * 1e3))
            trade_id = trade_data.get("trade_id", str(timestamp_ms))

            trade_message = OrderBookMessage(
                OrderBookMessageType.TRADE,
                {
                    "trading_pair": trading_pair,
                    "trade_type": trade_type,
                    "trade_id": trade_id,
                    "price": float(trade_data.get("price", 0)),
                    "amount": float(trade_data.get("size", 0)),
                },
                timestamp=float(timestamp_ms) * 1e-3,
            )
            message_queue.put_nowait(trade_message)

    # ------------------------------------------------------------------
    # Funding info parsing
    # ------------------------------------------------------------------

    async def _parse_funding_info_message(
            self, raw_message: Dict[str, Any], message_queue: asyncio.Queue
    ):
        """
        Parse a market_price update from the Decibel WS and enqueue a
        ``FundingInfoUpdate``.

        Expected ``raw_message`` structure::

            {
                "topic": "market_price:0xabc...",
                "data": {
                    "oracle_px": "50000.0",
                    "mark_px": "50010.0",
                    "funding_rate_bps": "1.5",
                    "timestamp": 1700000000000
                }
            }
        """
        try:
            topic = raw_message.get("topic", "")
            market_addr = self._extract_market_addr_from_topic(topic)
            trading_pair = await self._market_addr_to_trading_pair(market_addr)

            if trading_pair not in self._trading_pairs:
                return

            data = raw_message.get("data", {})

            # funding_rate_bps is in basis points (1 bps = 0.0001).
            # Convert to a rate fraction.
            funding_rate_bps = Decimal(str(data.get("funding_rate_bps", "0")))
            rate = funding_rate_bps / Decimal("10000")

            funding_info = FundingInfoUpdate(
                trading_pair=trading_pair,
                index_price=Decimal(str(data.get("oracle_px", "0"))),
                mark_price=Decimal(str(data.get("mark_px", "0"))),
                next_funding_utc_timestamp=self._next_funding_time(),
                rate=rate,
            )
            message_queue.put_nowait(funding_info)
        except Exception as e:
            self.logger().debug(f"Error parsing funding info message: {e}")

    async def _request_complete_funding_info(self, trading_pair: str) -> Optional[Dict[str, Any]]:
        """
        Fetch funding/price info from the REST prices endpoint for a single
        trading pair.

        Returns the price data dict or ``None`` if the market is not found.
        """
        market_addr = await self._trading_pair_to_market_addr(trading_pair)
        params = {"market_addr": market_addr}

        try:
            data = await self._connector._api_get(
                path_url=CONSTANTS.PRICES_URL,
                params=params,
            )
            if isinstance(data, list):
                # Filter for the correct market if multiple are returned
                for item in data:
                    if item.get("market_addr") == market_addr:
                        return item
                return data[0] if data else None
            return data
        except Exception:
            self.logger().exception(f"Error fetching funding info for {trading_pair}")
            return None

    def _next_funding_time(self) -> int:
        """
        Estimate the next funding settlement timestamp.

        Decibel settles funding every 1 hour (similar to most perpetual DEXes).
        """
        return int(((time.time() // 3600) + 1) * 3600)

    # ------------------------------------------------------------------
    # Dynamic subscription / unsubscription
    # ------------------------------------------------------------------

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        """
        Subscribe to order book, trade, and funding channels for a single
        trading pair on an existing WebSocket connection.
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot subscribe to {trading_pair}: WebSocket connection not established."
            )
            return False

        try:
            market_addr = await self._trading_pair_to_market_addr(trading_pair)

            depth_payload = {
                "method": "subscribe",
                "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=depth_payload))

            trades_payload = {
                "method": "subscribe",
                "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=trades_payload))

            market_price_payload = {
                "method": "subscribe",
                "topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=market_price_payload))

            self.add_trading_pair(trading_pair)
            self.logger().info(f"Successfully subscribed to {trading_pair}")
            return True

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error subscribing to {trading_pair}: {e}")
            return False

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        """
        Unsubscribe from order book, trade, and funding channels for a single
        trading pair on an existing WebSocket connection.
        """
        if self._ws_assistant is None:
            self.logger().warning(
                f"Cannot unsubscribe from {trading_pair}: WebSocket connection not established."
            )
            return False

        try:
            market_addr = await self._trading_pair_to_market_addr(trading_pair)

            depth_payload = {
                "method": "unsubscribe",
                "topic": f"{CONSTANTS.WS_DEPTH_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=depth_payload))

            trades_payload = {
                "method": "unsubscribe",
                "topic": f"{CONSTANTS.WS_TRADES_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=trades_payload))

            market_price_payload = {
                "method": "unsubscribe",
                "topic": f"{CONSTANTS.WS_MARKET_PRICE_TOPIC}:{market_addr}",
            }
            await self._ws_assistant.send(WSJSONRequest(payload=market_price_payload))

            self.remove_trading_pair(trading_pair)
            self.logger().info(f"Successfully unsubscribed from {trading_pair}")
            return True

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().error(f"Error unsubscribing from {trading_pair}: {e}")
            return False
