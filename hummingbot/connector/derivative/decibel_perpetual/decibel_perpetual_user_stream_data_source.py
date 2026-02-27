import asyncio
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS
import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_web_utils as web_utils
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth


class DecibelPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    HEARTBEAT_TIME_INTERVAL = CONSTANTS.HEARTBEAT_TIME_INTERVAL

    _logger: Optional[HummingbotLogger] = None

    def __init__(
            self,
            auth: 'DecibelPerpetualAuth',
            trading_pairs: List[str],
            connector: object,
            api_factory: WebAssistantsFactory,
            domain: str = CONSTANTS.DOMAIN,
    ):
        super().__init__()
        self._domain = domain
        self._api_factory = api_factory
        self._auth = auth
        self._connector = connector
        self._trading_pairs: List[str] = trading_pairs

    @property
    def last_recv_time(self) -> float:
        if self._ws_assistant:
            return self._ws_assistant.last_recv_time
        return 0

    async def _get_ws_assistant(self) -> WSAssistant:
        if self._ws_assistant is None:
            self._ws_assistant = await self._api_factory.get_ws_assistant()
        return self._ws_assistant

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange.

        Decibel WebSocket authentication is performed via the Sec-Websocket-Protocol
        header during the connection handshake, not via per-message auth.
        """
        ws: WSAssistant = await self._get_ws_assistant()
        url = web_utils.wss_url(self._domain)
        ws_headers = self._auth.get_ws_auth_headers()
        await ws.connect(
            ws_url=url,
            ping_timeout=self.HEARTBEAT_TIME_INTERVAL,
            ws_headers=ws_headers,
        )
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to order update and user trade events through the provided websocket connection.

        Decibel uses topic-based subscriptions in the format:
          - order_updates:{accountAddr}
          - user_trades:{accountAddr}

        :param websocket_assistant: the websocket assistant used to connect to the exchange
        """
        try:
            account_address = self._auth.trading_account

            order_updates_payload = {
                "method": "subscribe",
                "topic": f"{CONSTANTS.WS_ORDER_UPDATES_TOPIC}:{account_address}",
            }
            subscribe_order_updates_request: WSJSONRequest = WSJSONRequest(
                payload=order_updates_payload,
                is_auth_required=True,
            )

            user_trades_payload = {
                "method": "subscribe",
                "topic": f"{CONSTANTS.WS_USER_TRADES_TOPIC}:{account_address}",
            }
            subscribe_user_trades_request: WSJSONRequest = WSJSONRequest(
                payload=user_trades_payload,
                is_auth_required=True,
            )

            await websocket_assistant.send(subscribe_order_updates_request)
            await websocket_assistant.send(subscribe_user_trades_request)

            self.logger().info("Subscribed to private order updates and user trades channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to user streams...")
            raise

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        """
        Routes incoming WebSocket messages to the output queue based on the topic field.

        Decibel messages use a "topic" field (not "channel") to identify the message type.
        Messages with error payloads raise an IOError. Only messages matching the subscribed
        user topics (order_updates, user_trades) are forwarded to the queue.
        """
        if event_message.get("error") is not None:
            err_msg = event_message.get("error", {})
            if isinstance(err_msg, dict):
                err_msg = err_msg.get("message", str(event_message.get("error")))
            raise IOError({
                "label": "WSS_ERROR",
                "message": f"Error received via websocket - {err_msg}."
            })

        topic = event_message.get("topic", "")
        # The topic includes the account address suffix (e.g. "order_updates:0xabc..."),
        # so we check if it starts with one of the known user stream topic prefixes.
        if topic.startswith(CONSTANTS.WS_ORDER_UPDATES_TOPIC) or topic.startswith(CONSTANTS.WS_USER_TRADES_TOPIC):
            queue.put_nowait(event_message)

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        """
        Processes incoming WebSocket messages in a loop. On timeout, sends a ping to keep
        the connection alive. The server sends ping frames every 30s which the WSAssistant
        handles automatically with pong responses.
        """
        while True:
            try:
                await super()._process_websocket_messages(
                    websocket_assistant=websocket_assistant,
                    queue=queue,
                )
            except asyncio.TimeoutError:
                ping_request = WSJSONRequest(payload={"method": "ping"})
                await websocket_assistant.send(ping_request)
