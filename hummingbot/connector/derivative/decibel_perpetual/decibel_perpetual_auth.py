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
