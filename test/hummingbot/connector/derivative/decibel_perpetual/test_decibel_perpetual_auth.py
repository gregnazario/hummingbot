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
