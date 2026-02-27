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
