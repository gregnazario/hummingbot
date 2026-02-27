# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_utils.py
import unittest
from decimal import Decimal

from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_utils import (
    CENTRALIZED,
    DEFAULT_FEES,
    EXAMPLE_PAIR,
    KEYS,
    OTHER_DOMAINS,
    OTHER_DOMAINS_KEYS,
    DecibelPerpetualConfigMap,
    DecibelPerpetualTestnetConfigMap,
)


class DecibelPerpetualUtilsTest(unittest.TestCase):
    def test_default_fees(self):
        self.assertEqual(DEFAULT_FEES.maker_percent_fee_decimal, Decimal("0.00011"))
        self.assertEqual(DEFAULT_FEES.taker_percent_fee_decimal, Decimal("0.00034"))

    def test_centralized_is_false(self):
        self.assertFalse(CENTRALIZED)

    def test_example_pair(self):
        self.assertEqual(EXAMPLE_PAIR, "BTC-PERP")

    def test_keys_config(self):
        self.assertIsInstance(KEYS, DecibelPerpetualConfigMap)
        self.assertEqual(KEYS.connector, "decibel_perpetual")

    def test_other_domains(self):
        self.assertIn("decibel_perpetual_testnet", OTHER_DOMAINS)

    def test_testnet_keys_config(self):
        testnet_keys = OTHER_DOMAINS_KEYS["decibel_perpetual_testnet"]
        self.assertIsInstance(testnet_keys, DecibelPerpetualTestnetConfigMap)
        self.assertEqual(testnet_keys.connector, "decibel_perpetual_testnet")
