"""
Tests for DecibelAptosClient static utility methods.

Only the pure-computation methods (format_value, parse_value) are tested here
since they require neither network access nor the aptos-sdk package.  The
on-chain methods (place_order, cancel_order) involve real Aptos transactions
and will be covered by integration tests.
"""
import unittest
from decimal import Decimal

from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_aptos_client import DecibelAptosClient
from hummingbot.connector.derivative.decibel_perpetual import decibel_perpetual_constants as CONSTANTS


class DecibelAptosClientFormatValueTest(unittest.TestCase):
    """Tests for DecibelAptosClient.format_value (Decimal -> u64)."""

    def test_format_value_integer(self):
        result = DecibelAptosClient.format_value(Decimal("5"))
        self.assertEqual(result, 5_000_000_000)

    def test_format_value_decimal(self):
        result = DecibelAptosClient.format_value(Decimal("5.67"))
        self.assertEqual(result, 5_670_000_000)

    def test_format_value_small(self):
        result = DecibelAptosClient.format_value(Decimal("0.001"))
        self.assertEqual(result, 1_000_000)

    def test_format_value_zero(self):
        result = DecibelAptosClient.format_value(Decimal("0"))
        self.assertEqual(result, 0)

    def test_format_value_large(self):
        result = DecibelAptosClient.format_value(Decimal("100000"))
        self.assertEqual(result, 100_000_000_000_000)

    def test_format_value_max_precision(self):
        """9 decimal places is the maximum on-chain precision."""
        result = DecibelAptosClient.format_value(Decimal("1.123456789"))
        self.assertEqual(result, 1_123_456_789)

    def test_format_value_very_small(self):
        """The smallest representable value is 1e-9."""
        result = DecibelAptosClient.format_value(Decimal("0.000000001"))
        self.assertEqual(result, 1)


class DecibelAptosClientParseValueTest(unittest.TestCase):
    """Tests for DecibelAptosClient.parse_value (u64 -> Decimal)."""

    def test_parse_value_basic(self):
        result = DecibelAptosClient.parse_value(5_670_000_000)
        self.assertEqual(result, Decimal("5.67"))

    def test_parse_value_integer(self):
        result = DecibelAptosClient.parse_value(5_000_000_000)
        self.assertEqual(result, Decimal("5"))

    def test_parse_value_zero(self):
        result = DecibelAptosClient.parse_value(0)
        self.assertEqual(result, Decimal("0"))

    def test_parse_value_one_unit(self):
        result = DecibelAptosClient.parse_value(1)
        self.assertEqual(result, Decimal("0.000000001"))

    def test_parse_value_large(self):
        result = DecibelAptosClient.parse_value(100_000_000_000_000)
        self.assertEqual(result, Decimal("100000"))


class DecibelAptosClientRoundtripTest(unittest.TestCase):
    """Tests for format_value <-> parse_value round-trip consistency."""

    def test_roundtrip_simple(self):
        original = Decimal("123.456789")
        encoded = DecibelAptosClient.format_value(original)
        decoded = DecibelAptosClient.parse_value(encoded)
        self.assertEqual(decoded, original)

    def test_roundtrip_zero(self):
        original = Decimal("0")
        encoded = DecibelAptosClient.format_value(original)
        decoded = DecibelAptosClient.parse_value(encoded)
        self.assertEqual(decoded, original)

    def test_roundtrip_whole_number(self):
        original = Decimal("42")
        encoded = DecibelAptosClient.format_value(original)
        decoded = DecibelAptosClient.parse_value(encoded)
        self.assertEqual(decoded, original)

    def test_roundtrip_many_decimals(self):
        original = Decimal("0.123456789")
        encoded = DecibelAptosClient.format_value(original)
        decoded = DecibelAptosClient.parse_value(encoded)
        self.assertEqual(decoded, original)


class DecibelAptosClientConstantsTest(unittest.TestCase):
    """Verify that the constants used by the client are correctly defined."""

    def test_on_chain_decimal_places(self):
        self.assertEqual(CONSTANTS.ON_CHAIN_DECIMAL_PLACES, 9)

    def test_tif_values(self):
        self.assertEqual(CONSTANTS.TIF_GTC, 0)
        self.assertEqual(CONSTANTS.TIF_POST_ONLY, 1)
        self.assertEqual(CONSTANTS.TIF_IOC, 2)

    def test_package_addresses_are_hex(self):
        self.assertTrue(CONSTANTS.MAINNET_PACKAGE_ADDRESS.startswith("0x"))
        self.assertTrue(CONSTANTS.TESTNET_PACKAGE_ADDRESS.startswith("0x"))

    def test_move_module_names(self):
        self.assertEqual(CONSTANTS.DEX_ACCOUNTS_MODULE, "dex_accounts")
        self.assertEqual(CONSTANTS.PLACE_ORDER_FUNCTION, "place_order_to_subaccount")
        self.assertEqual(CONSTANTS.CANCEL_ORDER_FUNCTION, "cancel_order_to_subaccount")


class DecibelAptosClientDeriveMarketAddressTest(unittest.TestCase):
    """Test derive_market_address placeholder."""

    def test_derive_market_address_raises(self):
        with self.assertRaises(NotImplementedError):
            DecibelAptosClient.derive_market_address(
                CONSTANTS.MAINNET_PACKAGE_ADDRESS, "BTC-PERP"
            )


if __name__ == "__main__":
    unittest.main()
