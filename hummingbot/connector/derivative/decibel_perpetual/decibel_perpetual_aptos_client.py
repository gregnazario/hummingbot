import hashlib
import logging
from decimal import Decimal
from typing import Optional

import hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_constants as CONSTANTS

logger = logging.getLogger(__name__)

# Guard the aptos-sdk imports so the module can be loaded for static-method-only
# usage even when the SDK is not installed.  The on-chain methods (place_order,
# cancel_order) will raise ImportError at call time if the SDK is missing.
try:
    from aptos_sdk.account import Account
    from aptos_sdk.account_address import AccountAddress
    from aptos_sdk.async_client import RestClient
    from aptos_sdk.bcs import Serializer
    from aptos_sdk.transactions import EntryFunction, TransactionArgument, TransactionPayload
    _APTOS_SDK_AVAILABLE = True
except ImportError:
    _APTOS_SDK_AVAILABLE = False
    logger.warning(
        "aptos-sdk is not installed. On-chain order placement/cancellation will not work. "
        "Install with: pip install aptos-sdk"
    )


def _require_aptos_sdk():
    """Raise a clear error when the SDK is needed but not installed."""
    if not _APTOS_SDK_AVAILABLE:
        raise ImportError(
            "aptos-sdk is required for on-chain transactions but is not installed. "
            "Install with: pip install aptos-sdk"
        )


class DecibelAptosClient:
    """
    Wraps the Aptos SDK to build, sign, and submit Decibel on-chain transactions.

    Handles:
    - Order placement via place_order_to_subaccount
    - Order cancellation via cancel_order_to_subaccount
    - Price/size formatting to 9-decimal u64 integers
    - Market address derivation
    """

    def __init__(self, private_key: str, domain: str = CONSTANTS.DOMAIN):
        _require_aptos_sdk()

        self._domain = domain
        self._account = Account.load_key(private_key)
        node_url = (
            CONSTANTS.APTOS_NODE_URL
            if domain == CONSTANTS.DOMAIN
            else CONSTANTS.APTOS_TESTNET_NODE_URL
        )
        self._client = RestClient(node_url)
        self._package_address = AccountAddress.from_str(
            CONSTANTS.MAINNET_PACKAGE_ADDRESS
            if domain == CONSTANTS.DOMAIN
            else CONSTANTS.TESTNET_PACKAGE_ADDRESS
        )

    @property
    def account_address(self) -> str:
        """Return the hex-encoded Aptos account address derived from the private key."""
        return str(self._account.address())

    async def place_order(
        self,
        subaccount: str,
        market_addr: str,
        price: Decimal,
        size: Decimal,
        is_buy: bool,
        tif: int = CONSTANTS.TIF_GTC,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> str:
        """
        Place an order on Decibel via an Aptos on-chain transaction.

        Args:
            subaccount: The trading sub-account object address (hex).
            market_addr: The market object address (hex).
            price: Order price as a human-readable Decimal.
            size: Order size as a human-readable Decimal.
            is_buy: True for buy (long), False for sell (short).
            tif: Time-in-force flag (0=GTC, 1=PostOnly, 2=IOC).
            reduce_only: Whether the order is reduce-only.
            client_order_id: Optional client-assigned order ID string.

        Returns:
            The transaction hash (hex string).
        """
        _require_aptos_sdk()

        price_u64 = self.format_value(price)
        size_u64 = self.format_value(size)

        # Convert client_order_id string to a u128 via MD5 hash.
        # If not provided, default to 0.
        cloid_u128 = 0
        if client_order_id:
            md5 = hashlib.md5()
            md5.update(client_order_id.encode("utf-8"))
            cloid_u128 = int(md5.hexdigest(), 16)

        function = EntryFunction.natural(
            f"{self._package_address}::{CONSTANTS.DEX_ACCOUNTS_MODULE}",
            CONSTANTS.PLACE_ORDER_FUNCTION,
            [],  # type_args
            [
                TransactionArgument(AccountAddress.from_str(subaccount), Serializer.struct),
                TransactionArgument(AccountAddress.from_str(market_addr), Serializer.struct),
                TransactionArgument(price_u64, Serializer.u64),
                TransactionArgument(size_u64, Serializer.u64),
                TransactionArgument(is_buy, Serializer.bool),
                TransactionArgument(tif, Serializer.u8),
                TransactionArgument(reduce_only, Serializer.bool),
                TransactionArgument(cloid_u128, Serializer.u128),
                TransactionArgument(0, Serializer.u64),   # stop_price
                TransactionArgument(0, Serializer.u64),   # tp_trigger_price
                TransactionArgument(0, Serializer.u64),   # tp_limit_price
                TransactionArgument(0, Serializer.u64),   # sl_trigger_price
                TransactionArgument(0, Serializer.u64),   # sl_limit_price
                TransactionArgument(AccountAddress.from_str("0x0"), Serializer.struct),  # builder_addr
                TransactionArgument(0, Serializer.u64),   # builder_fee_bps
            ],
        )
        payload = TransactionPayload(function)
        signed_txn = await self._client.create_bcs_signed_transaction(self._account, payload)
        tx_hash = await self._client.submit_bcs_transaction(signed_txn)
        await self._client.wait_for_transaction(tx_hash)
        return tx_hash

    async def cancel_order(
        self,
        subaccount: str,
        market_addr: str,
        order_id: int,
    ) -> str:
        """
        Cancel an order on Decibel via an Aptos on-chain transaction.

        Args:
            subaccount: The trading sub-account object address (hex).
            market_addr: The market object address (hex).
            order_id: The on-chain order ID (u128).

        Returns:
            The transaction hash (hex string).
        """
        _require_aptos_sdk()

        function = EntryFunction.natural(
            f"{self._package_address}::{CONSTANTS.DEX_ACCOUNTS_MODULE}",
            CONSTANTS.CANCEL_ORDER_FUNCTION,
            [],  # type_args
            [
                TransactionArgument(AccountAddress.from_str(subaccount), Serializer.struct),
                TransactionArgument(order_id, Serializer.u128),
                TransactionArgument(AccountAddress.from_str(market_addr), Serializer.struct),
            ],
        )
        payload = TransactionPayload(function)
        signed_txn = await self._client.create_bcs_signed_transaction(self._account, payload)
        tx_hash = await self._client.submit_bcs_transaction(signed_txn)
        await self._client.wait_for_transaction(tx_hash)
        return tx_hash

    # ------------------------------------------------------------------
    # Static utility methods (no SDK / network required)
    # ------------------------------------------------------------------

    @staticmethod
    def format_value(value: Decimal) -> int:
        """
        Convert a human-readable decimal value to a 9-decimal u64 integer for
        on-chain use.

        Example:  Decimal("5.67")  ->  5_670_000_000
        """
        return int(value * Decimal(10 ** CONSTANTS.ON_CHAIN_DECIMAL_PLACES))

    @staticmethod
    def parse_value(raw: int) -> Decimal:
        """
        Convert a 9-decimal u64 integer from the chain back to a human-readable
        Decimal.

        Example:  5_670_000_000  ->  Decimal("5.67")
        """
        return Decimal(raw) / Decimal(10 ** CONSTANTS.ON_CHAIN_DECIMAL_PLACES)

    @staticmethod
    def derive_market_address(package_address: str, market_name: str) -> str:
        """
        Derive a market's object address from the package address and market name.

        Note: In practice, market addresses should be fetched from the REST API
        via ``GET /api/v1/markets`` and cached.  This method is a placeholder
        matching the Decibel TypeScript SDK's derivation scheme.
        """
        raise NotImplementedError(
            "Use GET /api/v1/markets to fetch market addresses instead of deriving them"
        )

    async def close(self):
        """Clean up the async REST client."""
        if _APTOS_SDK_AVAILABLE and hasattr(self, "_client"):
            await self._client.close()
