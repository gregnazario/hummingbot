import asyncio
import hashlib
import time
from decimal import Decimal
from typing import Any, AsyncIterable, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.decibel_perpetual import (
    decibel_perpetual_constants as CONSTANTS,
    decibel_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_api_order_book_data_source import (
    DecibelPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_aptos_client import DecibelAptosClient
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_auth import DecibelPerpetualAuth
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_user_stream_data_source import (
    DecibelPerpetualUserStreamDataSource,
)
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair, get_new_client_order_id
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

bpm_logger = None


class DecibelPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils

    SHORT_POLL_INTERVAL = 5.0
    LONG_POLL_INTERVAL = 12.0

    def __init__(
            self,
            balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
            rate_limits_share_pct: Decimal = Decimal("100"),
            decibel_perpetual_api_key: str = None,
            decibel_perpetual_secret_key: str = None,
            decibel_perpetual_trading_account: Optional[str] = None,
            trading_pairs: Optional[List[str]] = None,
            trading_required: bool = True,
            domain: str = CONSTANTS.DOMAIN,
    ):
        self._api_key = decibel_perpetual_api_key
        self._secret_key = decibel_perpetual_secret_key
        self._trading_account = decibel_perpetual_trading_account
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._domain = domain
        self._position_mode = None
        self._last_trade_history_timestamp = None

        # Maps market_name (e.g. "BTC-PERP") -> market_addr (0x...)
        self.market_name_to_addr: Dict[str, str] = {}
        # Reverse map: market_addr -> market_name
        self.addr_to_market_name: Dict[str, str] = {}

        # Aptos client for on-chain order placement/cancellation.
        # Initialized lazily after super().__init__ to avoid issues if secret_key is None.
        self._aptos_client: Optional[DecibelAptosClient] = None

        super().__init__(balance_asset_limit, rate_limits_share_pct)

    # ------------------------------------------------------------------
    # Properties required by PerpetualDerivativePyBase / ExchangePyBase
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        return self._domain

    @property
    def authenticator(self) -> Optional[DecibelPerpetualAuth]:
        if self._trading_required:
            return DecibelPerpetualAuth(
                api_key=self._api_key,
                secret_key=self._secret_key,
                trading_account=self._trading_account,
            )
        return None

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self) -> str:
        return self._domain

    @property
    def client_order_id_max_length(self) -> int:
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self) -> str:
        return CONSTANTS.BROKER_ID

    @property
    def trading_rules_request_path(self) -> str:
        return CONSTANTS.MARKETS_URL

    @property
    def trading_pairs_request_path(self) -> str:
        return CONSTANTS.MARKETS_URL

    @property
    def check_network_request_path(self) -> str:
        return CONSTANTS.MARKETS_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        # On-chain cancellation waits for transaction confirmation
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return 120

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _get_aptos_client(self) -> DecibelAptosClient:
        """Lazily create the Aptos client for on-chain transactions."""
        if self._aptos_client is None:
            self._aptos_client = DecibelAptosClient(
                private_key=self._secret_key,
                domain=self._domain,
            )
        return self._aptos_client

    def _get_trading_account(self) -> str:
        """Return the trading account address (subaccount).

        Raises if it has not been set or discovered yet.
        """
        account = self._auth.trading_account if self._auth else self._trading_account
        if not account:
            raise ValueError(
                "Trading account (subaccount) has not been set. "
                "Either provide it in config or wait for auto-discovery during initialization."
            )
        return account

    # ------------------------------------------------------------------
    # Network / connectivity
    # ------------------------------------------------------------------

    async def _make_network_check_request(self):
        await self._api_get(path_url=self.check_network_request_path)

    # ------------------------------------------------------------------
    # Supported types / modes
    # ------------------------------------------------------------------

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self):
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.buy_order_collateral_token

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.sell_order_collateral_token

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        return False

    # ------------------------------------------------------------------
    # Factory methods
    # ------------------------------------------------------------------

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            auth=self._auth,
        )

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return DecibelPerpetualAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return DecibelPerpetualUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    # ------------------------------------------------------------------
    # Trading pair symbol map & trading rules initialization
    # ------------------------------------------------------------------

    async def _initialize_trading_pair_symbol_map(self):
        try:
            exchange_info = await self._api_get(
                path_url=self.trading_pairs_request_path,
            )
            self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)

            # Auto-discover subaccount if not provided
            await self._auto_discover_trading_account()

        except Exception:
            self.logger().exception("There was an error requesting exchange info.")

    async def _auto_discover_trading_account(self):
        """
        If no trading_account was provided, discover the first subaccount via REST API.
        The API wallet address is derived from the Aptos private key.
        """
        if self._auth is not None and self._auth.trading_account:
            return  # Already set

        if not self._trading_required:
            return  # No need for trading account if not trading

        try:
            aptos_client = self._get_aptos_client()
            api_wallet_address = aptos_client.account_address

            subaccounts_response = await self._api_get(
                path_url=CONSTANTS.SUBACCOUNTS_URL,
                params={"account": api_wallet_address},
                is_auth_required=True,
            )

            if isinstance(subaccounts_response, list) and len(subaccounts_response) > 0:
                # Use the first subaccount
                first_sub = subaccounts_response[0]
                discovered_account = first_sub if isinstance(first_sub, str) else first_sub.get("address", "")
                if discovered_account and self._auth is not None:
                    self._auth.trading_account = discovered_account
                    self._trading_account = discovered_account
                    self.logger().info(
                        f"Auto-discovered trading account (subaccount): {discovered_account}"
                    )
                else:
                    self.logger().warning("Subaccount response was empty or had no address field.")
            else:
                self.logger().warning(
                    "No subaccounts found for the API wallet. "
                    "Please create a subaccount or provide one in the config."
                )
        except Exception:
            self.logger().exception("Error during subaccount auto-discovery.")

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Any):
        """
        Build the bidict symbol map from the /api/v1/markets response.

        The response is expected to be a list of market dicts, each containing:
        - market_addr: the on-chain market address
        - market_name: e.g. "BTC-PERP"
        - sz_decimals, px_decimals, tick_size, min_size, lot_size, max_leverage, mode
        """
        mapping = bidict()
        new_name_to_addr = {}
        new_addr_to_name = {}

        markets = exchange_info if isinstance(exchange_info, list) else exchange_info.get("markets", exchange_info)
        if not isinstance(markets, list):
            markets = []

        for market_data in filter(web_utils.is_exchange_information_valid, markets):
            market_name = market_data.get("market_name", "")
            market_addr = market_data.get("market_addr", "")

            if not market_name or not market_addr:
                continue

            # Build address maps in temporaries to avoid race with WS handlers
            new_name_to_addr[market_name] = market_addr
            new_addr_to_name[market_addr] = market_name

            # Derive hummingbot trading pair.
            # Decibel market names are like "BTC-PERP".
            # The base is everything before the last "-", quote is USDC.
            base = market_name.rsplit("-", 1)[0]
            quote = CONSTANTS.CURRENCY
            trading_pair = combine_to_hb_trading_pair(base, quote)

            if trading_pair in mapping.inverse:
                self._resolve_trading_pair_symbols_duplicate(mapping, market_name, base, quote)
            else:
                mapping[market_name] = trading_pair

        # Atomically swap the maps to avoid empty-map race with concurrent WS handlers
        self.market_name_to_addr = new_name_to_addr
        self.addr_to_market_name = new_addr_to_name
        self._set_trading_pair_symbol_map(mapping)

    async def _make_trading_rules_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        return exchange_info

    async def _make_trading_pairs_request(self) -> Any:
        exchange_info = await self._api_get(path_url=self.trading_pairs_request_path)
        return exchange_info

    async def _update_trading_rules(self):
        exchange_info = await self._api_get(path_url=self.trading_rules_request_path)
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info=exchange_info)
        trading_rules_list = await self._format_trading_rules(exchange_info)
        self._trading_rules.clear()
        for trading_rule in trading_rules_list:
            self._trading_rules[trading_rule.trading_pair] = trading_rule

    async def _format_trading_rules(self, exchange_info_dict: Any) -> List[TradingRule]:
        """
        Parse the /api/v1/markets response into TradingRule objects.

        Each market dict contains:
        - market_name, market_addr
        - sz_decimals: number of decimal places for size
        - px_decimals: number of decimal places for price
        - tick_size: minimum price increment
        - min_size: minimum order size
        - lot_size: minimum size increment
        - max_leverage: maximum leverage
        - mode: "Open" | "CloseOnly" | etc.
        """
        return_val: list = []
        markets = exchange_info_dict if isinstance(exchange_info_dict, list) else exchange_info_dict.get("markets", exchange_info_dict)
        if not isinstance(markets, list):
            markets = []

        for market_data in filter(web_utils.is_exchange_information_valid, markets):
            try:
                market_name = market_data.get("market_name", "")
                market_addr = market_data.get("market_addr", "")

                # Update address maps
                if market_name and market_addr:
                    self.market_name_to_addr[market_name] = market_addr
                    self.addr_to_market_name[market_addr] = market_name

                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=market_name)

                sz_decimals = int(market_data.get("sz_decimals", 8))
                px_decimals = int(market_data.get("px_decimals", 8))
                tick_size = Decimal(str(market_data.get("tick_size", 10 ** -px_decimals)))
                min_size = Decimal(str(market_data.get("min_size", 10 ** -sz_decimals)))
                lot_size = Decimal(str(market_data.get("lot_size", 10 ** -sz_decimals)))

                collateral_token = CONSTANTS.CURRENCY

                return_val.append(
                    TradingRule(
                        trading_pair,
                        min_order_size=min_size,
                        min_price_increment=tick_size,
                        min_base_amount_increment=lot_size,
                        buy_order_collateral_token=collateral_token,
                        sell_order_collateral_token=collateral_token,
                    )
                )
            except Exception:
                self.logger().error(
                    f"Error parsing the trading pair rule {market_data}. Skipping.",
                    exc_info=True,
                )

        return return_val

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _resolve_trading_pair_symbols_duplicate(self, mapping: bidict, new_exchange_symbol: str, base: str, quote: str):
        expected_exchange_symbol = f"{base}{quote}"
        trading_pair = combine_to_hb_trading_pair(base, quote)
        current_exchange_symbol = mapping.inverse[trading_pair]
        if current_exchange_symbol == expected_exchange_symbol:
            pass
        elif new_exchange_symbol == expected_exchange_symbol:
            mapping.pop(current_exchange_symbol)
            mapping[new_exchange_symbol] = trading_pair
        else:
            self.logger().error(
                f"Could not resolve the exchange symbols {new_exchange_symbol} and {current_exchange_symbol}")
            mapping.pop(current_exchange_symbol)

    # ------------------------------------------------------------------
    # Price quantization
    # ------------------------------------------------------------------

    def quantize_order_price(self, trading_pair: str, price: Decimal) -> Decimal:
        """
        Applies trading rule to quantize order price.
        Uses sig-figs rounding similar to HyperLiquid.
        """
        d_price = Decimal(round(float(f"{price:.5g}"), 6))
        return d_price

    # ------------------------------------------------------------------
    # All pairs prices (for ticker)
    # ------------------------------------------------------------------

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        res: List[Dict[str, str]] = []
        try:
            prices_data = await self._api_get(path_url=CONSTANTS.PRICES_URL)
            if isinstance(prices_data, list):
                for item in prices_data:
                    market_addr = item.get("market_addr", "")
                    market_name = self.addr_to_market_name.get(market_addr, "")
                    mark_px = item.get("mark_px", "0")
                    if market_name:
                        res.append({
                            "symbol": market_name,
                            "price": str(mark_px),
                        })
        except Exception:
            self.logger().exception("Error fetching all pairs prices.")
        return res

    # ------------------------------------------------------------------
    # Status polling
    # ------------------------------------------------------------------

    async def _status_polling_loop_fetch_updates(self):
        await safe_gather(
            self._update_trade_history(),
            self._update_order_status(),
            self._update_balances(),
            self._update_positions(),
        )

    async def _update_order_status(self):
        await self._update_orders()

    async def _update_lost_orders_status(self):
        await self._update_lost_orders()

    # ------------------------------------------------------------------
    # Fee calculation
    # ------------------------------------------------------------------

    def _get_fee(
            self,
            base_currency: str,
            quote_currency: str,
            order_type: OrderType,
            order_side: TradeType,
            position_action: PositionAction,
            amount: Decimal,
            price: Decimal = s_decimal_NaN,
            is_maker: Optional[bool] = None,
    ) -> TradeFeeBase:
        is_maker = is_maker or False
        fee = build_trade_fee(
            self.name,
            is_maker,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )
        return fee

    async def _update_trading_fees(self):
        """Update fees information from the exchange."""
        pass

    # ------------------------------------------------------------------
    # Order placement
    # ------------------------------------------------------------------

    def buy(
            self,
            trading_pair: str,
            amount: Decimal,
            order_type=OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs,
    ) -> str:
        order_id = get_new_client_order_id(
            is_buy=True,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length,
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode("utf-8"))
        hex_order_id = f"0x{md5.hexdigest()}"

        if order_type is OrderType.MARKET:
            reference_price = self.get_mid_price(trading_pair) if price.is_nan() else price
            price = self.quantize_order_price(
                trading_pair, reference_price * Decimal(1 + CONSTANTS.MARKET_ORDER_SLIPPAGE)
            )

        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.BUY,
                order_id=hex_order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs,
            )
        )
        return hex_order_id

    def sell(
            self,
            trading_pair: str,
            amount: Decimal,
            order_type: OrderType = OrderType.LIMIT,
            price: Decimal = s_decimal_NaN,
            **kwargs,
    ) -> str:
        order_id = get_new_client_order_id(
            is_buy=False,
            trading_pair=trading_pair,
            hbot_order_id_prefix=self.client_order_id_prefix,
            max_id_len=self.client_order_id_max_length,
        )
        md5 = hashlib.md5()
        md5.update(order_id.encode("utf-8"))
        hex_order_id = f"0x{md5.hexdigest()}"

        if order_type is OrderType.MARKET:
            reference_price = self.get_mid_price(trading_pair) if price.is_nan() else price
            price = self.quantize_order_price(
                trading_pair, reference_price * Decimal(1 - CONSTANTS.MARKET_ORDER_SLIPPAGE)
            )

        safe_ensure_future(
            self._create_order(
                trade_type=TradeType.SELL,
                order_id=hex_order_id,
                trading_pair=trading_pair,
                amount=amount,
                order_type=order_type,
                price=price,
                **kwargs,
            )
        )
        return hex_order_id

    async def _place_order(
            self,
            order_id: str,
            trading_pair: str,
            amount: Decimal,
            trade_type: TradeType,
            order_type: OrderType,
            price: Decimal,
            position_action: PositionAction = PositionAction.NIL,
            **kwargs,
    ) -> Tuple[str, float]:
        """
        Place an order on Decibel via an on-chain Aptos transaction.

        Returns a tuple of (exchange_order_id, timestamp).
        The transaction hash is used as a temporary exchange_order_id; the real
        order_id will arrive via the WS order_updates channel.
        """
        market_name = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        market_addr = self.market_name_to_addr.get(market_name)
        if not market_addr:
            raise ValueError(f"Market address not found for {market_name}. Has trading rules been initialized?")

        subaccount = self._get_trading_account()

        # Map OrderType -> TIF
        if order_type is OrderType.LIMIT:
            tif = CONSTANTS.TIF_GTC
        elif order_type is OrderType.LIMIT_MAKER:
            tif = CONSTANTS.TIF_POST_ONLY
        elif order_type is OrderType.MARKET:
            tif = CONSTANTS.TIF_IOC
        else:
            tif = CONSTANTS.TIF_GTC

        is_buy = trade_type is TradeType.BUY
        reduce_only = position_action == PositionAction.CLOSE

        aptos_client = self._get_aptos_client()
        tx_hash = await aptos_client.place_order(
            subaccount=subaccount,
            market_addr=market_addr,
            price=price,
            size=amount,
            is_buy=is_buy,
            tif=tif,
            reduce_only=reduce_only,
            client_order_id=order_id,
        )

        # Use the transaction hash as a temporary exchange_order_id.
        # The real order_id will come from the WS order_updates channel and
        # update_exchange_order_id will be called at that point.
        return (str(tx_hash), self.current_timestamp)

    # ------------------------------------------------------------------
    # Order cancellation
    # ------------------------------------------------------------------

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        """
        Cancel an order on Decibel via an on-chain Aptos transaction.

        The exchange_order_id from the tracked order is the on-chain order ID (integer).
        """
        market_name = await self.exchange_symbol_associated_to_pair(
            trading_pair=tracked_order.trading_pair
        )
        market_addr = self.market_name_to_addr.get(market_name)
        if not market_addr:
            raise ValueError(f"Market address not found for {market_name}")

        subaccount = self._get_trading_account()

        try:
            exchange_order_id = tracked_order.exchange_order_id
            if not exchange_order_id:
                exchange_order_id = await tracked_order.get_exchange_order_id()
        except asyncio.TimeoutError:
            self.logger().warning(
                f"Order {order_id} does not have an exchange order id yet. Cannot cancel."
            )
            await self._order_tracker.process_order_not_found(order_id)
            raise IOError(f"Exchange order id not available for order {order_id}")

        # The exchange_order_id should be an integer (on-chain order ID).
        # If it's still the tx_hash (string), we may not be able to cancel yet.
        try:
            order_id_int = int(exchange_order_id)
        except (ValueError, TypeError):
            self.logger().warning(
                f"Cannot cancel order {order_id}: exchange_order_id '{exchange_order_id}' "
                f"is not a valid integer order ID (may still be a tx hash). "
                f"Waiting for WS order_updates to provide the real order ID."
            )
            await self._order_tracker.process_order_not_found(order_id)
            raise IOError(
                f"Exchange order id '{exchange_order_id}' is not a numeric on-chain order ID"
            )

        aptos_client = self._get_aptos_client()
        try:
            await aptos_client.cancel_order(
                subaccount=subaccount,
                market_addr=market_addr,
                order_id=order_id_int,
            )
            return True
        except Exception as e:
            if CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(e):
                self.logger().debug(
                    f"The order {order_id} does not exist on Decibel. No cancellation needed."
                )
                await self._order_tracker.process_order_not_found(order_id)
            raise

    # ------------------------------------------------------------------
    # Trade history polling
    # ------------------------------------------------------------------

    async def _update_trade_history(self):
        """
        Fetch recent trade fills for all fillable orders from the REST API.
        """
        orders = list(self._order_tracker.all_fillable_orders.values())
        all_fillable_orders = self._order_tracker.all_fillable_orders_by_exchange_order_id

        if len(orders) == 0:
            return

        try:
            trading_account = self._get_trading_account()
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.TRADE_HISTORY_URL,
                params={"account": trading_account},
                is_auth_required=True,
            )
            if isinstance(all_fills_response, list):
                for trade_fill in all_fills_response:
                    self._process_trade_rs_event_message(
                        order_fill=trade_fill,
                        all_fillable_order=all_fillable_orders,
                    )
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Failed to fetch trade updates. Error: {request_error}",
                exc_info=request_error,
            )

    def _process_trade_rs_event_message(self, order_fill: Dict[str, Any], all_fillable_order: Dict):
        """
        Process a single trade fill from the REST API response.

        Expected fields: trade_id, price, size, fee_amount, is_buy, order_id, client_order_id
        """
        exchange_order_id = str(order_fill.get("order_id", ""))
        fillable_order = all_fillable_order.get(exchange_order_id)

        if fillable_order is None:
            # Try matching by client_order_id
            client_oid = order_fill.get("client_order_id", "")
            if client_oid:
                for order in self._order_tracker.all_fillable_orders.values():
                    if order.client_order_id == client_oid:
                        fillable_order = order
                        break

        if fillable_order is not None:
            fee_asset = fillable_order.quote_asset
            fee_amount = Decimal(str(order_fill.get("fee_amount", "0")))

            position_action = fillable_order.position if fillable_order.position != PositionAction.NIL else PositionAction.OPEN

            fee = TradeFeeBase.new_perpetual_fee(
                fee_schema=self.trade_fee_schema(),
                position_action=position_action,
                percent_token=fee_asset,
                flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
            )

            fill_price = Decimal(str(order_fill.get("price", "0")))
            fill_size = Decimal(str(order_fill.get("size", "0")))

            trade_update = TradeUpdate(
                trade_id=str(order_fill.get("trade_id", "")),
                client_order_id=fillable_order.client_order_id,
                exchange_order_id=exchange_order_id,
                trading_pair=fillable_order.trading_pair,
                fee=fee,
                fill_base_amount=fill_size,
                fill_quote_amount=fill_price * fill_size,
                fill_price=fill_price,
                fill_timestamp=order_fill.get("unix_ms", time.time() * 1e3) * 1e-3,
            )
            self._order_tracker.process_trade_update(trade_update)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        # Trade updates are handled by _update_trade_history and WS user_trades
        return []

    # ------------------------------------------------------------------
    # Order status request
    # ------------------------------------------------------------------

    async def _handle_update_error_for_active_order(self, order: InFlightOrder, error: Exception):
        try:
            raise error
        except (asyncio.TimeoutError, KeyError):
            self.logger().debug(
                f"Tracked order {order.client_order_id} does not have an exchange id. "
                f"Attempting fetch in next polling interval."
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)
        except asyncio.CancelledError:
            raise
        except Exception as request_error:
            self.logger().warning(
                f"Error fetching status update for the active order {order.client_order_id}: {request_error}.",
            )
            await self._order_tracker.process_order_not_found(order.client_order_id)

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        """
        Fetch the status of a single order from the REST API.

        Decibel uses GET /api/v1/open_orders or /api/v1/order_history to look up orders.
        We first check open_orders, then fall back to order_history.
        """
        client_order_id = tracked_order.client_order_id
        trading_account = self._get_trading_account()

        try:
            if tracked_order.exchange_order_id:
                exchange_order_id = tracked_order.exchange_order_id
            else:
                exchange_order_id = await tracked_order.get_exchange_order_id()
        except asyncio.TimeoutError:
            exchange_order_id = None

        # Try open orders first
        order_data = await self._find_order_in_open_orders(trading_account, exchange_order_id, client_order_id)

        # Fall back to order history
        if order_data is None:
            order_data = await self._find_order_in_history(trading_account, exchange_order_id, client_order_id)

        if order_data is None:
            raise IOError(f"Order {client_order_id} (exchange: {exchange_order_id}) not found in API response")

        current_state = order_data.get("status", "open")
        resolved_exchange_order_id = str(
            tracked_order.exchange_order_id
            if tracked_order.exchange_order_id
            else order_data.get("order_id", "")
        )

        _order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_data.get("unix_ms", time.time() * 1e3) * 1e-3,
            new_state=CONSTANTS.ORDER_STATE.get(current_state, CONSTANTS.ORDER_STATE.get("open")),
            client_order_id=order_data.get("client_order_id", client_order_id),
            exchange_order_id=resolved_exchange_order_id,
        )
        return _order_update

    async def _find_order_in_open_orders(
            self, trading_account: str, exchange_order_id: Optional[str], client_order_id: str
    ) -> Optional[Dict[str, Any]]:
        """Search for an order in the open orders endpoint."""
        try:
            open_orders = await self._api_get(
                path_url=CONSTANTS.OPEN_ORDERS_URL,
                params={"account": trading_account},
                is_auth_required=True,
            )
            if isinstance(open_orders, list):
                for order in open_orders:
                    if exchange_order_id and str(order.get("order_id", "")) == str(exchange_order_id):
                        return order
                    if order.get("client_order_id", "") == client_order_id:
                        return order
        except Exception:
            self.logger().debug("Error searching open orders.", exc_info=True)
        return None

    async def _find_order_in_history(
            self, trading_account: str, exchange_order_id: Optional[str], client_order_id: str
    ) -> Optional[Dict[str, Any]]:
        """Search for an order in the order history endpoint."""
        try:
            order_history = await self._api_get(
                path_url=CONSTANTS.ORDER_HISTORY_URL,
                params={"account": trading_account},
                is_auth_required=True,
            )
            if isinstance(order_history, list):
                for order in order_history:
                    if exchange_order_id and str(order.get("order_id", "")) == str(exchange_order_id):
                        return order
                    if order.get("client_order_id", "") == client_order_id:
                        return order
        except Exception:
            self.logger().debug("Error searching order history.", exc_info=True)
        return None

    # ------------------------------------------------------------------
    # User stream event processing
    # ------------------------------------------------------------------

    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Decibel. Check API key and network connection.",
                )
                await self._sleep(1.0)

    async def _user_stream_event_listener(self):
        """
        Listens to messages from _user_stream_tracker.user_stream queue.
        Processes order updates and user trade events from WS.
        """
        user_topic_prefixes = [
            CONSTANTS.WS_ORDER_UPDATES_TOPIC,
            CONSTANTS.WS_USER_TRADES_TOPIC,
        ]
        async for event_message in self._iter_user_event_queue():
            try:
                if isinstance(event_message, dict):
                    topic: str = event_message.get("topic", "")
                    data = event_message.get("data", None)
                elif event_message is asyncio.CancelledError:
                    raise asyncio.CancelledError
                else:
                    raise Exception(event_message)

                matched = False
                for prefix in user_topic_prefixes:
                    if topic.startswith(prefix):
                        matched = True
                        break

                if not matched:
                    self.logger().error(
                        f"Unexpected message in user stream: {event_message}.", exc_info=True
                    )
                    continue

                if topic.startswith(CONSTANTS.WS_ORDER_UPDATES_TOPIC):
                    # data can be a single order update dict or a list
                    if isinstance(data, list):
                        for order_msg in data:
                            self._process_order_message(order_msg)
                    elif isinstance(data, dict):
                        self._process_order_message(data)

                elif topic.startswith(CONSTANTS.WS_USER_TRADES_TOPIC):
                    # data can be a single trade dict or a list
                    if isinstance(data, list):
                        for trade_msg in data:
                            await self._process_trade_message(trade_msg)
                    elif isinstance(data, dict):
                        await self._process_trade_message(data)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error(
                    "Unexpected error in user stream listener loop.", exc_info=True
                )
                await self._sleep(5.0)

    def _process_order_message(self, order_msg: Dict[str, Any]):
        """
        Process an order update from the WS order_updates channel.

        Expected fields: order_id, client_order_id, status, price, remaining_size, unix_ms
        """
        client_order_id = str(order_msg.get("client_order_id", ""))
        tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)

        if not tracked_order:
            # Fallback: try matching by exchange_order_id
            exchange_oid = str(order_msg.get("order_id", ""))
            if exchange_oid:
                for order in self._order_tracker.all_updatable_orders.values():
                    if order.exchange_order_id == exchange_oid:
                        tracked_order = order
                        client_order_id = order.client_order_id
                        break

        if not tracked_order:
            self.logger().debug(
                f"Ignoring order message with client_order_id {client_order_id}: not in in_flight_orders."
            )
            return

        exchange_order_id = str(order_msg.get("order_id", ""))
        current_state = order_msg.get("status", "open")

        # Update the exchange order ID if we only had the tx hash before
        if exchange_order_id:
            tracked_order.update_exchange_order_id(exchange_order_id)

        order_update: OrderUpdate = OrderUpdate(
            trading_pair=tracked_order.trading_pair,
            update_timestamp=order_msg.get("unix_ms", time.time() * 1e3) * 1e-3,
            new_state=CONSTANTS.ORDER_STATE.get(current_state, CONSTANTS.ORDER_STATE.get("open")),
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
        )
        self._order_tracker.process_order_update(order_update=order_update)

    async def _process_trade_message(self, trade: Dict[str, Any], client_order_id: Optional[str] = None):
        """
        Process a user trade from the WS user_trades channel.

        Expected fields: trade_id, price, size, fee_amount, is_buy, order_id, client_order_id
        """
        exchange_order_id = str(trade.get("order_id", ""))
        tracked_order = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(exchange_order_id)

        if tracked_order is None:
            # Try matching by client_order_id from the trade message
            trade_client_id = trade.get("client_order_id", "")
            if trade_client_id:
                tracked_order = self._order_tracker.all_fillable_orders.get(trade_client_id)

        if tracked_order is None:
            # Scan already-resolved exchange order IDs without blocking
            for order in self._order_tracker.all_fillable_orders.values():
                if order.exchange_order_id == exchange_order_id:
                    tracked_order = order
                    break

        if tracked_order is None:
            self.logger().debug(
                f"Ignoring trade message with order_id {exchange_order_id}: not in in_flight_orders."
            )
            return

        fee_asset = tracked_order.quote_asset
        fee_amount = Decimal(str(trade.get("fee_amount", "0")))

        position_action = tracked_order.position if tracked_order.position != PositionAction.NIL else PositionAction.OPEN

        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_asset,
            flat_fees=[TokenAmount(amount=fee_amount, token=fee_asset)],
        )

        fill_price = Decimal(str(trade.get("price", "0")))
        fill_size = Decimal(str(trade.get("size", "0")))

        trade_update: TradeUpdate = TradeUpdate(
            trade_id=str(trade.get("trade_id", "")),
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=tracked_order.trading_pair,
            fill_timestamp=trade.get("unix_ms", time.time() * 1e3) * 1e-3,
            fill_price=fill_price,
            fill_base_amount=fill_size,
            fill_quote_amount=fill_price * fill_size,
            fee=fee,
        )
        self._order_tracker.process_trade_update(trade_update)

    # ------------------------------------------------------------------
    # Balance tracking
    # ------------------------------------------------------------------

    async def _update_balances(self):
        """
        Fetch account balances from GET /api/v1/account_overviews.

        Response fields:
        - perp_equity_balance -> total balance
        - usdc_cross_withdrawable_balance -> available balance
        """
        try:
            trading_account = self._get_trading_account()
        except ValueError:
            self.logger().warning("Trading account not configured. Skipping balance update.")
            return

        account_info = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_OVERVIEW_URL,
            params={"account": trading_account},
            is_auth_required=True,
        )

        # The response may be a dict, a list with one element, or None
        if account_info is None:
            account_info = {}
        elif isinstance(account_info, list):
            account_info = account_info[0] if account_info else {}

        quote = CONSTANTS.CURRENCY
        self._account_balances[quote] = Decimal(
            str(account_info.get("perp_equity_balance", "0"))
        )
        self._account_available_balances[quote] = Decimal(
            str(account_info.get("usdc_cross_withdrawable_balance", "0"))
        )

    # ------------------------------------------------------------------
    # Position tracking
    # ------------------------------------------------------------------

    async def _update_positions(self):
        """
        Fetch account positions from GET /api/v1/account_positions.

        Response is a list of position dicts with fields:
        - market_addr, market_name
        - size: signed position size (positive = long, negative = short)
        - entry_price
        - unrealized_funding
        - estimated_liquidation_price
        - user_leverage
        """
        try:
            trading_account = self._get_trading_account()
        except ValueError:
            self.logger().warning("Trading account not configured. Skipping position update.")
            return

        all_positions = await self._api_get(
            path_url=CONSTANTS.ACCOUNT_POSITIONS_URL,
            params={"account": trading_account},
            is_auth_required=True,
        )

        if not isinstance(all_positions, list):
            all_positions = []

        for position_data in all_positions:
            market_name = position_data.get("market_name", "")

            try:
                hb_trading_pair = await self.trading_pair_associated_to_exchange_symbol(market_name)
            except KeyError:
                self.logger().debug(f"Skipping position for unmapped market: {market_name}")
                continue

            amount = Decimal(str(position_data.get("size", "0")))
            position_side = PositionSide.LONG if amount > 0 else PositionSide.SHORT
            entry_price = Decimal(str(position_data.get("entry_price", "0")))
            unrealized_pnl = Decimal(str(position_data.get("unrealized_pnl", "0")))
            leverage = Decimal(str(position_data.get("user_leverage", "1")))

            pos_key = self._perpetual_trading.position_key(hb_trading_pair, position_side)

            if amount != 0:
                _position = Position(
                    trading_pair=hb_trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage,
                )
                self._perpetual_trading.set_position(pos_key, _position)
            else:
                self._perpetual_trading.remove_position(pos_key)

        if not all_positions:
            keys = list(self._perpetual_trading.account_positions.keys())
            for key in keys:
                self._perpetual_trading.remove_position(key)

    # ------------------------------------------------------------------
    # Position mode / leverage
    # ------------------------------------------------------------------

    async def _get_position_mode(self) -> Optional[PositionMode]:
        return PositionMode.ONEWAY

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        msg = ""
        success = True
        initial_mode = await self._get_position_mode()
        if initial_mode != mode:
            msg = "Decibel only supports the ONEWAY position mode."
            success = False
        return success, msg

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        """
        Decibel leverage is set on-chain or via the exchange's interface.
        For now, we log the requested leverage but note that the actual leverage
        is managed by the exchange's risk engine per-subaccount.
        """
        self.logger().info(
            f"Leverage setting requested for {trading_pair}: {leverage}x. "
            f"Decibel manages leverage at the subaccount level."
        )
        # Return success to allow Hummingbot to track the leverage setting locally
        return True, ""

    # ------------------------------------------------------------------
    # Funding
    # ------------------------------------------------------------------

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[int, Decimal, Decimal]:
        """
        Fetch the last funding payment for a trading pair.

        Uses GET /api/v1/funding_rate_history with the trading account.
        """
        try:
            trading_account = self._get_trading_account()
            market_name = await self.exchange_symbol_associated_to_pair(trading_pair)
            market_addr = self.market_name_to_addr.get(market_name, "")

            funding_response = await self._api_get(
                path_url=CONSTANTS.FUNDING_RATE_HISTORY_URL,
                params={
                    "account": trading_account,
                    "market_addr": market_addr,
                },
                is_auth_required=True,
            )

            if isinstance(funding_response, list) and len(funding_response) > 0:
                # Take the most recent funding payment
                latest = funding_response[0]
                timestamp = latest.get("unix_ms", 0) * 1e-3
                funding_rate = Decimal(str(latest.get("funding_rate", "0")))
                payment = Decimal(str(latest.get("payment_amount", "0")))

                if payment != Decimal("0"):
                    return timestamp, funding_rate, payment

        except Exception:
            self.logger().debug(
                f"Error fetching funding payment for {trading_pair}.", exc_info=True
            )

        return 0, Decimal("-1"), Decimal("-1")

    # ------------------------------------------------------------------
    # Last traded price
    # ------------------------------------------------------------------

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        """
        Fetch the last traded / mark price for a trading pair via the prices endpoint.
        """
        try:
            market_name = await self.exchange_symbol_associated_to_pair(
                trading_pair=trading_pair,
            )
        except KeyError:
            # Trading pair not in symbol map yet, try extracting from trading pair
            market_name = trading_pair.split("-")[0] if "-" in trading_pair else trading_pair

        market_addr = self.market_name_to_addr.get(market_name, "")

        if market_addr:
            try:
                prices_data = await self._api_get(
                    path_url=CONSTANTS.PRICES_URL,
                    params={"market_addr": market_addr},
                )
                if isinstance(prices_data, list) and len(prices_data) > 0:
                    return float(prices_data[0].get("mark_px", 0))
                elif isinstance(prices_data, dict):
                    return float(prices_data.get("mark_px", 0))
            except Exception as e:
                self.logger().error(
                    f"Error fetching last traded price for {trading_pair}: {e}"
                )

        # Fallback: fetch all prices and find matching market
        try:
            all_prices = await self._api_get(path_url=CONSTANTS.PRICES_URL)
            if isinstance(all_prices, list):
                for item in all_prices:
                    item_addr = item.get("market_addr", "")
                    item_name = self.addr_to_market_name.get(item_addr, "")
                    if item_name == market_name or item_addr == market_addr:
                        return float(item.get("mark_px", 0))
        except Exception as e:
            self.logger().error(
                f"Error fetching all prices for {trading_pair}: {e}"
            )

        raise RuntimeError(
            f"Price not found for trading_pair={trading_pair}, market_name={market_name}"
        )
