from decimal import Decimal
from typing import Optional

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.00011"),  # 0.011% maker
    taker_percent_fee_decimal=Decimal("0.00034"),  # 0.034% taker
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = False  # On-chain DEX
EXAMPLE_PAIR = "BTC-PERP"
BROKER_ID = "HBOT"


class DecibelPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "decibel_perpetual"
    decibel_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Decibel API key (Bearer token for REST/WS)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos private key (hex, for signing on-chain transactions)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_trading_account: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your trading account (subaccount) address (leave blank to auto-discover)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="decibel_perpetual")


KEYS = DecibelPerpetualConfigMap.model_construct()

OTHER_DOMAINS = ["decibel_perpetual_testnet"]
OTHER_DOMAINS_PARAMETER = {"decibel_perpetual_testnet": "decibel_perpetual_testnet"}
OTHER_DOMAINS_EXAMPLE_PAIR = {"decibel_perpetual_testnet": "BTC-PERP"}
OTHER_DOMAINS_DEFAULT_FEES = {"decibel_perpetual_testnet": [0.011, 0.034]}


class DecibelPerpetualTestnetConfigMap(BaseConnectorConfigMap):
    connector: str = "decibel_perpetual_testnet"
    decibel_perpetual_testnet_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Decibel API key (Bearer token for REST/WS)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_testnet_secret_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aptos private key (hex, for signing on-chain transactions)",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    decibel_perpetual_testnet_trading_account: Optional[str] = Field(
        default=None,
        json_schema_extra={
            "prompt": "Enter your trading account (subaccount) address (leave blank to auto-discover)",
            "is_secure": False,
            "is_connect_key": True,
            "prompt_on_new": True,
        }
    )
    model_config = ConfigDict(title="decibel_perpetual_testnet")


OTHER_DOMAINS_KEYS = {
    "decibel_perpetual_testnet": DecibelPerpetualTestnetConfigMap.model_construct()
}
