# test/hummingbot/connector/derivative/decibel_perpetual/test_decibel_perpetual_web_utils.py
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.derivative.decibel_perpetual import decibel_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.decibel_perpetual.decibel_perpetual_web_utils import (
    build_api_factory,
    build_api_factory_without_time_synchronizer_pre_processor,
    create_throttler,
    get_current_server_time,
    is_exchange_information_valid,
    private_rest_url,
    public_rest_url,
    rest_url,
    wss_url,
    DecibelPerpetualRESTPreProcessor,
)


class DecibelPerpetualWebUtilsTest(IsolatedAsyncioWrapperTestCase):
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

    def test_private_rest_url_delegates_to_rest_url(self):
        url = private_rest_url("/api/v1/orders", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(url, f"{CONSTANTS.TESTNET_BASE_URL}/api/v1/orders")

    def test_public_rest_url_delegates_to_rest_url(self):
        url = public_rest_url("/api/v1/markets", domain=CONSTANTS.TESTNET_DOMAIN)
        self.assertEqual(url, f"{CONSTANTS.TESTNET_BASE_URL}/api/v1/markets")

    def test_create_throttler(self):
        throttler = create_throttler()
        self.assertIsNotNone(throttler)

    def test_build_api_factory_without_time_synchronizer(self):
        throttler = create_throttler()
        factory = build_api_factory_without_time_synchronizer_pre_processor(throttler)
        self.assertIsNotNone(factory)

    def test_build_api_factory_default(self):
        factory = build_api_factory()
        self.assertIsNotNone(factory)

    async def test_get_current_server_time(self):
        throttler = create_throttler()
        t = await get_current_server_time(throttler, CONSTANTS.TESTNET_DOMAIN)
        self.assertIsInstance(t, float)
        self.assertGreater(t, 0)

    async def test_rest_pre_processor_sets_headers(self):
        from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
        processor = DecibelPerpetualRESTPreProcessor()
        request = RESTRequest(method=RESTMethod.GET, url="https://example.com")
        result = await processor.pre_process(request)
        self.assertEqual(result.headers["Content-Type"], "application/json")

    async def test_rest_pre_processor_preserves_existing_headers(self):
        from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTMethod
        processor = DecibelPerpetualRESTPreProcessor()
        request = RESTRequest(method=RESTMethod.GET, url="https://example.com", headers={"X-Custom": "value"})
        result = await processor.pre_process(request)
        self.assertEqual(result.headers["Content-Type"], "application/json")
        self.assertEqual(result.headers["X-Custom"], "value")
