import time

import pytest

from etl_homework import crawler


@pytest.fixture
def biannce_client(load_dotenv):
    return crawler.BinanceAPICrawler()


@pytest.fixture
def mempool_client():
    return crawler.MemPoolAPICrawler()


class TestBinanceCrawler:

    def test_should_return_historic_btc_prices(self, biannce_client):
        historical_prices = biannce_client.get_historic_price(limit=10)
        for ts, price in historical_prices:
            print(
                f"时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))}, 价格: ${price}"
            )
        assert len(historical_prices) == 10, historical_prices


class TestMemPoolCrawler:

    def test_should_return_historic_hash_rate(self, mempool_client):
        historic_hash_rate = mempool_client.get_historic_hash_rate()
        assert len(historic_hash_rate) > 0, historic_hash_rate
