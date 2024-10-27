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

    @pytest.mark.xfail(reason="binance may fail in US area")
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
        assert len(historic_hash_rate) == 30, historic_hash_rate

    def test_should_return_historic_difficulty(self, mempool_client):
        historic_difficulty = mempool_client.get_historic_difficulty()
        assert len(historic_difficulty) > 0, historic_difficulty
        assert len(historic_difficulty[0]) == 4, historic_difficulty

    def test_should_return_current_hash_rate_and_difficulty(self, mempool_client):
        hash_rate, difficulty = mempool_client.get_current_hash_rate()
        assert isinstance(hash_rate, int), hash_rate
        assert isinstance(difficulty, float), difficulty
