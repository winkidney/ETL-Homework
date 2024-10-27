import time

import pytest

from etl_homework import flows
from etl_homework import models


@pytest.mark.xfail(reason="binance may not used in US area")
def test_coin_price_fetch(prefect_unittest_conf):
    ret = flows.task_fetch_current_coin_price("BTC", "USD")
    assert ret.id is not None
    cp = models.CoinPrice.select().where(models.CoinPrice.id == ret.id).get()
    assert cp is not None
    assert time.time() - cp.start_timestamp.timestamp() < 1, cp.start_timestamp
    assert cp.price > 0, cp.price
    assert models.CoinPrice.select().where(models.CoinPrice.id == ret.id).count() == 1


def test_btc_network_stats_update(prefect_unittest_conf):
    nd, nhr = flows.task_fetch_current_btc_network_hash_rate_and_difficulty()
    assert nd.id is not None, nd
    assert nhr.id is not None, nhr
    assert time.time() - nd.start_timestamp.timestamp() < 1, nd.start_timestamp
    assert nd.difficulty > 0, nd.difficulty
    assert nhr.hash_rate > 0, nhr.hash_rate
