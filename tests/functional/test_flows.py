import time

from etl_homework import flows
from etl_homework import models


def test_coin_price_fetch(prefect_unittest_conf):
    ret = flows.task_fetch_current_coin_price("BTC", "USD")
    assert ret.id is not None
    cp = models.CoinPrice.select().where(models.CoinPrice.id == ret.id).get()
    assert cp is not None
    assert time.time() - cp.start_timestamp.timestamp() < 1, cp.start_timestamp
    assert cp.price > 0, cp.price
