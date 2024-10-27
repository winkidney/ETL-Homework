from etl_homework import flows


def test_coin_price_fetch(prefect_unittest_conf):
    ret = flows.task_fetch_current_coin_price("BTC", "USD")
    assert ret.id is not None
