import time

import pytest

from etl_homework import flows
from etl_homework import models


def is_load_dotenv():
    return True


def test_coin_price_fetch(prefect_unittest_conf, request, load_dotenv):
    if not load_dotenv:
        mark = pytest.mark.xfail(reason="binance may not used in US area")
        request.node.add_marker(mark)

    ret = flows.task_fetch_current_coin_price("BTC", "USD")
    assert ret.id is not None
    cp = models.CoinPrice.select().where(models.CoinPrice.id == ret.id).get()
    assert cp is not None
    assert time.time() > cp.start_timestamp > 0, cp.start_timestamp
    assert cp.price > 0, cp.price
    assert models.CoinPrice.select().where(models.CoinPrice.id == ret.id).count() == 1


def test_btc_network_stats_update(prefect_unittest_conf):
    nd, nhr = flows.task_fetch_current_btc_network_hash_rate_and_difficulty()
    assert nd.id is not None, nd
    assert nhr.id is not None, nhr
    nd = (
        models.NetworkDifficulty.select()
        .where(models.NetworkDifficulty.id == nd.id)
        .get()
    )
    nhr = (
        models.NetworkHashRate.select().where(models.NetworkHashRate.id == nhr.id).get()
    )
    assert time.time() > nd.start_timestamp > 0, nd.start_timestamp
    assert time.time() > nhr.start_timestamp > 0, nd.start_timestamp
    assert nd.difficulty > 0, nd.difficulty
    assert nhr.hash_rate > 0, nhr.hash_rate


def test_should_code_start_insert_correct_data(
    prefect_unittest_conf, load_dotenv, request
):
    if not load_dotenv:
        mark = pytest.mark.xfail(reason="binance may not used in US area")
        request.node.add_marker(mark)
    history_price, history_difficulty, history_hash_rate = (
        flows.task_btc_network_stats_and_price_bootstrap()
    )
    assert len(history_price) == len(history_difficulty)
    assert len(history_hash_rate) == len(history_difficulty)
    assert models.CoinPrice.select().count() == len(history_price), history_price
    assert models.NetworkDifficulty.select().count() == len(
        history_difficulty
    ), history_difficulty
    assert models.NetworkHashRate.select().count() == len(
        history_hash_rate
    ), history_hash_rate
    assert models.BTCUSDPriceWithNetStat.select().count() == len(history_price)
