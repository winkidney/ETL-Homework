import time

from prefect import flow, task, get_run_logger

from etl_homework.crawler import BinanceAPICrawler, MemPoolAPICrawler, TimeFrames
from etl_homework import models, dbs
from etl_homework.dbs import with_db
from etl_homework.transformers import CoinMetrics
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from Prefect! 🤗")

    if goodbye:
        print(f"Goodbye {name}!")
    return 0


@task(name="get-btc-price", task_run_name="get-btc-price-in-{coin}/{currency}")
def task_fetch_current_coin_price(coin: str, currency: str):
    logger = get_run_logger()
    client = BinanceAPICrawler(logger)
    recent_price = client.get_historic_price(
        coin=coin, currency=currency, timeframe=TimeFrames.FIVE_MINUTES
    )[-1]
    price_column, created = models.CoinPrice.get_or_create(
        coin=coin,
        currency=currency,
        start_timestamp=recent_price[0],
        price=recent_price[1],
        timeframe=TimeFrames.FIVE_MINUTES,
    )
    logger.info(
        f"\nPrice updated: pair={coin}/{currency} ts={price_column.start_timestamp}, created={created}, value={price_column.price}"
    )
    return price_column


@task(
    name="get-btc-hash-rate-and-difficulty",
    task_run_name="get-btc-hash-rate-and-difficulty",
)
def task_fetch_current_btc_network_hash_rate_and_difficulty():
    logger = get_run_logger()
    client = MemPoolAPICrawler(logger)
    hash_rate, difficulty = client.get_current_hash_rate_difficulty()
    recent_timestamp = get_last_recent_n_minutes_timestamp(int(time.time()), minutes=5)

    models.NetworkDifficulty.select("id").where(
        models.NetworkDifficulty.start_timestamp == recent_timestamp,
        models.NetworkDifficulty.timeframe == TimeFrames.FIVE_MINUTES,
    )
    nd, nd_created = models.NetworkDifficulty.get_or_create(
        network="BTC",
        start_timestamp=recent_timestamp,
        timeframe=TimeFrames.FIVE_MINUTES,
        defaults=dict(
            difficulty=difficulty,
        ),
    )
    nhr, nhr_created = models.NetworkHashRate.get_or_create(
        network="BTC",
        start_timestamp=recent_timestamp,
        timeframe=TimeFrames.FIVE_MINUTES,
        defaults=dict(
            hash_rate=hash_rate,
        ),
    )
    logger.info(
        f"\nHashRate:  ts={nhr.start_timestamp}, created={nd_created}, value={nhr.hash_rate}"
        + f"\nDifficulty: ts={nd.start_timestamp}, created={nhr_created}, value={nd.difficulty}"
    )
    return nd, nhr


@task(
    name="transform-coin-network-status-and-price",
    task_run_name="transform-coin-network-status-and-price",
)
def transform_coin_network_status_and_price(
    price: models.CoinPrice, nd: models.NetworkDifficulty, nhr: models.NetworkHashRate
):
    metric, created = CoinMetrics.merge_coin_metric(price, nd, nhr)
    logger = get_run_logger()
    logger.info(
        (
            f"\nCoinMetric:  ts={nhr.start_timestamp}, created={created},"
            f" price/hash_rate/difficulty={metric.price, metric.hash_rate, metric.difficulty}"
        )
    )


@flow(log_prints=True, retries=10, retry_delay_seconds=5)
@with_db
def flow_update_btc_network_stats_and_price():
    with dbs.crawler_db_proxy.atomic():
        # TODO(winkidney): concurrent execution for fetching task
        nd, nhr = task_fetch_current_btc_network_hash_rate_and_difficulty()
        price = task_fetch_current_coin_price(coin="BTC", currency="USD")
        transform_coin_network_status_and_price(price, nd, nhr)


@flow(log_prints=True, retries=10, retry_delay_seconds=5)
def send_report_for_btc_network_stats_and_price():
    pass
