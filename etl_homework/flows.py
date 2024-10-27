import time

from prefect import flow, task, get_run_logger
from prefect.transactions import transaction

from etl_homework.crawler import BinanceAPICrawler, MemPoolAPICrawler, TimeFrames
from etl_homework import models, dbs
from etl_homework.dbs import with_db
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from Prefect! 🤗")

    if goodbye:
        print(f"Goodbye {name}!")
    return 0


@task(name="get-btc-price", task_run_name="get-btc-price-in-{coin}/{currency}")
def task_fetch_current_coin_price(coin: str, currency: str):
    client = BinanceAPICrawler(get_run_logger())
    recent_price = client.get_historic_price(
        coin=coin, currency=currency, timeframe=TimeFrames.FIVE_MINUTES
    )[-1]
    price_column = models.CoinPrice(
        coin=coin,
        currency=currency,
        timestamp=recent_price[0],
        price=recent_price[1],
        timeframe=TimeFrames.FIVE_MINUTES,
    )
    price_column.save()
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
    with dbs.crawler_db_proxy.atomic():
        nd = models.NetworkDifficulty(
            network="BTC",
            start_timestamp=recent_timestamp,
            timeframe=TimeFrames.FIVE_MINUTES,
            difficulty=difficulty,
        )
        nhr = models.NetworkHashRate(
            network="BTC",
            start_timestamp=recent_timestamp,
            timeframe=TimeFrames.FIVE_MINUTES,
            hash_rate=hash_rate,
        )
        nd.save()
        nhr.save()
    logger.info(
        f"\nHashRate:   ts={nhr.start_timestamp}, value={nhr.hash_rate}"
        + f"\nDifficulty: ts={nd.start_timestamp}, value={nd.difficulty}"
    )
    return nd, nhr


@flow(log_prints=True, retries=10, retry_delay_seconds=5)
@with_db
def flow_update_btc_network_stats_and_price():
    task_fetch_current_btc_network_hash_rate_and_difficulty()
    task_fetch_current_coin_price(coin="BTC", currency="USD")
