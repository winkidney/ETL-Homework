import os
import time

import dotenv

from prefect import flow, task, get_run_logger
from prefect_email import EmailServerCredentials, email_send_message

from etl_homework.crawler import BinanceAPICrawler, MemPoolAPICrawler, TimeFrames
from etl_homework import models, dbs
from etl_homework.dbs import with_db
from etl_homework.transformers import CoinMetrics
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp
from etl_homework import exporters


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
        timeframe=TimeFrames.FIVE_MINUTES,
        defaults=dict(price=recent_price[1]),
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


def task_send_csv_report(csv_report: str):
    """
    :param csv_report: abs path of the report
    """
    dotenv.load_dotenv()
    logger = get_run_logger()

    email_server_credentials = EmailServerCredentials.load("etl-gmail-sender")
    target_emails = os.environ.get("REPORT_EMAIL_TARGETS")

    if not target_emails:
        logger.info("skip email sending since no email detected in OS-ENV")
        return
    target_emails = target_emails.split(",")

    subjects = []
    for email_address in target_emails:
        title = "ETL Report of <BTC-Network-Status-and-Price>"
        subject = email_send_message.with_options(
            name=f"{title}: {email_address}"
        ).submit(
            email_server_credentials=email_server_credentials,
            subject=title,
            msg="this email contains the report for server",
            email_to=email_address,
            attachments=[csv_report],
        )
        subjects.append(subject)

    for subject in subjects:
        subject.wait()
    return subjects


@task(name="bootstrap-to-fetch-history-data")
def task_btc_network_stats_and_price_bootstrap():
    """
    load initial data from network crawlers
    """
    coin = "BTC"
    network = "BTC"
    currency = "USD"
    timeframe = TimeFrames.FIVE_MINUTES

    logger = get_run_logger()

    cp_cls = models.CoinPrice
    nhr_cls = models.NetworkHashRate
    nd_cls = models.NetworkDifficulty

    last_recent_coin_price = (
        cp_cls.select().order_by(cp_cls.start_timestamp.desc()).first()
    )
    last_recent_hash_rate = (
        nhr_cls.select().order_by(nhr_cls.start_timestamp.desc()).first()
    )
    last_recent_difficulty = (
        nd_cls.select().order_by(nd_cls.start_timestamp.desc()).first()
    )
    cp_start_ts, nhr_start_ts, nd_start_ts = 0, 0, 0

    if last_recent_coin_price:
        cp_start_ts = last_recent_coin_price.start_timestamp
    if last_recent_hash_rate:
        nhr_start_ts = last_recent_hash_rate.start_timestamp
    if last_recent_difficulty:
        nd_start_ts = last_recent_difficulty.start_timestamp

    client = MemPoolAPICrawler(logger)
    bn_client = BinanceAPICrawler(logger)
    history_price = bn_client.get_historic_price(limit=bn_client.NUM_MAX_KLINE_CANDLES)
    history_difficulty = client.get_historic_difficulty()
    history_hash_rate = client.get_historic_hash_rate()

    actual_start_ts = max(
        (history_price[0][0], history_difficulty[0][0], history_hash_rate[0][0])
    )
    valid_data_ends_at = min((cp_start_ts, nhr_start_ts, nd_start_ts))
    actual_start_ts = max((actual_start_ts, valid_data_ends_at))
    logger.info(f"actual start ts is: {actual_start_ts}")
    history_price = tuple(filter(lambda x: x[0] >= actual_start_ts, history_price))
    history_difficulty = tuple(
        filter(lambda x: x[0] >= actual_start_ts, history_difficulty)
    )
    history_hash_rate = tuple(
        filter(lambda x: x[0] >= actual_start_ts, history_hash_rate)
    )
    for price, difficulty, hash_rate in zip(
        history_price, history_difficulty, history_hash_rate
    ):
        price_column, price_created = models.CoinPrice.get_or_create(
            coin=coin,
            currency=currency,
            start_timestamp=price[0],
            timeframe=timeframe,
            defaults=dict(
                price=price[1],
            ),
        )
        nd, nd_created = models.NetworkDifficulty.get_or_create(
            network=network,
            start_timestamp=difficulty[0],
            timeframe=timeframe,
            defaults=dict(
                difficulty=difficulty[1],
            ),
        )
        nhr, nhr_created = models.NetworkHashRate.get_or_create(
            network=network,
            start_timestamp=hash_rate[0],
            timeframe=timeframe,
            defaults=dict(
                hash_rate=hash_rate[1],
            ),
        )
        metric, metric_created = CoinMetrics.merge_coin_metric(price_column, nd, nhr)
        logger.info(
            (
                "cold-starter: price/difficulty/hash-rate/metric"
                f" ts={metric.start_timestamp}"
                f" created={price_created, nd_created, nhr_created, metric_created}"
                f" values={price_column.price, nd.difficulty, nhr.hash_rate}..."
            )
        )

    return history_price, history_difficulty, history_hash_rate


@flow(log_prints=True, retries=10, retry_delay_seconds=5, timeout_seconds=50)
@with_db
def flow_cold_start():
    task_btc_network_stats_and_price_bootstrap()


@flow(log_prints=True, retries=2, retry_delay_seconds=5, timeout_seconds=10)
@with_db
def flow_send_full_report_for_btc_network_stats_and_price():
    _, csv_report = exporters.CoinMetricsExporter.export()
    task_send_csv_report(csv_report)
    os.remove(csv_report)
