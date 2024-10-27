import prefect.logging
from ccxt.static_dependencies.marshmallow.utils import timestamp
from prefect import flow, task, get_run_logger

from etl_homework.crawler import BinanceAPICrawler
from etl_homework.models import CoinPrice


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from Prefect! 🤗")

    if goodbye:
        print(f"Goodbye {name}!")
    return 0


@task(name="get-btc-price", task_run_name="get-btc-price-in-{coin}/{currency}")
def task_fetch_current_coin_price(coin: str, currency: str):
    client = BinanceAPICrawler(get_run_logger())
    recent_price = client.get_historic_price(coin=coin, currency=currency)[-1]
    price_column = CoinPrice(
        coin=coin,
        currency=currency,
        timestamp=recent_price[0],
        price=recent_price[1],
    )
    price_column.save()
    return price_column


@flow(log_prints=True, retries=10, retry_delay_seconds=5)
def flow_update_coin_price():
    return task_fetch_current_coin_price(coin="BTC", currency="USD")
