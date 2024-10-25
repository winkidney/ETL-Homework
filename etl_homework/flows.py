from prefect import flow, task


@flow(log_prints=True)
def hello_world(name: str = "world", goodbye: bool = False):
    print(f"Hello {name} from Prefect! 🤗")

    if goodbye:
        print(f"Goodbye {name}!")
    return 0


@task(name="get-btc-price", task_run_name="get-btc-price-in-{currency}")
def task_fetch_coin_price(currency: str):
    pass


@flow(log_prints=True, retries=10, retry_delay_seconds=5)
def flow_update_coin_price():
    pass
