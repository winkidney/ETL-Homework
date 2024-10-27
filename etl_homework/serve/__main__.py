from etl_homework import flows
from prefect import serve


def start_in_production():
    from etl_homework.dbs import bind_production_db

    bind_production_db()

    target_flows = [
        flows.hello_world.to_deployment(
            name="hello-world",
            cron="* * * * *",
        ),
        flows.flow_update_btc_network_stats_and_price.to_deployment(
            name="update-coin-price",
            cron="* * * * *",
        ),
    ]
    serve(*target_flows)


if __name__ == "__main__":
    start_in_production()
