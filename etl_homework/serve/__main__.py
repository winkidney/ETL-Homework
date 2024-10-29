from etl_homework import flows
from prefect import serve

from etl_homework.dbs import bind_production_db


def start_in_production():
    bind_production_db()
    target_flows = [
        flows.flow_send_full_report_for_btc_network_stats_and_price.to_deployment(
            name="send-daily-csv-report",
            cron="1 0 * * *",
        ),
        flows.flow_update_btc_network_stats_and_price.to_deployment(
            name="update-coin-price",
            cron="* * * * *",
        ),
    ]
    serve(*target_flows)


if __name__ == "__main__":
    start_in_production()
