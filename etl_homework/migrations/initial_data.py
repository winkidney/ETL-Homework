from etl_homework import dbs
from etl_homework import models

CRAWLER_TABLES = [
    models.NetworkDifficulty,
    models.NetworkHashRate,
    models.CoinPrice,
]
GENERATED_DATA_TABLES = [
    models.BTCUSDPriceWithNetStat,
]


def execute_migration():
    dbs.crawler_db_proxy.create_tables(CRAWLER_TABLES)
    dbs.generated_db_proxy.create_tables(GENERATED_DATA_TABLES)


def de_migration():
    dbs.crawler_db_proxy.drop_tables(
        CRAWLER_TABLES,
    )
    dbs.generated_db_proxy.drop_tables(
        GENERATED_DATA_TABLES,
    )


if __name__ == "__main__":
    dbs.bind_production_db()
    execute_migration()
