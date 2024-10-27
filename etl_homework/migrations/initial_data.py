from etl_homework import dbs
from etl_homework import models
from etl_homework.models import NetworkHashRate, CoinPrice, GeneratedData

CRAWLER_TABLES = [models.NetworkDifficulty, NetworkHashRate, CoinPrice]
GENERATED_DATA_TABLES = [GeneratedData]


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
