from etl_homework import dbs
from etl_homework import models
from etl_homework.models import NetworkHasRate, CoinPrice


def execute_migration():
    dbs.crawler_db.create_tables(
        [models.NetworkDifficulty, NetworkHasRate, CoinPrice],
    )
    dbs.generated_db.create_tables(
        [models.GeneratedData],
    )


if __name__ == "__main__":
    execute_migration()
