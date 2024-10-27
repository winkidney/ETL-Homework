import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="module", autouse=False)
def prefect_unittest_conf():
    with prefect_test_harness():
        yield


@pytest.fixture(scope="function")
def mock_db():
    from peewee import SqliteDatabase
    from etl_homework import dbs

    dbs.generated_db = SqliteDatabase(":memory:")
    dbs.crawler_db = SqliteDatabase(":memory:")


@pytest.fixture(autouse=True, scope="function")
def database(mock_db):
    from etl_homework.migrations import initial_data

    initial_data.execute_migration()
