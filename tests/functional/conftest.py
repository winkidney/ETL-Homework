import os

import dotenv
import pytest


@pytest.fixture(scope="session")
def project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


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


@pytest.fixture
def load_dotenv(request, project_root):
    env_file = os.path.join(project_root, ".env")
    if not os.path.exists(env_file):
        return
    succeed = dotenv.load_dotenv(env_file)

    def finalizer():
        for k, v in dotenv.dotenv_values().items():
            if k in os.environ:
                del os.environ[k]

    request.addfinalizer(finalizer)
    return succeed
