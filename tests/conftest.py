import pytest


@pytest.fixture(scope="session")
def mock_db(request):
    from etl_homework import dbs

    dbs.bind_test_db()


@pytest.fixture(autouse=True, scope="function")
def database(mock_db, request):
    from etl_homework.migrations import initial_data

    initial_data.execute_migration()

    def finalizer():
        initial_data.de_migration()

    request.addfinalizer(finalizer)
