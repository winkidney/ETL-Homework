import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="session", autouse=True)
def prefect_unittest_conf():
    with prefect_test_harness():
        yield
