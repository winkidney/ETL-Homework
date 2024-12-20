import os

import dotenv
import pytest
from prefect.testing.utilities import prefect_test_harness


@pytest.fixture(scope="module", autouse=False)
def prefect_unittest_conf():
    with prefect_test_harness():
        yield


@pytest.fixture(scope="session")
def project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


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
