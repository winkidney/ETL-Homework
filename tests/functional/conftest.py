import os

import dotenv
import pytest


@pytest.fixture(scope="session")
def project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))


@pytest.fixture
def load_dotenv(request, project_root):
    succeed = dotenv.load_dotenv(os.path.join(project_root, ".env"))
    assert succeed

    def finalizer():
        for k, v in dotenv.dotenv_values().items():
            if k in os.environ:
                del os.environ[k]

    request.addfinalizer(finalizer)
    return succeed
