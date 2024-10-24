import os
import pytest
import sys


@pytest.fixture(autouse=True)
def conf_python_path():
    sys.path.append(
        os.path.dirname(
            os.path.join(
                os.path.abspath(__file__),
                "../",
            )
        )
    )
