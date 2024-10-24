import os
import signal
import time

import pytest
import subprocess

import requests


@pytest.fixture(scope="session")
def prefect_test_server(request):
    process = subprocess.Popen(
        ["make", "serve-api"],
        shell=True,
    )

    def finalizer():
        os.kill(process.pid, signal.SIGTERM)
        process.wait()

    request.addfinalizer(finalizer)

    start_time = time.time()
    while time.time() - start_time < 10:
        try:
            resp = requests.get("http://localhost:4222", timeout=(2, 2))
        except requests.exceptions.ConnectionError:
            time.sleep(1)
            continue
        if resp.status_code != 200:
            continue

    return process
