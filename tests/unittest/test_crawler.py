import pytest

from etl_homework import crawler


@pytest.mark.parametrize(
    "req_ts, last_price_ts, num_minutes, is_valid",
    (
        (0, 0, 10, True),
        (600, 600, 10, True),
        (650, 600, 10, True),
        (660, 600, 10, True),
        (1199, 600, 10, True),
        (1200, 600, 10, False),
    ),
)
def test_should_price_ts_validation_works_as_expected(
    req_ts, last_price_ts, num_minutes, is_valid
):
    assert crawler.is_price_ts_valid(req_ts, last_price_ts, num_minutes) is is_valid
