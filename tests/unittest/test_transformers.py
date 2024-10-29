import pytest

from etl_homework import transformers


@pytest.mark.parametrize(
    "start_timestamp, end_timestamp, expected_start_ts, expected_end_ts, expected_num_ts",
    (
        (0, 0, None, None, 0),
        (1, 60 * 5 * 1 - 1, None, None, 0),
        (1, 60 * 5 * 1 - 1, None, None, 0),
        (0, 60 * 5 * 1 - 1, 0, 0, 1),
        (0, 60 * 5 * 1, 0, 0, 1),
        (0, 60 * 5 * 1 + 1, 0, 60 * 5 * 1, 2),
        (0, 60 * 5 * 2 - 1, 0, 60 * 5 * 1, 2),
        (1, 60 * 5 * 2 - 1, 60 * 5 * 1, 60 * 5 * 1, 1),
    ),
)
def test_should_get_ts_range_by_timeframe(
    start_timestamp, end_timestamp, expected_start_ts, expected_end_ts, expected_num_ts
):
    ret = transformers.get_ts_range_by_timeframe(
        start_timestamp,
        end_timestamp,
        time_frame="5m",
    )
    start_ts, end_ts, num_ts = ret

    assert num_ts == expected_num_ts, ret
    if expected_num_ts > 0:
        assert start_ts == expected_start_ts, ret
        assert end_ts == expected_end_ts, ret


@pytest.mark.parametrize(
    "start_timestamp, end_timestamp, start_ts, end_ts, expected_num_ts",
    (
        (0, 0, None, None, 0),
        (1, 2, None, None, 0),
        (0, 60 * 5 * 1 - 1, 0, 0, 1),
        (0, 60 * 5 * 1, 0, 0, 1),
        (0, 60 * 5 * 1 + 1, 0, 60 * 5, 2),
        (0, 60 * 5 * 2 - 1, 0, 60 * 5 * 1, 2),
        (0, 60 * 5 * 2 + 1, 0, 60 * 5 * 2, 3),
        (1, 60 * 5 * 2 + 1, 60 * 5, 60 * 5 * 2, 2),
    ),
)
def test_should_get_correct_tf_indexes(
    start_timestamp,
    end_timestamp,
    expected_num_ts,
    start_ts,
    end_ts,
):
    indexes = transformers.generate_tf_indexes(start_timestamp, end_timestamp, "5m")
    assert len(indexes) == expected_num_ts
    if expected_num_ts > 0:
        assert indexes[0] == start_ts, indexes
        assert indexes[-1] == end_ts, indexes
