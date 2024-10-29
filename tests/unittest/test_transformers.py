import timeit

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


@pytest.mark.parametrize(
    "is_super_big_int, ts_indexes, original_ts, original_data, expected",
    (
        (
            False,
            (0, 300, 600, 900, 1200),
            (0, 600, 900, 1200),
            (0, 60, 90, 120),
            (0, 30, 60, 90, 120),
        ),
        (
            False,
            (0, 300, 600, 900, 1200),
            (0, 600, 1200),
            (0, 60, 120),
            (0, 30, 60, 90, 120),
        ),
        (
            True,
            (0, 300, 600, 900, 1200),
            (0, 600, 900, 1200),
            (0, 200000000000000000000, 300000000000000000000, 400000000000000000000),
            (
                0,
                100000000000000000000,
                200000000000000000000,
                300000000000000000000,
                400000000000000000000,
            ),
        ),
        (
            True,
            (0, 300, 600, 900, 1200),
            (0, 600, 900, 1200),
            (0, 200000000000000000002, 300000000000000000004, 400000000000000000008),
            (
                0,
                100000000000000000001,
                200000000000000000002,
                300000000000000000004,
                400000000000000000008,
            ),
        ),
        (
            True,
            (0, 300, 600, 900, 1200),
            (0, 1200),
            (0, 800000000000000000016),
            (
                0,
                200000000000000000004,
                400000000000000000008,
                600000000000000000012,
                800000000000000000016,
            ),
        ),
    ),
)
def test_should_interpolate_data_as_expected(
    is_super_big_int, ts_indexes, original_ts, original_data, expected
):
    interpolated = transformers.interpolate_data(
        ts_indexes, original_ts, original_data, force_int=is_super_big_int
    )
    assert interpolated == expected


def test_should_interpolate_date_with_given_timeout():
    generated = transformers.generate_tf_indexes(1729900800, 1730073600, "5m")
    is_super_big_int, ts_indexes, original_ts, original_data, expected = (
        True,
        generated,
        (generated[0], generated[50], generated[200], generated[-1]),
        (0, 300000000000000000004, 400000000000000000008, 800000000000000000016),
        (0, 300000000000000000004, 400000000000000000008, 800000000000000000016),
    )
    assert len(
        transformers.interpolate_data(
            ts_indexes, original_ts, original_data, force_int=is_super_big_int
        )
    ) == len(generated)

    exper = "transformers.interpolate_data(ts_indexes, original_ts, original_data, force_int=is_super_big_int)"
    context = locals()
    context.update(globals())
    total_cost = timeit.timeit(
        exper,
        number=1000,
        globals=context,
    )
    assert total_cost <= 6
