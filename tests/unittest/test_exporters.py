import csv
import time

import pytest

from etl_homework import exporters
from etl_homework import models
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


@pytest.fixture
def initial_metric(database, request):
    current_ts = int(time.time())
    previous_5m_ts = get_last_recent_n_minutes_timestamp(current_ts)
    valid_data = []
    for n in range(10):
        line = dict(
            timeframe="5m",
            start_timestamp=previous_5m_ts + (60 * 5 * n),
            price=n * 100,
            difficulty=n * 100,
            hash_rate=n * 1000,
        )
        valid_data.append(line)
    models.BTCUSDPriceWithNetStat.insert_many(valid_data).execute()

    def finalizer():
        models.BTCUSDPriceWithNetStat.delete().execute()

    request.addfinalizer(finalizer)

    return valid_data


class TestCoinMetrics:

    def test_should_export_full_csv_file(self, initial_metric):
        csv_name, file_path = exporters.CoinMetricsExporter.export()
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            all_data = list(reader)
            assert len(all_data) == len(initial_metric) + 1, all_data
            ts1 = all_data[1][1]  # first line
            ts2 = all_data[-1][1]  # last line
            assert ts2 > ts1

            try:
                int(all_data[1][1])
            except ValueError:
                assert False, (type(all_data[1][1]), all_data[1][1])

    @pytest.mark.parametrize(
        "offset_seconds, num_bars",
        (
            (-1, 10),
            (0, 10),
            (1, 9),
        ),
    )
    def test_should_export_expected_csv_file_with_start_ts(
        self, initial_metric, offset_seconds, num_bars
    ):
        start_ts = initial_metric[0]["start_timestamp"]
        csv_name, file_path = exporters.CoinMetricsExporter.export(
            start_timestamp=start_ts + offset_seconds
        )
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            all_data = list(reader)
            assert len(all_data) - 1 == num_bars, all_data
            ts1 = all_data[1][1]  # first line
            ts2 = all_data[-1][1]  # last line
            assert ts2 > ts1

    @pytest.mark.parametrize(
        "offset_seconds, num_bars",
        (
            (-1, 9),
            (0, 9),
            (1, 10),
        ),
    )
    def test_should_export_expected_csv_file_with_end_ts(
        self, initial_metric, offset_seconds, num_bars
    ):
        start_ts = initial_metric[-1]["start_timestamp"]
        csv_name, file_path = exporters.CoinMetricsExporter.export(
            end_timestamp=start_ts + offset_seconds
        )
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            all_data = list(reader)
            assert len(all_data) - 1 == num_bars, all_data
            ts1 = all_data[1][1]  # first line
            ts2 = all_data[-1][1]  # last line
            assert ts2 > ts1

    @pytest.mark.parametrize(
        "start_offset_seconds, end_offset_seconds, num_bars", ((1, -1, 8),)
    )
    def test_should_export_expected_csv_file_with_start_and_end_ts(
        self, initial_metric, start_offset_seconds, end_offset_seconds, num_bars
    ):
        start_ts = initial_metric[0]["start_timestamp"]
        end_ts = initial_metric[-1]["start_timestamp"]
        csv_name, file_path = exporters.CoinMetricsExporter.export(
            start_timestamp=start_ts + start_offset_seconds,
            end_timestamp=end_ts + end_offset_seconds,
        )
        with open(file_path, "r", encoding="utf-8") as csv_file:
            reader = csv.reader(csv_file)
            all_data = list(reader)
            assert len(all_data) - 1 == num_bars, all_data
            ts1 = all_data[1][1]  # first line
            ts2 = all_data[-1][1]  # last line
            assert ts2 > ts1
