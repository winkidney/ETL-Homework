import numpy as np
import pandas as pd

from etl_homework import models
from etl_homework.constants import TimeFrames
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


class DataQualityTooLow(ValueError):
    pass


def get_ts_range_by_timeframe(
    start_timestamp: int, end_timestamp: int, time_frame: str
):
    """
    前开后闭 [start_ts, end_ts) 的方式，按指定的 frequency/time_frame 寻找时间点
    :param start_timestamp:
    :param end_timestamp:
    :param time_frame:
    :return:
    """
    tf_minutes = TimeFrames.to_int(time_frame)
    step_seconds = tf_minutes * 60

    start_ts = get_last_recent_n_minutes_timestamp(start_timestamp, tf_minutes)
    if start_timestamp > start_ts:
        start_ts += step_seconds
    end_ts = get_last_recent_n_minutes_timestamp(end_timestamp, tf_minutes)
    if end_timestamp <= end_ts:
        end_ts = end_ts - step_seconds
    if end_ts < start_ts:
        return None, None, 0
    if start_ts == end_ts:
        return start_ts, end_ts, 1
    else:
        num_ts = (end_ts - start_ts) // step_seconds
        if end_timestamp > end_ts:
            num_ts += 1
        return start_ts, end_ts, num_ts


def generate_tf_indexes(start_timestamp: int, end_timestamp: int, time_frame: str):
    step_minutes = TimeFrames.to_int(time_frame)
    start_ts, end_ts, num_ts = get_ts_range_by_timeframe(
        start_timestamp, end_timestamp, time_frame
    )
    indexes = tuple(
        ((start_ts + (step_minutes * 60 * index)) for index in range(num_ts))
    )
    return indexes


def interpolate_data_for_small_numbers(
    ts_indexes,
    original_ts_list: list or tuple,
    original_data: list or tuple,
    out_type=float,
):
    columns = ("2be_interpolate",)
    data_set = {
        "2be_interpolate": original_data,
    }
    series = pd.DataFrame(
        data=data_set,
        index=original_ts_list,
        columns=columns,
    )
    series["2be_interpolate"] = pd.to_numeric(series["2be_interpolate"])
    series = series.reindex(index=ts_indexes)
    if str(series.iloc[0, 0]) == str(np.nan):
        raise DataQualityTooLow("quality low, starting point missed")
    if str(series.iloc[-1, 0]) == str(np.nan):
        raise DataQualityTooLow("quality low, end point missed")
    interpolated = series.interpolate(method="linear", columns=columns)
    out_data = tuple(interpolated["2be_interpolate"].astype(out_type))
    return out_data


def interpolate_data(
    ts_indexes,
    original_ts_list: list or tuple,
    original_data: list or tuple,
    force_int=False,
):
    """
    Perform linear interpolation on given time series data.
    interpolate method in Numpy and pandas doesn't support python's super-big int type.
    hash-rate number is out of range of Int64 or Float64.

    :param ts_indexes: The list of time indices for the full range.
    :param original_ts_list: The original timestamps corresponding to the original data.
    :param original_data: The list of original data points, with None for missing values.
    :param force_int: force the type to handle super-big int, required for super-big int.
    :return: tuple: A tuple of interpolated values in the specified output type.
    """
    # TODO(winkidney): may have performance issue on super-large arrays.
    ts_indexes = ts_indexes
    original_ts_list = original_ts_list
    original_data = original_data

    interpolated_data = [None] * len(ts_indexes)

    for original_ts, data in zip(original_ts_list, original_data):
        if data is not None:
            index = ts_indexes.index(original_ts)
            interpolated_data[index] = data

    for idx2fill in range(len(interpolated_data)):
        if interpolated_data[idx2fill] is None:
            # Find the nearest known points on the left and right
            left_index = None
            right_index = None

            for index in range(idx2fill - 1, -1, -1):
                if interpolated_data[index] is not None:
                    left_index = index
                    break

            for index in range(idx2fill + 1, len(interpolated_data)):
                if interpolated_data[index] is not None:
                    right_index = index
                    break

            if left_index is not None and right_index is not None:
                x0 = ts_indexes[left_index]
                y0 = interpolated_data[left_index]
                x1 = ts_indexes[right_index]
                y1 = interpolated_data[right_index]

                if force_int:
                    interpolated_data[idx2fill] = y0 + (y1 - y0) * (
                        ts_indexes[idx2fill] - x0
                    ) // (x1 - x0)
                else:
                    interpolated_data[idx2fill] = y0 + (y1 - y0) * (
                        ts_indexes[idx2fill] - x0
                    ) / (x1 - x0)

    return tuple(interpolated_data)


class CoinMetrics:

    @classmethod
    def merge_coin_metric(
        cls,
        price: models.CoinPrice,
        nd: models.NetworkDifficulty,
        nhr: models.NetworkHashRate,
    ) -> (models.BTCUSDPriceWithNetStat, bool):
        if not (
            price.start_timestamp == nd.start_timestamp
            and nd.start_timestamp == nhr.start_timestamp
        ):
            raise ValueError(
                "failed to verify coin metrics, start_timestamp not the same"
            )
        obj, created = models.BTCUSDPriceWithNetStat.get_or_create(
            timeframe=price.timeframe,
            start_timestamp=price.start_timestamp,
            defaults=dict(
                price=price.price,
                hash_rate=nhr.hash_rate,
                difficulty=nd.difficulty,
            ),
        )
        return obj, created
