from etl_homework import models
from etl_homework.crawler import TimeFrames
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


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


def interpolate_data(ts_indexes, original_data):
    pass


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
