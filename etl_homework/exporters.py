import csv
import datetime
import os
import time
from tempfile import gettempdir

from etl_homework import models
from etl_homework.crawler import TimeFrames
from etl_homework.utils.time import get_last_recent_n_minutes_timestamp


class CoinMetricsExporter:

    @classmethod
    def export_previous_utc_day_report(cls):
        # should always be executed after 24H to get correct start timestamp
        a_day = 60 * 60 * 24
        start_ts = get_last_recent_n_minutes_timestamp(int(time.time()), a_day)
        start_ts = get_last_recent_n_minutes_timestamp(start_ts - 60, a_day)
        return cls.export(
            start_timestamp=start_ts,
        )

    @classmethod
    def export(
        cls,
        start_timestamp=None,
        end_timestamp=None,
        timeframe=TimeFrames.FIVE_MINUTES,
    ) -> (str, str):
        # TODO(winkidney): optmize CSV export speed by reduce cost of object-serialize.
        md = models.BTCUSDPriceWithNetStat
        header = ["timeframe", "start_timestamp", "price", "hash_rate", "difficulty"]
        query = md.select().where(md.timeframe == timeframe)
        time_str = datetime.datetime.utcnow().isoformat()
        time_str = time_str.replace(":", "-")
        ts_name = f"coin_metrics_{time_str}"
        if start_timestamp is not None:
            query = query.where(md.start_timestamp >= start_timestamp)
            ts_name += "_" + str(start_timestamp)
        if end_timestamp is not None:
            query = query.where(md.start_timestamp < end_timestamp)
            ts_name += "_" + str(start_timestamp)

        query = query.order_by(md.start_timestamp.asc())
        csv_name = f"{ts_name}.csv"
        file_path = os.path.join(gettempdir(), csv_name)
        with open(file_path, mode="w", encoding="utf-8") as fp:
            writer = csv.writer(fp)
            writer.writerow(header)
            for row in query.execute():
                row_data = row.as_list(header)
                writer.writerow(row_data)

        return csv_name, file_path
