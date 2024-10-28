import csv
import os.path

from playhouse.dataset import DataSet

from etl_homework import models
from etl_homework.crawler import TimeFrames


class CoinMetricsExporter:
    @classmethod
    def export(
        cls,
        start_timestamp=None,
        end_timestamp=None,
        out_file_prefix="coin-metrics",
        timeframe=TimeFrames.FIVE_MINUTES,
    ):
        md = models.BTCUSDPriceWithNetStat
        header = ["timeframe", "start_timestamp", "price", "hash_rate", "difficulty"]
        query = md.select().where(md.timeframe == timeframe)
        ts_name = f"{out_file_prefix}"
        if start_timestamp is not None:
            query = query.where(md.start_timestamp >= start_timestamp)
            ts_name += "_" + str(start_timestamp)
        if end_timestamp is not None:
            query = query.where(md.start_timestamp < end_timestamp)
            ts_name += "_" + str(start_timestamp)

        query = query.order_by(md.start_timestamp.asc())
        csv_name = f"{ts_name}_.csv"

        with open(csv_name, mode="w", encoding="utf-8") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in query.execute():
                row_data = row.as_list(header)
                writer.writerow(row_data)
        return os.path.abspath(csv_name)
