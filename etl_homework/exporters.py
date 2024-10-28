import csv
from tempfile import NamedTemporaryFile

from etl_homework import models
from etl_homework.crawler import TimeFrames


class CoinMetricsExporter:
    @classmethod
    def export(
        cls,
        start_timestamp=None,
        end_timestamp=None,
        timeframe=TimeFrames.FIVE_MINUTES,
    ) -> NamedTemporaryFile:
        # TODO(winkidney): optmize CSV export speed by reduce cost of object-serialize.
        md = models.BTCUSDPriceWithNetStat
        header = ["timeframe", "start_timestamp", "price", "hash_rate", "difficulty"]
        query = md.select().where(md.timeframe == timeframe)
        ts_name = "coin_metrics"
        if start_timestamp is not None:
            query = query.where(md.start_timestamp >= start_timestamp)
            ts_name += "_" + str(start_timestamp)
        if end_timestamp is not None:
            query = query.where(md.start_timestamp < end_timestamp)
            ts_name += "_" + str(start_timestamp)

        query = query.order_by(md.start_timestamp.asc())
        csv_name = f"{ts_name}.csv"
        csv_file = NamedTemporaryFile(mode="w+", delete=True, encoding="utf-8")

        writer = csv.writer(csv_file)
        writer.writerow(header)
        for row in query.execute():
            row_data = row.as_list(header)
            writer.writerow(row_data)
        csv_file.seek(0)

        return csv_name, csv_file
