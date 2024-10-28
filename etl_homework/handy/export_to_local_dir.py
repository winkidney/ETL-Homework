import os

from etl_homework import exporters
from etl_homework.dbs import with_db


@with_db
def export_to_local_dir():
    _, csv_report = exporters.CoinMetricsExporter.export()
    new_file = os.path.basename(csv_report)
    os.rename(csv_report, new_file)
    print("exported to: ", new_file)


if __name__ == "__main__":
    export_to_local_dir()
