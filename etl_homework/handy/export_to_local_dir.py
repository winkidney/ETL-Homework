import os
import stat
import shutil

from etl_homework import exporters
from etl_homework.dbs import with_db


@with_db
def export_to_local_dir():
    _, csv_report = exporters.CoinMetricsExporter.export()
    new_file = os.path.basename(csv_report)
    shutil.copy(csv_report, new_file)
    os.chmod(
        new_file,
        stat.S_IRUSR
        | stat.S_IWUSR
        | stat.S_IRGRP
        | stat.S_IWGRP
        | stat.S_IROTH
        | stat.S_IWOTH,
    )
    os.remove(csv_report)
    print("exported to: ", new_file)


if __name__ == "__main__":
    export_to_local_dir()
