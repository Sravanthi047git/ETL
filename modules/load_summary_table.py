import os
import datetime
from code.common.emr_utils import run_on_hive
from code.config import key_path, ip


if __name__ == '__main__' :
    dir_path = os.getcwd()
    file_path = os.path.join(dir_path, "./scripts/load_summary_table.hql")

    # run_date = '2021-01-11'
    run_date = datetime.datetime.today().strftime('%Y-%m-%d')

    print("running summary table load job of the file ", file_path)
    run_on_hive(file_path, run_date, key_path, ip)