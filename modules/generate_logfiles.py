# generate_logfiles.py
import os
from code.common.s3_utils import copy_to_s3
from code.common.system_utils import execute_local, del_local_file
import datetime

def generate_data():
    path = "/Users/BigData_ETL_Project/Fake-Apache-Log-Generator/"
    os.chdir(path)

    #cmd = ["./venv/bin/python", "generate_fake_profiles.py"]

    #cmd = ["/Users/bin/python", "generate_fake_profiles.py"]
    cmd = ["/usr/local/bin/python3.8", "generate_fake_profiles.py"]
    execute_local(cmd)

    # List files in current directory
    files = os.listdir(os.curdir)
    # particular files from a directories
    lst = [path + k for k in files if 'access_log' in k]
    print(lst)
    return lst

def run(date, location):
    log_files = generate_data()
    copy_to_s3(location + date + '/', log_files)
    del_local_file(log_files)

# this is required if you want an entry point
# for your code
if __name__ == '__main__':
    location = 's3://twinkle-de-playground/raw/access-log/'
    # current_date = '2021-01-11'
    current_date = datetime.datetime.today().strftime('%Y-%m-%d')
    print("Current Date :", current_date)
    run(current_date, location)
