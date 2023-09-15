import os
import sys
sys.path.append( os.path.dirname(os.path.abspath('')) )
#sys.path.append( os.path.abspath(os.path.join(os.path.dirname(os.path.abspath('')), '..', 'linkage-saude')) )

import glob
import yaml
import time
import zipfile
import argparse
import datetime as dt

from epimonitor.collector import SivepPipe

# -- create interface for command line arguments
# ---- 1. name of the yaml file containing the credentials
# ---- 2. sleep time between page refresh
# ---- 3. output file to store requisition log
# ---- 4. max number of page refresh
# ---- 5. period considered - easy way to always update the last epi week.

basepath = rf'{os.environ["HOMEDRIVE"]}'+os.path.join(os.environ["HOMEPATH"], "Documents", "data", "SIVEP-GRIPE")

# -- create the download folder
cur_date = dt.date.today()
date_text = rf"\{cur_date.day:2.0f}-{cur_date.month:2.0f}-{cur_date.year}".replace(" ", "0")
download_folder = r'C:\Users\higor.monteiro\Documents\data\SIVEP-Gripe\DOWNLOAD_AUTO'+date_text
if not os.path.isdir(download_folder):
    os.mkdir(download_folder)

# -- get the credentials and start the collection
config_fname = "millena.yaml"
with open(os.path.join(basepath, 'CREDENTIALS', config_fname), mode="rt", encoding="utf-8") as file:
    config = yaml.safe_load(file)

collector = SivepPipe(config['certification'], download_folder=download_folder, headless=False)
collector.login()

# -- request and download data
requisition_list = []
requisition_logfile = os.path.join(download_folder, f"REQLOG-{cur_date.day:2.0f}-{cur_date.month:2.0f}-{cur_date.year}.csv".replace(" ", "0"))

collector.request_dbf('2018', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2018', '26', '52', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

collector.request_dbf('2019', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2019', '26', '52', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

collector.request_dbf('2020', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2020', '26', '52', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

collector.request_dbf('2021', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2021', '26', '52', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

collector.request_dbf('2022', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2022', '26', '52', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

collector.request_dbf('2023', '1', '25', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]
collector.request_dbf('2023', '26', '36', requisition_export=requisition_logfile).query_file().download_file(sleep_time=7, verbose=True, max_loop=100)
requisition_list += [collector.requisition_number]

# -- verify whether all data was downloaded
count = 0
while True and count<=20:
    if all([ elem+'DBF.zip' in os.listdir(download_folder) for elem in requisition_list ]):
        break
    else:
        time.sleep(1)
        count += 1

if count==21:
    print('not all files were downloaded.')
else:
    print('all files downloaded.')
