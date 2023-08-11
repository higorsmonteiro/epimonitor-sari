import os
import sys
sys.path.append( os.path.dirname(os.path.abspath('')) )

import glob
import shutil
import zipfile
import numpy as np
import pandas as pd
import datetime as dt
from simpledbf import Dbf5

from epimonitor import WarehouseSUS

# -- create interface for command line arguments
# ---- 1. ...
# ---- 2. ...
# ---- 3. ...
# ---- 4. ...
# ---- 5. ...

# ---------- Database Connection ----------
datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
suspath = os.path.join(datapath, "DATASUS_WAREHOUSE", "datasus_pessoas.db") # it shouldn't be here in this script
engine_url = f"sqlite:///{suspath}"

warehouse = WarehouseSUS(engine_url)
engine = warehouse.db_init()

# ---------- Inject Data ----------

# -- folder where downloaded data is stored
basefolder = os.path.join(os.environ["HOMEPATH"], 'Documents', 'data', 'SIVEP-Gripe', 'DOWNLOAD_AUTO')

# -- collection folders are named with dates
date_folders = [ dt.date(int(n.split("-")[2]), int(n.split("-")[1]), int(n.split("-")[0]) ) for n in os.listdir(basefolder)  ]

# -- select the most recent data
nargmax = date_folders.index(max(date_folders))
selected_folder = os.listdir(basefolder)[nargmax]

# -- create separated folder for uncompressed files
extracted_path = os.path.join(basefolder, selected_folder, "EXTRACTED")
if not os.path.isdir(extracted_path):
    os.mkdir(extracted_path)

list_of_zipfiles = [ os.path.basename(x) for x in glob.glob(os.path.join(basefolder, selected_folder, "*.zip")) ]

for current_file in list_of_zipfiles:
    # -- extract file
    print(f'extracting {current_file} ... ', end='')
    with zipfile.ZipFile( os.path.join(basefolder, selected_folder, current_file), 'r') as fzip:
        fzip.extractall(extracted_path)

    # -- load and preprocess DBF file
    dbf_file = glob.glob(os.path.join(extracted_path, "*.dbf"))[0] # it should be just one
    
    print(f'loading {os.path.basename(dbf_file)} ({Dbf5(dbf_file, codec="latin").numrec} records) ... ', end='')
    cur_sivep = Dbf5(dbf_file, codec='latin').to_dataframe()
    cur_sivep["DT_NASC"] = pd.to_datetime(cur_sivep["DT_NASC"], format="%d/%m/%Y", errors="coerce")
    cur_sivep["DT_NOTIFIC"] = pd.to_datetime(cur_sivep["DT_NOTIFIC"], format="%d/%m/%Y", errors="coerce")

    min_year, max_year = cur_sivep["DT_NOTIFIC"].min().year, cur_sivep["DT_NOTIFIC"].max().year
    list_of_ids = []
    for current_year in np.arange(min_year, max_year+1, 1):
        list_of_ids += [ pd.DataFrame(warehouse.query_id('sivep_gripe', current_year)) ]
    list_of_ids = pd.concat(list_of_ids)
    if list_of_ids.shape[0]>0:
        list_of_ids = list_of_ids["ID_SIVEP"]

    # -- remove from the dbf the records already present in the database
    cur_sivep_new = cur_sivep[~cur_sivep["NU_NOTIFIC"].isin(list_of_ids)].copy()
    print(f"{cur_sivep_new.shape[0]} new records to be added to the database ... ", end='')

    # -- insert records
    warehouse.insert('sivep_gripe', cur_sivep_new, batchsize=200, verbose=False)
    
    # -- delete extracted file
    print("done.")
    os.remove(dbf_file)
    
shutil.rmtree(extracted_path)