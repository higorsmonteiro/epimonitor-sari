import os
import sys
sys.path.append( os.path.dirname(os.path.abspath('')) )

import json
import glob
import shutil
import joblib
import zipfile
from tqdm import tqdm
import numpy as np
import pandas as pd
import datetime as dt

from epimonitor import WarehouseSUS
from epimonitor.process_layer import ProcessSivep
from epimonitor.data_matching import Deduple

# -- create interface for command line arguments
# ---- 1. name of the yaml file containing the credentials
# ---- 2. sleep time between page refresh

# ---------- Database Connection ----------
datapath = os.path.join(os.environ["HOMEPATH"], "Documents", "data")
basepath = os.path.join(os.environ["HOMEPATH"], "Documents", "data", "SIVEP-GRIPE")
suspath = os.path.join(datapath, "DATASUS_WAREHOUSE", "datasus_pessoas.db") # it shouldn't be here in this script
engine_url = f"sqlite:///{suspath}"

warehouse = WarehouseSUS(engine_url)
engine = warehouse.db_init()

# -- retrieve the pairs that were already compared previously
query_pairs = pd.DataFrame( warehouse.query_all(table_name='label_sivep_gripe') )
if query_pairs.shape[0]>0:
    query_pairs = query_pairs[["ID1", "ID2"]].copy()
else:
    query_pairs = []

if len(query_pairs):
    query_pairs = list(query_pairs.itertuples(index=False, name=None))

# -- now retrieve the SIVEP-GRIPE data
# ---- the period of the data could be an input of the script
period = (dt.datetime(2020, 1, 1), dt.datetime.today())
query_data = pd.DataFrame( warehouse.query_period(table_name='sivep_gripe', date_col="DATA_NOTIFICACAO", period=period) )

# -- process the data for standardization
processor = ProcessSivep(query_data, 'ID_SIVEP')
processor.basic_standardize().specific_standardize()
processed_data = processor.data

# -- load the classifiers
gbt_model = joblib.load(os.path.join(basepath, "TRAINED_MODELS", "GRADBOOST_SIVEP04SET2023.joblib"))
rnf_model = joblib.load(os.path.join(basepath, "TRAINED_MODELS", "RANDFOREST_SIVEP04SET2023.joblib"))
lgt_model = joblib.load(os.path.join(basepath, "TRAINED_MODELS", "LOGITREG_SIVEP04SET2023.joblib"))

# -- build the similarity matrix
deduple = Deduple(processed_data, left_id="ID_SIVEP", env_folder=None)
map_compare = {
    "cns": ["exact"], "cep": ["exact"], "cpf": ["exact"], "sexo": ["exact"],
    "nascimento_dia": ["exact"], "nascimento_mes": ["exact"], "nascimento_ano": ["exact"],
    "primeiro_nome_mae": ["string", None], "complemento_nome_mae": ["string", None],
    "primeiro_nome": ["string", None], 
    "complemento_nome": ["string", None], "bairro": ["string", None]
}
deduple.set_linkage(map_compare, string_method="damerau_levenshtein").define_pairs("FONETICA_N", window=3)

# ---- remove pairs that were already compared previously
deduple.candidate_pairs = deduple.candidate_pairs.drop(query_pairs, errors='ignore')
print(f"Pairs to be effectively compared: {deduple.candidate_pairs.shape[0]}")

# -- compare and generate the similarity matrix
deduple.perform_linkage("FONETICA_N", window=3, threshold=0.60)
ranks = processed_data.set_index('ID_SIVEP')[["rank_primeiro_nome", "rank_primeiro_nome_mae"]]
deduple._comparison_matrix = deduple.comparison_matrix.merge(ranks, left_on=["ID_SIVEP_1"], right_index=True, how="left")
deduple._comparison_matrix["rank_primeiro_nome_mae"] = deduple._comparison_matrix["rank_primeiro_nome_mae"].fillna(7)

# -- classify pairs
pair_ids, X_sel = deduple.comparison_matrix.reset_index().iloc[:,:2],  deduple.comparison_matrix.reset_index().iloc[:,2:].values

batchsize = 6000
Y_neg1, Y_neg2, Y_neg3 = [], [], []
for batch in tqdm(np.array_split(X_sel, np.arange(batchsize, X_sel.shape[0]+1, batchsize))):
    Y_neg1 += [ res[0] for res in gbt_model.predict_proba(batch) ]
    Y_neg2 += [ res[0] for res in rnf_model.predict_proba(batch) ]
    Y_neg3 += [ res[0] for res in lgt_model.predict_proba(batch) ]

pair_ids["FMT_PKEY"] = pair_ids["ID_SIVEP_1"] + "-" + pair_ids["ID_SIVEP_2"]
pair_ids["PROBA_NEGATIVO_MODELO_1"] = Y_neg1
pair_ids["PROBA_NEGATIVO_MODELO_2"] = Y_neg2
pair_ids["PROBA_NEGATIVO_MODELO_3"] = Y_neg3

# -- inject compared pairs
warehouse.insert('label_sivep_gripe', pair_ids, batchsize=500, verbose=True)
