'''
    Define the data models to store the main information on individuals 
    and linkage between different records.

    Author: Higor S. Monteiro
'''

import os
import numpy as np
import pandas as pd
from tqdm import tqdm
import datetime as dt
from simpledbf import Dbf5

# --> SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import Column, Table, MetaData
from sqlalchemy import select, insert, update, delete
from sqlalchemy import inspect, text
from sqlalchemy import DateTime, Integer, Numeric, String, Sequence, ForeignKey, CheckConstraint
from sqlalchemy.exc import InternalError, IntegrityError


# ---------- DATASUS MODELS ----------

class SivepGripe:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'sivep_gripe'

        # --> Define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID_SIVEP", String, primary_key=True),
            Column("DATA_NOTIFICACAO", DateTime, nullable=False),
            Column("NOME_PACIENTE", String, nullable=True),
            Column("DATA_NASCIMENTO", DateTime, nullable=True),
            Column("SEXO", String, nullable=True),
            Column("NOME_MAE", String, nullable=True),
             Column("LOGRADOURO", String, nullable=True),
            Column("LOGRADOURO_NUMERO", String, nullable=True),
            Column("BAIRRO_RESIDENCIA", String, nullable=True),
            Column("MUNICIPIO_RESIDENCIA", String, nullable=True),
            Column("CEP", String, nullable=True),
            Column("CNS", String, nullable=True),
            Column("CPF", String, nullable=True),
            Column("CNES", String, nullable=False),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- Define data mapping (could be import if too big)
        self.mapping = {
            "NU_NOTIFIC" : "ID_SIVEP",  "DT_NOTIFIC": "DATA_NOTIFICACAO",
            "NM_PACIENT": "NOME_PACIENTE", "DT_NASC": "DATA_NASCIMENTO",
            "NM_MAE_PAC": "NOME_MAE", "CO_MUN_RES": "MUNICIPIO_RESIDENCIA",
            "NM_BAIRRO": "BAIRRO_RESIDENCIA", "NM_LOGRADO": "LOGRADOURO",
            "NU_NUMERO": "LOGRADOURO_NUMERO", "NU_CEP": "CEP", 
            "NU_CNS": "CNS", "NU_CPF": "CPF", "CS_SEXO": "SEXO",
            "CO_UNI_NOT": "CNES",
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem 
    

# ---------- MATCHING DATA MODELS ----------

class PositivePairs:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'positive_pairs'

        # --> Define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID_1", String, nullable=False),
            Column("ID_2", String, nullable=False),
            Column("TABLE_1", String, nullable=False),
            Column("TABLE_2", String, nullable=False)
        )

        self.mapping = None