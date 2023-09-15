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
from sqlalchemy import DateTime, Integer, Numeric, String, Float, Sequence, ForeignKey, CheckConstraint
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
            Column("DATA_SINTOMAS", DateTime, nullable=True),
            Column("CLASSIFICACAO_FINAL", String, nullable=True),
            Column("CRITERIO", String, nullable=True),
            Column("EVOLUCAO", String, nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
            Column("ATUALIZADO_EM", DateTime, default=dt.datetime.now, onupdate=dt.datetime.now),
        )

        # -- define data mapping (could be import if too big)
        self.mapping = {
            "NU_NOTIFIC" : "ID_SIVEP",  "DT_NOTIFIC": "DATA_NOTIFICACAO",
            "NM_PACIENT": "NOME_PACIENTE", "DT_NASC": "DATA_NASCIMENTO",
            "NM_MAE_PAC": "NOME_MAE", "CO_MUN_RES": "MUNICIPIO_RESIDENCIA",
            "NM_BAIRRO": "BAIRRO_RESIDENCIA", "NM_LOGRADO": "LOGRADOURO",
            "NU_NUMERO": "LOGRADOURO_NUMERO", "NU_CEP": "CEP", 
            "NU_CNS": "CNS", "NU_CPF": "CPF", "CS_SEXO": "SEXO",
            "CO_UNI_NOT": "CNES", "DT_SIN_PRI": "DATA_SINTOMAS",
            "CLASSI_FIN": "CLASSIFICACAO_FINAL", "CRITERIO": "CRITERIO", 
            "EVOLUCAO": "EVOLUCAO",
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

class SimilaritySivepGripe:
    def __init__(self, metadata):
        self.metadata = metadata
        self.table_name = 'label_sivep_gripe'

        # --> define schema for table.
        self.model = Table(
            self.table_name, self.metadata,
            Column("ID1-ID2", String, primary_key=True),
            Column("ID1", String, nullable=False),
            Column("ID2", String, nullable=False),
            Column("PROBA_NEGATIVO_MODELO_1", Float(6), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_2", Float(6), nullable=True),
            Column("PROBA_NEGATIVO_MODELO_3", Float(6), nullable=True),
            Column("CRIADO_EM", DateTime, default=dt.datetime.now),
        )

        # -- define data mapping (could be imported if too big) - include all columns!
        self.mapping = {
            "ID_SIVEP_1" : "ID1",  "ID_SIVEP_2" : "ID2", "FMT_PKEY": "ID1-ID2",
            "PROBA_NEGATIVO_MODELO_1" : "PROBA_NEGATIVO_MODELO_1",
            "PROBA_NEGATIVO_MODELO_2": "PROBA_NEGATIVO_MODELO_2",
            "PROBA_NEGATIVO_MODELO_3": "PROBA_NEGATIVO_MODELO_3", 
            #"cns" : "CNS",  "cep" : "CEP", "cpf" : "CPF", "sexo": "SEXO",
            #"nascimento_dia" : "NASCIMENTO_DIA", "nascimento_mes" : "NASCIMENTO_MES", 
            #"nascimento_ano" : "NASCIMENTO_ANO", "primeiro_nome" : "PRIMEIRO_NOME",
            #"primeiro_nome_mae" : "PRIMEIRO_NOME_MAE", "complemento_nome": "COMPLEMENTO_NOME",
            #"complemento_nome_mae": "COMPLEMENTO_NOME_MAE", "bairro": "BAIRRO",
            #"rank_primeiro_nome": "RANK_PRIMEIRO_NOME", "rank_primeiro_nome_mae": "RANK_PRIMEIRO_NOME_MAE", 
        }

    def define(self):
        '''
            Return dictionary elements containing the data model and 
            the data mapping, respectively.
        '''
        table_elem = { self.table_name : self.model }
        mapping_elem = { self.table_name : self.mapping }
        return table_elem, mapping_elem