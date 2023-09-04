import re
import numpy as np
import pandas as pd
import epimonitor.utils as utils

class ProcessBase:
    '''
        Preprocessing layer for data already injected into the warehouse. Creates variables 
        for data matching. Input data should already follow a schema. Specific information
        should be handled by inherited classes.


        Args:
        -----
            raw_data:
                pandas.DataFrame.
            field_id:
                String. Name of the field containing the unique identifier of the provided data.

    '''
    def __init__(self, raw_data, field_id):
        self.field_id = field_id
        self._raw_data = raw_data.copy()
        self._data = pd.DataFrame(self._raw_data[[self.field_id]])

        self.base_fields = ["NOME_PACIENTE", "DATA_NASCIMENTO", "NOME_MAE"]

        if not all([ elem in self._raw_data.columns for elem in self.base_fields ]):
            raise Exception(f'At least one of the following essential fields are missing: {self.base_fields}')
        

    @property
    def raw_data(self):
        return self._raw_data

    @raw_data.setter
    def raw_data(self, x):
        raise Exception("Not possible to change this attribute.")

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, x):
        raise Exception("Not possible to change this attribute.")

    def basic_standardize(self):
        '''
            ...
        '''
        self._data["NOME_PACIENTE"] = self._raw_data["NOME_PACIENTE"].apply(lambda x: utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["NOME_MAE"] = self._raw_data["NOME_MAE"].apply(lambda x: utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan).apply(lambda x: re.sub(' {2,}', ' ', x) if pd.notna(x) else np.nan)
        self._data["DATA_NASCIMENTO"] = self._raw_data["DATA_NASCIMENTO"].copy()

        self._data["primeiro_nome"] = self._data["NOME_PACIENTE"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["complemento_nome"] = self._data["NOME_PACIENTE"].apply(lambda x: ' '.join(x.split(" ")[1:]) if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        
        self._data["primeiro_nome_mae"] = self._data["NOME_MAE"].apply(lambda x: x.split(" ")[0] if pd.notna(x) else np.nan )
        self._data["complemento_nome_mae"] = self._data["NOME_MAE"].apply(lambda x: ' '.join(x.split(" ")[1:]) if pd.notna(x) and len(x.split(" "))>1 else np.nan )
        
        self._data["nascimento_dia"] = self._data["DATA_NASCIMENTO"].apply(lambda x: x.day if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_mes"] = self._data["DATA_NASCIMENTO"].apply(lambda x: x.month if hasattr(x, 'day') and pd.notna(x) else np.nan)
        self._data["nascimento_ano"] = self._data["DATA_NASCIMENTO"].apply(lambda x: x.year if hasattr(x, 'day') and pd.notna(x) else np.nan)
        
        # -- general blocking variable
        self._data["FONETICA_N"] = self._data["NOME_PACIENTE"].apply(lambda x: f"{x.split(' ')[0]}{x.split(' ')[-1]}" if pd.notna(x) else np.nan)

        # -- frequency of first names
        fst_name_freq = self._data["primeiro_nome"].value_counts().reset_index().rename({"index": "primeiro_nome", "count": "count_primeiro_nome"}, axis=1)
        mother_fst_name_freq = self._data["primeiro_nome_mae"].value_counts().reset_index().rename({"index": "primeiro_nome_mae", "count": "count_primeiro_nome_mae"}, axis=1)

        self.freq_names = pd.concat([fst_name_freq, mother_fst_name_freq], axis=1)
        self.freq_names['norm_primeiro_nome'] = self.freq_names['count_primeiro_nome']/self._data.shape[0]
        self.freq_names['norm_primeiro_nome_mae'] = self.freq_names['count_primeiro_nome_mae']/self._data.shape[0]
        
        # ---- log transform the power law 
        nbins = 8
        log_xscale, lin_xscale = np.concatenate((np.array([0]), np.logspace(-6, 0, nbins))), np.arange(0, nbins, 1)
        self.freq_names["rank_primeiro_nome"] = pd.cut(self.freq_names["norm_primeiro_nome"], log_xscale, labels=lin_xscale).fillna(0)
        self.freq_names["rank_primeiro_nome_mae"] = pd.cut(self.freq_names["norm_primeiro_nome_mae"], log_xscale, labels=lin_xscale).fillna(0)

        self.freq_names = self.freq_names.drop(["count_primeiro_nome", "count_primeiro_nome_mae"], axis=1)
        self.freq_names = self.freq_names[["primeiro_nome", "norm_primeiro_nome", "rank_primeiro_nome", "primeiro_nome_mae", "norm_primeiro_nome_mae", "rank_primeiro_nome_mae"]].copy()

        merged_primeiro_nome = self._data[pd.notna(self._data["primeiro_nome"])][[self.field_id, "primeiro_nome"]].merge(self.freq_names[["primeiro_nome", "norm_primeiro_nome", "rank_primeiro_nome"]], on="primeiro_nome", how="left")
        merged_primeiro_nome_mae = self._data[pd.notna(self._data["primeiro_nome_mae"])][[self.field_id, "primeiro_nome_mae"]].merge(self.freq_names[["primeiro_nome_mae", "norm_primeiro_nome_mae", "rank_primeiro_nome_mae"]], on="primeiro_nome_mae", how="left")

        if 'norm_primeiro_nome' not in self._data.columns and 'norm_primeiro_nome_mae' not in self._data.columns:
            self._data = self._data.merge(merged_primeiro_nome[[self.field_id, "norm_primeiro_nome", "rank_primeiro_nome"]], on=self.field_id, how="left")
            self._data = self._data.merge(merged_primeiro_nome_mae[[self.field_id, "norm_primeiro_nome_mae", "rank_primeiro_nome_mae"]], on=self.field_id, how="left")

        return self # chaining

    def specific_standardize(self):
        return self

        