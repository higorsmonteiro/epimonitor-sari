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
        return self # chaining

    def specific_standardize(self):
        return self

        