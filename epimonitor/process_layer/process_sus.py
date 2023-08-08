import numpy as np
import pandas as pd
import epimonitor.utils as utils
from epimonitor.process_layer import ProcessBase 

class ProcessSivep(ProcessBase):
    db_type = "SIVEP-GRIPE"

    def specific_standardize(self):
        '''
        
        '''
        self._data["sexo"] = self._raw_data["SEXO"].apply(lambda x: x.upper().strip() if pd.notna(x) else np.nan)
        self._data["cpf"] = self._raw_data["CPF"].apply(lambda x: f"{x:11.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else x)
        self._data["cns"] = self._raw_data["CNS"].apply(lambda x: x if isinstance(x, str) and utils.cns_is_valid(x) and pd.notna(x) else ( f"{x:13.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        self._data["cep"] = self._raw_data["CEP"].apply(lambda x: x if isinstance(x, str) and pd.notna(x) else ( f"{x:8.0f}".replace(" ", "0") if not isinstance(x, str) and pd.notna(x) else np.nan))
        self._data["bairro"] = self._raw_data["BAIRRO_RESIDENCIA"].apply(lambda x: utils.uniformize_name(x.upper().strip(), sep=" ") if pd.notna(x) else np.nan)
        return self