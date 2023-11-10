import pandas as pd


def create_id(df, db_type, id_name):
    '''
    
    '''
    if db_type=="SINAN":
        notific_fmt = df["DT_NOTIFIC"].apply(lambda x: f"{x.day:2.0f}{x.month:2.0f}{x.year}".replace(" ", "0"))
        id_municip_fmt = df["ID_MUNICIP"].apply(lambda x: f"{x}")
        df[id_name] = df["ID_AGRAVO"]+df["NU_NOTIFIC"]+id_municip_fmt+notific_fmt
    if db_type=="SIM":
        df[id_name] = df["NUMERODO"].copy()
    return df