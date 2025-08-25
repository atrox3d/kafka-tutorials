from pathlib import Path
import pandas as pd

from . import data

def load_transactions(data_path:str|Path) -> pd.DataFrame:
    df = data.load_df(data_path)
    if not df:
        return pd.DataFrame()
    return pd.read_csv(data_path)


def save_transactions(df:pd.DataFrame, data_path:str|Path) -> None:
    data.save_df(df, data_path)


def update_transactions(data_path:str|Path, order:dict) -> None:
    update = pd.DataFrame([order])
    data.update_df(data_path, update)