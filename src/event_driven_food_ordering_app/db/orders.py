from pathlib import Path
import pandas as pd

from . import data

def load_orders(data_path:str|Path):
    df = data.load_df(data_path)
    if df is None:
        return pd.DataFrame()
    return pd.read_csv(data_path)


def save_orders(df:pd.DataFrame, data_path:str|Path):
    data.save_df(df, data_path)


def update_orders(data_path:str|Path, order:dict):
    update = pd.DataFrame([order])
    data.update_df(data_path, update)


def get_next_order_id(data_path:str|Path):
    df = load_orders(data_path)
    if df is None or (len(df) == 0):
        return 0
    return (df['order_id'].max() + 1)

