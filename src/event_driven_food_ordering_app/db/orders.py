from pathlib import Path
import pandas as pd


def load_orders(data_path:str|Path):
    data_path = Path(data_path)
    if not data_path.exists():
        return pd.DataFrame()
    return pd.read_csv(data_path)


def save_orders(df:pd.DataFrame, data_path:str|Path):
    df.to_csv(data_path, index=False)


def update_orders(data_path:str|Path, order:dict):
    data_dir = Path(data_path).parent
    data_dir.mkdir(exist_ok=True)
    df = load_orders(data_path)
    df = pd.concat([df, pd.DataFrame([order])], ignore_index=True)
    save_orders(df, data_path)

