from pathlib import Path
import pandas as pd


def load_df(data_path:str|Path):
    data_path = Path(data_path)
    if not data_path.exists():
        return pd.DataFrame()
    return pd.read_csv(data_path)


def save_df(df:pd.DataFrame, data_path:str|Path):
    df.to_csv(data_path, index=False)


def update_df(data_path:str|Path, update:pd.DataFrame):
    data_dir = Path(data_path).parent
    data_dir.mkdir(exist_ok=True)
    df = load_df(data_path)
    df = pd.concat([df, update], ignore_index=True)
    save_df(df, data_path)

