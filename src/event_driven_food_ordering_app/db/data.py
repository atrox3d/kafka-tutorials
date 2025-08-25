from pathlib import Path
import pandas as pd


def load_df(data_path:str|Path) -> pd.DataFrame | None:
    data_path = Path(data_path)
    if not data_path.exists():
        return None
    return pd.read_csv(data_path)


def save_df(df:pd.DataFrame, data_path:str|Path) -> None:
    data_dir = Path(data_path).parent
    data_dir.mkdir(exist_ok=True)
    df.to_csv(data_path, index=False)


def update_df(data_path:str|Path, update:pd.DataFrame) -> None:
    df = load_df(data_path)
    # pd.concat ignores df==None
    df = pd.concat([df, update], ignore_index=True)
    save_df(df, data_path)

