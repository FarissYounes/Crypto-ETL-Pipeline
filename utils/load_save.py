import pandas as pd

from enum import Enum
from pathlib import Path

class DataType(Enum):
    RAW = "data/raw"
    PROCESSED = "data/processed"

def get_storage_path(data_type: DataType, file_name: str) -> Path:
    base_path = Path(data_type.value)
    path = base_path / file_name
    return path

def save_file(file_name: str, data: list, data_type: DataType) :
    path = get_storage_path(data_type, file_name)
    df = pd.DataFrame(data)
    df.to_csv(path)

def load_data(file_name: str, data_type: DataType) -> pd.DataFrame :
    file_path = get_storage_path(data_type, file_name)
    df = pd.read_csv(file_path)
    return df
