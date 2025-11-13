import pandas as pd

from enum import Enum
from pathlib import Path

class DataType(Enum):
    RAW = "data/raw"
    PROCESSED = "data/processed"

def get_storage_path(data_type: DataType, filename: str) -> Path:
    base_path = Path(data_type.value)
    path = base_path / filename
    return path

def save_file(file_name: str, data: list, data_type: DataType) :
    path = get_storage_path(data_type, file_name)
    df = pd.DataFrame(data)
    df.to_csv(path)
