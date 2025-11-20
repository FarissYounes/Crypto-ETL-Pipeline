import pandas as pd
from utils.load_save import DataType, load_data

def load_data(file_name : str, data_type : DataType) -> pd.DataFrame :
    return load_data(data_type, file_name)
