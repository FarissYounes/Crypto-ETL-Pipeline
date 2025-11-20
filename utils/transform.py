import pandas as pd
import ast

def flattern_column(df: pd.DataFrame, column: str, to_keep=[]) -> pd.DataFrame:

    flat_rows = []

    for value in df[column] :
        
        data = {}

        if isinstance(value, str) :
            if value[0] == "{" and value[len(value) - 1] == "}" :
                data = ast.literal_eval(value)
        if isinstance(value, dict) : data = value

        flat_data = pd.json_normalize(data, sep="_")
        flat_data = flat_data.add_prefix(f"{column}_")
        
        if to_keep == [] : flat_rows.append(flat_data)
        else : flat_rows.append(flat_data[to_keep])

    flat_df = pd.concat(flat_rows, ignore_index=True)
    
    df = df.drop(columns=[column]).reset_index(drop=True)
    df = pd.concat([df, flat_df], axis=1)

    return df
