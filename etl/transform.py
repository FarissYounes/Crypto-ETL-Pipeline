import pandas as pd
from utils.load_save import load_data, DataType
from utils.transform import flattern_column

historical_data = load_data("historical_data.csv", DataType.RAW)
print(historical_data.columns)

historical_data = historical_data.drop(columns=["localization", "community_data", "public_interest_stats"])
historical_data = historical_data.join(pd.json_normalize(historical_data["image"]))

historical_data = flattern_column(historical_data, "image")

historical_data = flattern_column(historical_data, "market_data", ["market_data_current_price_usd"])

print(historical_data.columns)