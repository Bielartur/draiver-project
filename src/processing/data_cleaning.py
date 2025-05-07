# src/processing/data_cleaning.py
import pandas as pd
from fetch.api_client import fetch_dataframe

df = fetch_dataframe()
print(df.tail(10))