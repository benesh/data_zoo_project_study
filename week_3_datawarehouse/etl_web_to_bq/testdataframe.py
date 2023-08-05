import pandas as pd
import os 

months = ['02','01']
print(os.getcwd())

for month in months:
    url_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2020-{month}.parquet"
    df = pd.read_parquet(url_file)
    df.to_parquet(f"file_{month}_.parquet")
    resultgreen = df.dtypes
print(resultgreen)


