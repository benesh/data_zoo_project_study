from io import BytesIO
from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(name="download from GCS",log_prints=True,retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_from_gcs(color:str,year:int,month:int) -> Path:
    """Download from GCS Bucket"""
    gcs_path = f"data\{color}\{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("block-for-zoom-bucket")
    gcs_block.get_directory(gcs_path,f"../")
    return Path(f"../{gcs_path}")


@task(name="transform DataFrame",log_prints=True)
def transform(path:Path)-> pd.DataFrame :
     """Data cleaning example"""
     df = pd.read_parquet(path)
     print(f"pre:missing passenger count:{df['passenger_count'].isna().sum}")
     df["passenger_count"].fillna(0,inplace=True)
     print(f"pre:missing passenger count:{df['passenger_count'].isna().sum}")
     return df


@task(name="Write TO BQ",log_prints=True) 
def write_bq(df:pd.DataFrame) -> int :
     """WriteDataframe to Big Query """
     gcp_credentials_block = GcpCredentials.load("credentials-for-bucket-and-bq")

     df.to_gbq(
          destination_table="trips_data_all.rides",
          project_id="datazoo-0592",
          chunksize=500000,
          credentials=gcp_credentials_block.get_credentials_from_service_account(),
          if_exists="append"
     )

     return len(df)


@flow(name="main etl",log_prints=True)
def etl_gcs_to_bq(month, color, year) -> int:
    """Main ETL flow to load into Big Query"""
   
    path = extract_from_gcs(color,year,month)
    #df:pd.DataFrame = transform(path)
    df = pd.read_parquet(path)
    row_number = write_bq(df)
    return row_number


@flow(name="Flow_Parent_ETL",log_prints=True)
def etl_parent_flow(months : list[int] = [1,2], color:str = "green", year:int = 2020)-> None:
    total_row_number = 0
    for month in months:
        total_row_number += etl_gcs_to_bq(month, color, year)

    print(f"rows :{total_row_number}")


if __name__=='__main__':
    color="green"
    year=2019
    months=[2,3]
    etl_gcs_to_bq( months, color, year)
