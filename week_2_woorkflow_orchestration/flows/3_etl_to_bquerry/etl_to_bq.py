from io import BytesIO
from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(log_prints=True)
def extract_from_gcs(color:str,year:int,mounth:int) -> Path:
    """Download from GCS Bucket"""
    gcs_path = f"data\{color}\{color}_tripdata_{year}-{mounth:02}.parquet"
    #df = pd.DataFrame()
    #gcp_cloud_credentials= GcpCredentials.load("creds2-serivce-account")
    gcs_block = GcsBucket.load("block-for-zoom-bucket")
    #gcs_bucket = GcsBucket(gcp_credentials=gcp_cloud_credentials,bucket="dtc_data_lake_datazoo-0592")
    #gcs_block.download_object_to_path(gcs_path, f"{gcs_path}")
    gcs_block.get_directory(gcs_path,f"../")
    return Path(f"../{gcs_path}")

@task(log_prints=True)
def transform(path:Path)-> pd.DataFrame :
     """Data cleaning example"""
     df = pd.read_parquet(path)
     print(f"pre:missing passenger count:{df['passenger_count'].isna().sum}")
     df["passenger_count"].fillna(0,inplace=True)
     print(f"pre:missing passenger count:{df['passenger_count'].isna().sum}")
     return df

@task(log_prints=True)
def write_bq(df:pd.DataFrame) -> None :
     """WriteDataframe to Big Query """
     gcp_credentials_block = GcpCredentials.load("credentials-for-bucket-and-bq")

     df.to_gbq(
          destination_table="trips_data_all.rides",
          project_id="datazoo-0592",
          chunksize=500000,
          credentials=gcp_credentials_block.get_credentials_from_service_account(),
          if_exists="append"
     )

@flow(name="main etl ")
def etl_gcs_to_bq():
    """Main ETL flow to load into Big Query"""
    color="yellow"
    year=2021
    mounth=2

    path = extract_from_gcs(color,year,mounth)
    df:pd.DataFrame = transform(path)
    print(df)
    write_bq(df)


if __name__=='__main__':   
	etl_gcs_to_bq()
