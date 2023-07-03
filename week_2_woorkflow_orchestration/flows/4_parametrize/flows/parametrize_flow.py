from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url)-> pd.DataFrame:
	"""  Read taxi data from web into pandas"""
	# if randint(0,1) > 0 :  
	# 	raise Exception
	df= pd.read_csv(dataset_url)
	return df


@task(log_prints=True)
def clean(df : pd.DataFrame) -> pd.DataFrame:
	"""Fix some dtype issues"""

	df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
	df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
	print(df.head(2))
	print(f"columns : {df.dtypes}")
	print(f"rows : {len(df)}")
	return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color :str , dataset_file : str ) -> Path:
	""" Wite DataFRame out locally """
	path = Path(f"data/{color}/{dataset_file}.parquet")
	df.to_parquet(path,compression="gzip")
	return path 

@task(log_prints=True)
def write_gcs(path: Path) -> Path:
	""" Upload local file to GCS """
	#gcs_block = GcsBucket.load("zoom-gcs")
	gcs_block = GcsBucket.load("block-for-zoom-bucket")
	gcs_block.upload_from_path(from_path=path,to_path=path)
	return

@flow(log_prints=True)
def etl_web_to_gcs(mounth:int,year:int,color:str) -> None:
	""" The main ETL function """
	
	dataset_file =  f"{color}_tripdata_{year}-{mounth:02}"
	dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
	#dataset_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

	df = fetch(dataset_url)
	df_clean = clean(df)
	path = write_local(df_clean, color, dataset_file)
	write_gcs(path)

@flow(log_prints=True)
def etl_parent_flow(mounths : list[int] = [1,2],year : int = 2021, color: str = "yellow" ) :
	for mounth in mounths :
		etl_web_to_gcs(mounth,year,color)


if __name__=='__main__': 
	color = "yellow"
	year = 2021
	mouths = [3,4,5]
	etl_parent_flow(mouths,year,color)