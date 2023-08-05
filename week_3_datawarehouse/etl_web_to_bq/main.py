from pathlib import Path 
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

@task(log_prints=True,retries=3)
def fetch(dataset_url)-> pd.DataFrame:
	"""  Read taxi data from web into pandas"""

	
	if "csv" in dataset_url:
		df= pd.read_csv(dataset_url)
	if "parquet" in dataset_url:
		df= pd.read_parquet(dataset_url)
	return df

@task(log_prints=True)
def clean(df : pd.DataFrame,type_color:str) -> pd.DataFrame:
	"""Fix some dtype issues"""
	if type_color == 'yellow' :
		df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
		df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
		df['passenger_count']=df['passenger_count'].astype('Int32')
		df['PULocationID'] = df['PULocationID'].astype('Int64')
		df['RatecodeID'] = df['RatecodeID'].astype('Int64')
		df['VendorID'] = df['VendorID'].astype('Int64')
		df['payment_type'] = df['payment_type'].astype('Int32')
		df['DOLocationID'] = df['DOLocationID'].astype('Int64')
	if type_color =='green':
		df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
		df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
		df['passenger_count']=df['passenger_count'].astype('Int32')
		df['PULocationID'] = df['PULocationID'].astype('Int64')
		df['RatecodeID'] = df['RatecodeID'].astype('Int64')
		df['VendorID'] = df['VendorID'].astype('Int64')
		df['payment_type'] = df['payment_type'].astype('Int32')
		df['DOLocationID'] = df['DOLocationID'].astype('Int64')
		df['trip_type'] = df['DOLocationID'].astype('Int32')
	if type_color == 'fhv' :
		df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
		df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'])
		
	print(df.head(2))
	print(f"columns : {df.dtypes}") 
	print(f"rows : {len(df)}")
	return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, type_color :str , dataset_file : str ) -> str:
	""" Wite DataFRame out locally """
	path = f"data/{type_color}/{dataset_file}.parquet"
	df.to_parquet(path,compression='gzip')
	return path 	

@task(log_prints=True)
def write_gcs(path: str) -> None:
	""" Upload local file to GCS """
	gcs_block = GcsBucket.load("block-for-zoom-bucket")
	gcs_block.upload_from_path(from_path=path,to_path=path)
	return

@flow(name="Etl_to_gcs",log_prints=True)
def etl_web_to_gcs(url_root:str, type_color:str,year:int,month:int,extension : str) -> None:
	""" The main ETL function """
	data_file_url,dataset_file = make_url_file(url_root,type_color,month,year,extension)
	df = fetch(data_file_url)
	df_clean = clean(df,type_color)
	path = write_local(df_clean, type_color, dataset_file)
	print(path)
	write_gcs(path)


@task(log_prints=True,name='MAKE File method')
def make_url_file(url_root:str,type_color:str,month:str ,year:int,extension)->str :
	dataset_file =  f"{type_color}_tripdata_{year}-{month:02}"
	if "github" in url_root :
		dataset_url = f"{url_root}/{type_color}/{dataset_file}.{extension}"
	else :
		dataset_url = f"{url_root}/{dataset_file}.{extension}"
	return dataset_url,dataset_file
	

@flow(name="ETL_Parent",log_prints=True)
def elt_parent_flow(url_root:str,type_color:str,years:list[int] =[2019],months:list[int] = [2,3], extension:str = "cvs") -> None:
	for year in years :
		for month in months :
			etl_web_to_gcs(url_root,type_color,year,month,extension)


if __name__=='__main__':
    #https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz
	
	url_root_1="https://d37ci6vzurychx.cloudfront.net/trip-data"
	url_root = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"
	type_color ="yellow"
	#years = [2019,2020,2021]
	years = [2019]
	months = [1,2,3,4,5,6,7,8,9,10,11,12]
	#months = [3]
	extension = "parquet"
	elt_parent_flow(url_root_1,type_color,years,months,extension)



