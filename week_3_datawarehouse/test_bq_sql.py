from google.cloud import bigquery
from pathlib import Path
import os

#os.environ['GOOGLE_AUTH_CREDENTIALS'] = 'datazoo-0592-1222fbd91a39.json'
os.environ['GOOGLE_AUTH_CREDENTIALS'] = 'datazoo-0592-0c18dfa8a0f2.json'

# Construct a BigQuery client object.
#credentiel_file = Path("/credsjsonforbigqueryandbuckets/datazoo-0592-1222fbd91a39")
#client = bigquery.Client(credentials=credentiel_file,project="datazoo-0592")
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the destination table.
#table_id = "datazoo-0592.nytaxi.fhv_esternal_from_python"
#table_id = "datazoo-0592.nytaxi.fhv_materialized_table"

#job_config = bigquery.QueryJobConfig()

sql = """
    SELECT DISTINCT(affiliated_base_number)
FROM `nytaxi.fhv_materialized_partitionned_table`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
"""

# Start the query, passing in the extra configuration.
query_job = client.query(sql,locaiton='US', job_config=bigquery.QueryJobConfig(max=5000000),job_id_prefix='job_high_quality_billed')  # Make an API request.

for row in query_job.result():  # Wait for the job to complete.
    print(row)

#print("Query results loaded to the table {}".format(table_id))