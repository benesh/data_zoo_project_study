from prefect_gcp.bigquery import BigQueryWarehouse


with BigQueryWarehouse.load("block-big-query") as warehouse:
    operation = '''
    CREATE OR REPLACE TABLE nytaxi.fhv_from_local_table
    AS (
        SELECT *
        FROM `nytaxi.fhv_materialized_partitionned_table`
        WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31'
        LIMIT 10);
    '''
   
    result = warehouse.fetch_all(operation)
    print(result)



"""
SELECT DISTINCT(affiliated_base_number)
FROM `nytaxi.fhv_materialized_partitionned_table`
WHERE DATE(pickup_datetime) BETWEEN '2019-03-01' and '2019-03-31';
"""
"""
'''
        SELECT word, word_count
        FROM `bigquery-public-data.samples.shakespeare`
        WHERE corpus = %(corpus)s
        AND word_count >= %(min_word_count)s
        ORDER BY word_count DESC
        LIMIT 9;
    '''
"""