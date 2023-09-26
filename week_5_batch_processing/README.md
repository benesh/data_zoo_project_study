* lunching spark job to the local spark cluster with arguments
```bash
python 10_CReate_local_cluster.py \
--input_green=data/pq/green/2020/*/ \
--input_yellow=data/pq/yellow/2020/*/ \
--output=data/report-2020
```

* Submitting spark job to the local spark cluster with arguments
```bash
URL='spark://BenOmar.:7077'
```

```bash
spark-submit \
    --master=$URL \
    10_CReate_local_cluster.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

* submit job in dataproc
```bash
        --input_green=gs://data_zoo_bucket_free/data/pq/green/2021/*/ \
        --input_yellow=gs://data_zoo_bucket_free/data/pq/yellow/2021/*/ \
        --output=gs://data_zoo_bucket_free/data/report-2021 \
```

* cloud command to submit pyspark job

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=data-zoocamp-cluster \
    --region=us-central1 \
    gs://data_zoo_bucket_free/code/10_CReate_local_cluster_cloud_code.py \
    -- \
      --input_green=gs://data_zoo_bucket_free/data/pq/green/2020/*/ \
      --input_yellow=gs://data_zoo_bucket_free/data/pq/yellow/2020/*/ \
      --output=gs://data_zoo_bucket_free/data/report-2020
```

* cloud command to submit pyspark job with saving to Bigquery

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=data-zoocamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://data_zoo_bucket_free/code/10_CReate_local_cluster_cloud_bigquery_code.py \
    -- \
      --input_green=gs://data_zoo_bucket_free/data/pq/green/2020/*/ \
      --input_yellow=gs://data_zoo_bucket_free/data/pq/yellow/2020/*/ \
      --output=trips_data_all.reports-2020
```