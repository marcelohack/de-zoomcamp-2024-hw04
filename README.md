# Data Engineering Zoomcamp 2024 - Homework 4

### Instructions

1. Run the terraform to create the GCP stack ("trips_data_all" dataset on BigQuery and the dbt-data-lake storage bucket)

```console
cd gcp_stack
terraform init
terraform apply
```

After finishing the the homework, destroy the stack:

```console
terraform destroy # You might need to empty the dbt-data-lake storage bucket
```

2. Run the data ingestion script (src/web_to_gcs.py) that will copy the (Yellow, Green 2019/2020 and FHV 2019) datasets from "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/" to the dbt-data-lake storage bucket

```console
python src/web_to_gcs.py
```

3. Load the data into BigQuery for the Dbt processing

```console
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp.trips_data_all.green_tripdata_external`
OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-dbt-data-lake/green/*.parquet']
);

CREATE OR REPLACE TABLE `de-zoomcamp.trips_data_all.green_tripdata` AS 
SELECT * FROM `de-zoomcamp.trips_data_all.green_tripdata_external`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp.trips_data_all.yellow_tripdata_external`
OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-dbt-data-lake/yellow/*.parquet']
);

CREATE OR REPLACE TABLE `de-zoomcamp.trips_data_all.yellow_tripdata` AS 
SELECT * FROM `de-zoomcamp.trips_data_all.yellow_tripdata_external`;

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp.trips_data_all.fhv_tripdata_external`
OPTIONS (
    format ="PARQUET",
    uris = ['gs://de-zoomcamp-dbt-data-lake/fhv/*.parquet']
);

CREATE OR REPLACE TABLE `de-zoomcamp.trips_data_all.fhv_tripdata` AS 
SELECT * FROM `de-zoomcamp.trips_data_all.fhv_tripdata_external`;

DROP TABLE `de-zoomcamp.trips_data_all.green_tripdata_external`;
DROP TABLE `de-zoomcamp.trips_data_all.yellow_tripdata_external`;
DROP TABLE `de-zoomcamp.trips_data_all.fhv_tripdata_external`;
```

After finishing the Dbt processing by the end of the homework, delete all data:

```console
DROP TABLE `de-zoomcamp.trips_data_all.green_tripdata`;
DROP TABLE `de-zoomcamp.trips_data_all.yellow_tripdata`;
DROP TABLE `de-zoomcamp.trips_data_all.fhv_tripdata`;
```

### Questions  
Please check the [Homework Execution](./homework_execution.md) document.
