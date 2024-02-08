import io
import os
import requests
import pandas as pd
import pyarrow
from google.cloud import storage

"""
Pre-reqs: 
1. `pip install pandas pyarrow google-cloud-storage`
2. Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account key
3. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

yellow_taxi_dtype = {
    'VendorID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),
    'trip_distance': float,
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'payment_type': pd.Int64Dtype(),
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'congestion_surcharge': float
}
yellow_taxi_parse_dates = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']

green_taxy_dtype = {
    'VendorID': pd.Int64Dtype(),
    'RatecodeID': pd.Int64Dtype(),
    'store_and_fwd_flag': str,
    'PULocationID': pd.Int64Dtype(),
    'DOLocationID': pd.Int64Dtype(),
    'passenger_count': pd.Int64Dtype(),        
    'trip_distance': float,
    'fare_amount': float,
    'extra': float,
    'mta_tax': float,
    'tip_amount': float,
    'tolls_amount': float,
    'ehail_fee': float,
    'improvement_surcharge': float,
    'total_amount': float,
    'payment_type': pd.Int64Dtype(),
    'trip_type': pd.Int64Dtype(),
    'congestion_surcharge': float
}
green_taxy_parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

fhv_dtype = {
    'dispatching_base_num': str,
    'PUlocationID': pd.Int64Dtype(),
    'DOlocationID': pd.Int64Dtype(),
    'SR_Flag': pd.Int64Dtype(),
    'Affiliated_base_number': str
}
fhv_parse_dates = ['pickup_datetime', 'dropOff_datetime']

# services = ['fhv','green','yellow']

services_metainfo = {
    'yellow': {'dtype': yellow_taxi_dtype, 'parse_dates': yellow_taxi_parse_dates},
    'green': {'dtype': green_taxy_dtype, 'parse_dates': green_taxy_parse_dates},
    'fhv': {'dtype': fhv_dtype, 'parse_dates': fhv_parse_dates}
}

base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'

CREDENTIALS = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "~/.google/credentials/google_credentials.json")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        service_metainfo = services_metainfo[service]

        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'
        print(f"File: {file_name}")

        request_url = base_url + service + '/' + file_name
        print(f"Request: {request_url}")
        df = pd.read_csv(request_url, sep=',', compression='gzip', dtype=service_metainfo['dtype'])
        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(file_name, engine='pyarrow')
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")

if __name__ == "__main__":
    print(f"Credentials: {CREDENTIALS}")
    print(f"Bucket: {BUCKET}")
    # web_to_gcs('2019', 'green')
    # web_to_gcs('2020', 'green')
    web_to_gcs('2019', 'yellow')
    web_to_gcs('2020', 'yellow')
    web_to_gcs('2019', 'fhv')
