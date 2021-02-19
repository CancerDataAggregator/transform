import requests
import sys
import time
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage, exceptions


def retry_get(endpoint, params, base_retry_interval=180.0):
    retry_interval = base_retry_interval
    while True:
        result = requests.get(endpoint, params=params)
        if result.ok:
            return result
        else:
            sys.stderr.write(f"API call failed. Retrying in {retry_interval}s ...\n")
            time.sleep(retry_interval)
            retry_interval *= 2
def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"
    
    try:
        storage_client = storage.Client()
    except:
        GCSKey=input("""Implicit access to GCS did not work.
        Input location of service-account key""")
        storage_client = storage.Client.from_service_account_json(GCSKey)

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )
