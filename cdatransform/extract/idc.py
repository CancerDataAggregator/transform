import argparse
from google.cloud import bigquery, storage
from google.oauth2 import service_account

idc_fields = [
    "PatientID", 
    "BodyPartExamined", 
    "StudyDescription", 
    "Modality", 
    "collection_id", 
    "crdc_study_uuid", 
    "crdc_series_uuid", 
    "crdc_instance_uuid", 
    "Program", 
    "tcia_tumorLocation", 
    "source_DOI", 
    "AnatomicRegionSequence",
]
def implicit_storage_cred():
    from google.cloud import storage

    # If you don't specify credentials when constructing the client, the
    # client library will look for credentials in the environment.
    storage_client = storage.Client()
    return storage_client
def query_idc_to_table(idc_fields,**kwargs):
    dest_table_id = "gdc-bq-sample.idc_test.dicom_pivot_wave1"
    key_path = kwargs.get('gsa_key','../../../../GCS-service-account-key.json')
    try:
        credentials = implicit_storage_cred()
    except:
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )

    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

    job_config = bigquery.QueryJobConfig(
        allow_large_results=True, destination=dest_table_id)#, use_legacy_sql=True

    sql = ' '.join(["""
    SELECT""",""", """.join(idc_fields),"""
    FROM canceridc-data.idc_views.dicom_pivot_wave1
    """])

    # Start the query, passing in the extra configuration.
    query_job = client.query(sql, job_config=job_config)  # Make an API request.
    query_job.result()  # Wait for the job to complete.
def table_to_bucket(**kwargs):
    # Save destination table to GCS bucket
    key_path = kwargs.get('gsa_key')
    bucket_name = 'gdc-bq-sample-bucket/druth'
    project = 'gdc-bq-sample'
    dataset_id = 'idc_test'
    table = 'dicom_pivot_wave1'
    try:
        credentials = implicit_storage_cred()
    except:
        credentials = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
    destination_uri = "gs://{}/{}".format(bucket_name, "idc-test.jsonl.gz")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.job.ExtractJobConfig()
    job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
    job_config.compression = bigquery.Compression.GZIP

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        job_config=job_config,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

def download_blob(bucket_name, source_blob_name, destination_file_name,**kwargs):
    """Downloads a blob from the bucket."""
    key_path = kwargs.get('gsa_key')
    try:
        storage_client = storage.Client()
    except:
        storage_client = storage.Client.from_service_account_json(key_path)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to {}.".format(
            source_blob_name, destination_file_name
        )
    )    
def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    #parser.add_argument("out_file", help="Out file name. Should end with .gz")
    #parser.add_argument("cache_file", help="Use (or generate if missing) cache file.")
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--case", help="Extract just this case")
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)"
    )
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    args = parser.parse_args()
    gsa_key = args.gsa_key
    #gdc = GDC(
    #    cache_file=pathlib.Path(args.cache_file)
    #)
    #gdc.save_cases(
    #    args.out_file, case_ids=get_case_ids(case=args.case, case_list_file=args.cases)
    #)
    
    ## Query the IDC view and save to table
    query_idc_to_table(idc_fields,gsa_key = gsa_key)
    # Save the table to GCS
    table_to_bucket(gsa_key = gsa_key)
    # Download file from GCS
    bucket_name = 'gdc-bq-sample-bucket'
    source_blob = 'druth/idc-test.jsonl.gz'
    destination = 'idc-test.jsonl.gz'
    download_blob(bucket_name,source_blob,destination,gsa_key = gsa_key) 

if __name__ == "__main__":
    main()