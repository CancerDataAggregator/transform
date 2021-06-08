import argparse
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from cdatransform.lib import get_case_ids

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

class IDC:
    def __init__(
        self,
        gsa_key = '../../GCS-service-account-key.json',
        cases_file = None,
        case = None,
        dest_table_id = 'gdc-bq-sample.idc_test.dicom_pivot_wave1',
        dest_bucket = 'gdc-bq-sample-bucket',
        dest_bucket_file_name = 'druth/idc-extract.jsonl.gz',
        out_file = 'idc-test.jsonl.gz'
    ) -> None:
        self.gsa_key = gsa_key
        self.dest_table_id = dest_table_id
        self.dest_bucket = dest_bucket
        self.out_file = out_file
        self.dest_bucket_file_name = dest_bucket_file_name
        self.service_account_cred = self._service_account_cred()
        self.case_ids = get_case_ids(case=case, case_list_file=cases_file)
        
    def _service_account_cred(self):
        key_path = self.gsa_key
        try:
            credentials = service_account.Credentials()
        except:
            credentials = service_account.Credentials.from_service_account_file(
                key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return credentials
        
    def query_idc_to_table(self,idc_fields):
        dest_table_id = self.dest_table_id
        credentials = self.service_account_cred

        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

        job_config = bigquery.QueryJobConfig(
            allow_large_results=True, destination=dest_table_id,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        sql = ' '.join(["""
        SELECT""",""", """.join(idc_fields),"""
        FROM canceridc-data.idc_views.dicom_pivot_wave1
        """])
        if self.case_ids is not None:
            sql = ' '.join([sql, "WHERE crdc_instance_uuid in ('"])
            cases_str = "','".join(self.case_ids)
            sql = sql + cases_str + "')"

        # Start the query, passing in the extra configuration.
        query_job = client.query(sql, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.
    
    def table_to_bucket(self):
        # Save destination table to GCS bucket
        bucket_name = self.dest_bucket
        dataset_id = 'idc_test'
        table = 'dicom_pivot_wave1'
        credentials = self.service_account_cred
        project = credentials.project_id
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

    def download_blob(self):
        """Downloads a blob from the bucket."""
        key_path = self.gsa_key
        bucket_name = self.dest_bucket
        source_blob_name = self.dest_bucket_file_name
        destination_file_name = self.out_file
        
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
    parser.add_argument("out_file", help="Out file name. Should end with .gz",
                        default = 'idc_extract.jsonl.gz')
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--case", help="Extract just this case", default = None     )
    parser.add_argument(
        "--cases", help="Optional file with list of case ids (one to a line)", default = None
    )
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    parser.add_argument("--make_bq_table", help="Create new BQ permanent table from IDC view",
                       default = False, type = bool )
    parser.add_argument("--make_bucket_file", help="Create new file in GCS from permanent table",
                       default = False, type = bool )
    parser.add_argument("--dest_table_id", help="Permanent table destination after querying IDC",
                       default = 'gdc-bq-sample.idc_test.dicom_pivot_wave1')
    parser.add_argument("--dest_bucket", help="GCS bucket", default = 'gdc-bq-sample-bucket')
    parser.add_argument("--dest_bucket_file_name", help="GCS bucket file name",
                       default = 'idc-test.jsonl.gz')
    args = parser.parse_args()
    make_bq_table = args.make_bq_table
    make_bucket_file = args.make_bucket_file
    out_file = args.out_file
    idc = IDC(
        gsa_key = args.gsa_key,
        dest_table_id = args.dest_table_id,
        cases_file = args.cases,
        case = args.case,
        dest_bucket = args.dest_bucket,
        dest_bucket_file_name = args.dest_bucket_file_name,
        out_file = out_file,
    )
    if make_bq_table:
        idc.query_idc_to_table(idc_fields)
    if make_bucket_file:
        idc.table_to_bucket()
    idc.download_blob() 

if __name__ == "__main__":
    main()