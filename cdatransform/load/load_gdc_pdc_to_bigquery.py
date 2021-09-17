import argparse
import json
from google.cloud import bigquery, storage
from google.oauth2 import service_account
# Construct a BigQuery client object.
class Load:
    def __init__(
        self,
        gsa_key='../../GCS-service-account-key.json',
        gsa_info=None,
        data_file=None,
        dest_table_id='gdc-bq-sample.idc_test.dicom_pivot_v3',
        schema=None,
        
        # dest_bucket='gdc-bq-sample-bucket',
        # dest_bucket_file_name='druth/idc-extract.jsonl.gz',
        # out_file='idc-test.jsonl.gz'
    ) -> None:
        self.gsa_key = gsa_key
        self.gsa_info = gsa_info
        self.dest_table_id = dest_table_id
        self.data_file = data_file
        self.schema = self._get_json_schema(schema)
        self.service_account_cred = self._service_account_cred()

    def _get_json_schema(self, schema):
        f = open(schema, )
        return json.load(f)

    def _service_account_cred(self):
        key_path = self.gsa_key
        gsa_info = self.gsa_info
        try:
            credentials = service_account.Credentials()
        except Exception:
            if self.gsa_info is not None:
                credentials = service_account.Credentials.from_service_account_info(
                    gsa_info)
            else:
                credentials = service_account.Credentials.from_service_account_file(
                    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return credentials

    def load_data(self):
        dest_table_id = self.dest_table_id
        credentials = self.service_account_cred
        client = bigquery.Client(credentials=credentials, project=credentials.project_id,)
        #client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, schema=self.schema,
        )

        with open(self.data_file, "rb") as source_file:
            job = client.load_table_from_file(source_file, dest_table_id, job_config=job_config)

        job.result()  # Waits for the job to complete.

        table = client.get_table(dest_table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), dest_table_id
            )
        )
def main():
    parser = argparse.ArgumentParser(description="Load transformed data from DC to a BQ table.")
    parser.add_argument("data_file", help="Location of transformed data file")
    parser.add_argument("schema_file", help="Location of schema file")
    parser.add_argument("--dest_table_id", help="Permanent table destination after querying IDC",
                        default='gdc-bq-sample.dev.upload_test')
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--gsa_info", help="json content of GSA key or github.secret")
    args = parser.parse_args()
    load = Load(
        gsa_key=args.gsa_key,
        gsa_info=args.gsa_info,
        dest_table_id=args.dest_table_id,
        data_file=args.data_file,
        schema = args.schema_file
    )
    load.load_data()

if __name__ == "__main__":
    main()
