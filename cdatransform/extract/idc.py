import yaml
from yaml import Loader
import argparse
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from cdatransform.lib import get_case_ids
import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
import jsonlines
import gzip
import os


class IDC:
    def __init__(
        self,
        gsa_key="../../GCS-service-account-key.json",
        gsa_info=None,
        patients_file=None,
        patient=None,
        files_file=None,
        file=None,
        dest_table_id="broad-cda-dev.github_test.dicom_pivot_v4",
        mapping=None,  # yaml.load(open("IDC_mapping.yml", "r"), Loader=Loader),
        source_table="canceridc-data.idc_v4.dicom_pivot_v4",
        endpoint=None,
        dest_bucket="gdc-bq-sample-bucket",
        dest_bucket_file_name="druth/idc-extract.jsonl.gz",
        out_file="idc-test.jsonl.gz",
    ) -> None:
        self.gsa_key = gsa_key
        self.gsa_info = gsa_info
        self.dest_table_id = dest_table_id
        self.endpoint = endpoint
        self.dest_bucket = dest_bucket
        self.out_file = out_file
        self.dest_bucket_file_name = dest_bucket_file_name
        self.service_account_cred = self._service_account_cred()
        self.mapping = self._init_mapping(mapping)
        self.patient_ids = get_case_ids(case=patient, case_list_file=patients_file)
        self.file_ids = get_case_ids(case=file, case_list_file=files_file)
        self.source_table = source_table
        self.transform_query = self._query_build()

    def _service_account_cred(self):
        key_path = self.gsa_key
        gsa_info = self.gsa_info
        try:
            credentials = service_account.Credentials()
        except Exception:
            if self.gsa_info is not None:
                credentials = service_account.Credentials.from_service_account_info(
                    gsa_info
                )
            else:
                credentials = service_account.Credentials.from_service_account_file(
                    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
                )
        return credentials

    def _init_mapping(self, mapping):
        for entity, MorT_dict in mapping.items():
            if "Transformations" in MorT_dict:
                mapping[entity]["Transformations"] = tr.functionalize_trans_dict(
                    mapping[entity]["Transformations"]
                )
        return mapping

    def query_idc_to_table(self):
        dest_table_id = self.dest_table_id
        credentials = self.service_account_cred

        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )

        job_config = bigquery.QueryJobConfig(
            allow_large_results=True,
            destination=dest_table_id,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        sql = self.transform_query

        # Start the query, passing in the extra configuration.
        query_job = client.query(
            sql, location="US", job_config=job_config
        )  # Make an API request.
        query_job.result()  # Wait for the job to complete.

    def table_to_bucket(self):
        # Save destination table to GCS bucket
        bucket_name = self.dest_bucket
        dataset_id = self.dest_table_id.split(".")[1]
        table = self.dest_table_id.split(".")[2]
        credentials = self.service_account_cred
        project = credentials.project_id
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        # dest_bucket_file_name = self.dest_bucket_file_name.split('.')
        # dest_bucket_file_name.insert(-2,'*')
        # dest_bucket_file_name='.'.join(dest_bucket_file_name)
        destination_uri = "gs://{}/{}".format(bucket_name, self.dest_bucket_file_name)
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = (
            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        )
        job_config.compression = bigquery.Compression.GZIP

        extract_job = client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            # Location must match that of the source table.
            location="US",
        )  # API request
        print(extract_job.result())
        print(str(extract_job.destination_uris))  # Waits for job to complete.

    def download_blob(self):
        """Downloads a blob from the bucket."""
        key_path = self.gsa_key
        bucket_name = self.dest_bucket
        source_blob_name = self.dest_bucket_file_name
        destination_file_name = self.out_file

        try:
            storage_client = storage.Client()
        except Exception:
            storage_client = storage.Client.from_service_account_json(key_path)
        bucket = storage_client.bucket(bucket_name)
        prefix = source_blob_name.split("*")
        if len(prefix) > 1:
            # concatenate files together. One big file downloads faster than many small ones
            # Find all 'wildcard' named files
            blobs = bucket.list_blobs(prefix=prefix[0])
            # Put them together
            bucket.blob(destination_file_name).compose(blobs)
            # Download big file
            blob = bucket.blob(destination_file_name)
            blob.download_to_filename(destination_file_name)
            # Delete smaller files from bucket
            for blob in blobs:
                blob.delete()
            # blob_names = []
            # for blob in blobs:
            #    blob_names.append(blob.name)
            #    blob.download_to_filename(blob.name)
            # with gzip.open(destination_file_name, 'w') as outfile:
            #    writer = jsonlines.Writer(outfile)
            #    for fname in blob_names:
            #        with gzip.open(fname, 'r') as infile:
            #            reader = jsonlines.Reader(infile)
            #            for line in reader:
            #                writer.write(line)
            #        os.remove(fname)
        else:
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)

        print(
            "Blob {} downloaded to {}.".format(source_blob_name, destination_file_name)
        )

    def add_udf_to_field_query(self, val_split, mapping_transform):
        for transform in mapping_transform:
            val_split = transform[0].__name__ + "(" + val_split
            # if len(transform[1]) > 1:
            #    val_split = val_split + ", [" +",".join([str(item) for item in transform[1][1:]])+"]"
            val_split = val_split + ")"
        return val_split

    def field_line(self, k, val, entity):
        if isinstance(val, str):
            val_split = val.split(".")
            if len(val_split) > 1:
                val_split.pop(0)
                val_split = val_split[0]
            elif val_split[0] == "NULL":
                val_split = "STRING(NULL)"
            else:
                val_split = "'" + val_split[0] + "'"
            if k == "subject_associated_project":
                val_split = "[" + val_split + "]"
            if self.mapping[entity].get("Transformations", None) is not None:
                if self.mapping[entity]["Transformations"].get(k, None) is not None:
                    val_split = self.add_udf_to_field_query(
                        val_split, self.mapping[entity]["Transformations"][k]
                    )

            return (val_split) + """ AS """ + k
        elif isinstance(val, dict):
            temp = "[STRUCT("
            keys = list(val.keys())
            for index in range(len(keys)):
                temp += self.field_line(keys[index], val[keys[index]], entity)
                if index < len(keys) - 1:
                    temp += ", "
            temp += ")] AS " + k
            return temp
        else:
            return """Null""" + """ AS """ + k
        return val

    def add_entity_fields(self, entity):
        entity_string = ""
        keys = list(self.mapping[entity]["Mapping"].keys())
        for index in range(len(self.mapping[entity]["Mapping"].keys())):
            entity_string += self.field_line(
                keys[index], self.mapping[entity]["Mapping"][keys[index]], entity
            )
            if index < len(keys) - 1:
                entity_string += """, """
        return entity_string

    def build_where_patients(self):
        where = ""
        if self.patient_ids is not None:
            where = """ WHERE PatientID in ('"""
            where += """','""".join(self.patient_ids) + """')"""
        return where

    def build_where_files(self):
        where = ""
        if self.file_ids is not None:
            where = """WHERE crdc_instance_uuid in ("""
            where += """','""".join(self.file_ids) + """')"""
        return where

    def create_udf_str(self, func_desc):
        out = (
            "CREATE TEMP FUNCTION "
            + func_desc[0].__name__
            + "(x "
            + func_desc[1][0]
            + ") RETURNS "
        )
        out = out + func_desc[1][0] + " AS ("
        if len(func_desc[1]) == 1:
            out = out + func_desc[0]()
        else:
            out = out + func_desc[0](func_desc[1][1:])
        out = out + "); "
        return out

    def create_all_udfs(self):
        functions_added = []
        all_udfs = ""
        for entity, Map_and_Trans in self.mapping.items():
            if Map_and_Trans.get("Transformations", None) is not None:
                for field, functions in Map_and_Trans.get("Transformations").items():
                    for function in functions:
                        if function[0] not in functions_added:
                            all_udfs = all_udfs + self.create_udf_str(function)
        return all_udfs

    def add_linkers(self, entity):
        linkers_str = """ """
        keys = list(self.mapping[entity]["Linkers"].keys())
        for index in range(len(self.mapping[entity]["Linkers"].keys())):
            if self.mapping[entity]["Linkers"][keys[index]] is not None:
                field = self.mapping[entity]["Linkers"][keys[index]]
                field = field.split(".")
                field = field.pop()
                field = str(field)
                linkers_str += """ARRAY_AGG(""" + field + """) as """ + keys[index]
            else:
                linkers_str += """ARRAY<STRING>[] as """ + keys[index]
            if index < len(keys) - 1:
                linkers_str += """, """
        return linkers_str

    def _query_build(self, **kwargs):
        query = self.create_all_udfs()
        query += """ SELECT """
        if self.endpoint == "Patient":
            query += self.add_entity_fields("Patient")
            query += """, """
            query += self.add_linkers("Patient")
            # add WHERE statement if just looking for specific patients
            query += """ FROM `""" + self.source_table + """`"""
            query += self.build_where_patients()
            query += """ GROUP by id, species, collection_id"""
        elif self.endpoint == "File":
            query += self.add_entity_fields("File")
            query += """, [STRUCT("""
            query += self.add_entity_fields("Patient")
            # add WHERE statement if just looking for specific patients
            query += """)] AS Subject FROM `""" + self.source_table + """`"""
            query += self.build_where_files()
            query += """ GROUP by id, gcs_url, Modality, collection_id, PatientID, tcia_species"""
        print(query)
        return query


def main():
    parser = argparse.ArgumentParser(description="Pull case data from GDC API.")
    parser.add_argument("mapping_file", help="Location of IDC mapping file")
    parser.add_argument(
        "--dest_table_id",
        help="Permanent table destination after querying IDC",
        default="broad-cda-dev.github_testing.idc_patient_testing",
    )
    parser.add_argument(
        "--source_table",
        help="IDC source table to be queried",
        default="canceridc-data.idc_v4.dicom_pivot_v4",
    )
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--endpoint", help="Patient of File endpoint")
    parser.add_argument("--gsa_info", help="json content of GSA key or github.secret")
    parser.add_argument("--patient", help="Extract just this patient", default=None)
    parser.add_argument(
        "--patients",
        help="Optional file with list of patient ids (one to a line)",
        default=None,
    )
    parser.add_argument("--file", help="Extract just this file", default=None)
    parser.add_argument(
        "--files",
        help="Optional file with list of file ids (one to a line)",
        default=None,
    )
    parser.add_argument(
        "--out_file",
        help="Out file name. Should end with .gz",
        default="idc_extract.jsonl.gz",
    )
    parser.add_argument("--cache", help="Use cached files.", action="store_true")
    parser.add_argument(
        "--make_bq_table",
        help="Create new BQ permanent table from IDC view",
        default=False,
        type=bool,
    )
    parser.add_argument(
        "--make_bucket_file",
        help="Create new file in GCS from permanent table",
        default=False,
        type=bool,
    )
    parser.add_argument("--dest_bucket", help="GCS bucket", default="broad-cda-dev")
    parser.add_argument(
        "--dest_bucket_file_name",
        help="GCS bucket file name",
        default="public/idc-test.jsonl.gz",
    )
    args = parser.parse_args()
    make_bq_table = args.make_bq_table
    # make_bucket_file = args.make_bucket_file
    mapping = yaml.load(open(args.mapping_file, "r"), Loader=Loader)
    # out_file = args.out_file
    idc = IDC(
        gsa_key=args.gsa_key,
        gsa_info=args.gsa_info,
        dest_table_id=args.dest_table_id,
        patients_file=args.patients,
        patient=args.patient,
        files_file=args.files,
        file=args.file,
        mapping=mapping,
        source_table=args.source_table,
        endpoint=args.endpoint,
        dest_bucket=args.dest_bucket,
        dest_bucket_file_name=args.dest_bucket_file_name,
        out_file=args.out_file,
    )
    if make_bq_table:
        idc.query_idc_to_table()
    if args.make_bucket_file:
        idc.table_to_bucket()
    idc.download_blob()


if __name__ == "__main__":
    main()
