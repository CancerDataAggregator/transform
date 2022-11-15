from typing import Union
from typing_extensions import Literal
from dags.cdatransform.lib import get_ids
import dags.cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
from math import ceil
import jsonlines
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from dags.cdatransform.services.storage_service import StorageService
from dags.cdatransform.transform.lib import get_transformation_mapping


class IDC:
    def __init__(
        self,
        patients_file=None,
        patient=None,
        files_file=None,
        file=None,
        bq_project_dataset="broad-cda-dev.github_test",
        dest_table_id="broad-cda-dev.github_test.dicom_pivot_v9",
        mapping_file="IDC_mapping.yml",  # yaml.load(open("IDC_mapping.yml", "r"), Loader=Loader),
        source_table="bigquery-public-data.idc_v9.dicom_pivot_v9",
        endpoint=None,
        dest_bucket="gdc-bq-sample-bucket",
        dest_bucket_file_name="idc-extract.jsonl.gz",
        out_file="idc-test.jsonl.gz",
    ) -> None:
        self.dest_table_id = dest_table_id
        self.endpoint = endpoint
        self.dest_bucket = dest_bucket
        self.out_file = out_file
        self.dest_bucket_file_name = dest_bucket_file_name
        self.storage_service = StorageService()
        self.service_account_cred = self.storage_service.get_credentials()
        self.mapping: dict = get_transformation_mapping(mapping_file)
        self.patient_ids = get_ids(id=patient, id_list_file=patients_file)
        self.file_ids = get_ids(id=file, id_list_file=files_file)
        self.source_table = source_table
        self.transform_query = self._query_build()
        self.max_blobs_compose = 32  # GCS limit on how many blobs to compose together
        project_dataset_split = bq_project_dataset.split(".")
        self.project = project_dataset_split[0]
        self.dataset = project_dataset_split[1]

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
        destination_uri = "gs://{}/{}".format(bucket_name.replace("gs://", ""), self.dest_bucket_file_name)
        dataset_ref = bigquery.DatasetReference(self.project, self.dataset)
        table_ref = dataset_ref.table(table)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = (
            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        )
        job_config.compression = bigquery.Compression.GZIP
        # job_config.destinationTableProperties = {"description": """IDC data version - v.9.0,
        #    IDC extraction date - 05/24/2022"""}
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
        bucket_name_split = self.dest_bucket.replace("gs://", "").split("/")
        bucket_name = bucket_name_split[0]
        source_blob_name = self.dest_bucket_file_name
        destination_file_name = self.out_file

        bucket = self.storage_service.get_bucket(bucket_name)
        prefix = source_blob_name.split("*")

        bucket_prefix = ""
        if len(bucket_name_split) > 1:
            bucket_prefix = ""
            for i in range(1, len(bucket_name_split)):
                slash_or_empty = "/" if len(bucket_prefix) > 0 else ""
                bucket_prefix = f"{bucket_prefix}{slash_or_empty}{bucket_name_split[i]}"
            prefix = f"{bucket_prefix}/{prefix[0]}"
        else:
            bucket_prefix = bucket_name_split[0]

        if len(source_blob_name.split("*")) > 1:
            # concatenate files together. One big file downloads faster than many small ones
            # Find all 'wildcard' named files
            blobs = []
            for blob in bucket.list_blobs(prefix=prefix):
                blobs.append(blob)
            if len(blobs) > self.max_blobs_compose:
                lst_blobs = []
                groups = ceil(len(blobs) / self.max_blobs_compose)
                for i in range(groups):
                    min_index = i * self.max_blobs_compose
                    max_index = min(
                        min_index + self.max_blobs_compose, len(blobs)
                    )  # - 1
                    print(str(min_index) + ":" + str(max_index))
                    # lst_blobs.append(blobs[min_index:max_index])
                    bucket.blob(prefix + str(i) + ".jsonl.gz").compose(
                        blobs[min_index:max_index]
                    )
                for blob in blobs:
                    blob.delete()
                blobs = []
                for blob in bucket.list_blobs(prefix=prefix):
                    blobs.append(blob)
            for blob in blobs:
                print(blob.name)
            # Put them together
            slash_or_empty = "/" if len(bucket_prefix) > 0 else ""
            final_destination = f"{bucket_prefix}{slash_or_empty}{destination_file_name}"
            bucket.blob(f"{final_destination}").compose(blobs)
            return f"gs://{bucket_name_split[0]}/{final_destination}"
            # Delete smaller files from bucket
            # for blob in blobs:
            #    blob.delete()
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
        return source_blob_name

    def add_udf_to_field_query(self, val_split, mapping_transform):
        for transform in mapping_transform:
            temp = transform[0].__name__ + "("
            if isinstance(val_split, list):
                templst = []
                for i in val_split:
                    templst.append(str(i))
                temp += ",".join(templst)
            elif isinstance(val_split, str):
                temp += val_split
            # if len(transform[1]) > 1:
            #    val_split = val_split + ", [" +",".join([str(item) for item in transform[1][1:]])+"]"
            temp += ")"
        return temp

    def add_linkers(self, entity):
        linkers_str = """ """
        keys = list(self.mapping[entity]["Linkers"].keys())
        for index in range(len(self.mapping[entity]["Linkers"].keys())):
            if self.mapping[entity]["Linkers"][keys[index]] is not None:
                field: Union[str, list] = self.mapping[entity]["Linkers"][keys[index]]
                linkers_str += """ARRAY_AGG("""
                if isinstance(field, str):
                    _field: list[str] = field.split(".")
                    field: str = str(_field.pop())
                elif isinstance(field, list):
                    var_splits = []
                    for var in field:
                        if isinstance(var, str):
                            val_split = var.split(".")
                            if len(val_split) > 1:
                                val_split.pop(0)
                                val_split = val_split[0]
                            elif val_split[0] == "NULL":
                                val_split = "STRING(NULL)"
                            else:
                                val_split = "'" + val_split[0] + "'"
                        else:
                            val_split = var
                        var_splits.append(val_split)
                    print("var_splits before (linkers)")
                    print(var_splits)
                    if self.mapping[entity].get("Transformations", None) is not None:
                        if (
                            self.mapping[entity]["Transformations"].get(
                                keys[index], None
                            )
                            is not None
                        ):
                            var_splits = self.add_udf_to_field_query(
                                var_splits,
                                self.mapping[entity]["Transformations"][keys[index]],
                            )
                    print("var_splits (linkers)")
                    print(var_splits)
                    field = var_splits
                linkers_str += field + """) AS """ + keys[index]
            else:
                linkers_str += """ARRAY<STRING>[] AS """ + keys[index]
            if index < len(keys) - 1:
                linkers_str += """, """
        return linkers_str

    def field_line(self, k, val, entity, **kwargs):
        # kwargs includes is_linker
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
            print(keys)
            for index in range(len(keys)):
                temp += self.field_line(keys[index], val[keys[index]], entity)
                if index < len(keys) - 1:
                    temp += ", "
            temp += ")] AS " + k
            print("temp")
            print(temp)
            return temp
        elif isinstance(val, list):
            var_splits = []
            for var in val:
                if isinstance(var, str):
                    val_split = var.split(".")
                    if len(val_split) > 1:
                        val_split.pop(0)
                        val_split = val_split[0]
                    elif val_split[0] == "NULL":
                        val_split = "STRING(NULL)"
                    else:
                        val_split = "'" + val_split[0] + "'"
                else:
                    val_split = var
                var_splits.append(val_split)
            print("var_splits before")
            print(var_splits)
            if self.mapping[entity].get("Transformations", None) is not None:
                if self.mapping[entity]["Transformations"].get(k, None) is not None:
                    var_splits = self.add_udf_to_field_query(
                        var_splits, self.mapping[entity]["Transformations"][k]
                    )
            print("var_splits")
            print(var_splits)
            var_splits += " AS " + k
            return var_splits

        else:
            return """Null""" + """ AS """ + k
        return val

    def add_entity_fields(self, entity):
        entity_string = ""
        keys = list(self.mapping[entity]["Mapping"].keys())
        print(keys)
        for index in range(len(self.mapping[entity]["Mapping"].keys())):
            print(index)
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
            where = """WHERE crdc_instance_uuid in ('"""
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
        print()
        if len(func_desc[1]) == 1:
            out = out + func_desc[0]()
        else:
            out = out + func_desc[0](func_desc[1][1:])
        out = out + "); "
        return out

    def create_udf_str_v2(self, func_desc):
        var_list = ["x", "y", "z", "a", "b", "c"]
        var_used = []
        out = "CREATE TEMP FUNCTION " + func_desc[0].__name__ + "("
        num_var = len(func_desc[1])
        for var in range(num_var):
            out += var_list[var] + " " + func_desc[1][var]
            if var < num_var - 1:
                out += ", "
        out += ") RETURNS "
        out += func_desc[2] + " AS ("
        print()
        out = out + func_desc[0](var_list[0:num_var])
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
                            all_udfs = all_udfs + self.create_udf_str_v2(function)
                            functions_added.append(function[0])
        return all_udfs

    def _query_build(self, **kwargs):
        query = self.create_all_udfs()
        query += """ SELECT """
        if self.endpoint == "Patient":
            query += self.add_entity_fields("Patient")
            query += """, [STRUCT("""
            query += self.add_entity_fields("ResearchSubject")
            # query += """, """
            # query += self.add_linkers("ResearchSubject")
            query += """)] AS ResearchSubject"""  # , """
            # query += self.add_linkers("Patient")
            # query += """ARRAY_AGG(STRUCT("""
            # query += self.add_entity_fields("File")
            # query += """)) as File """
            # add WHERE statement if just looking for specific patients
            query += """ FROM `""" + self.source_table + """`"""
            query += self.build_where_patients()
            query += """ GROUP by id, species, collection_id, tcia_tumorLocation"""
        elif self.endpoint == "File":
            query += self.add_entity_fields("File")
            query += ""","""
            # query += """, [STRUCT("""
            # query += self.add_entity_fields("Patient")
            # add WHERE statement if just looking for specific patients
            query += self.add_linkers("File")
            # query += """)] AS Subjects"""
            # query += """, [STRUCT("""
            # query += self.add_entity_fields("ResearchSubject")
            # query += """)] AS ResearchSubject"""
            query += """ FROM `""" + self.source_table + """`"""
            query += self.build_where_files()
            query += """ GROUP by id, gcs_url, Modality, collection_id, PatientID, tcia_species, tcia_tumorLocation, crdc_series_uuid"""
        print(query)
        return query


endpoint_type = Union[Literal["Patient"], Literal["File"]]


# def main(
#     mapping_file: str,
#     dest_table_id: str,
#     gsa_key: str,
#     endpoint: endpoint_type,
#     gsa_info: str,
#     patient: Optional[str] = None,
#     patients: Optional[list[str]] = None,
#     source_table: str = "bigquery-public-data.idc_v9.dicom_pivot_v9",
#     file: Optional[str] = None,
#     files: Optional[str] = None,
#     out_file: str = "idc_extract.jsonl.gz",
#     make_bq_table: bool = False,
#     make_bucket_file: bool = False,
#     dest_bucket: str = "broad-cda-dev",
#     dest_bucket_file_name: str = "public/idc-test.jsonl.gz",
# ):
#     """_summary_

#     Args:
#         mapping_file (str): "Location of IDC mapping file"
#         dest_table_id (str): "Permanent table destination after querying IDC"
#         gsa_key (str): "Location of user GSA key
#         endpoint (endpoint_type):"Patient of File endpoint"
#         gsa_info: str "json content of GSA key or github.secret"
#         patient (Optional[str]): "Extract just this patient"
#         patients (Optional[list[str]]): "Optional file with list of patient ids (one to a line)"
#         source_table (str): "IDC source table to be queried"
#         file(Optional[str]): "Extract just this file"
#         files(Optional[list[str]]) "Optional file with list of file ids (one to a line)"
#         out_file (str): "Out file name. Should end with .gz"
#         make_bq_table (bool): "Create new BQ permanent table from IDC view"
#         make_bucket_file (bool): "Create new file in GCS from permanent table"
#         dest_bucket (str): "GCS bucket"
#         dest_bucket_file_name: (str) "GCS bucket file name"
#     """

#     make_bq_table = make_bq_table
#     # make_bucket_file = make_bucket_file
#     mapping = load(open(mapping_file, "r"), Loader=Loader)
#     # out_file = out_file
#     idc = IDC(
#         gsa_key=gsa_key,
#         gsa_info=gsa_info,
#         dest_table_id=dest_table_id,
#         patients_file=patients,
#         patient=patient,
#         files_file=files,
#         file=file,
#         mapping=mapping,
#         source_table=source_table,
#         endpoint=endpoint,
#         dest_bucket=dest_bucket,
#         dest_bucket_file_name=dest_bucket_file_name,
#         out_file=out_file,
#     )
#     if make_bq_table:
#         idc.query_idc_to_table()
#     if make_bucket_file:
#         idc.table_to_bucket()
#     idc.download_blob()
