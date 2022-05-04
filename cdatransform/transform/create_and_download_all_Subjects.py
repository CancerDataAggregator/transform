import argparse
from google.cloud import bigquery


def main():
    client = bigquery.Client()
    bucket_name = "my-bucket"
    project = "bigquery-public-data"
    dataset_id = "samples"
    table_id = "shakespeare"

    destination_uri = "gs://{}/{}".format(bucket_name, "shakespeare.csv")
    dataset_ref = bigquery.DatasetReference(project, dataset_id)
    table_ref = dataset_ref.table(table_id)

    extract_job = client.extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location="US",
    )  # API request
    extract_job.result()  # Waits for job to complete.

    print(
        "Exported {}:{}.{} to {}".format(project, dataset_id, table_id, destination_uri)
    )
