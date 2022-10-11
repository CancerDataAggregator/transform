import argparse
import json

from google.cloud import bigquery  # , storage
from google.oauth2 import service_account
from Load import Load


def main():
    parser = argparse.ArgumentParser(
        description="Load transformed data from DC to a BQ table."
    )
    parser.add_argument("data_file", help="Location of transformed data file")
    parser.add_argument("schema_file", help="Location of schema file")
    parser.add_argument(
        "--dest_table_id",
        help="Permanent table destination after querying IDC",
        default="gdc-bq-sample.dev.upload_test",
    )
    parser.add_argument("--gsa_key", help="Location of user GSA key")
    parser.add_argument("--gsa_info", help="json content of GSA key or github.secret")
    args = parser.parse_args()
    load = Load(
        gsa_key=args.gsa_key,
        gsa_info=args.gsa_info,
        dest_table_id=args.dest_table_id,
        data_file=args.data_file,
        schema=args.schema_file,
    )
    load.load_data()


if __name__ == "__main__":
    main()
