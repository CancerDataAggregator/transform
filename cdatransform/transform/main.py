import argparse
import gzip
import sys
import time

import yaml
from yaml import Loader
import jsonlines
import cdatransform.transform.transform_lib.transform_with_YAML_v1 as tr
from cdatransform.lib import get_ids


def main():

    parser = argparse.ArgumentParser(
        prog="Transform", description="Transform source DC jsonl to Harmonized jsonl"
    )
    parser.add_argument("input", help="Input data file.")
    parser.add_argument("output", help="Output data file.")
    parser.add_argument("map_trans", help="Mapping and Transformations file.")
    parser.add_argument(
        "--endpoint", help="endpoint from which the file was generated. e.g. cases"
    )
    parser.add_argument("--log", default="transform.log", help="Name of log file.")
    parser.add_argument("--id", help="Transform just this case/file")
    parser.add_argument(
        "--ids", help="Optional file with list of case/file ids (one to a line)"
    )
    # parser.add_argument("endpoint", help="endpoint from which the file was generated. e.g. cases")
    args = parser.parse_args()

    MandT: dict[str, dict[str, dict]] = yaml.load(
        open(args.map_trans, "r"), Loader=Loader
    )
    for entity, MorT_dict in MandT.items():
        if "Transformations" in MorT_dict:
            MandT[entity]["Transformations"] = tr.functionalize_trans_dict(
                MandT[entity]["Transformations"]
            )
    transform = tr.Transform(MandT, args.endpoint)
    t0 = time.time()
    count = 0
    id_list = get_ids(id=args.id, id_list_file=args.ids)

    with gzip.open(args.input, "r") as infp:
        reader = jsonlines.Reader(infp)
        with gzip.open(args.output, "w") as outfp:
            writer = jsonlines.Writer(outfp)
            for line in reader:
                if id_list is None or line.get("id") in id_list:
                    writer.write(transform(line))
                    count += 1
                    if count % 1000 == 0:
                        sys.stderr.write(
                            f"Processed {count} {args.endpoint} ({time.time() - t0}).\n"
                        )

    sys.stderr.write(f"Processed {count} {args.endpoint} ({time.time() - t0}).\n")


if __name__ == "__main__":
    # execute only if run as a script
    main()
