import json
import sys
import gzip
import yaml

with gzip.open(sys.argv[1], "r") as in_fp:
    obj = json.load(in_fp)  # We can do this because the jsonl contains only one object

    with open(sys.argv[2], "w") as out_fp:
        yaml.dump(obj, out_fp, width=120, indent=2)
