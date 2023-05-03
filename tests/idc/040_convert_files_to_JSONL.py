#!/usr/bin/env python3 -u

from etl.load.cda_loader import CDA_loader

loader = CDA_loader()

loader.transform_merged_files_to_jsonl()


