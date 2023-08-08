#!/usr/bin/env python3 -u

from cda_etl.load.cda_loader import CDA_loader

loader = CDA_loader()

loader.transform_files_to_SQL()


