#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.load.cda_loader import CDA_loader

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

tsv_dir = sys.argv[1]

if not path.isdir( tsv_dir ):
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

target_tables = {
    'file_anatomic_site',
    'file_tumor_vs_normal',
    'observation',
    'treatment'
}

# EXECUTION

loader = CDA_loader()

# Compute X_nulls tables for all target tables.
for target_table in target_tables:
    loader.make_null_TSV( tsv_dir, target_table )


