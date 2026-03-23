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

# Ensure that files associated with no subjects and subjects associated with
# no files are all represented (as associated with <null>) in the
# file_describes_subject table. Query construction logic in the API
# relies on the assumption that all files and subjects are represented
# in file_describes_subject (2026-03-20).
loader.complete_file_describes_subject( tsv_dir )


