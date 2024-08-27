#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.load.cda_loader import CDA_loader

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

tsv_dir = sys.argv[1]

if not path.isdir( tsv_dir ):
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

# EXECUTION

loader = CDA_loader()

loader.transform_dir_to_SQL( tsv_dir )


