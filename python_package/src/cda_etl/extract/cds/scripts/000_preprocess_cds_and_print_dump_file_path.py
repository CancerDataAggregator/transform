#!/usr/bin/env python3 -u

import re
import sys

from os import path, listdir, makedirs
from shutil import copy

# PARAMETERS

input_dir = path.join( 'extracted_data', 'cds_initial_database_dump' )

extraction_date_file = path.join( input_dir, 'cds_extraction_date.txt' )

version_file = path.join( input_dir, 'cds_version.txt' )

extraction_dir = path.join( 'extracted_data', 'cds' )

metadata_dir = path.join( extraction_dir, '__CDS_release_metadata' )

# EXECUTION

if not path.isdir( input_dir ):
    
    sys.exit( f"FATAL: Required CDS database-dump directory '{input_dir}' not found; aborting." )

found_dumps = set()

found_extraction_date_file = False

found_version_file = False

for file_basename in sorted( listdir( input_dir ) ):
    
    if re.search( r'\.jsonl\.gz$', file_basename, flags=re.IGNORECASE ) is not None:
        
        found_dumps.add( file_basename )

    elif file_basename == path.basename( extraction_date_file ):
        
        found_extraction_date_file = True

    elif file_basename == path.basename( version_file ):
        
        found_version_file = True

if len( found_dumps ) != 1:
    
    sys.exit( f"FATAL: Found {len( found_dumps )} *.jsonl.gz files in '{input_dir}'; should be exactly one. Cannot continue." )

if not found_extraction_date_file:
    
    sys.exit( f"FATAL: Cannot locate required extraction date file '{extraction_date_file}'; aborting." )

if not found_version_file:
    
    sys.exit( f"FATAL: Cannot locate required CDS version-name file '{version_file}'; aborting." )

for output_dir in [ extraction_dir, metadata_dir ]:
    
    if not path.isdir( output_dir ):
        
        makedirs( output_dir )

copy( extraction_date_file, metadata_dir )

copy( version_file, metadata_dir )

print( path.join( input_dir, sorted( found_dumps )[0] ) )


