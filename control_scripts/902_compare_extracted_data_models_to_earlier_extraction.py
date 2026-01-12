#!/usr/bin/env python3 -u

import re
import sys

from os import listdir, path

# ARGUMENT

if len( sys.argv ) != 3:
    sys.exit( f"\n   Usage: {sys.argv[0]} <historical CDA ETL output directory containing an extracted_data/ subdir> <current CDA ETL output directory>\n" )

earlier_extraction_root = path.join( sys.argv[1], 'extracted_data' )

current_extraction_root = path.join( sys.argv[2], 'extracted_data' )

if not ( path.isdir( earlier_extraction_root ) and path.isdir( current_extraction_root ) ):
    sys.exit( f"\n   Usage: {sys.argv[0]} <historical CDA ETL output directory containing an extracted_data/ subdir> <current CDA ETL output directory>\n" )

# EXECUTION

# GDC

earlier_gdc_root = path.join( earlier_extraction_root, 'gdc', 'all_TSV_output' )

current_gdc_root = path.join( current_extraction_root, 'gdc', 'all_TSV_output' )

earlier_gdc_files = sorted( [ f for f in listdir( earlier_gdc_root ) if path.isfile( path.join( earlier_gdc_root, f ) ) ] )

current_gdc_files = sorted( [ f for f in listdir( current_gdc_root ) if path.isfile( path.join( current_gdc_root, f ) ) ] )

fileset_changed = False

files_that_disappeared = set()

for f in earlier_gdc_files:
    if f not in current_gdc_files:
        files_that_disappeared.add( f )
        fileset_changed = True

files_that_appeared = set()

for f in current_gdc_files:
    if f not in earlier_gdc_files:
        files_that_appeared.add( f )
        fileset_changed = True

if fileset_changed:
    if len( files_that_disappeared ) > 0:
        print( f"Extraction files that disappeared between {earlier_gdc_root} and {current_gdc_root}: {sorted( files_that_disappeared )}" )

    if len( files_that_appeared ) > 0:
        print( f"New extraction files in {current_gdc_root} not seen in {earlier_gdc_root}: {sorted( files_that_appeared )}" )

else:
    print( f"List of extraction files is the same for both {earlier_gdc_root} and {current_gdc_root}." )

headers_changed = False

for f in [ tsv for tsv in earlier_gdc_files if re.search( r'\.tsv$', tsv ) is not None ]:
    if f in current_gdc_files:
        earlier_headers = list()
        current_headers = list()

        with open( path.join( earlier_gdc_root, f ) ) as IN:
            earlier_headers = sorted( next( IN ).rstrip( '\n' ).split( '\t' ) )

        with open( path.join( current_gdc_root, f ) ) as IN:
            current_headers = sorted( next( IN ).rstrip( '\n' ).split( '\t' ) )

        headers_that_disappeared = set()

        for h in earlier_headers:
            if h not in current_headers:
                headers_that_disappeared.add( h )
                headers_changed = True

        headers_that_appeared = set()

        for h in current_headers:
            if h not in earlier_headers:
                headers_that_appeared.add( h )
                headers_changed = True

        if len( headers_that_disappeared ) > 0:
            print( f"[{f}] Headers that disappeared between {earlier_gdc_root} and {current_gdc_root}: {sorted( headers_that_disappeared )}" )

        if len( headers_that_appeared ) > 0:
            print( f"[{f}] New headers that appeared between {earlier_gdc_root} and {current_gdc_root}: {sorted( headers_that_appeared )}" )

if not headers_changed:
    print( f"No header changes were detected for any TSV files between {earlier_gdc_root} and {current_gdc_root}." )


