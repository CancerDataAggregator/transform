#!/usr/bin/env python3 -u

import re
import sys

from os import path

from cda_etl.lib import map_columns_one_to_one

file_table = path.join( 'extracted_data', 'gdc', 'all_TSV_output', 'file.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__GDC_supplemental_metadata' )

output_file = path.join( output_dir, 'format_assertions_by_file_extension.GDC.tsv' )

file_table_column_map = map_columns_one_to_one( file_table, 'file_name', 'data_format' )

formats_by_extension = dict()

global_extension_count = dict()

# Stop whenever we encounter any one of these strings (irrespective of case) when
# tokenizing filenames by splitting on '.' and walking backwards to construct
# maximal useful extension sequences. Validated by hand 2025-03-19.

safe_stoppers = {
    
    'bai',
    'bam',
    'bedpe',
    'cel',
    'idat',
    'jp2',
    'json',
    'loom',
    'maf',
    'pdf',
    'svs',
    'tar',
    'tbi',
    'tsv',
    'txt',
    'vcf',
    'xlsx',
    'xml'
}

for file_name in file_table_column_map:
    
    file_format = file_table_column_map[file_name]

    file_extension = ''

    for suffix_component in reversed( file_name.split( '.' ) ):
        
        # Stop on encountering .-delimited substring tokens that are certainly UUIDs,
        # or match other patterns known to indicate the presence of no further useful information.

        if re.search( r'[-_]?TCGA[-_]', suffix_component ) is not None \
            or re.search( r'^MATCH-', suffix_component ) is not None \
            or re.search( r'^CTSP-', suffix_component ) is not None \
            or re.search( r'^CDDP-[A-Z0-9]{4}$', suffix_component ) is not None \
            or re.search( r'^ER-[A-Z0-9]{4}$', suffix_component ) is not None \
            or re.search( r'[-_]?TARGET[-_]', suffix_component ) is not None \
            or re.search( r'[-_]?HTMCP[-_]', suffix_component ) is not None \
            or re.search( r'[-_]?BLGSP[-_]', suffix_component ) is not None \
            or re.search( r'^org_', suffix_component ) is not None \
            or re.search( r'^edu_', suffix_component ) is not None \
            or re.search( r'^MP2PRT[-_]', suffix_component ) is not None \
            or re.search( r'^1030-?SM', suffix_component ) is not None \
            or re.search( r'_noid_Red$', suffix_component ) is not None \
            or re.search( r'_noid_Grn$', suffix_component ) is not None \
            or re.search( r'gdc_realn$', suffix_component ) is not None \
            or re.search( r'GenomeWideSNP', suffix_component) is not None \
            or re.search( r'^DLP[-_]', suffix_component ) is not None \
            or re.search( r'^HCM[-_]', suffix_component ) is not None \
            or re.search( r'^GHYLL[-_]', suffix_component ) is not None \
            or re.search( r'^CMMP1COG', suffix_component ) is not None \
            or re.search( r'^ER0\d\d\d\s', suffix_component ) is not None \
            or re.search( r'^\d+$', suffix_component ) is not None \
            or re.search( r'^[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}$', suffix_component ) is not None:

            break

        elif len( suffix_component ) >= 15:
            
            print( *[ suffix_component, file_extension, file_format ], sep='\t', file=sys.stderr )

            break

        elif len( file_extension ) == 0:
            
            file_extension = suffix_component

        else:
            
            file_extension = f"{suffix_component}.{file_extension}"

        if suffix_component.lower() in safe_stoppers:
            
            break

    if file_extension not in formats_by_extension:
        
        formats_by_extension[file_extension] = dict()

    if file_extension not in global_extension_count:
        
        global_extension_count[file_extension] = 1

    else:
        
        global_extension_count[file_extension] = global_extension_count[file_extension] + 1

    if file_format not in formats_by_extension[file_extension]:
        
        formats_by_extension[file_extension][file_format] = 1

    else:
        
        formats_by_extension[file_extension][file_format] = formats_by_extension[file_extension][file_format] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'count', 'filename_extension', 'asserted_file_format' ], sep='\t', file=OUT )

    # Sort first (descending) by total number of observations per extension,

    for extension_count_pair in sorted( global_extension_count.items(), key=lambda x: ( x[1] ), reverse=True ):
        
        file_extension = extension_count_pair[0]

        # then (descending) by total number of observations per (extension, format-assertion) pair, breaking ties between counts within the same extension by lexical sort (ascending) on format-assertion.

        for format_count_pair in sorted( formats_by_extension[file_extension].items(), key=lambda x: ( 1/(x[1]), x[0] ) ):
            
            file_format = format_count_pair[0]

            count = format_count_pair[1]

            print( *[ count, file_extension, file_format ], sep='\t', file=OUT )


