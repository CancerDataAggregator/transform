#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'PDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated; cases without containing studies removed;
# groups of multiple files sharing the same FileMetadata.file_location removed.

tsv_input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

file_case_input_tsv = path.join( tsv_input_root, 'File', 'File.case_id.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

file_describes_subject_output_tsv = path.join( tsv_output_root, 'file_describes_subject.tsv' )

# EXECUTION

# Load CDA subject IDs for case_id values.

case_id_to_cda_subject_id = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'Case.case_id':
            
            case_id_to_cda_subject_id[value] = entity_id

file_case = map_columns_one_to_many( file_case_input_tsv, 'file_id', 'case_id' )

cda_file_describes_subject = dict()

for file_id in file_case:
    
    for case_id in file_case[file_id]:
        
        cda_subject_id = case_id_to_cda_subject_id[case_id]

        if file_id not in cda_file_describes_subject:
            
            cda_file_describes_subject[file_id] = set()

        cda_file_describes_subject[file_id].add( cda_subject_id )

print( f"[{get_current_timestamp()}] Writing {file_describes_subject_output_tsv}...", end='', file=sys.stderr )

with open( file_describes_subject_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', file=OUT )

    for file_id in sorted( cda_file_describes_subject ):
        
        for subject_id in sorted( cda_file_describes_subject[file_id] ):
            
            print( *[ file_id, subject_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


