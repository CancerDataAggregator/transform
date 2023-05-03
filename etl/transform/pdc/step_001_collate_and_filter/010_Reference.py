#!/usr/bin/env python -u

import sys

from os import path, makedirs, rename

# SUBROUTINES

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

input_root = 'extracted_data/pdc'

input_dir = path.join( input_root, 'Reference' )

reference_input_tsv = path.join( input_dir, 'Reference.tsv' )

output_root = 'merged_data/pdc'

case_input_tsv = path.join( output_root, 'Case', 'Case.tsv' )

study_input_tsv = path.join( output_root, 'Study', 'Study.tsv' )

case_id_gdc_id_output_tsv = path.join( output_root, 'Case', 'Case.gdc_id.tsv' )

study_id_dbgap_id_output_tsv = path.join( output_root, 'Study', 'Study.dbgap_id.tsv' )

# EXECUTION

if not path.exists( output_root ):
    
    makedirs( output_root )

# There are stale IDs in the reference file. Only save maps for PDC records that exist.

seen_case_ids = set()

with open( case_input_tsv ) as IN:
    
    header = next(IN)

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        seen_case_ids.add( line.split('\t')[0] )

seen_study_ids = set()

with open( study_input_tsv ) as IN:
    
    header = next(IN)

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        seen_study_ids.add( line.split('\t')[0] )

with open( reference_input_tsv ) as IN, open( case_id_gdc_id_output_tsv, 'w' ) as CASE_GDC, open( study_id_dbgap_id_output_tsv, 'w' ) as STUDY_DBGAP:
    
    input_colnames = next(IN).rstrip('\n').split('\t')

    print( *[ 'case_id', 'gdc_case_id' ], sep='\t', end='\n', file=CASE_GDC )

    print( *[ 'study_id', 'dbgap_id' ], sep='\t', end='\n', file=STUDY_DBGAP )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        current_record = dict( zip( input_colnames, line.split('\t') ) )

        if current_record['entity_type'] == 'study' and current_record['reference_resource_shortname'].lower() == 'dbgap':
            
            study_id = current_record['entity_id']

            dbgap_id = current_record['reference_entity_alias']

            if len( study_id ) > 0 and study_id in seen_study_ids and len( dbgap_id ) > 0:
                
                print( *[ study_id, dbgap_id ], sep='\t', end='\n', file=STUDY_DBGAP )

        elif current_record['entity_type'] == 'case' and current_record['reference_resource_shortname'].lower() == 'gdc':
            
            case_id = current_record['entity_id']

            gdc_case_id = ''

            if current_record['entity_submitter_id'] == current_record['reference_entity_alias']:
                
                gdc_case_id = current_record['reference_entity_id']

            else:
                
                gdc_case_id = current_record['reference_entity_alias']

            if len( case_id ) > 0 and case_id in seen_case_ids and len( gdc_case_id ) > 0:
                
                print( *[ case_id, gdc_case_id ], sep='\t', end='\n', file=CASE_GDC )

sort_file_with_header( case_id_gdc_id_output_tsv )

sort_file_with_header( study_id_dbgap_id_output_tsv )


