#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'ICDC'

# Unmodified extracted data, converted directly from JSON to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

case_file_input_tsv = path.join( tsv_input_root, 'case', 'case.file_uuid.tsv' )

diagnosis_file_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.file_uuid.tsv' )

diagnosis_case_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

file_sample_input_tsv = path.join( tsv_input_root, 'file', 'file.sample_id.tsv' )

sample_case_input_tsv = path.join( tsv_input_root, 'sample', 'sample.case_id.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

file_describes_subject_output_tsv = path.join( tsv_output_root, 'file_describes_subject.tsv' )

###################
# EXECUTION

# file->case

file_from_case = map_columns_one_to_many( case_file_input_tsv, 'file.uuid', 'case_id' )

# file->diagnosis->case

file_of_diagnosis = map_columns_one_to_many( diagnosis_file_input_tsv, 'file.uuid', 'diagnosis_id' )

diagnosis_of_case = map_columns_one_to_many( diagnosis_case_input_tsv, 'diagnosis_id', 'case_id' )

for file_id in file_of_diagnosis:
    
    for diagnosis_id in file_of_diagnosis[file_id]:
        
        # This should break with a key error if there's no case corresponding to some diagnosis record.

        for case_id in diagnosis_of_case[diagnosis_id]:
            
            if file_id not in file_from_case:
                
                file_from_case[file_id] = set()

            file_from_case[file_id].add( case_id )

# file->sample->case

file_describes_sample = map_columns_one_to_many( file_sample_input_tsv, 'file.uuid', 'sample_id' )

sample_from_case = map_columns_one_to_many( sample_case_input_tsv, 'sample_id', 'case_id' )

for file_id in file_describes_sample:
    
    for sample_id in file_describes_sample[file_id]:
        
        # This should break with a key error if there's no case corresponding to some sample record.

        for case_id in sample_from_case[sample_id]:
            
            if file_id not in file_from_case:
                
                file_from_case[file_id] = set()

            file_from_case[file_id].add( case_id )

# Load CDA subject IDs for case_id values.

case_id_to_cda_subject_id = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table == 'subject' and data_source == upstream_data_source and source_field == 'case.case_id':
            
            case_id_to_cda_subject_id[value] = entity_id

cda_file_describes_subject = dict()

for file_id in file_from_case:
    
    for case_id in file_from_case[file_id]:
        
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


