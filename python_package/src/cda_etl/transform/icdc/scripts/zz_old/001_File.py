#!/usr/bin/env python -u

import json
import sys

from os import makedirs, path

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/icdc'

file_input_tsv = path.join( input_root, 'file', 'file.tsv' )

file_study_input_tsv = path.join( input_root, 'file', 'file.clinical_study_designation.tsv' )

file_case_input_tsv = path.join( input_root, 'file', 'file.case_id.tsv' )

case_study_input_tsv = path.join( input_root, 'case', 'case.clinical_study_designation.tsv' )

file_sample_input_tsv = path.join( input_root, 'file', 'file.sample_id.tsv' )

sample_case_input_tsv = path.join( input_root, 'sample', 'sample.case_id.tsv' )

file_diagnosis_input_tsv = path.join( input_root, 'file', 'file.diagnosis_id.tsv' )

diagnosis_case_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

output_root = 'cda_tsvs/icdc_raw_unharmonized'

file_output_tsv = path.join( output_root, 'file.tsv' )

file_identifier_output_tsv = path.join( output_root, 'file_identifier.tsv' )

file_associated_project_output_tsv = path.join( output_root, 'file_associated_project.tsv' )

output_column_names = [
    'id',
    'label',
    'data_category',
    'data_type',
    'file_format',
    'drs_uri',
    'byte_size',
    'checksum',
    'data_modality',
    'imaging_modality',
    'dbgap_accession_number',
    'imaging_series'
]

for output_dir in [ output_root ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# EXECUTION

# Try to map every file to at least one study. Each of the maps we consume is partial.

# file -> study

file_study = map_columns_one_to_many( file_study_input_tsv, 'file.uuid', 'clinical_study_designation' )

# file -> case -> study

file_case = map_columns_one_to_many( file_case_input_tsv, 'file.uuid', 'case_id' )

case_study = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'clinical_study_designation' )

for file_uuid in file_case:
    
    if file_uuid not in file_study:
        
        file_study[file_uuid] = set()

    for case_id in file_case[file_uuid]:
        
        if case_id not in case_study:
            
            sys.exit( f"FATAL: case {case_id} not in any study; downstream assumptions broken, aborting." )

        for clinical_study_designation in case_study[case_id]:
            
            file_study[file_uuid].add( clinical_study_designation )

# file -> diagnosis -> case -> study

file_diagnosis = map_columns_one_to_many( file_diagnosis_input_tsv, 'file.uuid', 'diagnosis_id' )

diagnosis_case = map_columns_one_to_many( diagnosis_case_input_tsv, 'diagnosis_id', 'case_id' )

for file_uuid in file_diagnosis:
    
    if file_uuid not in file_study:
        
        file_study[file_uuid] = set()

    for diagnosis_id in file_diagnosis[file_uuid]:
        
        if diagnosis_id not in diagnosis_case:
            
            sys.exit( f"FATAL: diagnosis {diagnosis_id} not assigned to any case; downstream assumptions broken, aborting." )

        for case_id in diagnosis_case[diagnosis_id]:
            
            if case_id not in case_study:
                
                sys.exit( f"FATAL: case {case_id} (<- diagnosis_id {diagnosis_id} <- file.uuid {file_uuid}) not in any study; downstream assumptions broken, aborting." )

            for clinical_study_designation in case_study[case_id]:
                
                file_study[file_uuid].add( clinical_study_designation )

# file -> sample -> case -> study

file_sample = map_columns_one_to_many( file_sample_input_tsv, 'file.uuid', 'sample_id' )

sample_case = map_columns_one_to_many( sample_case_input_tsv, 'sample_id', 'case_id' )

for file_uuid in file_sample:
    
    if file_uuid not in file_study:
        
        file_study[file_uuid] = set()

    for sample_id in file_sample[file_uuid]:
        
        if sample_id not in sample_case:
            
            sys.exit( f"FATAL: sample {sample_id} not assigned to any case; downstream assumptions broken, aborting." )

        for case_id in sample_case[sample_id]:
            
            if case_id not in case_study:
                
                sys.exit( f"FATAL: case {case_id} (<- sample_id {sample_id} <- file.uuid {file_uuid}) not in any study; downstream assumptions broken, aborting." )

            for clinical_study_designation in case_study[case_id]:
                
                file_study[file_uuid].add( clinical_study_designation )

# Evaluate file->study coverage completeness.

total_file_records = 0

with open( file_input_tsv, 'rb' ) as IN:
    
    total_file_records = sum( 1 for _ in IN )

total_file_records = total_file_records - 1

study_connected_file_records = len( file_study )

if study_connected_file_records != total_file_records:
    
    sys.exit( f"FATAL: Only attributed {study_connected_file_records} / {total_file_records} file records to studies; downstream assumptions broken, aborting." )

# Useful for debug only, given paucity of other logging at time of writing.
# 
# print( f"Successfully attributed each of {total_file_records} total file records to at least one study." )

# Transcode core `file` data from native ICDC entities into CDA TSVs.

with open( file_input_tsv ) as FILE_IN, open( file_output_tsv, 'w' ) as FILE_OUT, open( file_identifier_output_tsv, 'w' ) as FILE_IDENTIFIER, open( file_associated_project_output_tsv, 'w' ) as FILE_ASSOCIATED_PROJECT:
    
    input_column_names = next( FILE_IN ).rstrip( '\n' ).split( '\t' )

    print( *output_column_names, sep='\t', end='\n', file=FILE_OUT )

    print( *[ 'file_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=FILE_IDENTIFIER )
    print( *[ 'file_id', 'associated_project' ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

    for line in [ next_line.rstrip( '\n' ) for next_line in FILE_IN ]:
        
        input_record = dict( zip( input_column_names, line.split( '\t' ) ) )

        file_uuid = input_record['uuid']

        if file_uuid not in file_study:
            
            sys.exit( f"FATAL: file.uuid {file_uuid} is not in any study. Downstream assumptions broken; aborting.\n" )

        # Populate `file_identifier`.
        # 
        # Don't make more than one line per file record, here: A) it's pointless and
        # B) it messes up downstream assumptions that are handy to keep around, like
        # 'we can walk down the `file` table, the `file_data_source` table and the
        # `file_identifier` table online, without caching, and the line we're on in all three
        # tables will always describe the same file record'

        print( *[ file_uuid, 'ICDC', 'file.uuid', file_uuid ], sep='\t', end='\n', file=FILE_IDENTIFIER )

        # Populate `file_associated_project`.

        for clinical_study_designation in sorted( file_study[file_uuid] ):
            
            print( *[ file_uuid, clinical_study_designation ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

        # Populate the main `file` table.

        output_record = dict()

        output_record['id'] = file_uuid
        output_record['label'] = input_record['file_name']
        output_record['data_category'] = input_record['file_type'] # I know this looks like a typo. It isn't -- check the values against those from other DCs in this field.
        output_record['data_type'] = ''
        output_record['file_format'] = input_record['file_format']
        output_record['drs_uri'] = 'drs://dg.4DFC:' + file_uuid
        output_record['byte_size'] = '' if input_record['file_size'] == '' else int( float( input_record['file_size'] ) )
        output_record['checksum'] = input_record['md5sum']
        output_record['data_modality'] = ''
        output_record['imaging_modality'] = ''
        output_record['dbgap_accession_number'] = ''
        output_record['imaging_series'] = ''

        print( *[ output_record[column_name] for column_name in output_column_names ], sep='\t', end='\n', file=FILE_OUT )


