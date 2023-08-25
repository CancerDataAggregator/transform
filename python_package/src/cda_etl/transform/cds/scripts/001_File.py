#!/usr/bin/env python -u

import sys

from os import makedirs, path

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/cds'

file_input_tsv = path.join( input_root, 'file', 'file.tsv' )

study_input_tsv = path.join( input_root, 'study', 'study.tsv' )

study_file_input_tsv = path.join( input_root, 'study', 'study.file_id.tsv' )

output_root = 'cda_tsvs/cds'

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

file_study_name = map_columns_one_to_many( study_file_input_tsv, 'file_id', 'study_name' )

study_name_phs_accession = map_columns_one_to_one( study_input_tsv, 'study_name', 'phs_accession' )

file_study = dict()

for file_id in file_study_name:
    
    # Assumption valid at time of writing: there is exactly one study per file. Set a trap in case that changes.

    study_count = 0

    for study_name in file_study_name[file_id]:
        
        file_study[file_id] = study_name_phs_accession[study_name]

        study_count = study_count + 1

    if study_count == 0:
        
        sys.exit( f"FATAL: file {file_id} is associated with no studies, breaking downstream assumptions. Also, for what it's worth, this message should never be possible. Aborting." )

    elif study_count > 1:
        
        sys.exit( f"FATAL: file {file_id} is associated with more than one study, breaking downstream assumptions. Aborting." )

# EXECUTION

for output_dir in [ output_root ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

with open( file_input_tsv ) as FILE_IN, open( file_output_tsv, 'w' ) as FILE_OUT, open( file_identifier_output_tsv, 'w' ) as FILE_IDENTIFIER, open( file_associated_project_output_tsv, 'w' ) as FILE_ASSOCIATED_PROJECT:
    
    input_column_names = next( FILE_IN ).rstrip( '\n' ).split( '\t' )

    print( *output_column_names, sep='\t', end='\n', file=FILE_OUT )

    print( *[ 'file_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=FILE_IDENTIFIER )
    print( *[ 'file_id', 'associated_project' ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

    for line in [ next_line.rstrip( '\n' ) for next_line in FILE_IN ]:
        
        input_record = dict( zip( input_column_names, line.split( '\t' ) ) )

        file_id = input_record['file_id']

        if file_id not in file_study:
            
            sys.exit(f"FATAL: file_id {file_id} is not in any study. Downstream assumptions broken; aborting.\n")

        print( *[ file_id, 'CDS', 'file.file_id', file_id ], sep='\t', end='\n', file=FILE_IDENTIFIER )

        output_record = dict()

        output_record['id'] = file_id
        output_record['label'] = input_record['file_name']
        output_record['data_category'] = input_record['experimental_strategy_and_data_subtypes']
        output_record['data_type'] = ''
        output_record['file_format'] = input_record['file_type']
        output_record['drs_uri'] = 'drs://' + file_id
        output_record['byte_size'] = '' if input_record['file_size'] == '' else int( float( input_record['file_size'] ) )
        output_record['checksum'] = input_record['md5sum']
        output_record['data_modality'] = ''
        output_record['imaging_modality'] = ''
        output_record['dbgap_accession_number'] = file_study[file_id]

        print( *[ file_id, file_study[file_id] ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

        output_record['imaging_series'] = ''

        print( *[ output_record[column_name] for column_name in output_column_names ], sep='\t', end='\n', file=FILE_OUT )


