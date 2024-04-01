#!/usr/bin/env python -u

import sys

from os import makedirs, path

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/pdc_postprocessed'

file_input_tsv = path.join( input_root, 'File', 'File.tsv' )

file_study_input_tsv = path.join( input_root, 'File', 'File.study_id.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

study_dbgap_input_tsv = path.join( input_root, 'Study', 'Study.dbgap_id.tsv' )

output_root = 'cda_tsvs/pdc_raw_unharmonized'

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

file_study = map_columns_one_to_many( file_study_input_tsv, 'file_id', 'study_id' )

pdc_study_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'pdc_study_id' )

study_dbgap_id = map_columns_one_to_one( study_dbgap_input_tsv, 'study_id', 'dbgap_id' )

# EXECUTION

for output_dir in [ output_root ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

with open( file_input_tsv ) as FILE_IN, open( file_output_tsv, 'w' ) as FILE_OUT, open( file_identifier_output_tsv, 'w' ) as FILE_IDENTIFIER, open( file_associated_project_output_tsv, 'w' ) as FILE_ASSOCIATED_PROJECT:
    
    header = next(FILE_IN).rstrip('\n')

    input_column_names = header.split('\t')

    print( *output_column_names, sep='\t', end='\n', file=FILE_OUT )
    print( *[ 'file_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=FILE_IDENTIFIER )
    print( *[ 'file_id', 'associated_project' ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

    for line in [ next_line.rstrip('\n') for next_line in FILE_IN ]:
        
        input_record = dict( zip( input_column_names, line.split('\t') ) )

        file_id = input_record['file_id']

        print( *[ file_id, 'PDC', 'FileMetadata.file_id', file_id ], sep='\t', end='\n', file=FILE_IDENTIFIER )

        output_record = dict()

        output_record['id'] = file_id
        output_record['label'] = input_record['file_name']
        output_record['data_category'] = input_record['data_category']
        output_record['data_type'] = input_record['file_type']
        output_record['file_format'] = input_record['file_format']
        output_record['drs_uri'] = 'drs://dg.4DFC:' + file_id
        output_record['byte_size'] = input_record['file_size']
        output_record['checksum'] = input_record['md5sum']
        output_record['data_modality'] = 'Proteomic'
        output_record['imaging_modality'] = ''
        output_record['dbgap_accession_number'] = ''

        if file_id not in file_study:
            
            sys.exit(f"FATAL: file_id {file_id} is not in any study. Downstream assumptions broken; aborting.\n")

        for study_id in sorted( file_study[file_id] ):
            
            print( *[ file_id, pdc_study_id[study_id] ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

            if study_id in study_dbgap_id:
                
                # Even if a file is in multiple PDC "Study" objects, the dbGaP phs ID should always be the same.

                output_record['dbgap_accession_number'] = study_dbgap_id[study_id]

        output_record['imaging_series'] = ''

        print( *[ output_record[column_name] for column_name in output_column_names ], sep='\t', end='\n', file=FILE_OUT )


