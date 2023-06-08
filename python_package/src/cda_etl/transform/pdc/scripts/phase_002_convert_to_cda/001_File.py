#!/usr/bin/env python -u

import sys

from os import makedirs, path

# SUBROUTINES

def map_columns_one_to_one( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            return_map[current_from] = current_to

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add(current_to)

    return return_map

# PARAMETERS

input_root = 'extracted_data/pdc_postprocessed'

file_input_tsv = path.join( input_root, 'File', 'File.tsv' )

file_study_input_tsv = path.join( input_root, 'File', 'File.study_id.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

study_dbgap_input_tsv = path.join( input_root, 'Study', 'Study.dbgap_id.tsv' )

output_root = 'cda_tsvs/pdc'

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


