#!/usr/bin/env python -u

import json
import sys

from os import makedirs, path

from cda_etl.lib import map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

input_root = 'extracted_data/cds'

file_input_tsv = path.join( input_root, 'file.tsv' )

study_input_tsv = path.join( input_root, 'study.tsv' )

file_study_input_tsv = path.join( input_root, 'file_from_study.tsv' )

genomic_info_input_tsv = path.join( input_root, 'genomic_info.tsv' )

genomic_info_file_input_tsv = path.join( input_root, 'genomic_info_of_file.tsv' )

output_root = 'cda_tsvs/cds_raw_unharmonized'

file_output_tsv = path.join( output_root, 'file.tsv' )

file_identifier_output_tsv = path.join( output_root, 'file_identifier.tsv' )

file_associated_project_output_tsv = path.join( output_root, 'file_associated_project.tsv' )

cds_aux_dir = path.join( 'auxiliary_metadata', '__CDS_supplemental_metadata' )

warning_log = path.join( cds_aux_dir, 'cds_file_warning_log.txt' )

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

file_uuid_study_uuid = map_columns_one_to_many( file_study_input_tsv, 'file_uuid', 'study_uuid' )

study_uuid_study_name = map_columns_one_to_one( study_input_tsv, 'uuid', 'study_name' )

study_name_phs_accession = map_columns_one_to_one( study_input_tsv, 'study_name', 'phs_accession' )

file_uuid_phs_accession = dict()

for file_uuid in file_uuid_study_uuid:
    
    # Assumption valid at time of writing: there is exactly one study per file. Set a trap in case that changes.

    study_count = 0

    for study_uuid in file_uuid_study_uuid[file_uuid]:
        
        file_uuid_phs_accession[file_uuid] = study_name_phs_accession[study_uuid_study_name[study_uuid]]

        study_count = study_count + 1

    if study_count == 0:
        
        sys.exit( f"FATAL: file {file_uuid} is associated with no studies, breaking downstream assumptions. Also, for what it's worth, this message should never be possible. Aborting." )

    elif study_count > 1:
        
        sys.exit( f"FATAL: file {file_uuid} is associated with more than one study, breaking downstream assumptions. Aborting." )

file_uuid_genomic_info_uuid = map_columns_one_to_many( genomic_info_file_input_tsv, 'file_uuid', 'genomic_info_uuid' )

genomic_info_library_strategy = map_columns_one_to_one( genomic_info_input_tsv, 'uuid', 'library_strategy' )

# Pre-load file.data_category values from genomic_info.library_strategy in associated genomic_info records.

file_uuid_data_category = dict()

# Record conflicts.

clashes_observed = dict()

for file_uuid in file_uuid_genomic_info_uuid:
    
    library_strategy = ''

    for genomic_info_uuid in file_uuid_genomic_info_uuid[file_uuid]:
        
        next_library_strategy = genomic_info_library_strategy[genomic_info_uuid]

        if next_library_strategy is not None and next_library_strategy != '':
            
            if library_strategy == '':
                
                library_strategy = next_library_strategy

            elif library_strategy != next_library_strategy:
                
                # We've now seen two distinct non-null values for genomic_info.library_strategy associated with this file_uuid. Record all values.

                if file_uuid not in clashes_observed:
                    
                    clashes_observed[file_uuid] = set()

                clashes_observed[file_uuid].add( library_strategy )

                clashes_observed[file_uuid].add( next_library_strategy )

    file_uuid_data_category[file_uuid] = library_strategy

with open( warning_log, 'w' ) as WARN:
    
    print( *[ 'file_uuid', 'nonunique__genomic_info.library_strategy' ], sep='\t', file=WARN )

    for file_uuid in sorted( clashes_observed ):
        
        for library_strategy in sorted( clashes_observed[file_uuid] ):
            
            print( *[ file_uuid, library_strategy ], sep='\t', file=WARN )

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

        file_uuid = input_record['uuid']

        file_id = input_record['file_id']

        if file_uuid not in file_uuid_phs_accession:
            
            sys.exit( f"FATAL: file_id {file_id} is not in any study. Downstream assumptions broken; aborting.\n" )

        if file_uuid not in file_uuid_data_category:
            
            file_uuid_data_category[file_uuid] = ''

        # Don't do this. A) it's pointless and B) it messes up downstream assumptions that are handy to
        # keep around, like 'we can walk down the file table, the file_data_source table and the
        # file_identifier table online, without caching, and the line we're on in all three files
        # will always describe the same file'
        # 
        # print( *[ file_id, 'CDS', 'file.uuid', file_uuid ], sep='\t', end='\n', file=FILE_IDENTIFIER )

        print( *[ file_id, 'CDS', 'file.file_id', file_id ], sep='\t', end='\n', file=FILE_IDENTIFIER )

        output_record = dict()

        output_record['id'] = file_id
        output_record['label'] = input_record['file_name']
        output_record['data_category'] = file_uuid_data_category[file_uuid]
        output_record['data_type'] = ''
        output_record['file_format'] = input_record['file_type']
        output_record['drs_uri'] = 'drs://' + file_id
        output_record['byte_size'] = '' if input_record['file_size'] == '' else input_record['file_size']
        output_record['checksum'] = input_record['md5sum']
        output_record['data_modality'] = ''
        output_record['imaging_modality'] = ''
        output_record['dbgap_accession_number'] = file_uuid_phs_accession[file_uuid]

        print( *[ file_id, file_uuid_phs_accession[file_uuid] ], sep='\t', end='\n', file=FILE_ASSOCIATED_PROJECT )

        output_record['imaging_series'] = ''

        print( *[ output_record[column_name] for column_name in output_column_names ], sep='\t', end='\n', file=FILE_OUT )


