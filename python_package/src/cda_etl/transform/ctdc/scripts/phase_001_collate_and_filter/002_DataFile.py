#!/usr/bin/env python -u

import re
import shutil
import sys

from os import path, makedirs

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one
# def map_columns_one_to_many( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):
# def map_columns_one_to_one( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):

# PARAMETERS

input_root = path.join( 'extracted_data', 'ctdc' )

data_file_input_dir = path.join( input_root, 'DataFile' )
data_file_input_tsvs = [
    path.join( data_file_input_dir, 'DataFile.from_StudyDataFileByStudyShortName.data_files.tsv' ),
    path.join( data_file_input_dir, 'DataFile.from_StudyDataFileByStudyShortName.study_data_files.tsv' ),
    path.join( data_file_input_dir, 'DataFile.from_biospecimenOverview.tsv' ),
    path.join( data_file_input_dir, 'DataFile.from_participantAndBiospecimenFilesByStudyId.data_files.tsv' ),
    path.join( data_file_input_dir, 'DataFile.from_participantOverview.tsv' ),
    path.join( data_file_input_dir, 'DataFile.from_studyZipFileQuery.zip_files.tsv' )
]
data_file_participant_id_input_tsvs = [
    path.join( data_file_input_dir, 'DataFile.participant_id.from_fileOverview.tsv' )
]
data_file_specimen_record_id_input_tsvs = [
    path.join( data_file_input_dir, 'DataFile.specimen_record_id.from_fileOverview.tsv' )
]

file_overview_input_dir = path.join( input_root, 'FileOverview' )
file_overview_input_tsvs = [
    path.join( file_overview_input_dir, 'FileOverview.from_biospecimen_data_files.tsv' ),
    path.join( file_overview_input_dir, 'FileOverview.from_fileOverview.tsv' )
]

participant_input_dir = path.join( input_root, 'Participant' )
participant_data_file_uuid_input_tsvs = [
    path.join( participant_input_dir, 'Participant.data_file_uuid.from_participantOverview.tsv' ),
    path.join( participant_input_dir, 'Participant.data_file_uuid.from_participant_data_files.tsv' )
]

participant_overview_input_dir = path.join( input_root, 'ParticipantOverview' )
participant_overview_input_tsv = path.join( participant_overview_input_dir, 'ParticipantOverview.tsv' )

specimen_input_dir = path.join( input_root, 'Specimen' )
specimen_data_file_uuid_input_tsvs = [
    path.join( specimen_input_dir, 'Specimen.data_file_uuid.from_biospecimenOverview.tsv' ),
    path.join( specimen_input_dir, 'Specimen.data_file_uuid.from_biospecimen_data_files.tsv' )
]

output_root = path.join( 'extracted_data', 'ctdc_postprocessed' )

data_file_output_dir = path.join( output_root, 'DataFile' )
data_file_output_tsv = path.join( data_file_output_dir, 'DataFile.tsv' )
data_file_participant_id_output_tsv = path.join( data_file_output_dir, 'DataFile.participant_id.tsv' )
data_file_specimen_record_id_output_tsv = path.join( data_file_output_dir, 'DataFile.specimen_record_id.tsv' )

# EXECUTION

for output_dir in [ output_root, data_file_output_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

data_file = dict()
data_file_columns = list()

################################################################################
# Collate all DataFile records together from the DataFile.from_* input TSVs.
for data_file_input_tsv in data_file_input_tsvs:
    current_record_set = load_tsv_as_dict( data_file_input_tsv )
    # ASSUMPTIONS:
    #     * All of these input TSVs have identical headers
    #     * First column is primary key (in this case data_file_uuid)
    for data_file_uuid in current_record_set.keys():
        if data_file_uuid not in data_file:
            data_file[data_file_uuid] = dict()
        # Cache column names for later.
        if len( data_file_columns ) == 0:
            # ASSUMPTION: Python preserves dict insert order.
            data_file_columns = list( current_record_set[data_file_uuid].keys() ).copy()
        for column_name in current_record_set[data_file_uuid]:
            # Check for clashes as we go.
            if column_name not in data_file[data_file_uuid]:
                data_file[data_file_uuid][column_name] = current_record_set[data_file_uuid][column_name]
            elif data_file[data_file_uuid][column_name] != current_record_set[data_file_uuid][column_name]:
                # Halt, see what's up.
                sys.exit( f"FATAL: DataFile mismatch in {data_file_input_tsv}: already loaded {data_file_uuid}->{column_name} == {data_file[data_file_uuid][column_name]}; now seeing '{current_record_set[data_file_uuid][column_name]}'" )

# Verify loaded data against all DataFile records reflected in the FileOverview.from_* input TSVs.
for file_overview_input_tsv in file_overview_input_tsvs:
    current_record_set = load_tsv_as_dict( file_overview_input_tsv )
    # ASSUMPTIONS:
    #     * All of these input TSVs have identical headers
    #     * First column is primary key (in this case data_file_uuid)
    for data_file_uuid in current_record_set.keys():
        if data_file_uuid not in data_file:
            # This shouldn't happen.
            sys.exit( f"FATAL: data_file_uuid '{data_file_uuid}' present in {file_overview_input_tsv} but not loaded from DataFile.from_* input TSVs; please investigate." )
        for column_name in data_file[data_file_uuid]:
            # FileOverview has columns we don't care about. Only check already-loaded column names.
            if column_name in current_record_set[data_file_uuid]:
                # Check for clashes as we go. Ignore nulls in FileOverview records, they happen.
                if current_record_set[data_file_uuid][column_name] is not None and current_record_set[data_file_uuid][column_name] != '' and data_file[data_file_uuid][column_name] != current_record_set[data_file_uuid][column_name]:
                    # Halt, see what's up.
                    sys.exit( f"FATAL: FileOverview mismatch in {file_overview_input_tsv}: already loaded {data_file_uuid}->{column_name} == {data_file[data_file_uuid][column_name]}; now seeing '{current_record_set[data_file_uuid][column_name]}'" )

# Write aggregated DataFile records to output TSV.
with open( data_file_output_tsv, 'w' ) as OUT:
    print( *data_file_columns, sep='\t', end='\n', file=OUT )
    for data_file_uuid in sorted( data_file ):
        data_file_row = list()
        for column_name in data_file_columns:
            data_file_row.append( data_file[data_file_uuid][column_name] )
        print( *data_file_row, sep='\t', end='\n', file=OUT )

################################################################################
# Gather all DataFile->Participant links and summarize in one output map.
data_file_uuid_to_participant_id = dict()

for input_map_file in [ map_file for sub_list in [ data_file_participant_id_input_tsvs, file_overview_input_tsvs, participant_data_file_uuid_input_tsvs ] for map_file in sub_list ]:
    current_map = map_columns_one_to_many( input_map_file, 'data_file_uuid', 'participant_id' )
    for data_file_uuid in current_map.keys():
        if data_file_uuid not in data_file_uuid_to_participant_id:
            data_file_uuid_to_participant_id[data_file_uuid] = set()
        for participant_id in current_map[data_file_uuid]:
            data_file_uuid_to_participant_id[data_file_uuid].add( participant_id )

# ParticipantOverview.data_file_uuid is a string encoding an array, and must be processed differently.
participant_id_to_data_file_uuid_array_string = map_columns_one_to_one( participant_overview_input_tsv, 'participant_id', 'data_file_uuid' )
for participant_id in sorted( participant_id_to_data_file_uuid_array_string ):
    if participant_id_to_data_file_uuid_array_string[participant_id] is not None and participant_id_to_data_file_uuid_array_string[participant_id] != '':
        fixed_string = re.sub( r'^\[\s*', r'', participant_id_to_data_file_uuid_array_string[participant_id].strip() )
        fixed_string = re.sub( r'\s*\]$', r'', fixed_string )
        target_files = [ file_id.strip() for file_id in fixed_string.split( ',' )  ]
        for data_file_uuid in sorted( target_files ):
            if data_file_uuid not in data_file_uuid_to_participant_id:
                sys.exit( f"FATAL: DataFile {data_file_uuid} referenced in {participant_overview_input_tsv}, associated with participant_id {participant_id}, but this DataFile was not found in any of the DataFile->Participant source maps. Please investigate." )
            if participant_id not in data_file_uuid_to_participant_id[data_file_uuid]:
                sys.exit( "FATAL: DataFile {data_file_uuid} referenced in {participant_overview_input_tsv}, associated with participant_id {participant_id}, but this DataFile was not associated with that participant in any of the DataFile->Participant source maps. Please investigate." )

with open( data_file_participant_id_output_tsv, 'w' ) as OUT:
    print( *[ 'data_file_uuid', 'participant_id' ], sep='\t', end='\n', file=OUT )
    for data_file_uuid in sorted( data_file_uuid_to_participant_id ):
        for participant_id in sorted( data_file_uuid_to_participant_id[data_file_uuid] ):
            print( *[ data_file_uuid, participant_id ], sep='\t', end='\n', file=OUT )

################################################################################
# Gather all DataFile->Specimen links and summarize in one output map.
data_file_uuid_to_specimen_record_id = dict()

for input_map_file in [ map_file for sub_list in [ data_file_specimen_record_id_input_tsvs, file_overview_input_tsvs, specimen_data_file_uuid_input_tsvs ] for map_file in sub_list ]:
    current_map = map_columns_one_to_many( input_map_file, 'data_file_uuid', 'specimen_record_id' )
    for data_file_uuid in current_map.keys():
        if data_file_uuid not in data_file_uuid_to_specimen_record_id:
            data_file_uuid_to_specimen_record_id[data_file_uuid] = set()
        for specimen_record_id in current_map[data_file_uuid]:
            # These are sometimes null.
            if specimen_record_id is not None and specimen_record_id != '':
                data_file_uuid_to_specimen_record_id[data_file_uuid].add( specimen_record_id )

with open( data_file_specimen_record_id_output_tsv, 'w' ) as OUT:
    print( *[ 'data_file_uuid', 'specimen_record_id' ], sep='\t', end='\n', file=OUT )
    for data_file_uuid in sorted( data_file_uuid_to_specimen_record_id ):
        for specimen_record_id in sorted( data_file_uuid_to_specimen_record_id[data_file_uuid] ):
            print( *[ data_file_uuid, specimen_record_id ], sep='\t', end='\n', file=OUT )


