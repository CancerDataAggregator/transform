#!/usr/bin/env python -u

import shutil

from os import path, makedirs

from cda_etl.lib import map_columns_one_to_many
# def map_columns_one_to_many( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):

# PARAMETERS

input_root = path.join( 'extracted_data', 'ctdc' )

study_input_dir = path.join( input_root, 'Study' )
study_input_tsv = path.join( study_input_dir, 'Study.tsv' )
study_data_file_uuid_input_tsvs = [
    path.join( study_input_dir, 'Study.data_file_uuid.from_StudyDataFileByStudyShortName.data_files.tsv' ),
    path.join( study_input_dir, 'Study.data_file_uuid.from_StudyDataFileByStudyShortName.study_data_files.tsv' ),
    path.join( study_input_dir, 'Study.data_file_uuid.from_participantAndBiospecimenFilesByStudyId.data_files.tsv' ),
    path.join( study_input_dir, 'Study.data_file_uuid.from_studyZipFileQuery.zip_files.tsv' )
]
study_participant_id_input_tsvs = [
    path.join( study_input_dir, 'Study.participant_id.tsv' )
]

biospecimen_overview_input_dir = path.join( input_root, 'BiospecimenOverview' )
biospecimen_overview_input_tsvs = [
    path.join( biospecimen_overview_input_dir, 'BiospecimenOverview.tsv' )
]

data_file_input_dir = path.join( input_root, 'DataFile' )
data_file_study_id_input_tsvs = [
    path.join( data_file_input_dir, 'DataFile.study_id.from_biospecimenOverview.tsv' )
]

file_overview_input_dir = path.join( input_root, 'FileOverview' )
file_overview_input_tsvs = [
    path.join( file_overview_input_dir, 'FileOverview.from_biospecimen_data_files.tsv' ),
    path.join( file_overview_input_dir, 'FileOverview.from_fileOverview.tsv' )
]

participant_overview_input_dir = path.join( input_root, 'ParticipantOverview' )
participant_overview_input_tsvs = [
    path.join( participant_overview_input_dir, 'ParticipantOverview.tsv' )
]

output_root = path.join( 'extracted_data', 'ctdc_postprocessed' )

study_output_dir = path.join( output_root, 'Study' )
study_output_tsv = path.join( study_output_dir, 'Study.tsv' )
study_data_file_uuid_output_tsv = path.join( study_output_dir, 'Study.data_file_uuid.tsv' )
study_participant_id_output_tsv = path.join( study_output_dir, 'Study.participant_id.tsv' )

# EXECUTION

for output_dir in [ output_root, study_output_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Copy the main Study data structure without modification.
shutil.copy2( study_input_tsv, study_output_tsv )

# Gather all Study->DataFile links and summarize in one output map.
study_id_to_data_file_uuid = dict()

for input_map_file in [ map_file for sub_list in [ study_data_file_uuid_input_tsvs, file_overview_input_tsvs, data_file_study_id_input_tsvs ] for map_file in sub_list ]:
    current_map = map_columns_one_to_many( input_map_file, 'study_id', 'data_file_uuid' )
    for study_id in current_map.keys():
        if study_id not in study_id_to_data_file_uuid:
            study_id_to_data_file_uuid[study_id] = set()
        for data_file_uuid in current_map[study_id]:
            study_id_to_data_file_uuid[study_id].add( data_file_uuid )

with open( study_data_file_uuid_output_tsv, 'w' ) as OUT:
    print( *[ 'study_id', 'data_file_uuid' ], sep='\t', end='\n', file=OUT )
    for study_id in sorted( study_id_to_data_file_uuid ):
        for data_file_uuid in sorted( study_id_to_data_file_uuid[study_id] ):
            print( *[ study_id, data_file_uuid ], sep='\t', end='\n', file=OUT )

# Gather all Study->Participant links and summarize in one output map.
study_id_to_participant_id = dict()

for input_map_file in [ map_file for sub_list in [ biospecimen_overview_input_tsvs, file_overview_input_tsvs, participant_overview_input_tsvs, study_participant_id_input_tsvs ] for map_file in sub_list ]:
    current_map = map_columns_one_to_many( input_map_file, 'study_id', 'participant_id' )
    for study_id in current_map.keys():
        if study_id not in study_id_to_participant_id:
            study_id_to_participant_id[study_id] = set()
        for participant_id in current_map[study_id]:
            study_id_to_participant_id[study_id].add( participant_id )

with open( study_participant_id_output_tsv, 'w' ) as OUT:
    print( *[ 'study_id', 'participant_id' ], sep='\t', end='\n', file=OUT )
    for study_id in sorted( study_id_to_participant_id ):
        for participant_id in sorted( study_id_to_participant_id[study_id] ):
            print( *[ study_id, participant_id ], sep='\t', end='\n', file=OUT )


