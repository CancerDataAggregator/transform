#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

# ARGUMENTS

if len( sys.argv ) != 4:
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <CTDC entity list> <IDC entity list> <output file>\n" )

ctdc_entity_list = sys.argv[1]
idc_entity_list = sys.argv[2]
output_file = sys.argv[3]

# EXECUTION

ctdc_participant_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='participant' )
ctdc_specimen_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='specimen' )

idc_collection_in_program = map_columns_one_to_one( idc_entity_list, 'original_collections_metadata.collection_id', 'original_collections_metadata.Program' )
idc_case_in_collection = map_columns_one_to_many( idc_entity_list, 'entity_submitter_id', 'original_collections_metadata.collection_id', where_field='entity_type', where_value='case' )
idc_sample_in_collection = map_columns_one_to_many( idc_entity_list, 'entity_submitter_id', 'original_collections_metadata.collection_id', where_field='entity_type', where_value='sample' )

idc_collection_match_count = dict()
idc_program_match_count = dict()
idc_collection_to_ctdc_study = dict()

for participant_id in ctdc_participant_in_study:
    
    if participant_id in idc_case_in_collection:
        for idc_collection in idc_case_in_collection[participant_id]:
            idc_program = idc_collection_in_program[idc_collection]

            if idc_collection not in idc_collection_match_count:
                idc_collection_match_count[idc_collection] = 1
            else:
                idc_collection_match_count[idc_collection] = idc_collection_match_count[idc_collection] + 1

            if idc_program not in idc_program_match_count:
                idc_program_match_count[idc_program] = 1
            else:
                idc_program_match_count[idc_program] = idc_program_match_count[idc_program] + 1

            for ctdc_study in ctdc_participant_in_study[participant_id]:
                if idc_collection not in idc_collection_to_ctdc_study:
                    idc_collection_to_ctdc_study[idc_collection] = dict()
                if ctdc_study not in idc_collection_to_ctdc_study[idc_collection]:
                    idc_collection_to_ctdc_study[idc_collection][ctdc_study] = 1
                else:
                    idc_collection_to_ctdc_study[idc_collection][ctdc_study] = idc_collection_to_ctdc_study[idc_collection][ctdc_study] + 1

for specimen_id in ctdc_specimen_in_study:
    
    if specimen_id in idc_sample_in_collection:
        for idc_collection in idc_sample_in_collection[specimen_id]:
            idc_program = idc_collection_in_program[idc_collection]

            if idc_collection not in idc_collection_match_count:
                idc_collection_match_count[idc_collection] = 1
            else:
                idc_collection_match_count[idc_collection] = idc_collection_match_count[idc_collection] + 1

            if idc_program not in idc_program_match_count:
                idc_program_match_count[idc_program] = 1
            else:
                idc_program_match_count[idc_program] = idc_program_match_count[idc_program] + 1

        for ctdc_study in ctdc_specimen_in_study[specimen_id]:
            if idc_collection not in idc_collection_to_ctdc_study:
                idc_collection_to_ctdc_study[idc_collection] = dict()
            if ctdc_study not in idc_collection_to_ctdc_study[idc_collection]:
                idc_collection_to_ctdc_study[idc_collection][ctdc_study] = 1
            else:
                idc_collection_to_ctdc_study[idc_collection][ctdc_study] = idc_collection_to_ctdc_study[idc_collection][ctdc_study] + 1

with open( output_file, 'w' ) as OUT:
    print( *[ 'match_count', 'IDC_program_name', 'IDC_collection_id', 'CTDC_study_id' ], sep='\t', file=OUT )

    for idc_program in [ program for program, match_count in sorted( idc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        for idc_collection in [ collection for collection, match_count in sorted( idc_collection_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            if idc_collection_in_program[idc_collection] == idc_program and idc_collection in idc_collection_to_ctdc_study:
                for ctdc_study in [ study for study, match_count in sorted( idc_collection_to_ctdc_study[idc_collection].items(), key=lambda item: item[1], reverse=True ) ]:
                    match_count = idc_collection_to_ctdc_study[idc_collection][ctdc_study]
                    print( *[ match_count, idc_program, idc_collection, ctdc_study ], sep='\t', file=OUT )


