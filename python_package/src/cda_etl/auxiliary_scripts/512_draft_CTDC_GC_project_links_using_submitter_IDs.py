#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

# ARGUMENTS

if len( sys.argv ) != 4:
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <CTDC entity list> <GC entity list> <output file>\n" )

ctdc_entity_list = sys.argv[1]
gc_entity_list = sys.argv[2]
output_file = sys.argv[3]

# EXECUTION

ctdc_participant_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='participant' )
ctdc_specimen_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='specimen' )

gc_study_in_program = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'program.program_acronym' )
gc_participant_in_study = map_columns_one_to_many( gc_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='participant' )
gc_sample_in_study = map_columns_one_to_many( gc_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='sample' )
gc_study_phs_accession = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'study.phs_accession' )
gc_study_name = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'study.study_name' )

gc_study_match_count = dict()
gc_program_match_count = dict()
gc_study_to_ctdc_study = dict()

for participant_id in ctdc_participant_in_study:
    
    if participant_id in gc_participant_in_study:
        for gc_study in gc_participant_in_study[participant_id]:
            gc_program = gc_study_in_program[gc_study]

            if gc_study not in gc_study_match_count:
                gc_study_match_count[gc_study] = 1
            else:
                gc_study_match_count[gc_study] = gc_study_match_count[gc_study] + 1

            if gc_program not in gc_program_match_count:
                gc_program_match_count[gc_program] = 1
            else:
                gc_program_match_count[gc_program] = gc_program_match_count[gc_program] + 1

            for ctdc_study in ctdc_participant_in_study[participant_id]:
                if gc_study not in gc_study_to_ctdc_study:
                    gc_study_to_ctdc_study[gc_study] = dict()
                if ctdc_study not in gc_study_to_ctdc_study[gc_study]:
                    gc_study_to_ctdc_study[gc_study][ctdc_study] = 1
                else:
                    gc_study_to_ctdc_study[gc_study][ctdc_study] = gc_study_to_ctdc_study[gc_study][ctdc_study] + 1

for specimen_id in ctdc_specimen_in_study:
    
    if specimen_id in gc_sample_in_study:
        for gc_study in gc_sample_in_study[specimen_id]:
            gc_program = gc_study_in_program[gc_study]

            if gc_study not in gc_study_match_count:
                gc_study_match_count[gc_study] = 1
            else:
                gc_study_match_count[gc_study] = gc_study_match_count[gc_study] + 1

            if gc_program not in gc_program_match_count:
                gc_program_match_count[gc_program] = 1
            else:
                gc_program_match_count[gc_program] = gc_program_match_count[gc_program] + 1

            for ctdc_study in ctdc_specimen_in_study[specimen_id]:
                if gc_study not in gc_study_to_ctdc_study:
                    gc_study_to_ctdc_study[gc_study] = dict()
                if ctdc_study not in gc_study_to_ctdc_study[gc_study]:
                    gc_study_to_ctdc_study[gc_study][ctdc_study] = 1
                else:
                    gc_study_to_ctdc_study[gc_study][ctdc_study] = gc_study_to_ctdc_study[gc_study][ctdc_study] + 1

with open( output_file, 'w' ) as OUT:
    print( *[ 'match_count', 'GC_program_acronym', 'GC_study_name', 'GC_study_phs_accession', 'GC_study_uuid', 'CTDC_study_id', ], sep='\t', file=OUT )

    for gc_program in [ program for program, match_count in sorted( gc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        for gc_study_uuid in [ study for study, match_count in sorted( gc_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            if gc_program == gc_study_in_program[gc_study_uuid]:
                if gc_study_uuid in gc_study_to_ctdc_study:
                    for ctdc_study in [ study for study, match_count in sorted( gc_study_to_ctdc_study[gc_study_uuid].items(), key=lambda item: item[1], reverse=True ) ]:
                        match_count = gc_study_to_ctdc_study[gc_study_uuid][ctdc_study]
                        study_name = gc_study_name[gc_study_uuid]
                        study_phs_accession = gc_study_phs_accession[gc_study_uuid]
                        print( *[ match_count, gc_program, study_name, study_phs_accession, gc_study_uuid, ctdc_study ], sep='\t', file=OUT )


