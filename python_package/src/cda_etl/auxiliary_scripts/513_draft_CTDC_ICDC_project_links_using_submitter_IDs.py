#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

# ARGUMENTS

if len( sys.argv ) != 4:
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <CTDC entity list> <ICDC entity list> <output file>\n" )

ctdc_entity_list = sys.argv[1]
icdc_entity_list = sys.argv[2]
output_file = sys.argv[3]

# EXECUTION

ctdc_participant_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='participant' )
ctdc_specimen_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='specimen' )

icdc_study_in_program = map_columns_one_to_one( icdc_entity_list, 'study.clinical_study_designation', 'program.program_acronym' )
icdc_study_name = map_columns_one_to_one( icdc_entity_list, 'study.clinical_study_designation', 'study.clinical_study_name' )
icdc_program_name = map_columns_one_to_one( icdc_entity_list, 'program.program_acronym', 'program.program_name' )
icdc_case_in_study = map_columns_one_to_many( icdc_entity_list, 'entity_id', 'study.clinical_study_designation', where_field='entity_type', where_value='case' )
icdc_sample_in_study = map_columns_one_to_many( icdc_entity_list, 'entity_id', 'study.clinical_study_designation', where_field='entity_type', where_value='sample' )

icdc_study_match_count = dict()
icdc_program_match_count = dict()
icdc_study_to_ctdc_study = dict()

for participant_id in ctdc_participant_in_study:
    
    if participant_id in icdc_case_in_study:
        for icdc_study in icdc_case_in_study[participant_id]:
            icdc_program = icdc_study_in_program[icdc_study]

            if icdc_study not in icdc_study_match_count:
                icdc_study_match_count[icdc_study] = 1
            else:
                icdc_study_match_count[icdc_study] = icdc_study_match_count[icdc_study] + 1

            if icdc_program not in icdc_program_match_count:
                icdc_program_match_count[icdc_program] = 1
            else:
                icdc_program_match_count[icdc_program] = icdc_program_match_count[icdc_program] + 1

            for ctdc_study in ctdc_participant_in_study[participant_id]:
                if icdc_study not in icdc_study_to_ctdc_study:
                    icdc_study_to_ctdc_study[icdc_study] = dict()
                if ctdc_study not in icdc_study_to_ctdc_study[icdc_study]:
                    icdc_study_to_ctdc_study[icdc_study][idc_collection] = 1
                else:
                    icdc_study_to_ctdc_study[icdc_study][ctdc_study] = icdc_study_to_ctdc_study[icdc_study][ctdc_study] + 1

for specimen_id in ctdc_specimen_in_study:
    
    if specimen_id in icdc_sample_in_study:
        for icdc_study in icdc_sample_in_study[submitter_id]:
            icdc_program = icdc_study_in_program[icdc_study]

            if icdc_study not in icdc_study_match_count:
                icdc_study_match_count[icdc_study] = 1
            else:
                icdc_study_match_count[icdc_study] = icdc_study_match_count[icdc_study] + 1

            if icdc_program not in icdc_program_match_count:
                icdc_program_match_count[icdc_program] = 1
            else:
                icdc_program_match_count[icdc_program] = icdc_program_match_count[icdc_program] + 1

            for ctdc_study in ctdc_specimen_in_study[specimen_id]:
                if icdc_study not in icdc_study_to_ctdc_study:
                    icdc_study_to_ctdc_study[icdc_study] = dict()
                if ctdc_study not in icdc_study_to_ctdc_study[icdc_study]:
                    icdc_study_to_ctdc_study[icdc_study][ctdc_study] = 1
                else:
                    icdc_study_to_ctdc_study[icdc_study][ctdc_study] = icdc_study_to_ctdc_study[icdc_study][ctdc_study] + 1

with open( output_file, 'w' ) as OUT:
    print( *[ 'match_count', 'ICDC_program_acronym', 'ICDC_program_name', 'ICDC_study_clinical_study_designation', 'ICDC_study_name', 'CTDC_study_id', ], sep='\t', file=OUT )

    for icdc_program_acronym in [ program for program, match_count in sorted( icdc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        for icdc_study_id in [ study for study, match_count in sorted( icdc_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            if icdc_program_acronym == icdc_study_in_program[icdc_study_id]:
                if icdc_study_id in icdc_study_to_ctdc_study:
                    for ctdc_study in [ study for study, match_count in sorted( icdc_study_to_ctdc_study[icdc_study_id].items(), key=lambda item: item[1], reverse=True ) ]:
                        match_count = icdc_study_to_ctdc_study[icdc_study_id][ctdc_study]
                        study_name = icdc_study_name[icdc_study_id]
                        program_name = icdc_program_name[icdc_program_acronym]
                        print( *[ match_count, icdc_program_acronym, program_name, icdc_study_id, study_name, ctdc_study ], sep='\t', file=OUT )


