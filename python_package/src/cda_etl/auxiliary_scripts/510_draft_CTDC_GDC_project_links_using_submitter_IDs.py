#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

# ARGUMENTS

if len( sys.argv ) != 4:
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <CTDC entity list> <GDC entity list> <output file>\n" )

ctdc_entity_list = sys.argv[1]
gdc_entity_list = sys.argv[2]
output_file = sys.argv[3]

# EXECUTION

ctdc_participant_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='participant' )
ctdc_specimen_in_study = map_columns_one_to_many( ctdc_entity_list, 'entity_id', 'Study.study_id', where_field='entity_type', where_value='specimen' )

gdc_project_in_program = map_columns_one_to_one( gdc_entity_list, 'project.project_id', 'program.name' )
gdc_case_in_project = map_columns_one_to_one( gdc_entity_list, 'entity_submitter_id', 'project.project_id', where_field='entity_type', where_value='case' )
gdc_sample_in_project = dict()
# Need to cover multiple entity_type values. My stored subroutines aren't that complex.
with open( gdc_entity_list ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
        if record['entity_type'] in { 'aliquot', 'analyte', 'portion', 'slide', 'sample' }:
            gdc_sample_in_project[record['entity_submitter_id']] = record['project.project_id']

gdc_project_match_count = dict()
gdc_program_match_count = dict()
gdc_project_to_ctdc_study = dict()

for participant_id in ctdc_participant_in_study:
    
    if participant_id in gdc_case_in_project:
        gdc_project = gdc_case_in_project[participant_id]
        gdc_program = gdc_project_in_program[gdc_project]

        if gdc_project not in gdc_project_match_count:
            gdc_project_match_count[gdc_project] = 1
        else:
            gdc_project_match_count[gdc_project] = gdc_project_match_count[gdc_project] + 1

        if gdc_program not in gdc_program_match_count:
            gdc_program_match_count[gdc_program] = 1
        else:
            gdc_program_match_count[gdc_program] = gdc_program_match_count[gdc_program] + 1

        for ctdc_study in ctdc_participant_in_study[participant_id]:
            if gdc_project not in gdc_project_to_ctdc_study:
                gdc_project_to_ctdc_study[gdc_project] = dict()
            if ctdc_study not in gdc_project_to_ctdc_study[gdc_project]:
                gdc_project_to_ctdc_study[gdc_project][ctdc_study] = 1
            else:
                gdc_project_to_ctdc_study[gdc_project][ctdc_study] = gdc_project_to_ctdc_study[gdc_project][ctdc_study] + 1

for specimen_id in ctdc_specimen_in_study:
    
    if specimen_id in gdc_sample_in_project:
        gdc_project = gdc_sample_in_project[specimen_id]
        gdc_program = gdc_project_in_program[gdc_project]

        if gdc_project not in gdc_project_match_count:
            gdc_project_match_count[gdc_project] = 1
        else:
            gdc_project_match_count[gdc_project] = gdc_project_match_count[gdc_project] + 1

        if gdc_program not in gdc_program_match_count:
            gdc_program_match_count[gdc_program] = 1
        else:
            gdc_program_match_count[gdc_program] = gdc_program_match_count[gdc_program] + 1

        for ctdc_study in ctdc_specimen_in_study[specimen_id]:
            if gdc_project not in gdc_project_to_ctdc_study:
                gdc_project_to_ctdc_study[gdc_project] = dict()
            if ctdc_study not in gdc_project_to_ctdc_study[gdc_project]:
                gdc_project_to_ctdc_study[gdc_project][ctdc_study] = 1
            else:
                gdc_project_to_ctdc_study[gdc_project][ctdc_study] = gdc_project_to_ctdc_study[gdc_project][ctdc_study] + 1

with open( output_file, 'w' ) as OUT:
    print( *[ 'match_count', 'GDC_program_name', 'GDC_project_id', 'CTDC_study_id', ], sep='\t', file=OUT )

    for gdc_program in [ program for program, match_count in sorted( gdc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        for gdc_project in [ project for project, match_count in sorted( gdc_project_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            if gdc_program == gdc_project_in_program[gdc_project]:
                if gdc_project in gdc_project_to_ctdc_study:
                    for ctdc_study in [ study for study, match_count in sorted( gdc_project_to_ctdc_study[gdc_project].items(), key=lambda item: item[1], reverse=True ) ]:
                        match_count = gdc_project_to_ctdc_study[gdc_project][ctdc_study]
                        print( *[ match_count, gdc_program, gdc_project, ctdc_study ], sep='\t', file=OUT )


