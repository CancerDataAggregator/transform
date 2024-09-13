#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC entity list> <PDC entity list> <output file>\n" )

idc_entity_list = sys.argv[1]

pdc_entity_list = sys.argv[2]

output_file = sys.argv[3]

match_list = re.findall( r'^(.*)\/[^\/]+$', output_file )

output_dir = '.'

if len( match_list ) > 0:
    
    output_dir = match_list[0]

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

idc_collection_in_program = map_columns_one_to_one( idc_entity_list, 'original_collections_metadata.collection_id', 'original_collections_metadata.Program' )

idc_case_submitter_id_to_collection = map_columns_one_to_many( idc_entity_list, 'entity_submitter_id', 'original_collections_metadata.collection_id', where_field='entity_type', where_value='case' )

idc_sample_submitter_id_to_collection = map_columns_one_to_many( idc_entity_list, 'entity_submitter_id', 'original_collections_metadata.collection_id', where_field='entity_type', where_value='sample' )

pdc_study_submitter_id_to_pdc_study_id = map_columns_one_to_one( pdc_entity_list, 'study.study_submitter_id', 'study.pdc_study_id' )

pdc_study_in_project = map_columns_one_to_one( pdc_entity_list, 'study.study_submitter_id', 'project.project_submitter_id' )

pdc_project_in_program = map_columns_one_to_one( pdc_entity_list, 'project.project_submitter_id', 'program.name' )

pdc_case_submitter_id_to_study = map_columns_one_to_many( pdc_entity_list, 'entity_submitter_id', 'study.study_submitter_id', where_field='entity_type', where_value='case' )

pdc_sample_submitter_id_to_study = dict()

# Need to cover multiple entity_type values. My stored subroutines aren't that complex.

with open( pdc_entity_list ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for next_line in IN:
        
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

        if record['entity_type'] in { 'aliquot', 'sample' }:
            
            pdc_sample_submitter_id_to_study[record['entity_submitter_id']] = record['study.study_submitter_id']

pdc_study_match_count = dict()

pdc_project_match_count = dict()

pdc_program_match_count = dict()

pdc_study_to_idc_collection = dict()

for submitter_id in idc_case_submitter_id_to_collection:
    
    if submitter_id in pdc_case_submitter_id_to_study:
        
        for pdc_study in pdc_case_submitter_id_to_study[submitter_id]:
            
            pdc_project = pdc_study_in_project[pdc_study]

            pdc_program = pdc_project_in_program[pdc_project]

            if pdc_study not in pdc_study_match_count:
                
                pdc_study_match_count[pdc_study] = 1

            else:
                
                pdc_study_match_count[pdc_study] = pdc_study_match_count[pdc_study] + 1

            if pdc_project not in pdc_project_match_count:
                
                pdc_project_match_count[pdc_project] = 1

            else:
                
                pdc_project_match_count[pdc_project] = pdc_project_match_count[pdc_project] + 1

            if pdc_program not in pdc_program_match_count:
                
                pdc_program_match_count[pdc_program] = 1

            else:
                
                pdc_program_match_count[pdc_program] = pdc_program_match_count[pdc_program] + 1

            for idc_collection in idc_case_submitter_id_to_collection[submitter_id]:
                
                if pdc_study not in pdc_study_to_idc_collection:
                    
                    pdc_study_to_idc_collection[pdc_study] = dict()

                if idc_collection not in pdc_study_to_idc_collection[pdc_study]:
                    
                    pdc_study_to_idc_collection[pdc_study][idc_collection] = 1

                else:
                    
                    pdc_study_to_idc_collection[pdc_study][idc_collection] = pdc_study_to_idc_collection[pdc_study][idc_collection] + 1

for submitter_id in idc_sample_submitter_id_to_collection:
    
    if submitter_id in pdc_sample_submitter_id_to_study:
        
        pdc_study = pdc_sample_submitter_id_to_study[submitter_id]

        pdc_project = pdc_study_in_project[pdc_study]

        pdc_program = pdc_project_in_program[pdc_project]

        if pdc_study not in pdc_study_match_count:
            
            pdc_study_match_count[pdc_study] = 1

        else:
            
            pdc_study_match_count[pdc_study] = pdc_study_match_count[pdc_study] + 1

        if pdc_project not in pdc_project_match_count:
            
            pdc_project_match_count[pdc_project] = 1

        else:
            
            pdc_project_match_count[pdc_project] = pdc_project_match_count[pdc_project] + 1

        if pdc_program not in pdc_program_match_count:
            
            pdc_program_match_count[pdc_program] = 1

        else:
            
            pdc_program_match_count[pdc_program] = pdc_program_match_count[pdc_program] + 1

        for idc_collection in idc_sample_submitter_id_to_collection[submitter_id]:
            
            if pdc_study not in pdc_study_to_idc_collection:
                
                pdc_study_to_idc_collection[pdc_study] = dict()

            if idc_collection not in pdc_study_to_idc_collection[pdc_study]:
                
                pdc_study_to_idc_collection[pdc_study][idc_collection] = 1

            else:
                
                pdc_study_to_idc_collection[pdc_study][idc_collection] = pdc_study_to_idc_collection[pdc_study][idc_collection] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'match_count', 'PDC_program_name', 'PDC_project_submitter_id', 'PDC_study_submitter_id', 'PDC_pdc_study_id', 'IDC_program_name', 'IDC_collection_id' ], sep='\t', file=OUT )

    for pdc_program in [ program for program, match_count in sorted( pdc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        
        for pdc_project in [ project for project, match_count in sorted( pdc_project_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            
            if pdc_project_in_program[pdc_project] == pdc_program:
                
                for pdc_study in [ study for study, match_count in sorted( pdc_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
                    
                    if pdc_study_in_project[pdc_study] == pdc_project and pdc_study in pdc_study_to_idc_collection:
                        
                        for idc_collection in [ collection for collection, match_count in sorted( pdc_study_to_idc_collection[pdc_study].items(), key=lambda item: item[1], reverse=True ) ]:
                            
                            match_count = pdc_study_to_idc_collection[pdc_study][idc_collection]

                            idc_program = idc_collection_in_program[idc_collection]

                            print( *[ match_count, pdc_program, pdc_project, pdc_study, pdc_study_submitter_id_to_pdc_study_id[pdc_study], idc_program, idc_collection ], sep='\t', file=OUT )


