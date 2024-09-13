#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC entity list> <GDC entity list> <output file>\n" )

idc_entity_list = sys.argv[1]

gdc_entity_list = sys.argv[2]

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

gdc_project_in_program = map_columns_one_to_one( gdc_entity_list, 'project.project_id', 'program.name' )

gdc_case_submitter_id_to_project = map_columns_one_to_one( gdc_entity_list, 'entity_submitter_id', 'project.project_id', where_field='entity_type', where_value='case' )

gdc_sample_submitter_id_to_project = dict()

# Need to cover multiple entity_type values. My stored subroutines aren't that complex.

with open( gdc_entity_list ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for next_line in IN:
        
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

        if record['entity_type'] in { 'aliquot', 'analyte', 'portion', 'slide', 'sample' }:
            
            gdc_sample_submitter_id_to_project[record['entity_submitter_id']] = record['project.project_id']

gdc_project_match_count = dict()

gdc_program_match_count = dict()

gdc_project_to_idc_collection = dict()

for submitter_id in idc_case_submitter_id_to_collection:
    
    if submitter_id in gdc_case_submitter_id_to_project:
        
        gdc_project = gdc_case_submitter_id_to_project[submitter_id]

        gdc_program = gdc_project_in_program[gdc_project]

        if gdc_project not in gdc_project_match_count:
            
            gdc_project_match_count[gdc_project] = 1

        else:
            
            gdc_project_match_count[gdc_project] = gdc_project_match_count[gdc_project] + 1

        if gdc_program not in gdc_program_match_count:
            
            gdc_program_match_count[gdc_program] = 1

        else:
            
            gdc_program_match_count[gdc_program] = gdc_program_match_count[gdc_program] + 1

        for idc_collection in idc_case_submitter_id_to_collection[submitter_id]:
            
            if gdc_project not in gdc_project_to_idc_collection:
                
                gdc_project_to_idc_collection[gdc_project] = dict()

            if idc_collection not in gdc_project_to_idc_collection[gdc_project]:
                
                gdc_project_to_idc_collection[gdc_project][idc_collection] = 1

            else:
                
                gdc_project_to_idc_collection[gdc_project][idc_collection] = gdc_project_to_idc_collection[gdc_project][idc_collection] + 1

for submitter_id in idc_sample_submitter_id_to_collection:
    
    if submitter_id in gdc_sample_submitter_id_to_project:
        
        gdc_project = gdc_sample_submitter_id_to_project[submitter_id]

        gdc_program = gdc_project_in_program[gdc_project]

        if gdc_project not in gdc_project_match_count:
            
            gdc_project_match_count[gdc_project] = 1

        else:
            
            gdc_project_match_count[gdc_project] = gdc_project_match_count[gdc_project] + 1

        if gdc_program not in gdc_program_match_count:
            
            gdc_program_match_count[gdc_program] = 1

        else:
            
            gdc_program_match_count[gdc_program] = gdc_program_match_count[gdc_program] + 1

        for idc_collection in idc_sample_submitter_id_to_collection[submitter_id]:
            
            if gdc_project not in gdc_project_to_idc_collection:
                
                gdc_project_to_idc_collection[gdc_project] = dict()

            if idc_collection not in gdc_project_to_idc_collection[gdc_project]:
                
                gdc_project_to_idc_collection[gdc_project][idc_collection] = 1

            else:
                
                gdc_project_to_idc_collection[gdc_project][idc_collection] = gdc_project_to_idc_collection[gdc_project][idc_collection] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'match_count', 'GDC_program_name', 'GDC_project_id', 'IDC_program_name', 'IDC_collection_id', ], sep='\t', file=OUT )

    for gdc_program in [ program for program, match_count in sorted( gdc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        
        for gdc_project in [ project for project, match_count in sorted( gdc_project_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            
            if gdc_program == gdc_project_in_program[gdc_project]:
                
                if gdc_project in gdc_project_to_idc_collection:
                    
                    for idc_collection in [ collection for collection, match_count in sorted( gdc_project_to_idc_collection[gdc_project].items(), key=lambda item: item[1], reverse=True ) ]:
                        
                        match_count = gdc_project_to_idc_collection[gdc_project][idc_collection]

                        idc_program = idc_collection_in_program[idc_collection]

                        print( *[ match_count, gdc_program, gdc_project, idc_program, idc_collection ], sep='\t', file=OUT )


