#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC entity list> <ICDC entity list> <output file>\n" )

idc_entity_list = sys.argv[1]

icdc_entity_list = sys.argv[2]

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

icdc_study_in_program = map_columns_one_to_one( icdc_entity_list, 'study.clinical_study_designation', 'program.program_acronym' )

icdc_study_name = map_columns_one_to_one( icdc_entity_list, 'study.clinical_study_designation', 'study.clinical_study_name' )

icdc_program_name = map_columns_one_to_one( icdc_entity_list, 'program.program_acronym', 'program.program_name' )

icdc_case_submitter_id_to_study = map_columns_one_to_many( icdc_entity_list, 'entity_id', 'study.clinical_study_designation', where_field='entity_type', where_value='case' )

icdc_sample_submitter_id_to_study = map_columns_one_to_many( icdc_entity_list, 'entity_id', 'study.clinical_study_designation', where_field='entity_type', where_value='sample' )

icdc_study_match_count = dict()

icdc_program_match_count = dict()

icdc_study_to_idc_collection = dict()

for submitter_id in idc_case_submitter_id_to_collection:
    
    if submitter_id in icdc_case_submitter_id_to_study:
        
        for icdc_study in icdc_case_submitter_id_to_study[submitter_id]:
            
            icdc_program = icdc_study_in_program[icdc_study]

            if icdc_study not in icdc_study_match_count:
                
                icdc_study_match_count[icdc_study] = 1

            else:
                
                icdc_study_match_count[icdc_study] = icdc_study_match_count[icdc_study] + 1

            if icdc_program not in icdc_program_match_count:
                
                icdc_program_match_count[icdc_program] = 1

            else:
                
                icdc_program_match_count[icdc_program] = icdc_program_match_count[icdc_program] + 1

            for idc_collection in idc_case_submitter_id_to_collection[submitter_id]:
                
                if icdc_study not in icdc_study_to_idc_collection:
                    
                    icdc_study_to_idc_collection[icdc_study] = dict()

                if idc_collection not in icdc_study_to_idc_collection[icdc_study]:
                    
                    icdc_study_to_idc_collection[icdc_study][idc_collection] = 1

                else:
                    
                    icdc_study_to_idc_collection[icdc_study][idc_collection] = icdc_study_to_idc_collection[icdc_study][idc_collection] + 1

for submitter_id in idc_sample_submitter_id_to_collection:
    
    if submitter_id in icdc_sample_submitter_id_to_study:
        
        for icdc_study in icdc_sample_submitter_id_to_study[submitter_id]:
            
            icdc_program = icdc_study_in_program[icdc_study]

            if icdc_study not in icdc_study_match_count:
                
                icdc_study_match_count[icdc_study] = 1

            else:
                
                icdc_study_match_count[icdc_study] = icdc_study_match_count[icdc_study] + 1

            if icdc_program not in icdc_program_match_count:
                
                icdc_program_match_count[icdc_program] = 1

            else:
                
                icdc_program_match_count[icdc_program] = icdc_program_match_count[icdc_program] + 1

            for idc_collection in idc_sample_submitter_id_to_collection[submitter_id]:
                
                if icdc_study not in icdc_study_to_idc_collection:
                    
                    icdc_study_to_idc_collection[icdc_study] = dict()

                if idc_collection not in icdc_study_to_idc_collection[icdc_study]:
                    
                    icdc_study_to_idc_collection[icdc_study][idc_collection] = 1

                else:
                    
                    icdc_study_to_idc_collection[icdc_study][idc_collection] = icdc_study_to_idc_collection[icdc_study][idc_collection] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'match_count', 'ICDC_program_acronym', 'ICDC_program_name', 'ICDC_study_clinical_study_designation', 'ICDC_study_name', 'IDC_program_name', 'IDC_collection_id', ], sep='\t', file=OUT )

    for icdc_program_acronym in [ program for program, match_count in sorted( icdc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        
        for icdc_study_id in [ study for study, match_count in sorted( icdc_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            
            if icdc_program_acronym == icdc_study_in_program[icdc_study_id]:
                
                if icdc_study_id in icdc_study_to_idc_collection:
                    
                    for idc_collection in [ collection for collection, match_count in sorted( icdc_study_to_idc_collection[icdc_study_id].items(), key=lambda item: item[1], reverse=True ) ]:
                        
                        match_count = icdc_study_to_idc_collection[icdc_study_id][idc_collection]

                        study_name = icdc_study_name[icdc_study_id]

                        program_name = icdc_program_name[icdc_program_acronym]

                        idc_program = idc_collection_in_program[idc_collection]

                        print( *[ match_count, icdc_program_acronym, program_name, icdc_study_id, study_name, idc_program, idc_collection ], sep='\t', file=OUT )


