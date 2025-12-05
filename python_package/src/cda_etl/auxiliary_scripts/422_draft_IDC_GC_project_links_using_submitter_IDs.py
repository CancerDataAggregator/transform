#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC entity list> <GC entity list> <output file>\n" )

idc_entity_list = sys.argv[1]

gc_entity_list = sys.argv[2]

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

gc_study_in_program = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'program.program_acronym' )

gc_participant_submitter_id_to_study = map_columns_one_to_many( gc_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='participant' )

gc_sample_submitter_id_to_study = map_columns_one_to_many( gc_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='sample' )

gc_study_phs_accession = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'study.phs_accession' )

gc_study_name = map_columns_one_to_one( gc_entity_list, 'study.uuid', 'study.study_name' )

gc_study_match_count = dict()

gc_program_match_count = dict()

gc_study_to_idc_collection = dict()

for submitter_id in idc_case_submitter_id_to_collection:
    
    if submitter_id in gc_participant_submitter_id_to_study:
        
        for gc_study in gc_participant_submitter_id_to_study[submitter_id]:
            
            gc_program = gc_study_in_program[gc_study]

            if gc_study not in gc_study_match_count:
                
                gc_study_match_count[gc_study] = 1

            else:
                
                gc_study_match_count[gc_study] = gc_study_match_count[gc_study] + 1

            if gc_program not in gc_program_match_count:
                
                gc_program_match_count[gc_program] = 1

            else:
                
                gc_program_match_count[gc_program] = gc_program_match_count[gc_program] + 1

            for idc_collection in idc_case_submitter_id_to_collection[submitter_id]:
                
                if gc_study not in gc_study_to_idc_collection:
                    
                    gc_study_to_idc_collection[gc_study] = dict()

                if idc_collection not in gc_study_to_idc_collection[gc_study]:
                    
                    gc_study_to_idc_collection[gc_study][idc_collection] = 1

                else:
                    
                    gc_study_to_idc_collection[gc_study][idc_collection] = gc_study_to_idc_collection[gc_study][idc_collection] + 1

for submitter_id in idc_sample_submitter_id_to_collection:
    
    if submitter_id in gc_sample_submitter_id_to_study:
        
        for gc_study in gc_sample_submitter_id_to_study[submitter_id]:
            
            gc_program = gc_study_in_program[gc_study]

            if gc_study not in gc_study_match_count:
                
                gc_study_match_count[gc_study] = 1

            else:
                
                gc_study_match_count[gc_study] = gc_study_match_count[gc_study] + 1

            if gc_program not in gc_program_match_count:
                
                gc_program_match_count[gc_program] = 1

            else:
                
                gc_program_match_count[gc_program] = gc_program_match_count[gc_program] + 1

            for idc_collection in idc_sample_submitter_id_to_collection[submitter_id]:
                
                if gc_study not in gc_study_to_idc_collection:
                    
                    gc_study_to_idc_collection[gc_study] = dict()

                if idc_collection not in gc_study_to_idc_collection[gc_study]:
                    
                    gc_study_to_idc_collection[gc_study][idc_collection] = 1

                else:
                    
                    gc_study_to_idc_collection[gc_study][idc_collection] = gc_study_to_idc_collection[gc_study][idc_collection] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'match_count', 'GC_program_acronym', 'GC_study_name', 'GC_study_phs_accession', 'GC_study_uuid', 'IDC_program_name', 'IDC_collection_id', ], sep='\t', file=OUT )

    for gc_program in [ program for program, match_count in sorted( gc_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        
        for gc_study_uuid in [ study for study, match_count in sorted( gc_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            
            if gc_program == gc_study_in_program[gc_study_uuid]:
                
                if gc_study_uuid in gc_study_to_idc_collection:
                    
                    for idc_collection in [ collection for collection, match_count in sorted( gc_study_to_idc_collection[gc_study_uuid].items(), key=lambda item: item[1], reverse=True ) ]:
                        
                        match_count = gc_study_to_idc_collection[gc_study_uuid][idc_collection]

                        study_name = gc_study_name[gc_study_uuid]

                        study_phs_accession = gc_study_phs_accession[gc_study_uuid]

                        idc_program = idc_collection_in_program[idc_collection]

                        print( *[ match_count, gc_program, study_name, study_phs_accession, gc_study_uuid, idc_program, idc_collection ], sep='\t', file=OUT )


