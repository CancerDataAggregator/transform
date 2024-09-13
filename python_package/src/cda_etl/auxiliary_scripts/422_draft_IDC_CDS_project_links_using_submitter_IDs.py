#!/usr/bin/env python3 -u

import re, sys

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# ARGUMENTS

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <IDC entity list> <CDS entity list> <output file>\n" )

idc_entity_list = sys.argv[1]

cds_entity_list = sys.argv[2]

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

cds_study_in_program = map_columns_one_to_one( cds_entity_list, 'study.uuid', 'program.program_acronym' )

cds_participant_submitter_id_to_study = map_columns_one_to_many( cds_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='participant' )

cds_sample_submitter_id_to_study = map_columns_one_to_many( cds_entity_list, 'entity_submitter_id', 'study.uuid', where_field='entity_type', where_value='sample' )

cds_study_phs_accession = map_columns_one_to_one( cds_entity_list, 'study.uuid', 'study.phs_accession' )

cds_study_name = map_columns_one_to_one( cds_entity_list, 'study.uuid', 'study.study_name' )

cds_study_match_count = dict()

cds_program_match_count = dict()

cds_study_to_idc_collection = dict()

for submitter_id in idc_case_submitter_id_to_collection:
    
    if submitter_id in cds_participant_submitter_id_to_study:
        
        for cds_study in cds_participant_submitter_id_to_study[submitter_id]:
            
            cds_program = cds_study_in_program[cds_study]

            if cds_study not in cds_study_match_count:
                
                cds_study_match_count[cds_study] = 1

            else:
                
                cds_study_match_count[cds_study] = cds_study_match_count[cds_study] + 1

            if cds_program not in cds_program_match_count:
                
                cds_program_match_count[cds_program] = 1

            else:
                
                cds_program_match_count[cds_program] = cds_program_match_count[cds_program] + 1

            for idc_collection in idc_case_submitter_id_to_collection[submitter_id]:
                
                if cds_study not in cds_study_to_idc_collection:
                    
                    cds_study_to_idc_collection[cds_study] = dict()

                if idc_collection not in cds_study_to_idc_collection[cds_study]:
                    
                    cds_study_to_idc_collection[cds_study][idc_collection] = 1

                else:
                    
                    cds_study_to_idc_collection[cds_study][idc_collection] = cds_study_to_idc_collection[cds_study][idc_collection] + 1

for submitter_id in idc_sample_submitter_id_to_collection:
    
    if submitter_id in cds_sample_submitter_id_to_study:
        
        for cds_study in cds_sample_submitter_id_to_study[submitter_id]:
            
            cds_program = cds_study_in_program[cds_study]

            if cds_study not in cds_study_match_count:
                
                cds_study_match_count[cds_study] = 1

            else:
                
                cds_study_match_count[cds_study] = cds_study_match_count[cds_study] + 1

            if cds_program not in cds_program_match_count:
                
                cds_program_match_count[cds_program] = 1

            else:
                
                cds_program_match_count[cds_program] = cds_program_match_count[cds_program] + 1

            for idc_collection in idc_sample_submitter_id_to_collection[submitter_id]:
                
                if cds_study not in cds_study_to_idc_collection:
                    
                    cds_study_to_idc_collection[cds_study] = dict()

                if idc_collection not in cds_study_to_idc_collection[cds_study]:
                    
                    cds_study_to_idc_collection[cds_study][idc_collection] = 1

                else:
                    
                    cds_study_to_idc_collection[cds_study][idc_collection] = cds_study_to_idc_collection[cds_study][idc_collection] + 1

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'match_count', 'CDS_program_acronym', 'CDS_study_name', 'CDS_study_phs_accession', 'CDS_study_uuid', 'IDC_program_name', 'IDC_collection_id', ], sep='\t', file=OUT )

    for cds_program in [ program for program, match_count in sorted( cds_program_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
        
        for cds_study_uuid in [ study for study, match_count in sorted( cds_study_match_count.items(), key=lambda item: item[1], reverse=True ) ]:
            
            if cds_program == cds_study_in_program[cds_study_uuid]:
                
                if cds_study_uuid in cds_study_to_idc_collection:
                    
                    for idc_collection in [ collection for collection, match_count in sorted( cds_study_to_idc_collection[cds_study_uuid].items(), key=lambda item: item[1], reverse=True ) ]:
                        
                        match_count = cds_study_to_idc_collection[cds_study_uuid][idc_collection]

                        study_name = cds_study_name[cds_study_uuid]

                        study_phs_accession = cds_study_phs_accession[cds_study_uuid]

                        idc_program = idc_collection_in_program[idc_collection]

                        print( *[ match_count, cds_program, study_name, study_phs_accession, cds_study_uuid, idc_program, idc_collection ], sep='\t', file=OUT )


