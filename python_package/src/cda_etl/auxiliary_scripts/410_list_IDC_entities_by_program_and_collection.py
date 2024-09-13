#!/usr/bin/env python -u

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, sort_file_with_header

from os import path, makedirs

# PARAMETERS

input_root = path.join( 'extracted_data', 'idc' )

idc_case_input_tsv = path.join( input_root, 'idc_case.tsv' )

tcga_biospecimen_input_tsv = path.join( input_root, 'tcga_biospecimen_rel9.tsv' )

aux_dir = path.join( 'auxiliary_metadata', '__IDC_supplemental_metadata' )

program_collection_input_tsv = path.join( aux_dir, 'IDC_all_programs_and_collections.tsv' )

output_file = path.join( aux_dir, 'IDC_entities_by_program_and_collection.tsv' )

# EXECUTION

if not path.exists( aux_dir ):
    
    makedirs( aux_dir )

collection_in_program = map_columns_one_to_one( program_collection_input_tsv, 'collection_id', 'Program' )

collection_id_to_name = map_columns_one_to_one( program_collection_input_tsv, 'collection_id', 'collection_name' )

collection_name_to_id = map_columns_one_to_one( program_collection_input_tsv, 'collection_name', 'collection_id' )

# `project_short_name` from this table will match `dicom_all.collection_name` for corresponding projects.

sample_in_collection = map_columns_one_to_one( tcga_biospecimen_input_tsv, 'sample_barcode', 'project_short_name' )

case_id_to_submitter_id = map_columns_one_to_one( idc_case_input_tsv, 'idc_case_id', 'submitter_case_id' )

case_in_collection = map_columns_one_to_one( idc_case_input_tsv, 'idc_case_id', 'collection_id' )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'original_collections_metadata.Program', 'original_collections_metadata.collection_id', 'original_collections_metadata.collection_name', 'entity_submitter_id', 'entity_id', 'entity_type' ], sep='\t', file=OUT )

    for idc_case_id in sorted( case_id_to_submitter_id ):
        
        submitter_id = case_id_to_submitter_id[idc_case_id]

        collection_id = case_in_collection[idc_case_id]

        program = collection_in_program[collection_id]

        collection_name = collection_id_to_name[collection_id]

        print( *[ program, collection_id, collection_name, submitter_id, idc_case_id, 'case' ], sep='\t', file=OUT )

    for sample_submitter_id in sorted( sample_in_collection ):
        
        collection_name = sample_in_collection[sample_submitter_id]

        # We got this collection_name from tcga_biospecimen_rel9.project_short_name -- is it present as an IDC `collection_id`?

        if collection_name in collection_name_to_id:
            
            collection_id = collection_name_to_id[collection_name]

            program = collection_in_program[collection_id]

            collection_name = collection_id_to_name[collection_id]

            print( *[ program, collection_id, collection_name, sample_submitter_id, '', 'sample' ], sep='\t', file=OUT )

sort_file_with_header( output_file )


