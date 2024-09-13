#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )

gdc_subject_tsv = path.join( gdc_cda_dir, 'subject.tsv' )

gdc_upstream_identifiers_tsv = path.join( gdc_cda_dir, 'upstream_identifiers.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )

idc_subject_tsv = path.join( idc_cda_dir, 'subject.tsv' )

idc_upstream_identifiers_tsv = path.join( idc_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

gdc_dir = path.join( aux_root, '__GDC_supplemental_metadata' )

gdc_entity_project_map = path.join( gdc_dir, 'GDC_entities_by_program_and_project.tsv' )

idc_dir = path.join( aux_root, '__IDC_supplemental_metadata' )

idc_entity_project_map = path.join( idc_dir, 'IDC_entities_by_program_and_collection.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

project_map_tsv = path.join( agg_project_dir, 'naive_IDC_GDC_project_id_map.hand_edited_to_remove_false_positives.tsv' )

agg_subject_dir = path.join( agg_root, 'subjects' )

output_file = path.join( agg_subject_dir, 'IDC_CDA_subjects_linked_to_GDC_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between GDC subject identifiers and projects.

# cda_table	id_alias	data_source	data_source_id_field_name	data_source_id_value
# subject	10	GDC	case.case_id	d70772f7-b776-44ef-8dc4-b379b2b9154a

gdc_case_id_to_cda_subject_alias = map_columns_one_to_one( gdc_upstream_identifiers_tsv, 'data_source_id_value', 'id_alias', where_field='data_source_id_field_name', where_value='case.case_id' )

gdc_subject_alias_to_subject_id = map_columns_one_to_one( gdc_subject_tsv, 'id_alias', 'id' )

gdc_project_id_and_case_submitter_id_to_cda_subject_alias = dict()

with open( gdc_entity_project_map ) as IN:
    
    # program.program_id	program.name	project.project_id	project.name	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'case':
            
            case_id = record['entity_id']

            case_submitter_id = record['entity_submitter_id']

            gdc_project_id = record['project.project_id']

            cda_subject_alias = gdc_case_id_to_cda_subject_alias[case_id]

            if gdc_project_id not in gdc_project_id_and_case_submitter_id_to_cda_subject_alias:
                
                gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id] = dict()

            if case_submitter_id in gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id] and gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id][case_submitter_id] != cda_subject_alias:
                
                # We definitely want to notice if one project/submitter_id pair is mapped to multiple CDA subjects. Hope we don't get here.

                sys.exit( f"FATAL: One GDC project_id/case_submitter_id pair ({gdc_project_id}/{case_submitter_id}) mapped to two CDA subject aliases ({gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id][case_submitter_id]} and {cda_subject_alias}); aborting." )

            else:
                
                gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id][case_submitter_id] = cda_subject_alias

# Load connecting information between IDC subject identifiers and studies.

# cda_table	id_alias	data_source	data_source_id_field_name	data_source_id_value
# subject	94939	IDC	dicom_all.idc_case_id	0b59317b-621a-459e-995f-f8c7c5a6faf8

idc_case_id_to_cda_subject_alias = map_columns_one_to_one( idc_upstream_identifiers_tsv, 'data_source_id_value', 'id_alias', where_field='data_source_id_field_name', where_value='dicom_all.idc_case_id' )

idc_subject_alias_to_subject_id = map_columns_one_to_one( idc_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between GDC projects and IDC studies.

idc_collection_id_to_gdc_project_id = map_columns_one_to_many( project_map_tsv, 'IDC_collection_id', 'GDC_project_id' )

# Save ID information for all records merged due to ( same case_submitter_id ) + ( allowed equivalence between containing GDC project and containing IDC study ).

idc_subject_alias_to_gdc_subject_alias = dict()

with open( idc_entity_project_map ) as IN:
    
    # original_collections_metadata.Program	original_collections_metadata.collection_id	original_collections_metadata.collection_name	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'case':
            
            case_id = record['entity_id']

            case_submitter_id = record['entity_submitter_id']

            collection_id = record['original_collections_metadata.collection_id']

            cda_subject_alias = idc_case_id_to_cda_subject_alias[case_id]

            if collection_id in idc_collection_id_to_gdc_project_id:
                
                for gdc_project_id in idc_collection_id_to_gdc_project_id[collection_id]:
                    
                    if gdc_project_id in gdc_project_id_and_case_submitter_id_to_cda_subject_alias and case_submitter_id in gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id]:
                        
                        gdc_subject_alias = gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id][case_submitter_id]

                        if cda_subject_alias in idc_subject_alias_to_gdc_subject_alias and idc_subject_alias_to_gdc_subject_alias[cda_subject_alias] != gdc_subject_alias:
                            
                            sys.exit( f"FATAL: One IDC CDA subject_alias ({cda_subject_alias}) mapped to two GDC CDA subject aliases ({idc_subject_alias_to_gdc_subject_alias[cda_subject_alias]} and {gdc_subject_alias}); aborting." )

                        else:
                            
                            idc_subject_alias_to_gdc_subject_alias[cda_subject_alias] = gdc_subject_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'IDC_subject_alias', 'IDC_subject_id', 'GDC_subject_alias', 'GDC_subject_id' ], sep='\t', file=OUT )

    for idc_subject_alias in sorted( idc_subject_alias_to_gdc_subject_alias ):
        
        idc_subject_id = idc_subject_alias_to_subject_id[idc_subject_alias]

        gdc_subject_alias = idc_subject_alias_to_gdc_subject_alias[idc_subject_alias]

        gdc_subject_id = gdc_subject_alias_to_subject_id[gdc_subject_alias]

        print( *[ idc_subject_alias, idc_subject_id, gdc_subject_alias, gdc_subject_id ], sep='\t', file=OUT )


