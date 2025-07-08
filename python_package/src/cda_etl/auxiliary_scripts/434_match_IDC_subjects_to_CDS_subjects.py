#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_subject_tsv = path.join( cds_cda_dir, 'subject.tsv' )

cds_upstream_identifiers_tsv = path.join( cds_cda_dir, 'upstream_identifiers.tsv' )

idc_cda_dir = path.join( cda_root, 'idc_002_decorated_harmonized' )

idc_subject_tsv = path.join( idc_cda_dir, 'subject.tsv' )

idc_upstream_identifiers_tsv = path.join( idc_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

cds_dir = path.join( aux_root, '__CDS_supplemental_metadata' )

cds_entity_project_map = path.join( cds_dir, 'CDS_entities_by_program_and_study.tsv' )

idc_dir = path.join( aux_root, '__IDC_supplemental_metadata' )

idc_entity_project_map = path.join( idc_dir, 'IDC_entities_by_program_and_collection.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

project_map_tsv = path.join( agg_project_dir, 'naive_IDC_CDS_project_id_map.hand_edited_to_remove_false_positives.tsv' )

agg_subject_dir = path.join( agg_root, 'subjects' )

output_file = path.join( agg_subject_dir, 'IDC_CDA_subjects_linked_to_CDS_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between CDS subject identifiers and projects.

# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	48470	CDS	participant.participant_id	00301d78915737fa100f

cds_participant_uuid_to_cda_subject_alias = map_columns_one_to_one( cds_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='participant.uuid' )

cds_subject_alias_to_subject_id = map_columns_one_to_one( cds_subject_tsv, 'id_alias', 'id' )

cds_study_id_and_participant_id_to_cda_subject_alias = dict()

with open( cds_entity_project_map ) as IN:
    
    # program.uuid	program.program_acronym	program.program_name	study.uuid	study.phs_accession	study.study_acronym	study.study_name	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'participant':
            
            participant_uuid = record['entity_id']

            participant_id = record['entity_submitter_id']

            study_id = record['study.uuid']

            cda_subject_alias = cds_participant_uuid_to_cda_subject_alias[participant_uuid]

            if study_id not in cds_study_id_and_participant_id_to_cda_subject_alias:
                
                cds_study_id_and_participant_id_to_cda_subject_alias[study_id] = dict()

            if participant_id in cds_study_id_and_participant_id_to_cda_subject_alias[study_id] and cds_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id] != cda_subject_alias:
                
                # We definitely want to notice if one project/submitter_id pair is mapped to multiple CDA subjects. Hope we don't get here.

                sys.exit( f"FATAL: One CDS study_id/participant_id pair ({study_id}/{participant_id}) mapped to two CDA subject aliases ({cds_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id]} and {cda_subject_alias}); aborting." )

            else:
                
                cds_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id] = cda_subject_alias

# Load connecting information between IDC subject identifiers and studies.

# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	94939	IDC	dicom_all.idc_case_id	0b59317b-621a-459e-995f-f8c7c5a6faf8

idc_case_id_to_cda_subject_alias = map_columns_one_to_one( idc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='dicom_all.idc_case_id' )

idc_subject_alias_to_subject_id = map_columns_one_to_one( idc_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between CDS studies and IDC studies.

idc_collection_id_to_cds_study_id = map_columns_one_to_many( project_map_tsv, 'IDC_collection_id', 'CDS_study_uuid' )

# Save ID information for all records merged due to ( same case_submitter_id ) + ( allowed equivalence between containing CDS study and containing IDC study ).

idc_subject_alias_to_cds_subject_alias = dict()

with open( idc_entity_project_map ) as IN:
    
    # original_collections_metadata.Program	original_collections_metadata.collection_id	original_collections_metadata.collection_name	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'case':
            
            case_id = record['entity_id']

            case_submitter_id = record['entity_submitter_id']

            collection_id = record['original_collections_metadata.collection_id']

            cda_subject_alias = idc_case_id_to_cda_subject_alias[case_id]

            if collection_id in idc_collection_id_to_cds_study_id:
                
                for cds_study_id in idc_collection_id_to_cds_study_id[collection_id]:
                    
                    if cds_study_id in cds_study_id_and_participant_id_to_cda_subject_alias and case_submitter_id in cds_study_id_and_participant_id_to_cda_subject_alias[cds_study_id]:
                        
                        cds_subject_alias = cds_study_id_and_participant_id_to_cda_subject_alias[cds_study_id][case_submitter_id]

                        if cda_subject_alias in idc_subject_alias_to_cds_subject_alias and idc_subject_alias_to_cds_subject_alias[cda_subject_alias] != cds_subject_alias:
                            
                            sys.exit( f"FATAL: One IDC CDA subject_alias ({cda_subject_alias}) mapped to two CDS CDA subject aliases ({idc_subject_alias_to_cds_subject_alias[cda_subject_alias]} and {cds_subject_alias}); aborting." )

                        else:
                            
                            idc_subject_alias_to_cds_subject_alias[cda_subject_alias] = cds_subject_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'IDC_subject_alias', 'IDC_subject_id', 'CDS_subject_alias', 'CDS_subject_id' ], sep='\t', file=OUT )

    for idc_subject_alias in sorted( idc_subject_alias_to_cds_subject_alias ):
        
        idc_subject_id = idc_subject_alias_to_subject_id[idc_subject_alias]

        cds_subject_alias = idc_subject_alias_to_cds_subject_alias[idc_subject_alias]

        cds_subject_id = cds_subject_alias_to_subject_id[cds_subject_alias]

        print( *[ idc_subject_alias, idc_subject_id, cds_subject_alias, cds_subject_id ], sep='\t', file=OUT )


