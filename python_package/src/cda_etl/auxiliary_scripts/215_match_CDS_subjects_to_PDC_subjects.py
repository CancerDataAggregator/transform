#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

pdc_cda_dir = path.join( cda_root, 'pdc_002_decorated_harmonized' )

pdc_subject_tsv = path.join( pdc_cda_dir, 'subject.tsv' )

pdc_upstream_identifiers_tsv = path.join( pdc_cda_dir, 'upstream_identifiers.tsv' )

cds_cda_dir = path.join( cda_root, 'cds_002_decorated_harmonized' )

cds_subject_tsv = path.join( cds_cda_dir, 'subject.tsv' )

cds_upstream_identifiers_tsv = path.join( cds_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

pdc_dir = path.join( aux_root, '__PDC_supplemental_metadata' )

pdc_entity_project_map = path.join( pdc_dir, 'PDC_entities_by_program_project_and_study.tsv' )

cds_dir = path.join( aux_root, '__CDS_supplemental_metadata' )

cds_entity_project_map = path.join( cds_dir, 'CDS_entities_by_program_and_study.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

project_map_tsv = path.join( agg_project_dir, 'naive_CDS_PDC_project_id_map.hand_edited_to_remove_false_positives.tsv' )

agg_subject_dir = path.join( agg_root, 'subjects' )

output_file = path.join( agg_subject_dir, 'CDS_CDA_subjects_linked_to_PDC_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between PDC subject identifiers and studies.

# cda_table	id_alias	data_source	data_source_id_field_name	data_source_id_value
# subject	44580	PDC	Case.case_id	c3d87659-d18b-4f58-8ff1-e4780d5e5a74

pdc_case_id_to_cda_subject_alias = map_columns_one_to_one( pdc_upstream_identifiers_tsv, 'data_source_id_value', 'id_alias', where_field='data_source_id_field_name', where_value='Case.case_id' )

pdc_subject_alias_to_subject_id = map_columns_one_to_one( pdc_subject_tsv, 'id_alias', 'id' )

pdc_study_id_and_case_submitter_id_to_cda_subject_alias = dict()

with open( pdc_entity_project_map ) as IN:
    
    # program.program_id	program.program_submitter_id	program.name	project.project_id	project.project_submitter_id	project.name	study.study_id	study.study_submitter_id	study.pdc_study_id	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'case':
            
            case_id = record['entity_id']

            case_submitter_id = record['entity_submitter_id']

            pdc_study_id = record['study.pdc_study_id']

            cda_subject_alias = pdc_case_id_to_cda_subject_alias[case_id]

            if pdc_study_id not in pdc_study_id_and_case_submitter_id_to_cda_subject_alias:
                
                pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id] = dict()

            if case_submitter_id in pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id] and pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id][case_submitter_id] != cda_subject_alias:
                
                # We definitely want to notice if one project/submitter_id pair is mapped to multiple CDA subjects. Hope we don't get here.

                sys.exit( f"FATAL: One PDC pdc_study_id/case_submitter_id pair ({pdc_study_id}/{case_submitter_id}) mapped to two CDA subject aliases ({pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id][case_submitter_id]} and {cda_subject_alias}); aborting." )

            else:
                
                pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id][case_submitter_id] = cda_subject_alias

# Load connecting information between CDS subject identifiers and studies.

# cda_table	id_alias	data_source	data_source_id_field_name	data_source_id_value
# subject	48470	CDS	participant.uuid	dd1a5308-2a5f-5c73-9984-c374b5111c5e

cds_participant_uuid_to_cda_subject_alias = map_columns_one_to_one( cds_upstream_identifiers_tsv, 'data_source_id_value', 'id_alias', where_field='data_source_id_field_name', where_value='participant.uuid' )

cds_subject_alias_to_subject_id = map_columns_one_to_one( cds_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between PDC studies and CDS studies.

cds_study_id_to_pdc_study_id = map_columns_one_to_many( project_map_tsv, 'CDS_study_uuid', 'PDC_pdc_study_id' )

# Save ID information for all records merged due to ( same case_submitter_id ) + ( allowed equivalence between containing PDC study and containing CDS study ).

cds_subject_alias_to_pdc_subject_alias = dict()

with open( cds_entity_project_map ) as IN:
    
    # program.uuid	program.program_acronym	program.program_name	study.uuid	study.phs_accession	study.study_acronym	study.study_name	entity_submitter_id	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'participant':
            
            participant_uuid = record['entity_id']

            participant_submitter_id = record['entity_submitter_id']

            study_id = record['study.uuid']

            cda_subject_alias = cds_participant_uuid_to_cda_subject_alias[participant_uuid]

            if study_id in cds_study_id_to_pdc_study_id:
                
                for pdc_study_id in cds_study_id_to_pdc_study_id[study_id]:
                    
                    if pdc_study_id in pdc_study_id_and_case_submitter_id_to_cda_subject_alias and participant_submitter_id in pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id]:
                        
                        pdc_subject_alias = pdc_study_id_and_case_submitter_id_to_cda_subject_alias[pdc_study_id][participant_submitter_id]

                        if cda_subject_alias in cds_subject_alias_to_pdc_subject_alias and cds_subject_alias_to_pdc_subject_alias[cda_subject_alias] != pdc_subject_alias:
                            
                            sys.exit( f"FATAL: One CDS CDA subject_alias ({cda_subject_alias}) mapped to two PDC CDA subject aliases ({cds_subject_alias_to_pdc_subject_alias[cda_subject_alias]} and {pdc_subject_alias}); aborting." )

                        else:
                            
                            cds_subject_alias_to_pdc_subject_alias[cda_subject_alias] = pdc_subject_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'CDS_subject_alias', 'CDS_subject_id', 'PDC_subject_alias', 'PDC_subject_id' ], sep='\t', file=OUT )

    for cds_subject_alias in sorted( cds_subject_alias_to_pdc_subject_alias ):
        
        cds_subject_id = cds_subject_alias_to_subject_id[cds_subject_alias]

        pdc_subject_alias = cds_subject_alias_to_pdc_subject_alias[cds_subject_alias]

        pdc_subject_id = pdc_subject_alias_to_subject_id[pdc_subject_alias]

        print( *[ cds_subject_alias, cds_subject_id, pdc_subject_alias, pdc_subject_id ], sep='\t', file=OUT )


