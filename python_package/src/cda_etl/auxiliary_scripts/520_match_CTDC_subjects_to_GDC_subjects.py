#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gdc_cda_dir = path.join( cda_root, 'gdc_002_decorated_harmonized' )
gdc_subject_tsv = path.join( gdc_cda_dir, 'subject.tsv' )
gdc_upstream_identifiers_tsv = path.join( gdc_cda_dir, 'upstream_identifiers.tsv' )

ctdc_cda_dir = path.join( cda_root, 'ctdc_002_decorated_harmonized' )
ctdc_subject_tsv = path.join( ctdc_cda_dir, 'subject.tsv' )
ctdc_upstream_identifiers_tsv = path.join( ctdc_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

gdc_dir = path.join( aux_root, '__GDC_supplemental_metadata' )
gdc_entity_project_map = path.join( gdc_dir, 'GDC_entities_by_program_and_project.tsv' )
ctdc_dir = path.join( aux_root, '__CTDC_supplemental_metadata' )
ctdc_entity_project_map = path.join( ctdc_dir, 'CTDC_entities_by_Study.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )
project_map_tsv = path.join( agg_project_dir, 'naive_CTDC_GDC_project_id_map.hand_edited_to_remove_false_positives.tsv' )
agg_subject_dir = path.join( agg_root, 'subjects' )
output_file = path.join( agg_subject_dir, 'CTDC_CDA_subjects_linked_to_GDC_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between GDC subject identifiers and projects.
# 
# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	10	GDC	case.case_id	d70772f7-b776-44ef-8dc4-b379b2b9154a
gdc_case_id_to_cda_subject_alias = map_columns_one_to_one( gdc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='case.case_id' )
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

# Load connecting information between CTDC subject identifiers and studies.
# 
# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	264771	CTDC	Participant.participant_id	PASYWD
ctdc_participant_id_to_cda_subject_alias = map_columns_one_to_one( ctdc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='Participant.participant_id' )
ctdc_subject_alias_to_subject_id = map_columns_one_to_one( ctdc_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between GDC projects and CTDC studies.
ctdc_study_id_to_gdc_project_id = map_columns_one_to_many( project_map_tsv, 'CTDC_study_id', 'GDC_project_id' )

# Save ID information for all records merged due to ( same case ID ) + ( allowed equivalence between containing GDC project and containing CTDC study ).
ctdc_subject_alias_to_gdc_subject_alias = dict()

with open( ctdc_entity_project_map ) as IN:
    # Study.study_id	Study.study_short_name	Study.study_accession	entity_id	entity_type
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'participant':
            participant_id = record['entity_id']
            study_id = record['Study.study_id']
            cda_subject_alias = ctdc_participant_id_to_cda_subject_alias[participant_id]
            if study_id in ctdc_study_id_to_gdc_project_id:
                for gdc_project_id in ctdc_study_id_to_gdc_project_id[study_id]:
                    if gdc_project_id in gdc_project_id_and_case_submitter_id_to_cda_subject_alias and participant_id in gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id]:
                        gdc_subject_alias = gdc_project_id_and_case_submitter_id_to_cda_subject_alias[gdc_project_id][participant_id]
                        if cda_subject_alias in ctdc_subject_alias_to_gdc_subject_alias and ctdc_subject_alias_to_gdc_subject_alias[cda_subject_alias] != gdc_subject_alias:
                            sys.exit( f"FATAL: One CTDC CDA subject_alias ({cda_subject_alias}) mapped to two GDC CDA subject aliases ({ctdc_subject_alias_to_gdc_subject_alias[cda_subject_alias]} and {gdc_subject_alias}); aborting." )
                        else:
                            ctdc_subject_alias_to_gdc_subject_alias[cda_subject_alias] = gdc_subject_alias

# Write results.
with open( output_file, 'w' ) as OUT:
    print( *[ 'CTDC_subject_alias', 'CTDC_subject_id', 'GDC_subject_alias', 'GDC_subject_id' ], sep='\t', file=OUT )

    for ctdc_subject_alias in sorted( ctdc_subject_alias_to_gdc_subject_alias ):
        ctdc_subject_id = ctdc_subject_alias_to_subject_id[ctdc_subject_alias]
        gdc_subject_alias = ctdc_subject_alias_to_gdc_subject_alias[ctdc_subject_alias]
        gdc_subject_id = gdc_subject_alias_to_subject_id[gdc_subject_alias]
        print( *[ ctdc_subject_alias, ctdc_subject_id, gdc_subject_alias, gdc_subject_id ], sep='\t', file=OUT )


