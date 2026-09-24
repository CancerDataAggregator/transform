#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )
gc_subject_tsv = path.join( gc_cda_dir, 'subject.tsv' )
gc_upstream_identifiers_tsv = path.join( gc_cda_dir, 'upstream_identifiers.tsv' )

ctdc_cda_dir = path.join( cda_root, 'ctdc_002_decorated_harmonized' )
ctdc_subject_tsv = path.join( ctdc_cda_dir, 'subject.tsv' )
ctdc_upstream_identifiers_tsv = path.join( ctdc_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

gc_dir = path.join( aux_root, '__GC_supplemental_metadata' )
gc_entity_project_map = path.join( gc_dir, 'GC_entities_by_program_and_study.tsv' )

ctdc_dir = path.join( aux_root, '__CTDC_supplemental_metadata' )
ctdc_entity_project_map = path.join( ctdc_dir, 'CTDC_entities_by_Study.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )
project_map_tsv = path.join( agg_project_dir, 'naive_CTDC_GC_project_id_map.hand_edited_to_remove_false_positives.tsv' )
agg_subject_dir = path.join( agg_root, 'subjects' )
output_file = path.join( agg_subject_dir, 'CTDC_CDA_subjects_linked_to_GC_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between GC subject identifiers and projects.
# 
# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	48470	GC	participant.participant_id	00301d78915737fa100f
gc_participant_uuid_to_cda_subject_alias = map_columns_one_to_one( gc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='participant.uuid' )
gc_subject_alias_to_subject_id = map_columns_one_to_one( gc_subject_tsv, 'id_alias', 'id' )
gc_study_id_and_participant_id_to_cda_subject_alias = dict()

with open( gc_entity_project_map ) as IN:
    # program.uuid	program.program_acronym	program.program_name	study.uuid	study.phs_accession	study.study_acronym	study.study_name	entity_submitter_id	entity_id	entity_type
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'participant':
            participant_uuid = record['entity_id']
            participant_id = record['entity_submitter_id']
            study_id = record['study.uuid']
            cda_subject_alias = gc_participant_uuid_to_cda_subject_alias[participant_uuid]
            if study_id not in gc_study_id_and_participant_id_to_cda_subject_alias:
                gc_study_id_and_participant_id_to_cda_subject_alias[study_id] = dict()
            if participant_id in gc_study_id_and_participant_id_to_cda_subject_alias[study_id] and gc_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id] != cda_subject_alias:
                # We definitely want to notice if one project/submitter_id pair is mapped to multiple CDA subjects. Hope we don't get here.
                sys.exit( f"FATAL: One GC study_id/participant_id pair ({study_id}/{participant_id}) mapped to two CDA subject aliases ({gc_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id]} and {cda_subject_alias}); aborting." )
            else:
                gc_study_id_and_participant_id_to_cda_subject_alias[study_id][participant_id] = cda_subject_alias

# Load connecting information between CTDC subject identifiers and studies.
# 
# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	264771	CTDC	Participant.participant_id	PASYWD
ctdc_participant_id_to_cda_subject_alias = map_columns_one_to_one( ctdc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='Participant.participant_id' )
ctdc_subject_alias_to_subject_id = map_columns_one_to_one( ctdc_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between GC projects and CTDC studies.
ctdc_study_id_to_gc_study_id = map_columns_one_to_many( project_map_tsv, 'CTDC_study_id', 'GC_study_uuid' )

# Save ID information for all records merged due to ( same case ID ) + ( allowed equivalence between containing GC project and containing CTDC study ).
ctdc_subject_alias_to_gc_subject_alias = dict()

with open( ctdc_entity_project_map ) as IN:
    # Study.study_id	Study.study_short_name	Study.study_accession	entity_id	entity_type
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'participant':
            participant_id = record['entity_id']
            study_id = record['Study.study_id']
            cda_subject_alias = ctdc_participant_id_to_cda_subject_alias[participant_id]
            if study_id in ctdc_study_id_to_gc_study_id:
                for gc_study_id in ctdc_study_id_to_gc_study_id[study_id]:
                    if gc_study_id in gc_study_id_and_participant_id_to_cda_subject_alias and participant_id in gc_study_id_and_participant_id_to_cda_subject_alias[gc_study_id]:
                        gc_subject_alias = gc_study_id_and_participant_id_to_cda_subject_alias[gc_study_id][participant_id]
                        if cda_subject_alias in ctdc_subject_alias_to_gc_subject_alias and ctdc_subject_alias_to_gc_subject_alias[cda_subject_alias] != gc_subject_alias:
                            sys.exit( f"FATAL: One CTDC CDA subject_alias ({cda_subject_alias}) mapped to two GC CDA subject aliases ({ctdc_subject_alias_to_gc_subject_alias[cda_subject_alias]} and {gc_subject_alias}); aborting." )
                        else:
                            ctdc_subject_alias_to_gc_subject_alias[cda_subject_alias] = gc_subject_alias

# Write results.
with open( output_file, 'w' ) as OUT:
    print( *[ 'CTDC_subject_alias', 'CTDC_subject_id', 'GC_subject_alias', 'GC_subject_id' ], sep='\t', file=OUT )

    for ctdc_subject_alias in sorted( ctdc_subject_alias_to_gc_subject_alias ):
        ctdc_subject_id = ctdc_subject_alias_to_subject_id[ctdc_subject_alias]
        gc_subject_alias = ctdc_subject_alias_to_gc_subject_alias[ctdc_subject_alias]
        gc_subject_id = gc_subject_alias_to_subject_id[gc_subject_alias]
        print( *[ ctdc_subject_alias, ctdc_subject_id, gc_subject_alias, gc_subject_id ], sep='\t', file=OUT )


