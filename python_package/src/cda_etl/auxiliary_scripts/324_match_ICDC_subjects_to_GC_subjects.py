#!/usr/bin/env python3 -u

import sys

from cda_etl.lib import map_columns_one_to_many, map_columns_one_to_one

from os import path

# PARAMETERS

cda_root = 'cda_tsvs'

gc_cda_dir = path.join( cda_root, 'gc_002_decorated_harmonized' )

gc_subject_tsv = path.join( gc_cda_dir, 'subject.tsv' )

gc_upstream_identifiers_tsv = path.join( gc_cda_dir, 'upstream_identifiers.tsv' )

icdc_cda_dir = path.join( cda_root, 'icdc_002_decorated_harmonized' )

icdc_subject_tsv = path.join( icdc_cda_dir, 'subject.tsv' )

icdc_upstream_identifiers_tsv = path.join( icdc_cda_dir, 'upstream_identifiers.tsv' )

aux_root = 'auxiliary_metadata'

gc_dir = path.join( aux_root, '__GC_supplemental_metadata' )

gc_entity_project_map = path.join( gc_dir, 'GC_entities_by_program_and_study.tsv' )

icdc_dir = path.join( aux_root, '__ICDC_supplemental_metadata' )

icdc_entity_project_map = path.join( icdc_dir, 'ICDC_entities_by_program_and_study.tsv' )

agg_root = path.join( aux_root, '__aggregation_logs' )

agg_project_dir = path.join( agg_root, 'projects' )

project_map_tsv = path.join( agg_project_dir, 'naive_ICDC_GC_project_id_map.hand_edited_to_remove_false_positives.tsv' )

agg_subject_dir = path.join( agg_root, 'subjects' )

output_file = path.join( agg_subject_dir, 'ICDC_CDA_subjects_linked_to_GC_CDA_subjects.tsv' )

# EXECUTION

# Load connecting information between GC subject identifiers and studies.

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

# Load connecting information between ICDC subject identifiers and studies.

# cda_table	id_alias	upstream_source	upstream_field	upstream_id
# subject	94256	ICDC	case.case_id	GLIOMA01-i_03A6

icdc_case_id_to_cda_subject_alias = map_columns_one_to_one( icdc_upstream_identifiers_tsv, 'upstream_id', 'id_alias', where_field='upstream_field', where_value='case.case_id' )

icdc_subject_alias_to_subject_id = map_columns_one_to_one( icdc_subject_tsv, 'id_alias', 'id' )

# Load hand-verified links between GC studies and ICDC studies.

icdc_study_id_to_gc_study_id = map_columns_one_to_many( project_map_tsv, 'ICDC_study_clinical_study_designation', 'GC_study_uuid' )

# Save ID information for all records merged due to ( same case_submitter_id ) + ( allowed equivalence between containing GC study and containing ICDC study ).

icdc_subject_alias_to_gc_subject_alias = dict()

with open( icdc_entity_project_map ) as IN:
    
    # program.program_name	program.program_acronym	study.clinical_study_designation	study.clinical_study_id	study.accession_id	study.clinical_study_name	entity_id	entity_type

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for record in [ dict( zip( column_names, line.rstrip( '\n' ).split( '\t' ) ) ) for line in IN ]:
        
        if record['entity_type'] == 'case':
            
            case_id = record['entity_id']

            icdc_study_id = record['study.clinical_study_designation']

            cda_subject_alias = icdc_case_id_to_cda_subject_alias[case_id]

            if icdc_study_id in icdc_study_id_to_gc_study_id:
                
                for gc_study_id in icdc_study_id_to_gc_study_id[icdc_study_id]:
                    
                    if gc_study_id in gc_study_id_and_participant_id_to_cda_subject_alias and case_id in gc_study_id_and_participant_id_to_cda_subject_alias[gc_study_id]:
                        
                        gc_subject_alias = gc_study_id_and_participant_id_to_cda_subject_alias[gc_study_id][case_id]

                        if cda_subject_alias in icdc_subject_alias_to_gc_subject_alias and icdc_subject_alias_to_gc_subject_alias[cda_subject_alias] != gc_subject_alias:
                            
                            sys.exit( f"FATAL: One ICDC CDA subject_alias ({cda_subject_alias}) mapped to two GC CDA subject aliases ({icdc_subject_alias_to_gc_subject_alias[cda_subject_alias]} and {gc_subject_alias}); aborting." )

                        else:
                            
                            icdc_subject_alias_to_gc_subject_alias[cda_subject_alias] = gc_subject_alias

# Write results.

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'ICDC_subject_alias', 'ICDC_subject_id', 'GC_subject_alias', 'GC_subject_id' ], sep='\t', file=OUT )

    for icdc_subject_alias in sorted( icdc_subject_alias_to_gc_subject_alias ):
        
        icdc_subject_id = icdc_subject_alias_to_subject_id[icdc_subject_alias]

        gc_subject_alias = icdc_subject_alias_to_gc_subject_alias[icdc_subject_alias]

        gc_subject_id = gc_subject_alias_to_subject_id[gc_subject_alias]

        print( *[ icdc_subject_alias, icdc_subject_id, gc_subject_alias, gc_subject_id ], sep='\t', file=OUT )


