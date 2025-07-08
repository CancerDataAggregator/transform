#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'CDS'

# Unmodified extracted data, converted directly from a Neo4j JSONL dump.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

participant_input_tsv = path.join( tsv_input_root, 'participant.tsv' )

participant_study_input_tsv = path.join( tsv_input_root, 'participant_in_study.tsv' )

study_input_tsv = path.join( tsv_input_root, 'study.tsv' )

study_program_input_tsv = path.join( tsv_input_root, 'study_in_program.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

project_tsv = path.join( tsv_output_root, 'project.tsv' )

project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

subject_output_tsv = path.join( tsv_output_root, 'subject.tsv' )

subject_in_project_output_tsv = path.join( tsv_output_root, 'subject_in_project.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_subject_output_dir = path.join( aux_output_root, 'subjects' )

subject_participant_merge_log = path.join( aux_subject_output_dir, f"{upstream_data_source}_all_groups_of_participant_ids_that_coalesced_into_a_single_CDA_subject_id.tsv" )

aux_value_output_dir = path.join( aux_output_root, 'values' )

subject_demographic_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_participant_clashes.race.ethnicity.tsv" )

participant_species_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_participant_study_clashes.organism_species.tsv" )

subject_species_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_participant_study_clashes.organism_species.tsv" )

# Table header sequences.

cda_subject_fields = [
    
    'id',
    'crdc_id',
    'species',
    'year_of_birth',
    'year_of_death',
    'cause_of_death',
    'race',
    'ethnicity'
]

upstream_identifiers_fields = [
    
    'cda_table',
    'id',
    'upstream_source',
    'upstream_field',
    'upstream_id'
]

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

for output_dir in [ aux_subject_output_dir, aux_value_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Computing subject identifier metadata and loading participant<->study associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Get submitter IDs.

# Load CDA IDs for studies and programs. Don't use any of the canned loader
# functions to load this data structure; it's got multiplicities that are
# generally mishandled if certain keys are assumed to be unique (e.g. a CDA
# subject can have multiple case_id values from the same data source, rendering any attempt
# to key this information on the first X columns incorrect or so cumbersome as to
# be pointless).

upstream_identifiers = dict()

with open( upstream_identifiers_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ cda_table, entity_id, data_source, source_field, value ] = line.split( '\t' )

        if cda_table not in upstream_identifiers:
            
            upstream_identifiers[cda_table] = dict()

        if entity_id not in upstream_identifiers[cda_table]:
            
            upstream_identifiers[cda_table][entity_id] = dict()

        if data_source not in upstream_identifiers[cda_table][entity_id]:
            
            upstream_identifiers[cda_table][entity_id][data_source] = dict()

        if source_field not in upstream_identifiers[cda_table][entity_id][data_source]:
            
            upstream_identifiers[cda_table][entity_id][data_source][source_field] = set()

        upstream_identifiers[cda_table][entity_id][data_source][source_field].add( value )

cda_project_id = dict()

program_acronym = dict()

for project_id in upstream_identifiers['project']:
    
    # Some of these are from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.

    if upstream_data_source in upstream_identifiers['project'][project_id]:
        
        if 'study.uuid' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['study.uuid']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} study.uuid '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'program.uuid' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['program.uuid']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program.uuid '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

            # Load program_acronym too.

            for value in upstream_identifiers['project'][project_id][upstream_data_source]['program.program_acronym']:
                
                if project_id not in program_acronym:
                    
                    program_acronym[project_id] = value

                elif program_acronym[project_id] != value:
                    
                    sys.exit( f"FATAL: {upstream_data_source} {project_id} unexpectedly associated with both program_acronym values {program_acronym[project_id]} and {value}; cannot continue, aborting." )

# Load CDA project record metadata and inter-project containment.

project = load_tsv_as_dict( project_tsv )

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Load participant metadata, create CDA subject records and associate them with CDA project records.

participant = load_tsv_as_dict( participant_input_tsv )

participant_study = map_columns_one_to_many( participant_study_input_tsv, 'participant_uuid', 'study_uuid' )

study_program = map_columns_one_to_one( study_program_input_tsv, 'study_uuid', 'program_uuid' )

cda_subject_id = dict()

original_participant_uuid = dict()

cda_subject_in_project = dict()

study_organism_species = map_columns_one_to_one( study_input_tsv, 'uuid', 'organism_species' )

participant_species = dict()

participant_species_data_clashes = dict()

for participant_uuid in participant:
    
    if participant_uuid not in participant_study:
        
        sys.exit( f"FATAL: No study.uuid associated with participant.uuid {participant_uuid}; cannot continue, aborting." )

    for study_uuid in participant_study[participant_uuid]:
        
        # Sometimes studies aren't in programs, as of March 2025. Default to using the study ID as the max aggregation space; update if a parent program is detected.

        new_cda_subject_id = f"STUDY_{study_uuid}.{participant[participant_uuid]['participant_id']}"

        if study_uuid in study_program:
            
            program_uuid = study_program[study_uuid]

            # It is assumed (safely - March 2025) that all programs have non-null program_acronym values. This is meant to break with a KeyError if that assumption is ever violated.

            new_cda_subject_id = f"{program_acronym[cda_project_id[program_uuid]]}.{participant[participant_uuid]['participant_id']}"

        if participant_uuid in cda_subject_id and cda_subject_id[participant_uuid] != new_cda_subject_id:
            
            sys.exit( f"FATAL: participant_uuid {participant_uuid} unexpectedly assigned to both CDA subject IDs {cda_subject_id[participant_uuid]} and {new_cda_subject_id}; cannot continue, aborting." )

        cda_subject_id[participant_uuid] = new_cda_subject_id

        # Sometimes this is an empty string. We'll handle that downstream when we create CDA subject records.

        current_species = study_organism_species[study_uuid]

        if participant_uuid not in participant_species or participant_species[participant_uuid] == '':
            
            participant_species[participant_uuid] = current_species

        elif participant_species[participant_uuid] != current_species and current_species != '':
            
            # Set up comparison and clash-tracking data structures.

            original_value = participant_species[participant_uuid]

            new_value = current_species

            if participant_uuid not in participant_species_data_clashes:
                
                participant_species_data_clashes[participant_uuid] = dict()

            if original_value not in participant_species_data_clashes[participant_uuid]:
                
                participant_species_data_clashes[participant_uuid][original_value] = dict()

            # Does the existing value match a pattern we know will be deleted later?

            if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                
                # Is the new value any better?

                if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                    
                    # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                    participant_species_data_clashes[participant_uuid][original_value][new_value] = False

                else:
                    
                    # Replace the old value with the new one.

                    participant_species[participant_uuid] = new_value

                    participant_species_data_clashes[participant_uuid][original_value][new_value] = True

            else:
                
                # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                participant_species_data_clashes[participant_uuid][original_value][new_value] = False

        # Track original participant_uuids for CDA subjects.

        if new_cda_subject_id not in original_participant_uuid:
            
            original_participant_uuid[new_cda_subject_id] = set()

        original_participant_uuid[new_cda_subject_id].add( participant_uuid )

        if new_cda_subject_id not in cda_subject_in_project:
            
            cda_subject_in_project[new_cda_subject_id] = set()

        cda_subject_in_project[new_cda_subject_id].add( cda_project_id[study_uuid] )

        for ancestor_project_id in get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_uuid] ):
            
            cda_subject_in_project[new_cda_subject_id].add( ancestor_project_id )

        # Record upstream case_id and case_submitter_id values for CDA subjects.

        if 'subject' not in upstream_identifiers:
            
            upstream_identifiers['subject'] = dict()

        if new_cda_subject_id not in upstream_identifiers['subject']:
            
            upstream_identifiers['subject'][new_cda_subject_id] = dict()

        if upstream_data_source not in upstream_identifiers['subject'][new_cda_subject_id]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source] = dict()

        if 'participant.participant_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.participant_id'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.participant_id'].add( participant[participant_uuid]['participant_id'] )

        if 'participant.uuid' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.uuid'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.uuid'].add( participant_uuid )

        if participant[participant_uuid]['dbGaP_subject_id'] != '':
            
            if 'participant.dbGaP_subject_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
                
                upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.dbGaP_subject_id'] = set()

            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.dbGaP_subject_id'].add( participant[participant_uuid]['dbGaP_subject_id'] )

print( 'done.', file=sys.stderr )

# Create CDA subject records.

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} participant and study metadata for CDA subject records...", end='', file=sys.stderr )

cda_subject_records = dict()

subject_demographic_data_clashes = dict()

subject_species_data_clashes = dict()

for subject_id in sorted( original_participant_uuid ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = ''
    cda_subject_records[subject_id]['year_of_birth'] = ''
    cda_subject_records[subject_id]['year_of_death'] = ''
    cda_subject_records[subject_id]['cause_of_death'] = ''
    cda_subject_records[subject_id]['race'] = ''
    cda_subject_records[subject_id]['ethnicity'] = ''

    for participant_uuid in sorted( original_participant_uuid[subject_id] ):
        
        for field_name in [ 'race', 'ethnicity' ]:
            
            if participant[participant_uuid][field_name] != '':
                
                if cda_subject_records[subject_id][field_name] == '':
                    
                    cda_subject_records[subject_id][field_name] = participant[participant_uuid][field_name]

                elif cda_subject_records[subject_id][field_name] != participant[participant_uuid][field_name]:
                    
                    # Set up comparison and clash-tracking data structures.

                    original_value = cda_subject_records[subject_id][field_name]

                    new_value = participant[participant_uuid][field_name]

                    if subject_id not in subject_demographic_data_clashes:
                        
                        subject_demographic_data_clashes[subject_id] = dict()

                    if field_name not in subject_demographic_data_clashes[subject_id]:
                        
                        subject_demographic_data_clashes[subject_id][field_name] = dict()

                    if original_value not in subject_demographic_data_clashes[subject_id][field_name]:
                        
                        subject_demographic_data_clashes[subject_id][field_name][original_value] = dict()

                    # Does the existing value match a pattern we know will be deleted later?

                    if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                        
                        # Is the new value any better?

                        if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                            
                            # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                            subject_demographic_data_clashes[subject_id][field_name][original_value][new_value] = False

                        else:
                            
                            # Replace the old value with the new one.

                            cda_subject_records[subject_id][field_name] = new_value

                            subject_demographic_data_clashes[subject_id][field_name][original_value][new_value] = True

                    else:
                        
                        # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                        subject_demographic_data_clashes[subject_id][field_name][original_value][new_value] = False

        # This should break with a key error if it's not present.

        current_species = participant_species[participant_uuid]

        if current_species != '':
            
            if cda_subject_records[subject_id]['species'] == '':
                
                cda_subject_records[subject_id]['species'] = current_species

            elif cda_subject_records[subject_id]['species'] != current_species:
                
                # Set up comparison and clash-tracking data structures.

                original_value = cda_subject_records[subject_id]['species']

                new_value = current_species

                if subject_id not in subject_species_data_clashes:
                    
                    subject_species_data_clashes[subject_id] = dict()

                if original_value not in subject_species_data_clashes[subject_id]:
                    
                    subject_species_data_clashes[subject_id][original_value] = dict()

                # Does the existing value match a pattern we know will be deleted later?

                if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                    
                    # Is the new value any better?

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                        subject_species_data_clashes[subject_id][original_value][new_value] = False

                    else:
                        
                        # Replace the old value with the new one.

                        cda_subject_records[subject_id]['species'] = new_value

                        subject_species_data_clashes[subject_id][original_value][new_value] = True

                else:
                    
                    # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                    subject_species_data_clashes[subject_id][original_value][new_value] = False

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA subject records.

with open( subject_output_tsv, 'w' ) as OUT:
    
    print( *cda_subject_fields, sep='\t', file=OUT )

    for subject_id in sorted( cda_subject_records ):
        
        output_row = list()

        subject_record = cda_subject_records[subject_id]

        for cda_subject_field in cda_subject_fields:
            
            output_row.append( subject_record[cda_subject_field] )

        print( *output_row, sep='\t', file=OUT )

# Write the subject<->project association.

with open( subject_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'subject_id', 'project_id' ], sep='\t', file=OUT )

    for subject_id in sorted( cda_subject_in_project ):
        
        for project_id in sorted( cda_subject_in_project[subject_id] ):
            
            print( *[ subject_id, project_id ], sep='\t', file=OUT )

# Update upstream_identifiers.

# upstream_identifiers_fields = [
#     
#     'cda_table',
#     'id',
#     'upstream_source',
#     'upstream_field',
#     'upstream_id'
# ]

# upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['participant.dbGaP_subject_id'].add( participant[participant_uuid]['dbGaP_subject_id'] )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_table in sorted( upstream_identifiers ):
        
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

# Log data clashes between different participant records for the same subject.

# subject_demographic_data_clashes[subject_id][field_name][original_value][new_value] = True

with open( subject_demographic_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_participant_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_demographic_data_clashes ):
        
        for field_name in sorted( subject_demographic_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_demographic_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_demographic_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_demographic_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log data clashes within the same participant across multiple studies with respect to the study.organism_species field.

# participant_species_data_clashes[participant_uuid][original_species][current_species] = True

with open( participant_species_data_clash_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}_participant_uuid", f"{upstream_data_source}_study_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for participant_uuid in sorted( participant_species_data_clashes ):
        
        field_name = 'organism_species'

        for original_value in sorted( participant_species_data_clashes[participant_uuid] ):
            
            for clashing_value in sorted( participant_species_data_clashes[participant_uuid][original_value] ):
                
                if participant_species_data_clashes[participant_uuid][original_value][clashing_value] == False:
                    
                    print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                else:
                    
                    print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log data clashes within the same CDA subject across multiple participants with respect to the 'species' field.

# subject_species_data_clashes[subject_id][original_value][new_value] = True

with open( subject_species_data_clash_log, 'w' ) as OUT:
    
    print( *[ f"CDA_subject_id", f"{upstream_data_source}_study_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_species_data_clashes ):
        
        field_name = 'organism_species'

        for original_value in sorted( subject_species_data_clashes[subject_id] ):
            
            for clashing_value in sorted( subject_species_data_clashes[subject_id][original_value] ):
                
                if subject_species_data_clashes[subject_id][original_value][clashing_value] == False:
                    
                    print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                else:
                    
                    print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log each set of participant_uuids that coalesced into a single CDA subject.

# original_participant_uuid[new_cda_subject_id].add( participant_uuid )

with open( subject_participant_merge_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}.participant.uuid", f"{upstream_data_source}.participant.participant_id", 'CDA.subject.id' ], sep='\t', file=OUT )

    for new_cda_id in sorted( original_participant_uuid ):
        
        if len( original_participant_uuid[new_cda_id] ) > 1:
            
            for participant_uuid in sorted( original_participant_uuid[new_cda_id] ):
                
                print( *[ participant_uuid, participant[participant_uuid]['participant_id'], new_cda_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


