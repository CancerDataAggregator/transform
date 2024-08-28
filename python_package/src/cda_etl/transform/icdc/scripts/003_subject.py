#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'ICDC'

# Unmodified extracted data, converted directly from JSON to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

case_input_tsv = path.join( tsv_input_root, 'case', 'case.tsv' )

case_enrollment_input_tsv = path.join( tsv_input_root, 'case', 'case.enrollment_id.tsv' )

case_canine_individual_input_tsv = path.join( tsv_input_root, 'canine_individual', 'canine_individual.tsv' )

case_study_input_tsv = path.join( tsv_input_root, 'case', 'case.clinical_study_designation.tsv' )

# Beware compound keys: demographic_id is always null. At time of writing, ( case_id, demographic_id ) tuples are unique within demographic.tsv (irrespective of whether or not demographic_id is null).

demographic_input_tsv = path.join( tsv_input_root, 'demographic', 'demographic.tsv' )

registration_input_tsv = path.join( tsv_input_root, 'registration', 'registration.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample', 'sample.tsv' )

sample_case_input_tsv = path.join( tsv_input_root, 'sample', 'sample.case_id.tsv' )

program_study_input_tsv = path.join( tsv_input_root, 'program', 'program.clinical_study_designation.tsv' )

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

subject_case_merge_log = path.join( aux_subject_output_dir, f"{upstream_data_source}_all_groups_of_case_ids_that_coalesced_into_a_single_CDA_subject_id.tsv" )

aux_value_output_dir = path.join( aux_output_root, 'values' )

subject_demographic_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_case_clashes.year_of_birth.year_of_death.tsv" )

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
    'data_source',
    'data_source_id_field_name',
    'data_source_id_value'
]

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

for output_dir in [ aux_subject_output_dir, aux_value_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Computing subject identifier metadata and loading case<->study associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

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

for project_id in upstream_identifiers['project']:
    
    if upstream_data_source in upstream_identifiers['project'][project_id]:
        
        if 'study.clinical_study_designation' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['study.clinical_study_designation']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} study.clinical_study_designation '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'program.program_acronym' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['program.program_acronym']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program.program_acronym '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project record metadata and inter-project containment.

project = load_tsv_as_dict( project_tsv )

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Load case metadata, create CDA subject records and associate them with CDA project records.

case = load_tsv_as_dict( case_input_tsv )

case_study = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'clinical_study_designation' )

study_program = map_columns_one_to_one( program_study_input_tsv, 'clinical_study_designation', 'program_acronym' )

cda_subject_id = dict()

original_case_id = dict()

cda_subject_in_project = dict()

case_to_canine_individual = map_columns_one_to_one( case_canine_individual_input_tsv, 'case_id', 'canine_individual_id' )

case_to_registration_string = dict()

with open( registration_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for next_line in [ line.rstrip( '\n' ) for line in IN ]:
        
        record = dict( zip( column_names, next_line.split( '\t' ) ) )

        registration_string = f"{record['registration_origin']}.{record['registration_id']}"

        case_id = record['case_id']

        if case_id not in case_to_registration_string:
            
            case_to_registration_string[case_id] = set()

        case_to_registration_string[case_id].add( registration_string )

for case_id in case:
    
    if case_id not in case_study:
        
        sys.exit( f"FATAL: No study.clinical_study_designation associated with case.case_id {case_id}; cannot continue, aborting." )

    for study_id in case_study[case_id]:
        
        # This is meant to break obnoxiously if it generates a key errror.

        program_acronym = study_program[study_id]

        # This too.

        new_cda_subject_id = f"{program_acronym}.{case_id}"

        if case_id in cda_subject_id and cda_subject_id[case_id] != new_cda_subject_id:
            
            sys.exit( f"FATAL: case_id {case_id} unexpectedly assigned to both CDA subject IDs {cda_subject_id[case_id]} and {new_cda_subject_id}; cannot continue, aborting." )

        cda_subject_id[case_id] = new_cda_subject_id

        # Track original case_ids for CDA subjects.

        if new_cda_subject_id not in original_case_id:
            
            original_case_id[new_cda_subject_id] = set()

        original_case_id[new_cda_subject_id].add( case_id )

        if new_cda_subject_id not in cda_subject_in_project:
            
            cda_subject_in_project[new_cda_subject_id] = set()

        cda_subject_in_project[new_cda_subject_id].add( cda_project_id[study_id] )

        for ancestor_project_id in get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] ):
            
            cda_subject_in_project[new_cda_subject_id].add( ancestor_project_id )

        # Record upstream case_id and case_submitter_id values for CDA subjects.

        if 'subject' not in upstream_identifiers:
            
            upstream_identifiers['subject'] = dict()

        if new_cda_subject_id not in upstream_identifiers['subject']:
            
            upstream_identifiers['subject'][new_cda_subject_id] = dict()

        if upstream_data_source not in upstream_identifiers['subject'][new_cda_subject_id]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source] = dict()

        if 'case.case_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.case_id'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.case_id'].add( case_id )

        if case[case_id]['patient_id'] != '':
            
            patient_id = case[case_id]['patient_id']

            if 'case.patient_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
                
                upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.patient_id'] = set()

            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.patient_id'].add( patient_id )

        if case[case_id]['patient_first_name'] != '':
            
            patient_first_name = case[case_id]['patient_first_name']

            if 'case.patient_first_name' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
                
                upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.patient_first_name'] = set()

            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.patient_first_name'].add( patient_first_name )

        if case_id in case_to_canine_individual:
            
            ci_id = case_to_canine_individual[case_id]

            if 'case.canine_individual_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
                
                upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.canine_individual_id'] = set()

            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.canine_individual_id'].add( ci_id )

        if case_id in case_to_registration_string:
            
            for registration_string in case_to_registration_string[case_id]:
                
                if 'registration_string' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
                    
                    upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['registration_string'] = set()

                upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['registration_string'].add( registration_string )

print( 'done.', file=sys.stderr )

# Create CDA subject records.

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} case and study metadata for CDA subject records...", end='', file=sys.stderr )

cda_subject_records = dict()

demographic = load_tsv_as_dict( demographic_input_tsv, id_column_count=2 )

sample = load_tsv_as_dict( sample_input_tsv )

case_to_sample = map_columns_one_to_many( sample_case_input_tsv, 'case_id', 'sample_id' )

# year_of_birth, year_of_death

subject_demographic_data_clashes = dict()

for subject_id in sorted( original_case_id ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = 'Canis lupus familiaris'
    cda_subject_records[subject_id]['year_of_birth'] = ''
    cda_subject_records[subject_id]['year_of_death'] = ''
    cda_subject_records[subject_id]['cause_of_death'] = ''
    cda_subject_records[subject_id]['race'] = ''
    cda_subject_records[subject_id]['ethnicity'] = ''

    for case_id in sorted( original_case_id[subject_id] ):
        
        # This should break with a key error if there's no demographic data for a particular case. At time of writing (August 2024), there is one demographic record for each case.

        for demographic_id in demographic[case_id]:
            
            year_of_birth = re.sub( r'^([0-9]+)-.*$', r'\1', demographic[case_id][demographic_id]['date_of_birth'] )

            cda_field_name = 'year_of_birth'

            original_field_name = 'demographic.date_of_birth'

            if year_of_birth != '':
                
                if cda_subject_records[subject_id][cda_field_name] == '':
                    
                    cda_subject_records[subject_id][cda_field_name] = year_of_birth

                elif cda_subject_records[subject_id][cda_field_name] != year_of_birth:
                    
                    # Set up comparison and clash-tracking data structures.

                    original_value = cda_subject_records[subject_id][cda_field_name]

                    new_value = year_of_birth

                    if subject_id not in subject_demographic_data_clashes:
                        
                        subject_demographic_data_clashes[subject_id] = dict()

                    if original_field_name not in subject_demographic_data_clashes[subject_id]:
                        
                        subject_demographic_data_clashes[subject_id][original_field_name] = dict()

                    if original_value not in subject_demographic_data_clashes[subject_id][original_field_name]:
                        
                        subject_demographic_data_clashes[subject_id][original_field_name][original_value] = dict()

                    # Does the existing value match a pattern we know will be deleted later?

                    if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                        
                        # Is the new value any better?

                        if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                            
                            # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                            subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = False

                        else:
                            
                            # Replace the old value with the new one.

                            cda_subject_records[subject_id][cda_field_name] = new_value

                            subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = True

                    else:
                        
                        # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                        subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = False

        if case_id in case_to_sample:
            
            for sample_id in case_to_sample[case_id]:
                
                if re.search( r'^yes$', sample[sample_id]['necropsy_sample'], re.IGNORECASE ) is not None:
                    
                    year_of_death = re.sub( r'^([0-9]+)-.*$', r'\1', sample[sample_id]['date_of_sample_collection'] )

                    cda_field_name = 'year_of_death'

                    original_field_name = 'sample.date_of_sample_collection'

                    if year_of_death != '':
                        
                        if cda_subject_records[subject_id][cda_field_name] == '':
                            
                            cda_subject_records[subject_id][cda_field_name] = year_of_death

                        elif cda_subject_records[subject_id][cda_field_name] != year_of_death:
                            
                            # Set up comparison and clash-tracking data structures.

                            original_value = cda_subject_records[subject_id][cda_field_name]

                            new_value = year_of_death

                            if subject_id not in subject_demographic_data_clashes:
                                
                                subject_demographic_data_clashes[subject_id] = dict()

                            if original_field_name not in subject_demographic_data_clashes[subject_id]:
                                
                                subject_demographic_data_clashes[subject_id][original_field_name] = dict()

                            if original_value not in subject_demographic_data_clashes[subject_id][original_field_name]:
                                
                                subject_demographic_data_clashes[subject_id][original_field_name][original_value] = dict()

                            # Does the existing value match a pattern we know will be deleted later?

                            if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                                
                                # Is the new value any better?

                                if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                                    
                                    # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                                    subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = False

                                else:
                                    
                                    # Replace the old value with the new one.

                                    cda_subject_records[subject_id][cda_field_name] = new_value

                                    subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = True

                            else:
                                
                                # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                                subject_demographic_data_clashes[subject_id][original_field_name][original_value][new_value] = False

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
#     'data_source',
#     'data_source_id_field_name',
#     'data_source_id_value'
# ]

# upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['case.dbGaP_subject_id'].add( case[case_id]['dbGaP_subject_id'] )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_table in sorted( upstream_identifiers ):
        
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

# Log data clashes between different case records for the same subject.

# subject_demographic_data_clashes[subject_id][field_name][original_value][new_value] = True

with open( subject_demographic_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_demographic_data_clashes ):
        
        for field_name in sorted( subject_demographic_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_demographic_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_demographic_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_demographic_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log each set of case_ids that coalesced into a single CDA subject.

# original_case_id[new_cda_subject_id].add( case_id )

with open( subject_case_merge_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}.case.case_id", 'CDA.subject.id' ], sep='\t', file=OUT )

    for new_cda_id in sorted( original_case_id ):
        
        if len( original_case_id[new_cda_id] ) > 1:
            
            for case_id in sorted( original_case_id[new_cda_id] ):
                
                print( *[ case_id, new_cda_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


