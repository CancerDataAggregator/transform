#!/usr/bin/env python3 -u

import re
import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'PDC'

# Collated version of extracted data: referential integrity cleaned up;
# redundancy (based on multiple extraction routes to partial views of the
# same data) eliminated; cases without containing studies removed;
# groups of multiple files sharing the same FileMetadata.file_location removed.

tsv_input_root = path.join( 'extracted_data', 'pdc_postprocessed' )

case_input_tsv = path.join( tsv_input_root, 'Case', 'Case.tsv' )

case_sample_input_tsv = path.join( tsv_input_root, 'Case', 'Case.sample_id.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'Sample', 'Sample.tsv' )

case_demographic_input_tsv = path.join( tsv_input_root, 'Case', 'Case.demographic_id.tsv' )

demographic_input_tsv = path.join( tsv_input_root, 'Demographic', 'Demographic.tsv' )

case_study_input_tsv = path.join( tsv_input_root, 'Case', 'Case.study_id.tsv' )

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

demographic_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_Demographic_clashes.year_of_birth.year_of_death.cause_of_death.race.ethnicity.tsv" )

sample_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_Sample_clashes.taxon.tsv" )

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

print( f"[{get_current_timestamp()}] Computing subject identifier metadata and loading case<->study_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Get submitter IDs.

case_id_to_case_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'case_submitter_id' )

# Load CDA IDs for studies, projects and programs. Don't use any of the canned loader
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
    
    # Some of these are from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.

    if upstream_data_source in upstream_identifiers['project'][project_id]:
        
        if 'Study.study_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Study.study_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} study_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'Project.project_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Project.project_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} project_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'Program.program_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['Program.program_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project record metadata and inter-project containment.

project = load_tsv_as_dict( project_tsv )

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Any case_submitter_id values matching certain patterns (like /^[0-9]+$/) will not
# be merged across multiple projects (into unified CDA subject records), even
# if identical values appear across multiple projects, because these matches
# have upon examination been deemed spurious.
# 
# The spurious nature of these matches should be checked constantly: it's not
# impossible that some valid matches may turn up in the future, in which case
# we'd have to handle those differently.

submitter_id_patterns_not_to_merge_across_projects = get_submitter_id_patterns_not_to_merge_across_projects()

# Decide which case_ids go with which CDA subjects.

case_study = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'study_id' )

case_submitter_id_in_project = dict()

case_in_project = dict()

# First, load all ancestor projects grouped by case_submitter_id.

for case_id in case_study:
    
    case_submitter_id = case_id_to_case_submitter_id[case_id]

    containing_projects = set()

    for study_id in case_study[case_id]:
        
        if study_id not in cda_project_id:
            
            sys.exit( f"FATAL: Case {case_id} associated with study_id {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

        containing_projects.add( cda_project_id[study_id] )

        # The | here is a set-union operator.

        containing_projects = containing_projects | get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

    if case_id not in case_in_project:
        
        case_in_project[case_id] = set()

    case_in_project[case_id] = case_in_project[case_id] | containing_projects

    if case_submitter_id not in case_submitter_id_in_project:
        
        case_submitter_id_in_project[case_submitter_id] = set()

    case_submitter_id_in_project[case_submitter_id] = case_submitter_id_in_project[case_submitter_id] | containing_projects

# Now, assign CDA IDs to case_ids:
# 
#    {project_submitter_id}.{case_submitter_id}
# 
#    UNLESS
# 
#    case_submitter_id doesn't match (case-insensitive) /^ref$/, /^P?\d+$/, /^\d+$/, /pooled sample/
#    AND
#    case_submitter_id is in multiple projects in the same program,
#    IN WHICH CASE:
# 
#    {program_submitter_id}.{case_submitter_id}

cda_subject_id = dict()

original_case_id = dict()

cda_subject_in_project = dict()

for case_id in case_in_project:
    
    case_submitter_id = case_id_to_case_submitter_id[case_id]

    counts_by_type = dict()

    # print( case_id )

    # Save the last program seen. If it's unique, we may use it to build a CDA subject ID.

    last_program_short_name = ''

    for project_id in case_submitter_id_in_project[case_submitter_id]:
        
        current_type = project[project_id]['type']

        if current_type == 'program':
            
            last_program_short_name = project[project_id]['short_name']

        # print( *[ project_id, current_type ], sep='\t', file=sys.stderr )

        if current_type not in counts_by_type:
            
            counts_by_type[current_type] = 1

        else:
            
            counts_by_type[current_type] = counts_by_type[current_type] + 1

    submitter_id_passed = True

    for submitter_id_pattern in submitter_id_patterns_not_to_merge_across_projects:
        
        if re.search( submitter_id_pattern, case_submitter_id, re.IGNORECASE ) is not None:
            
            submitter_id_passed = False

    if submitter_id_passed and counts_by_type['project'] > 1:
        
        if counts_by_type['program'] > 1:
            
            sys.exit( f"FATAL: Multiple programs detected for case_submitter_id {case_submitter_id}; cannot create well-defined CDA subject ID, aborting. Probably need to add logic to block aggregation in cases like this." )

        else:
            
            # This case_id gets a program-based CDA subject ID.

            new_cda_id = f"{last_program_short_name}.{case_submitter_id}"

            if case_id in cda_subject_id and cda_subject_id[case_id] != new_cda_id:
                
                sys.exit( f"FATAL: case_id {case_id} receieved multiple CDA subject IDs: {cda_subject_id[case_id]} and {new_cda_id}. Cannot continue, aborting." )

            else:
                
                cda_subject_id[case_id] = new_cda_id

    else:
        
        # This case_id gets (the default) a project-based CDA subject ID. Make sure it's in just one project.

        project_count = 0

        last_project_short_name = ''

        for project_id in case_in_project[case_id]:
            
            current_type = project[project_id]['type']

            if current_type == 'project':
                
                project_count = project_count + 1

                last_project_short_name = project[project_id]['short_name']

        if project_count != 1:
            
            sys.exit( f"FATAL: case_id {case_id} not in exactly one project as expected (project count: {project_count}). Last observed was short_name '{last_project_short_name}'; cannot continue, please investigate." )

        else:
            
            new_cda_id = f"{last_project_short_name}.{case_submitter_id}"

            if case_id in cda_subject_id and cda_subject_id[case_id] != new_cda_id:
                
                sys.exit( f"FATAL: case_id {case_id} receieved multiple CDA subject IDs: {cda_subject_id[case_id]} and {new_cda_id}. Cannot continue, aborting." )

            else:
                
                cda_subject_id[case_id] = new_cda_id

    # Save the reverse ID map so we can recover case_ids later for each subject ID.

    if cda_subject_id[case_id] not in original_case_id:
        
        original_case_id[cda_subject_id[case_id]] = set()

    original_case_id[cda_subject_id[case_id]].add( case_id )

    # Record upstream case_id and case_submitter_id values for CDA subjects.

    if 'subject' not in upstream_identifiers:
        
        upstream_identifiers['subject'] = dict()

    if cda_subject_id[case_id] not in upstream_identifiers['subject']:
        
        upstream_identifiers['subject'][cda_subject_id[case_id]] = dict()

    if upstream_data_source not in upstream_identifiers['subject'][cda_subject_id[case_id]]:
        
        upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source] = dict()

    if 'Case.case_id' not in upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]:
        
        upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]['Case.case_id'] = set()

    if 'Case.case_submitter_id' not in upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]:
        
        upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]['Case.case_submitter_id'] = set()

    upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]['Case.case_id'].add( case_id )

    upstream_identifiers['subject'][cda_subject_id[case_id]][upstream_data_source]['Case.case_submitter_id'].add( case_submitter_id )

    # Populate the CDA subject_in_project relationship.

    for project_id in case_in_project[case_id]:
        
        # cda_subject_in_project

        if cda_subject_id[case_id] not in cda_subject_in_project:
            
            cda_subject_in_project[cda_subject_id[case_id]] = set()

        cda_subject_in_project[cda_subject_id[case_id]].add( project_id )

print( 'done.', file=sys.stderr )

# Create CDA subject records.

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} Demographic and Sample metadata for CDA subject records...", end='', file=sys.stderr )

case_has_demographic = map_columns_one_to_many( case_demographic_input_tsv, 'case_id', 'demographic_id' )

demographic = load_tsv_as_dict( demographic_input_tsv )

case_has_sample = map_columns_one_to_many( case_sample_input_tsv, 'case_id', 'sample_id' )

sample = load_tsv_as_dict( sample_input_tsv )

cda_subject_records = dict()

subject_demographic_data_clashes = dict()

subject_sample_data_clashes = dict()

for subject_id in sorted( original_case_id ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = ''
    cda_subject_records[subject_id]['year_of_birth'] = ''
    cda_subject_records[subject_id]['year_of_death'] = ''
    cda_subject_records[subject_id]['cause_of_death'] = ''
    cda_subject_records[subject_id]['race'] = ''
    cda_subject_records[subject_id]['ethnicity'] = ''

    for case_id in sorted( original_case_id[subject_id] ):
        
        if case_id in case_has_demographic:
            
            for demographic_id in sorted( case_has_demographic[case_id] ):
                
                for field_name in [ 'year_of_birth', 'year_of_death', 'cause_of_death', 'race', 'ethnicity' ]:
                    
                    if demographic[demographic_id][field_name] != '':
                        
                        if cda_subject_records[subject_id][field_name] == '':
                            
                            cda_subject_records[subject_id][field_name] = demographic[demographic_id][field_name]

                        elif cda_subject_records[subject_id][field_name] != demographic[demographic_id][field_name]:
                            
                            # Set up comparison and clash-tracking data structures.

                            original_value = cda_subject_records[subject_id][field_name]

                            new_value = demographic[demographic_id][field_name]

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

        if case_id in case_has_sample:
            
            for sample_id in sorted( case_has_sample[case_id] ):
                
                if sample[sample_id]['taxon'] != '':
                    
                    if cda_subject_records[subject_id]['species'] == '':
                        
                        cda_subject_records[subject_id]['species'] = sample[sample_id]['taxon']

                    elif cda_subject_records[subject_id]['species'] != sample[sample_id]['taxon']:
                        
                        # Set up comparison and clash-tracking data structures.

                        original_value = cda_subject_records[subject_id]['species']

                        new_value = sample[sample_id]['taxon']

                        if subject_id not in subject_sample_data_clashes:
                            
                            subject_sample_data_clashes[subject_id] = dict()

                        if 'taxon' not in subject_sample_data_clashes[subject_id]:
                            
                            subject_sample_data_clashes[subject_id]['taxon'] = dict()

                        if original_value not in subject_sample_data_clashes[subject_id]['taxon']:
                            
                            subject_sample_data_clashes[subject_id]['taxon'][original_value] = dict()

                        # Does the existing value match a pattern we know will be deleted later?

                        if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                            
                            # Is the new value any better?

                            if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                                
                                # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                                subject_sample_data_clashes[subject_id]['taxon'][original_value][new_value] = False

                            else:
                                
                                # Replace the old value with the new one.

                                cda_subject_records[subject_id]['species'] = new_value

                                subject_sample_data_clashes[subject_id]['taxon'][original_value][new_value] = True

                        else:
                            
                            # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                            subject_sample_data_clashes[subject_id]['taxon'][original_value][new_value] = False

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

# upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['Case.case_submitter_id'].add( case_submitter_id )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_table in sorted( upstream_identifiers ):
        
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

# Log data clashes between different Demographic records for the same Case.

with open( demographic_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_Demographic_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_demographic_data_clashes ):
        
        for field_name in sorted( subject_demographic_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_demographic_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_demographic_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_demographic_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log data clashes between different Sample records from the same Case.

with open( sample_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_Sample_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_sample_data_clashes ):
        
        for field_name in sorted( subject_sample_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_sample_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_sample_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_sample_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log each set of case_ids that coalesced into a single CDA subject.

with open( subject_case_merge_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}.Case.case_id", f"{upstream_data_source}.Case.case_submitter_id", 'CDA.subject.id' ], sep='\t', file=OUT )

    for new_cda_id in sorted( original_case_id ):
        
        if len( original_case_id[new_cda_id] ) > 1:
            
            for case_id in sorted( original_case_id[new_cda_id] ):
                
                print( *[ case_id, case_id_to_case_submitter_id[case_id], new_cda_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


