#!/usr/bin/env python3 -u

import re
import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'GDC'

# Extracted TSVs.

tsv_input_root = path.join( 'extracted_data', 'gdc', 'all_TSV_output' )

file_input_tsv = path.join( tsv_input_root, 'file.tsv' )

project_in_program_input_tsv = path.join( tsv_input_root, 'project_in_program.tsv' )

program_input_tsv = path.join( tsv_input_root, 'program.tsv' )

case_input_tsv = path.join( tsv_input_root, 'case.tsv' )

case_in_project_input_tsv = path.join( tsv_input_root, 'case_in_project.tsv' )

file_in_case_input_tsv = path.join( tsv_input_root, 'file_in_case.tsv' )

file_has_index_file_input_tsv = path.join( tsv_input_root, 'file_has_index_file.tsv' )

demographic_of_case_input_tsv = path.join( tsv_input_root, 'demographic_of_case.tsv' )

demographic_input_tsv = path.join( tsv_input_root, 'demographic.tsv' )

aliquot_of_analyte_input_tsv = path.join( tsv_input_root, 'aliquot_of_analyte.tsv' )

analyte_from_portion_input_tsv = path.join( tsv_input_root, 'analyte_from_portion.tsv' )

portion_from_sample_input_tsv = path.join( tsv_input_root, 'portion_from_sample.tsv' )

slide_from_portion_input_tsv = path.join( tsv_input_root, 'slide_from_portion.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample.tsv' )

file_associated_with_entity_input_tsv = path.join( tsv_input_root, 'file_associated_with_entity.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

file_output_tsv = path.join( tsv_output_root, 'file.tsv' )

file_anatomic_site_output_tsv = path.join( tsv_output_root, 'file_anatomic_site.tsv' )

file_tumor_vs_normal_output_tsv = path.join( tsv_output_root, 'file_tumor_vs_normal.tsv' )

file_describes_subject_output_tsv = path.join( tsv_output_root, 'file_describes_subject.tsv' )

file_in_project_output_tsv = path.join( tsv_output_root, 'file_in_project.tsv' )

subject_output_tsv = path.join( tsv_output_root, 'subject.tsv' )

subject_in_project_output_tsv = path.join( tsv_output_root, 'subject_in_project.tsv' )

# ETL metadata.

aux_output_root = path.join( 'auxiliary_metadata', '__aggregation_logs' )

aux_subject_output_dir = path.join( aux_output_root, 'subjects' )

subject_case_merge_log = path.join( aux_subject_output_dir, f"{upstream_data_source}_all_groups_of_case_ids_that_coalesced_into_a_single_CDA_subject_id.tsv" )

aux_value_output_dir = path.join( aux_output_root, 'values' )

demographic_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_demographic_clashes.year_of_birth.year_of_death.cause_of_death.race.ethnicity.tsv" )

# Table header sequences.

cda_file_fields = [
    
    'id',
    'crdc_id',
    'name',
    'description',
    'drs_uri',
    'access',
    'size',
    'format',
    'type',
    'category'
]

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

debug = False

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found in search metadata, to guide value
# replacement decisions in the event of clashes.

delete_everywhere = get_universal_value_deletion_patterns()

# EXECUTION

for output_dir in [ tsv_output_root, aux_subject_output_dir, aux_value_output_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

print( f"[{get_current_timestamp()}] Loading file metadata...", end='', file=sys.stderr )

# Load metadata for files.

cda_file_records = dict()

file = load_tsv_as_dict( file_input_tsv )

for file_id in file:
    
    cda_file_records[file_id] = dict()

    cda_file_records[file_id]['id'] = file_id
    cda_file_records[file_id]['crdc_id'] = ''
    cda_file_records[file_id]['name'] = file[file_id]['file_name']
    cda_file_records[file_id]['description'] = ''
    cda_file_records[file_id]['drs_uri'] = f"drs://dg.4dfc:{file_id}"
    cda_file_records[file_id]['access'] = file[file_id]['access']
    cda_file_records[file_id]['size'] = file[file_id]['file_size']
    cda_file_records[file_id]['format'] = file[file_id]['data_format']
    cda_file_records[file_id]['type'] = file[file_id]['data_type']
    cda_file_records[file_id]['category'] = file[file_id]['data_category']

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading case and project metadata...", end='', file=sys.stderr )

# Load project-in-program relationships.

project_in_program = map_columns_one_to_one( project_in_program_input_tsv, 'project_id', 'program_id' )

# Load program names.

program_name = map_columns_one_to_one( program_input_tsv, 'program_id', 'name' )

# Load case submitter IDs.

case_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'submitter_id' )

# Load case<->project relationships.

case_in_project = map_columns_one_to_many( case_in_project_input_tsv, 'case_id', 'project_id' )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Computing CDA subject IDs and logging case-level merges...", end='', file=sys.stderr )

# Compute CDA subject IDs for cases.

cda_subject_id = dict()

# Store a reverse map to document merges.

original_case_id = dict()

for case_id in case_in_project:
    
    for project_id in case_in_project[case_id]:
        
        new_cda_id = f"{program_name[project_in_program[project_id]]}.{case_submitter_id[case_id]}"

        if case_id in cda_subject_id:
            
            if cda_subject_id[case_id] != new_cda_id:
                
                sys.exit( f"FATAL: {upstream_data_source} case_id '{case_id}' mapped to two distinct CDA subject IDs: '{cda_subject_id[case_id]}' and '{new_cda_id}'. Assumptions violated; cannot resolve, aborting." )

        else:
            
            cda_subject_id[case_id] = new_cda_id

            if new_cda_id not in original_case_id:
                
                original_case_id[new_cda_id] = set()

            original_case_id[new_cda_id].add( case_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file<->case associations, CDA project IDs and crossrefs, CDA project hierarchy, and index_file containment...", end='', file=sys.stderr )

# Load file<->case relationships.

file_in_case = map_columns_one_to_many( file_in_case_input_tsv, 'file_id', 'case_id' )

# Load CDA IDs for projects and programs. Don't use any of the canned loader
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
        
        if 'project.project_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['project.project_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} project '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'program.program_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['program.program_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} program '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Load sets of index-file IDs for any files that have them.

file_has_index_file = map_columns_one_to_many( file_has_index_file_input_tsv, 'file_id', 'index_file_id' )

# Compute CDA file<->subject, subject<->project and file<->project relationships.

cda_file_describes_subject = dict()

cda_subject_in_project = dict()

cda_file_in_project = dict()

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Associating files with subjects, files with projects and subjects with projects...", end='', file=sys.stderr )

# Make sure we don't miss file-less cases.

covered_case_ids = set()

for file_id in file_in_case:
    
    for case_id in file_in_case[file_id]:
        
        covered_case_ids.add( case_id )

        # file<->subject

        if file_id not in cda_file_describes_subject:
            
            cda_file_describes_subject[file_id] = set()

        cda_file_describes_subject[file_id].add( cda_subject_id[case_id] )

        if file_id in file_has_index_file:
            
            for index_file_id in file_has_index_file[file_id]:
                
                if index_file_id not in cda_file_in_project:
                    
                    cda_file_describes_subject[index_file_id] = set()

                cda_file_describes_subject[index_file_id].add( cda_subject_id[case_id] )

        # This should break if we get a case unassigned to any project.

        for project_id in case_in_project[case_id]:
            
            # subject<->project

            if cda_subject_id[case_id] not in cda_subject_in_project:
                
                cda_subject_in_project[cda_subject_id[case_id]] = set()

            cda_subject_in_project[cda_subject_id[case_id]].add( cda_project_id[project_id] )

            # file<->project

            if file_id not in cda_file_in_project:
                
                cda_file_in_project[file_id] = set()

            cda_file_in_project[file_id].add( cda_project_id[project_id] )

            if file_id in file_has_index_file:
                
                for index_file_id in file_has_index_file[file_id]:
                    
                    if index_file_id not in cda_file_in_project:
                        
                        cda_file_in_project[index_file_id] = set()

                    cda_file_in_project[index_file_id].add( cda_project_id[project_id] )

            # update subject<->project and file<->project with data for (transitively) containing projects (programs, dbGaP studies, etc.) as well.

            # Recurse to get ancestor set.

            containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[project_id] )

            for ancestor_project_id in containing_projects:
                
                cda_subject_in_project[cda_subject_id[case_id]].add( ancestor_project_id )

                cda_file_in_project[file_id].add( ancestor_project_id )

                if file_id in file_has_index_file:
                    
                    for index_file_id in file_has_index_file[file_id]:
                        
                        if index_file_id not in cda_file_in_project:
                            
                            cda_file_in_project[index_file_id] = set()

                        cda_file_in_project[index_file_id].add( cda_project_id[project_id] )

# Make sure we don't miss file-less cases.

for case_id in case_in_project:
    
    if case_id not in covered_case_ids:
        
        for project_id in case_in_project[case_id]:
            
            # subject<->project

            if cda_subject_id[case_id] not in cda_subject_in_project:
                
                cda_subject_in_project[cda_subject_id[case_id]] = set()

            cda_subject_in_project[cda_subject_id[case_id]].add( cda_project_id[project_id] )

            # update subject<->project with data for (transitively) containing projects (programs, dbGaP studies, etc.) as well.

            # Recurse to get ancestor set.

            containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[project_id] )

            for ancestor_project_id in containing_projects:
                
                cda_subject_in_project[cda_subject_id[case_id]].add( ancestor_project_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading ID provenance for {upstream_data_source} cases / CDA subjects...", end='', file=sys.stderr )

# We've covered file_describes_subject and subject_in_project, but we haven't
# touched any cases not in projects (this is only imaginable as an error and
# does not currently occur, but why assume it never will?), and we want
# to make sure we cache identifier information for all cases. Thus:

case = load_tsv_as_dict( case_input_tsv )

# upstream_identifiers[cda_table][entity_id][data_source][source_field].add( value )

for upstream_case_id in case:
    
    if upstream_case_id not in cda_subject_id:
        
        cda_subject_id[upstream_case_id] = f"NO_PROGRAM_OR_PROJECT.{case_submitter_id[upstream_case_id]}"

        print( f"   CASE WITH NO PROJECT: {cda_subject_id[upstream_case_id]} == {upstream_case_id}", file=sys.stderr )

    if cda_subject_id[upstream_case_id] not in original_case_id:
        
        original_case_id[cda_subject_id[upstream_case_id]] = set()

    original_case_id[cda_subject_id[upstream_case_id]].add( upstream_case_id )

    new_cda_id = cda_subject_id[upstream_case_id]

    upstream_submitter_id = case_submitter_id[upstream_case_id]

    cda_table = 'subject'

    if cda_table not in upstream_identifiers:
        
        upstream_identifiers[cda_table] = dict()

    if new_cda_id not in upstream_identifiers[cda_table]:
        
        upstream_identifiers[cda_table][new_cda_id] = dict()

    if upstream_data_source not in upstream_identifiers[cda_table][new_cda_id]:
        
        upstream_identifiers[cda_table][new_cda_id][upstream_data_source] = dict()

    if 'case.case_id' not in upstream_identifiers[cda_table][new_cda_id][upstream_data_source]:
        
        upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['case.case_id'] = set()

    upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['case.case_id'].add( upstream_case_id )

    if 'case.submitter_id' not in upstream_identifiers[cda_table][new_cda_id][upstream_data_source]:
        
        upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['case.submitter_id'] = set()

    upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['case.submitter_id'].add( upstream_submitter_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} demographic metadata for CDA subject records...", end='', file=sys.stderr )

case_has_demographic = map_columns_one_to_many( demographic_of_case_input_tsv, 'case_id', 'demographic_id' )

demographic = load_tsv_as_dict( demographic_input_tsv )

cda_subject_records = dict()

subject_data_clashes = dict()

for subject_id in sorted( original_case_id ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = 'Homo sapiens'
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

                            if subject_id not in subject_data_clashes:
                                
                                subject_data_clashes[subject_id] = dict()

                            if field_name not in subject_data_clashes[subject_id]:
                                
                                subject_data_clashes[subject_id][field_name] = dict()

                            if original_value not in subject_data_clashes[subject_id][field_name]:
                                
                                subject_data_clashes[subject_id][field_name][original_value] = dict()

                            # Does the existing value match a pattern we know will be deleted later?

                            if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                                
                                # Is the new value any better?

                                if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                                    
                                    # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False: True by contrast indicates the replacement of the old value with the new one), but log the clash.

                                    subject_data_clashes[subject_id][field_name][original_value][new_value] = False

                                else:
                                    
                                    # Replace the old value with the new one.

                                    cda_subject_records[subject_id][field_name] = demographic[demographic_id][field_name]

                                    subject_data_clashes[subject_id][field_name][original_value][new_value] = True

                            else:
                                
                                # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                                subject_data_clashes[subject_id][field_name][original_value][new_value] = False

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading sample hierarchy and sample metadata...", end='', file=sys.stderr )

# Load the provenance hierarchy for the various sample types we have to deal with.

aliquot_of_analyte = map_columns_one_to_many( aliquot_of_analyte_input_tsv, 'aliquot_id', 'analyte_id' )

analyte_from_portion = map_columns_one_to_many( analyte_from_portion_input_tsv, 'analyte_id', 'portion_id' )

portion_from_sample = map_columns_one_to_many( portion_from_sample_input_tsv, 'portion_id', 'sample_id' )

slide_from_portion = map_columns_one_to_many( slide_from_portion_input_tsv, 'slide_id', 'portion_id' )

# Load sample metadata.

sample = load_tsv_as_dict( sample_input_tsv )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file-entity associations and importing relevant metadata from associated samples...", end='', file=sys.stderr )

# Load associations between files and other entities.

file_associated_with_entity = load_tsv_as_dict( file_associated_with_entity_input_tsv, id_column_count=2 )

# (Transitively) traverse file<->sample associations and attach relevant metadata to corresponding CDA file records.

file_anatomic_site = dict()

file_tumor_vs_normal = dict()

warning_files = set()

for file_id in file_associated_with_entity:
    
    ancestor_samples = set()

    for entity_id in file_associated_with_entity[file_id]:
        
        entity_type = file_associated_with_entity[file_id][entity_id]['entity_type']

        if entity_type == 'aliquot':
            
            for analyte_id in aliquot_of_analyte[entity_id]:
                
                for portion_id in analyte_from_portion[analyte_id]:
                    
                    for sample_id in portion_from_sample[portion_id]:
                        
                        ancestor_samples.add( sample_id )

        elif entity_type == 'slide':
            
            for portion_id in slide_from_portion[entity_id]:
                
                for sample_id in portion_from_sample[portion_id]:
                    
                    ancestor_samples.add( sample_id )

        elif entity_type == 'portion':
            
            for sample_id in portion_from_sample[entity_id]:
                
                ancestor_samples.add( sample_id )

        elif entity_type == 'sample':
            
            ancestor_samples.add( entity_id )

        elif entity_type != 'case':
            
            sys.exit( f"FATAL: unexpected entity type '{entity_type}' encountered in {file_associated_with_entity_input_tsv}; not clear how to proceed. Aborting." )

    if len( ancestor_samples ) > 1:
        
        warning_files.add( file_id )

        if file_id in file_has_index_file:
            
            for index_file_id in file_has_index_file[file_id]:
                
                warning_files.add( index_file_id )

    for ancestor_sample_id in ancestor_samples:
        
        if sample[ancestor_sample_id]['biospecimen_anatomic_site'] != '':
            
            if file_id not in file_anatomic_site:
                
                file_anatomic_site[file_id] = set()

            file_anatomic_site[file_id].add( sample[ancestor_sample_id]['biospecimen_anatomic_site'] )

            if file_id in file_has_index_file:
                
                for index_file_id in file_has_index_file[file_id]:
                    
                    if index_file_id not in file_anatomic_site:
                        
                        file_anatomic_site[index_file_id] = set()

                    file_anatomic_site[index_file_id].add( sample[ancestor_sample_id]['biospecimen_anatomic_site'] )

        # Can we determine tumor/normal information for this sample?

        tumor_normal_value = ''

        tissue_type_value = sample[ancestor_sample_id]['tissue_type']

        sample_type_value = sample[ancestor_sample_id]['sample_type']

        if tissue_type_value == '' or tissue_type_value.strip().lower() == 'peritumoral' or re.sub( r'\s', r'', tissue_type_value.strip().lower() ) in delete_everywhere:
            
            # sample.tissue_type, our usual default, is unusable. Can we infer something from sample.sample_type?

            if sample_type_value.strip().lower() in { 'normal adjacent tissue', 'primary tumor', 'solid tissue normal', 'tumor' }:
                
                tumor_normal_value = sample_type_value

            else:
                
                # We couldn't use sample_type; preserve the (unhelpful) tissue_type value for this (unharmonized) data pass.

                tumor_normal_value = tissue_type_value

        else:
            
            # sample.tissue_type exists and is not (equivalent to) null.
            # 
            # Right now (2025-03-18) extant values are { 'Tumor', 'Normal', 'Abnormal' }, all of which
            # are handled in the harmonization layer. Any unexpected values will be passed through
            # unmodified, to be detected by that downstream harmonization machinery.

            tumor_normal_value = tissue_type_value

        if tumor_normal_value != '':
            
            if file_id not in file_tumor_vs_normal:
                
                file_tumor_vs_normal[file_id] = set()

            file_tumor_vs_normal[file_id].add( tumor_normal_value )

            if file_id in file_has_index_file:
                
                for index_file_id in file_has_index_file[file_id]:
                    
                    if index_file_id not in file_tumor_vs_normal:
                        
                        file_tumor_vs_normal[index_file_id] = set()

                    file_tumor_vs_normal[index_file_id].add( tumor_normal_value )

if debug and len( warning_files ) > 0:
    
    print( f"\n\n   NOTE: processed {len( warning_files )} files associated with multiple distinct sample records; at most this many files may have multiple values for sample-derived fields like anatomic_site and tumor_vs_normal.\n\n...done.", file=sys.stderr )

else:
    
    print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Writing output TSVs to {tsv_output_root}...", end='', file=sys.stderr )

# Write the new CDA file records.

with open( file_output_tsv, 'w' ) as OUT:
    
    print( *cda_file_fields, sep='\t', file=OUT )

    for file_id in sorted( cda_file_records ):
        
        output_row = list()

        file_record = cda_file_records[file_id]

        for cda_file_field in cda_file_fields:
            
            output_row.append( file_record[cda_file_field] )

        print( *output_row, sep='\t', file=OUT )

# Write the new CDA subject records.

with open( subject_output_tsv, 'w' ) as OUT:
    
    print( *cda_subject_fields, sep='\t', file=OUT )

    for subject_id in sorted( cda_subject_records ):
        
        output_row = list()

        subject_record = cda_subject_records[subject_id]

        for cda_subject_field in cda_subject_fields:
            
            output_row.append( subject_record[cda_subject_field] )

        print( *output_row, sep='\t', file=OUT )

# Write the file<->subject association.

with open( file_describes_subject_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', file=OUT )

    for file_id in sorted( cda_file_describes_subject ):
        
        for subject_id in sorted( cda_file_describes_subject[file_id] ):
            
            print( *[ file_id, subject_id ], sep='\t', file=OUT )

# Write the file<->project association.

with open( file_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'project_id' ], sep='\t', file=OUT )

    for file_id in sorted( cda_file_in_project ):
        
        for project_id in sorted( cda_file_in_project[file_id] ):
            
            print( *[ file_id, project_id ], sep='\t', file=OUT )

# Write the subject<->project association.

with open( subject_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'subject_id', 'project_id' ], sep='\t', file=OUT )

    for subject_id in sorted( cda_subject_in_project ):
        
        for project_id in sorted( cda_subject_in_project[subject_id] ):
            
            print( *[ subject_id, project_id ], sep='\t', file=OUT )

# Write file->anatomic_site.

with open( file_anatomic_site_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'anatomic_site' ], sep='\t', file=OUT )

    for file_id in sorted( file_anatomic_site ):
        
        for anatomic_site in sorted( file_anatomic_site[file_id] ):
            
            print( *[ file_id, anatomic_site ], sep='\t', file=OUT )

# Write file->tumor_vs_normal.

with open( file_tumor_vs_normal_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'tumor_vs_normal' ], sep='\t', file=OUT )

    for file_id in sorted( file_tumor_vs_normal ):
        
        for tumor_vs_normal in sorted( file_tumor_vs_normal[file_id] ):
            
            print( *[ file_id, tumor_vs_normal ], sep='\t', file=OUT )

# Update upstream_identifiers.

# upstream_identifiers_fields = [
#     
#     'cda_table',
#     'id',
#     'data_source',
#     'data_source_id_field_name',
#     'data_source_id_value'
# ]

# upstream_identifiers[cda_table][new_cda_id][upstream_data_source]['case.submitter_id'].add( upstream_submitter_id )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_table in sorted( upstream_identifiers ):
        
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

# Log data clashes between different demographic records for the same subject.

with open( demographic_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_data_clashes ):
        
        for field_name in sorted( subject_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log each set of case_ids that coalesced into a single CDA subject.

with open( subject_case_merge_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}.case.case_id", f"{upstream_data_source}.case.submitter_id", 'CDA.subject.id' ], sep='\t', file=OUT )

    for new_cda_id in sorted( original_case_id ):
        
        if len( original_case_id[new_cda_id] ) > 1:
            
            for case_id in sorted( original_case_id[new_cda_id] ):
                
                print( *[ case_id, case_submitter_id[case_id], new_cda_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


