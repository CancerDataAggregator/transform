#!/usr/bin/env python3 -u

import json
import sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'ICDC'

# Unmodified extracted data, converted directly from JSON to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

file_input_tsv = path.join( tsv_input_root, 'file', 'file.tsv' )

study_input_tsv = path.join( tsv_input_root, 'study', 'study.tsv' )

file_study_input_tsv = path.join( tsv_input_root, 'file', 'file.clinical_study_designation.tsv' )

sample_input_tsv = path.join( tsv_input_root, 'sample', 'sample.tsv' )

file_sample_input_tsv = path.join( tsv_input_root, 'file', 'file.sample_id.tsv' )

sample_case_input_tsv = path.join( tsv_input_root, 'sample', 'sample.case_id.tsv' )

case_study_input_tsv = path.join( tsv_input_root, 'case', 'case.clinical_study_designation.tsv' )

case_file_input_tsv = path.join( tsv_input_root, 'case', 'case.file_uuid.tsv' )

diagnosis_file_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.file_uuid.tsv' )

diagnosis_case_input_tsv = path.join( tsv_input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

# CDA TSVs.

tsv_output_root = path.join( 'cda_tsvs', f"{upstream_data_source.lower()}_000_unharmonized" )

upstream_identifiers_tsv = path.join( tsv_output_root, 'upstream_identifiers.tsv' )

project_in_project_tsv = path.join( tsv_output_root, 'project_in_project.tsv' )

file_output_tsv = path.join( tsv_output_root, 'file.tsv' )

file_anatomic_site_output_tsv = path.join( tsv_output_root, 'file_anatomic_site.tsv' )

file_tumor_vs_normal_output_tsv = path.join( tsv_output_root, 'file_tumor_vs_normal.tsv' )

file_in_project_output_tsv = path.join( tsv_output_root, 'file_in_project.tsv' )

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

upstream_identifiers_fields = [
    
    'cda_table',
    'id',
    'data_source',
    'data_source_id_field_name',
    'data_source_id_value'
]

debug = False

# EXECUTION

print( f"[{get_current_timestamp()}] Loading file metadata...", end='', file=sys.stderr )

# Load metadata for files.

cda_file_records = dict()

file = load_tsv_as_dict( file_input_tsv )

for file_uuid in file:
    
    cda_file_records[file_uuid] = dict()

    cda_file_records[file_uuid]['id'] = file_uuid
    cda_file_records[file_uuid]['crdc_id'] = ''
    cda_file_records[file_uuid]['name'] = file[file_uuid]['file_name']
    cda_file_records[file_uuid]['description'] = file[file_uuid]['file_description']
    cda_file_records[file_uuid]['drs_uri'] = f"drs://dg.4dfc:{file_uuid.lower()}"
    cda_file_records[file_uuid]['access'] = 'Open' # All ICDC data is open access.
    cda_file_records[file_uuid]['size'] = int( float( file[file_uuid]['file_size'] ) )
    cda_file_records[file_uuid]['format'] = file[file_uuid]['file_format']
    cda_file_records[file_uuid]['type'] = ''
    cda_file_records[file_uuid]['category'] = file[file_uuid]['file_type']

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file<->study_id associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

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
                    
                    sys.exit( f"FATAL: {upstream_data_source} program.program_acronym .uuid '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project containment.

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Compute CDA file<->project relationships.

cda_file_in_project = dict()

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Associating files with projects...", end='', file=sys.stderr )

# None of the relationships directly expressed by ICDC covers all files. Load these in batches. At time of writing (August 2024), this process places every file in at least one study.

# Direct assertions: file->study. At time of writing (August 2024), this only covers 13 files.

file_study = map_columns_one_to_many( file_study_input_tsv, 'file.uuid', 'clinical_study_designation' )

study = load_tsv_as_dict( study_input_tsv )

for file_uuid in file_study:
    
    for study_id in file_study[file_uuid]:
        
        if study_id not in cda_project_id:
            
            sys.exit( f"FATAL: File {file_uuid} associated with clinical_study_designation {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

        if file_uuid not in cda_file_in_project:
            
            cda_file_in_project[file_uuid] = set()

        cda_file_in_project[file_uuid].add( cda_project_id[study_id] )

        containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

        for ancestor_project_id in containing_projects:
            
            cda_file_in_project[file_uuid].add( ancestor_project_id )

# file->sample->case->study.

file_describes_sample = map_columns_one_to_many( file_sample_input_tsv, 'file.uuid', 'sample_id' )

sample_from_case = map_columns_one_to_many( sample_case_input_tsv, 'sample_id', 'case_id' )

case_in_study = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'clinical_study_designation' )

for file_uuid in file_describes_sample:
    
    for sample_id in file_describes_sample[file_uuid]:
        
        if sample_id in sample_from_case:
            
            for case_id in sample_from_case[sample_id]:
                
                # This should break if there's a key error. At time of writing (August 2024), every case is in a study. If that changes, we shouldn't proceed without examining the new situation.

                for study_id in case_in_study[case_id]:
                    
                    if study_id not in cda_project_id:
                        
                        sys.exit( f"FATAL: Case {case_id} associated with clinical_study_designation {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

                    if file_uuid not in cda_file_in_project:
                        
                        cda_file_in_project[file_uuid] = set()

                    cda_file_in_project[file_uuid].add( cda_project_id[study_id] )

                    containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

                    for ancestor_project_id in containing_projects:
                        
                        cda_file_in_project[file_uuid].add( ancestor_project_id )

# file->case->study

file_from_case = map_columns_one_to_many( case_file_input_tsv, 'file.uuid', 'case_id' )

for file_uuid in file_from_case:
    
    for case_id in file_from_case[file_uuid]:
        
        # This should break if there's a key error. At time of writing (August 2024), every case is in a study. If that changes, we shouldn't proceed without examining the new situation.

        for study_id in case_in_study[case_id]:
            
            if study_id not in cda_project_id:
                
                sys.exit( f"FATAL: Case {case_id} associated with clinical_study_designation {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

            if file_uuid not in cda_file_in_project:
                
                cda_file_in_project[file_uuid] = set()

            cda_file_in_project[file_uuid].add( cda_project_id[study_id] )

            containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

            for ancestor_project_id in containing_projects:
                
                cda_file_in_project[file_uuid].add( ancestor_project_id )

# file->diagnosis->case->study

file_of_diagnosis = map_columns_one_to_many( diagnosis_file_input_tsv, 'file.uuid', 'diagnosis_id' )

diagnosis_of_case = map_columns_one_to_many( diagnosis_case_input_tsv, 'diagnosis_id', 'case_id' )

for file_uuid in file_of_diagnosis:
    
    for diagnosis_id in file_of_diagnosis[file_uuid]:
        
        # This should break if there's a key error. At time of writing (August 2024), every diagnosis is assigned to a case. If that changes, we shouldn't proceed without examining the new situation.

        for case_id in diagnosis_of_case[diagnosis_id]:
            
            # This should break if there's a key error. At time of writing (August 2024), every case is in a study. If that changes, we shouldn't proceed without examining the new situation.

            for study_id in case_in_study[case_id]:
                
                if study_id not in cda_project_id:
                    
                    sys.exit( f"FATAL: Case {case_id} associated with clinical_study_designation {study_id}, which is not represented in {upstream_identifiers_tsv}; cannot continue, aborting." )

                if file_uuid not in cda_file_in_project:
                    
                    cda_file_in_project[file_uuid] = set()

                cda_file_in_project[file_uuid].add( cda_project_id[study_id] )

                containing_projects = get_cda_project_ancestors( cda_project_in_project, cda_project_id[study_id] )

                for ancestor_project_id in containing_projects:
                    
                    cda_file_in_project[file_uuid].add( ancestor_project_id )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading sample metadata...", end='', file=sys.stderr )

# Load sample metadata.

sample = load_tsv_as_dict( sample_input_tsv )

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Loading file-sample associations and importing relevant metadata from associated samples...", end='', file=sys.stderr )

# Traverse file<->sample associations and attach relevant metadata to corresponding CDA file records.

file_anatomic_site = dict()

file_tumor_vs_normal = dict()

for file_uuid in file_describes_sample:
    
    for sample_id in file_describes_sample[file_uuid]:
        
        if sample[sample_id]['sample_site'] != '':
            
            if file_uuid not in file_anatomic_site:
                
                file_anatomic_site[file_uuid] = set()

            file_anatomic_site[file_uuid].add( sample[sample_id]['sample_site'] )

        if sample[sample_id]['general_sample_pathology'] != '':
            
            if file_uuid not in file_tumor_vs_normal:
                
                file_tumor_vs_normal[file_uuid] = set()

            file_tumor_vs_normal[file_uuid].add( sample[sample_id]['general_sample_pathology'] )

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

# Write the file<->project association.

with open( file_in_project_output_tsv, 'w' ) as OUT:
    
    print( *[ 'file_id', 'project_id' ], sep='\t', file=OUT )

    for file_id in sorted( cda_file_in_project ):
        
        for project_id in sorted( cda_file_in_project[file_id] ):
            
            print( *[ file_id, project_id ], sep='\t', file=OUT )

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

print( 'done.', file=sys.stderr )


