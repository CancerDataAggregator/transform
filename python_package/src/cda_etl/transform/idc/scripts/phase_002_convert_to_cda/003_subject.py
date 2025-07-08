#!/usr/bin/env python3 -u

import re, sys

from os import makedirs, path

from cda_etl.lib import get_cda_project_ancestors, get_current_timestamp, get_submitter_id_patterns_not_to_merge_across_projects, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many

# PARAMETERS

upstream_data_source = 'IDC'

# Extracted data, converted from JSONL to TSV.

tsv_input_root = path.join( 'extracted_data', upstream_data_source.lower() )

case_input_tsv = path.join( tsv_input_root, 'idc_case.tsv' )

case_patientid_input_tsv = path.join( tsv_input_root, 'idc_case.PatientID.tsv' )

tcga_clinical_input_tsv = path.join( tsv_input_root, 'tcga_clinical_rel9.tsv' )

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

subject_case_merge_log = path.join( aux_subject_output_dir, f"{upstream_data_source}_all_groups_of_idc_case_ids_that_coalesced_into_a_single_CDA_subject_id.tsv" )

aux_value_output_dir = path.join( aux_output_root, 'values' )

subject_data_clash_log = path.join( aux_value_output_dir, f"{upstream_data_source}_same_subject_case_clashes.species.year_of_birth.race.ethnicity.tsv" )

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

print( f"[{get_current_timestamp()}] Computing subject identifier metadata and loading case<->collection associations, CDA project IDs and crossrefs, and CDA project hierarchy...", end='', file=sys.stderr )

# Get submitter IDs.

# Load CDA IDs for collections and programs. Don't use any of the canned loader
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
    
    # Some of these may be from dbGaP; we won't need to translate those, just IDs from {upstream_data_source}.

    if upstream_data_source in upstream_identifiers['project'][project_id]:
        
        if 'original_collections_metadata.collection_id' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['original_collections_metadata.collection_id']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} original_collections_metadata.collection_id '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

        elif 'original_collections_metadata.Program' in upstream_identifiers['project'][project_id][upstream_data_source]:
            
            for value in upstream_identifiers['project'][project_id][upstream_data_source]['original_collections_metadata.Program']:
                
                if value not in cda_project_id:
                    
                    cda_project_id[value] = project_id

                elif cda_project_id[value] != project_id:
                    
                    sys.exit( f"FATAL: {upstream_data_source} original_collections_metadata.Program '{value}' unexpectedly assigned to both {cda_project_id[value]} and {project_id}; cannot continue, aborting." )

# Load CDA project record metadata and inter-project containment.

project = load_tsv_as_dict( project_tsv )

cda_project_in_project = map_columns_one_to_many( project_in_project_tsv, 'child_project_id', 'parent_project_id' )

# Load case metadata, create CDA subject records and associate them with CDA project records.

case = load_tsv_as_dict( case_input_tsv )

case_collection = map_columns_one_to_many( case_input_tsv, 'idc_case_id', 'collection_id' )

case_patientid = map_columns_one_to_many( case_patientid_input_tsv, 'idc_case_id', 'PatientID' )

cda_subject_id = dict()

original_idc_case_id = dict()

cda_subject_in_project = dict()

for idc_case_id in case:
    
    if idc_case_id not in case_collection:
        
        sys.exit( f"FATAL: No original_collections_metadata.collection_id associated with idc_case_id {idc_case_id}; cannot continue, aborting." )

    for collection_id in case_collection[idc_case_id]:
        
        # This too.

        new_cda_subject_id = f"{collection_id}.{case[idc_case_id]['submitter_case_id']}"

        if idc_case_id in cda_subject_id and cda_subject_id[idc_case_id] != new_cda_subject_id:
            
            sys.exit( f"FATAL: idc_case_id {idc_case_id} unexpectedly assigned to both CDA subject IDs {cda_subject_id[idc_case_id]} and {new_cda_subject_id}; cannot continue, aborting." )

        cda_subject_id[idc_case_id] = new_cda_subject_id

        # Track original idc_case_ids for CDA subjects.

        if new_cda_subject_id not in original_idc_case_id:
            
            original_idc_case_id[new_cda_subject_id] = set()

        original_idc_case_id[new_cda_subject_id].add( idc_case_id )

        if new_cda_subject_id not in cda_subject_in_project:
            
            cda_subject_in_project[new_cda_subject_id] = set()

        cda_subject_in_project[new_cda_subject_id].add( cda_project_id[collection_id] )

        for ancestor_project_id in get_cda_project_ancestors( cda_project_in_project, cda_project_id[collection_id] ):
            
            cda_subject_in_project[new_cda_subject_id].add( ancestor_project_id )

        # Record upstream dicom_all.idc_case_id, auxiliary_metadata.submitter_case_id, and (slightly buggy, so we don't do anything but document) dicom_all.PatientID values for CDA subjects.

        if 'subject' not in upstream_identifiers:
            
            upstream_identifiers['subject'] = dict()

        if new_cda_subject_id not in upstream_identifiers['subject']:
            
            upstream_identifiers['subject'][new_cda_subject_id] = dict()

        if upstream_data_source not in upstream_identifiers['subject'][new_cda_subject_id]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source] = dict()

        if 'dicom_all.idc_case_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['dicom_all.idc_case_id'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['dicom_all.idc_case_id'].add( idc_case_id )

        if 'auxiliary_metadata.submitter_case_id' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['auxiliary_metadata.submitter_case_id'] = set()

        upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['auxiliary_metadata.submitter_case_id'].add( case[idc_case_id]['submitter_case_id'] )

        if 'dicom_all.PatientID' not in upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['dicom_all.PatientID'] = set()

        for patientid in case_patientid[idc_case_id]:
            
            upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['dicom_all.PatientID'].add( patientid )

print( 'done.', file=sys.stderr )

# Create CDA subject records.

print( f"[{get_current_timestamp()}] Loading {upstream_data_source} case metadata for CDA subject records...", end='', file=sys.stderr )

cda_subject_records = dict()

subject_data_clashes = dict()

tcga_clinical = load_tsv_as_dict( tcga_clinical_input_tsv )

for subject_id in sorted( original_idc_case_id ):
    
    cda_subject_records[subject_id] = dict()

    cda_subject_records[subject_id]['id'] = subject_id
    cda_subject_records[subject_id]['crdc_id'] = ''
    cda_subject_records[subject_id]['species'] = ''
    cda_subject_records[subject_id]['year_of_birth'] = ''
    cda_subject_records[subject_id]['year_of_death'] = ''
    cda_subject_records[subject_id]['cause_of_death'] = ''
    cda_subject_records[subject_id]['race'] = ''
    cda_subject_records[subject_id]['ethnicity'] = ''

    for idc_case_id in sorted( original_idc_case_id[subject_id] ):
        
        case_submitter_id = case[idc_case_id]['submitter_case_id']

        if case_submitter_id in tcga_clinical:
            
            if tcga_clinical[case_submitter_id]['race'] is not None and tcga_clinical[case_submitter_id]['race'] != '':
                
                if cda_subject_records[subject_id]['race'] == '':
                    
                    cda_subject_records[subject_id]['race'] = tcga_clinical[case_submitter_id]['race']

                elif cda_subject_records[subject_id]['race'] != tcga_clinical[case_submitter_id]['race']:
                    
                    # Set up comparison and clash-tracking data structures.

                    original_value = cda_subject_records[subject_id]['race']

                    new_value = tcga_clinical[case_submitter_id]['race']

                    if subject_id not in subject_data_clashes:
                        
                        subject_data_clashes[subject_id] = dict()

                    if 'tcga_clinical_rel9.race' not in subject_data_clashes[subject_id]:
                        
                        subject_data_clashes[subject_id]['tcga_clinical_rel9.race'] = dict()

                    if original_value not in subject_data_clashes[subject_id]['tcga_clinical_rel9.race']:
                        
                        subject_data_clashes[subject_id]['tcga_clinical_rel9.race'][original_value] = dict()

                    # Does the existing value match a pattern we know will be deleted later?

                    if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                        
                        # Is the new value any better?

                        if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                            
                            # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                            subject_data_clashes[subject_id]['tcga_clinical_rel9.race'][original_value][new_value] = False

                        else:
                            
                            # Replace the old value with the new one.

                            cda_subject_records[subject_id]['race'] = new_value

                            subject_data_clashes[subject_id]['tcga_clinical_rel9.race'][original_value][new_value] = True

                    else:
                        
                        # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                        subject_data_clashes[subject_id]['tcga_clinical_rel9.race'][original_value][new_value] = False

        if case[idc_case_id]['PatientBirthDate'] is not None and case[idc_case_id]['PatientBirthDate'] != '':
            
            new_value = re.sub( r'^(\d+)-\d+-\d+$', r'\1', case[idc_case_id]['PatientBirthDate'] )

            if re.search( r'^\d\d\d\d$', new_value ) is None:
                
                sys.exit( f"FATAL: Could not parse year_of_birth from idc_case_id '{idc_case_id}' PatientBirthDate value '{case[idc_case_id]['PatientBirthDate']}'; aborting.")

            if cda_subject_records[subject_id]['year_of_birth'] == '':
                
                cda_subject_records[subject_id]['year_of_birth'] = new_value

            elif cda_subject_records[subject_id]['year_of_birth'] != new_value:
                
                # Set up comparison and clash-tracking data structures.

                original_value = cda_subject_records[subject_id]['year_of_birth']

                if subject_id not in subject_data_clashes:
                    
                    subject_data_clashes[subject_id] = dict()

                if 'dicom_all.PatientBirthDate' not in subject_data_clashes[subject_id]:
                    
                    subject_data_clashes[subject_id]['dicom_all.PatientBirthDate'] = dict()

                if original_value not in subject_data_clashes[subject_id]['dicom_all.PatientBirthDate']:
                    
                    subject_data_clashes[subject_id]['dicom_all.PatientBirthDate'][original_value] = dict()

                # Does the existing value match a pattern we know will be deleted later?

                if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                    
                    # Is the new value any better?

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                        subject_data_clashes[subject_id]['dicom_all.PatientBirthDate'][original_value][new_value] = False

                    else:
                        
                        # Replace the old value with the new one.

                        cda_subject_records[subject_id]['year_of_birth'] = new_value

                        subject_data_clashes[subject_id]['dicom_all.PatientBirthDate'][original_value][new_value] = True

                else:
                    
                    # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                    subject_data_clashes[subject_id]['dicom_all.PatientBirthDate'][original_value][new_value] = False

        if case[idc_case_id]['PatientSpeciesDescription'] is not None and case[idc_case_id]['PatientSpeciesDescription'] != '':
            
            if cda_subject_records[subject_id]['species'] == '':
                
                cda_subject_records[subject_id]['species'] = case[idc_case_id]['PatientSpeciesDescription']

            elif cda_subject_records[subject_id]['species'] != case[idc_case_id]['PatientSpeciesDescription']:
                
                # Set up comparison and clash-tracking data structures.

                original_value = cda_subject_records[subject_id]['species']

                new_value = case[idc_case_id]['PatientSpeciesDescription']

                if subject_id not in subject_data_clashes:
                    
                    subject_data_clashes[subject_id] = dict()

                if 'dicom_all.PatientSpeciesDescription' not in subject_data_clashes[subject_id]:
                    
                    subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription'] = dict()

                if original_value not in subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription']:
                    
                    subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription'][original_value] = dict()

                # Does the existing value match a pattern we know will be deleted later?

                if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                    
                    # Is the new value any better?

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                        subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription'][original_value][new_value] = False

                    else:
                        
                        # Replace the old value with the new one.

                        cda_subject_records[subject_id]['species'] = new_value

                        subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription'][original_value][new_value] = True

                else:
                    
                    # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                    subject_data_clashes[subject_id]['dicom_all.PatientSpeciesDescription'][original_value][new_value] = False

        if case[idc_case_id]['EthnicGroup'] is not None and case[idc_case_id]['EthnicGroup'] != '':
            
            if cda_subject_records[subject_id]['ethnicity'] == '':
                
                cda_subject_records[subject_id]['ethnicity'] = case[idc_case_id]['EthnicGroup']

            elif cda_subject_records[subject_id]['ethnicity'] != case[idc_case_id]['EthnicGroup']:
                
                # Set up comparison and clash-tracking data structures.

                original_value = cda_subject_records[subject_id]['ethnicity']

                new_value = case[idc_case_id]['EthnicGroup']

                if subject_id not in subject_data_clashes:
                    
                    subject_data_clashes[subject_id] = dict()

                if 'dicom_all.EthnicGroup' not in subject_data_clashes[subject_id]:
                    
                    subject_data_clashes[subject_id]['dicom_all.EthnicGroup'] = dict()

                if original_value not in subject_data_clashes[subject_id]['dicom_all.EthnicGroup']:
                    
                    subject_data_clashes[subject_id]['dicom_all.EthnicGroup'][original_value] = dict()

                # Does the existing value match a pattern we know will be deleted later?

                if re.sub( r'\s', r'', original_value.strip().lower() ) in delete_everywhere:
                    
                    # Is the new value any better?

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        # Both old and new values will eventually be deleted. Go with the first one observed (dict value of False. True, by contrast, indicates the replacement of the old value with the new one), but log the clash.

                        subject_data_clashes[subject_id]['dicom_all.EthnicGroup'][original_value][new_value] = False

                    else:
                        
                        # Replace the old value with the new one.

                        cda_subject_records[subject_id]['ethnicity'] = new_value

                        subject_data_clashes[subject_id]['dicom_all.EthnicGroup'][original_value][new_value] = True

                else:
                    
                    # The original value is not one known to be slated for deletion later, so there will be no replacement. Log the clash.

                    subject_data_clashes[subject_id]['dicom_all.EthnicGroup'][original_value][new_value] = False

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

# upstream_identifiers['subject'][new_cda_subject_id][upstream_data_source]['dicom_all.idc_case_id'].add( idc_case_id )

with open( upstream_identifiers_tsv, 'w' ) as OUT:
    
    print( *upstream_identifiers_fields, sep='\t', file=OUT )

    for cda_table in sorted( upstream_identifiers ):
        
        for cda_entity_id in sorted( upstream_identifiers[cda_table] ):
            
            for data_source in sorted( upstream_identifiers[cda_table][cda_entity_id] ):
                
                for source_field in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source] ):
                    
                    for value in sorted( upstream_identifiers[cda_table][cda_entity_id][data_source][source_field] ):
                        
                        print( *[ cda_table, cda_entity_id, data_source, source_field, value ], sep='\t', file=OUT )

# Log data clashes between different case records for the same subject.

# subject_data_clashes[subject_id]['dicom_all.EthnicGroup'][original_value][new_value] = False

with open( subject_data_clash_log, 'w' ) as OUT:
    
    print( *[ 'CDA_subject_id', f"{upstream_data_source}_field_name", f"observed_value_from_{upstream_data_source}", f"clashing_value_at_{upstream_data_source}", 'CDA_kept_value' ], sep='\t', file=OUT )

    for subject_id in sorted( subject_data_clashes ):
        
        for field_name in sorted( subject_data_clashes[subject_id] ):
            
            for original_value in sorted( subject_data_clashes[subject_id][field_name] ):
                
                for clashing_value in sorted( subject_data_clashes[subject_id][field_name][original_value] ):
                    
                    if subject_data_clashes[subject_id][field_name][original_value][clashing_value] == False:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, original_value ], sep='\t', file=OUT )

                    else:
                        
                        print( *[ subject_id, field_name, original_value, clashing_value, clashing_value ], sep='\t', file=OUT )

# Log each set of idc_case_ids that coalesced into a single CDA subject.

# original_idc_case_id[new_cda_subject_id].add( idc_case_id )

with open( subject_case_merge_log, 'w' ) as OUT:
    
    print( *[ f"{upstream_data_source}.dicom_all.idc_case_id", f"{upstream_data_source}.auxiliary_metadata.submitter_case_id", 'CDA.subject.id' ], sep='\t', file=OUT )

    for new_cda_id in sorted( original_idc_case_id ):
        
        if len( original_idc_case_id[new_cda_id] ) > 1:
            
            for idc_case_id in sorted( original_idc_case_id[new_cda_id] ):
                
                submitter_case_id = case[idc_case_id]['submitter_case_id']

                print( *[ idc_case_id, submitter_case_id, new_cda_id ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


