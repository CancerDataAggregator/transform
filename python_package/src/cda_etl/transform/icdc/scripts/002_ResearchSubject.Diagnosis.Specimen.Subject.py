#!/usr/bin/env python -u

import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, load_qualified_id_association

# PARAMETERS

input_root = path.join( 'extracted_data', 'icdc' )

demographic_input_tsv = path.join( input_root, 'demographic', 'demographic.tsv' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.tsv' )

diagnosis_case_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.case_id.tsv' )

file_input_tsv = path.join( input_root, 'file', 'file.tsv' )

file_sample_input_tsv = path.join( input_root, 'file', 'file.sample_id.tsv' )

case_input_tsv = path.join( input_root, 'case', 'case.tsv' )

case_study_input_tsv = path.join( input_root, 'case', 'case.clinical_study_designation.tsv' )

canine_individual_input_tsv = path.join( input_root, 'canine_individual', 'canine_individual.tsv' )

case_enrollment_input_tsv = path.join( input_root, 'case', 'case.enrollment_id.tsv' )

registration_input_tsv = path.join( input_root, 'registration', 'registration.tsv' )

program_input_tsv = path.join( input_root, 'program', 'program.tsv' )

sample_input_tsv = path.join( input_root, 'sample', 'sample.tsv' )

sample_case_input_tsv = path.join( input_root, 'sample', 'sample.case_id.tsv' )

study_input_tsv = path.join( input_root, 'study', 'study.tsv' )

program_study_input_tsv = path.join( input_root, 'program', 'program.clinical_study_designation.tsv' )

# Initial data structures

study = load_tsv_as_dict( study_input_tsv )

program = load_tsv_as_dict( program_input_tsv )

# Note assumption: each case is linked to exactly one demographic record. True at time of writing (2024-06).

demographic = load_tsv_as_dict( demographic_input_tsv )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

sample = load_tsv_as_dict( sample_input_tsv )

file_sample = map_columns_one_to_many( file_sample_input_tsv, 'file.uuid', 'sample_id' )
sample_file = map_columns_one_to_many( file_sample_input_tsv, 'sample_id', 'file.uuid' )

case_diagnosis = map_columns_one_to_many( diagnosis_case_input_tsv, 'case_id', 'diagnosis_id' )

case_sample = map_columns_one_to_many( sample_case_input_tsv, 'case_id', 'sample_id' )
sample_case = map_columns_one_to_many( sample_case_input_tsv, 'sample_id', 'case_id' )

# Note assumption: each case is in exactly one study. True at time of writing (2024-06).

case_study = map_columns_one_to_one( case_study_input_tsv, 'case_id', 'clinical_study_designation' )

study_program = map_columns_one_to_one( program_study_input_tsv, 'clinical_study_designation', 'program_acronym' )

case_patient_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'patient_id' )

case_canine_individual_id = map_columns_one_to_one( canine_individual_input_tsv, 'case_id', 'canine_individual_id' )

case_enrollment_id = map_columns_one_to_one( case_enrollment_input_tsv, 'case_id', 'enrollment_id' )

case_registration_origin_and_id = dict()

with open( registration_input_tsv ) as IN:
    
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        record = dict( zip( column_names, line.split( '\t' ) ) )

        if record['case_id'] not in case_registration_origin_and_id:
            
            case_registration_origin_and_id[record['case_id']] = set()

        case_registration_origin_and_id[record['case_id']].add( f"{record['registration_origin']}.{record['registration_id']}" )

case_file = dict()

for file_id in file_sample:
    
    for sample_id in file_sample[file_id]:
        
        if sample_id in sample_case:
            
            for case_id in sample_case[sample_id]:
                
                if case_id not in case_file:
                    
                    case_file[case_id] = set()

                case_file[case_id].add( file_id )

# Output files

output_root = path.join( 'cda_tsvs', 'icdc_raw_unharmonized' )

researchsubject_output_tsv = path.join( output_root, 'researchsubject.tsv' )

researchsubject_identifier_output_tsv = path.join( output_root, 'researchsubject_identifier.tsv' )

researchsubject_diagnosis_output_tsv = path.join( output_root, 'researchsubject_diagnosis.tsv' )

researchsubject_specimen_output_tsv = path.join( output_root, 'researchsubject_specimen.tsv' )

diagnosis_output_tsv = path.join( output_root, 'diagnosis.tsv' )

diagnosis_identifier_output_tsv = path.join( output_root, 'diagnosis_identifier.tsv' )

file_subject_output_tsv = path.join( output_root, 'file_subject.tsv' )

file_specimen_output_tsv = path.join( output_root, 'file_specimen.tsv' )

specimen_output_tsv = path.join( output_root, 'specimen.tsv' )

specimen_identifier_output_tsv = path.join( output_root, 'specimen_identifier.tsv' )

subject_output_tsv = path.join( output_root, 'subject.tsv' )

subject_identifier_output_tsv = path.join( output_root, 'subject_identifier.tsv' )

subject_associated_project_output_tsv = path.join( output_root, 'subject_associated_project.tsv' )

subject_researchsubject_output_tsv = path.join( output_root, 'subject_researchsubject.tsv' )

# Log files

icdc_aux_dir = path.join( 'auxiliary_metadata', '__ICDC_supplemental_metadata' )

entity_ids_projects_and_types = path.join( icdc_aux_dir, 'ICDC_entity_id_to_program_and_study.tsv' )

warning_log = path.join( icdc_aux_dir, 'icdc_warning_log.txt' )

rs_output_column_names = [
    'id',
    'member_of_research_project',
    'primary_diagnosis_condition',
    'primary_diagnosis_site'
]

diagnosis_output_column_names = [
    'id',
    'primary_diagnosis',
    'age_at_diagnosis',
    'morphology',
    'stage',
    'grade',
    'method_of_diagnosis'
]

subject_output_column_names = [
    'id',
    'species',
    'sex',
    'race',
    'ethnicity',
    'days_to_birth',
    'vital_status',
    'days_to_death',
    'cause_of_death'
]

# EXECUTION

for output_dir in [ output_root, icdc_aux_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Track one record per Subject.

subject_records = dict()

subject_associated_project = dict()
subject_researchsubject = dict()

subject_case_ids = dict()
subject_patient_ids = dict()
subject_canine_individual_ids = dict()
subject_enrollment_ids = dict()
subject_registration_origin_and_ids = dict()

file_subject = dict()

file_specimen = dict()

printed_diagnosis = set()

entity_studies_by_type = dict()

with open( researchsubject_output_tsv, 'w' ) as RS_OUT, open( researchsubject_identifier_output_tsv, 'w' ) as RS_IDENTIFIER, \
    open( researchsubject_diagnosis_output_tsv, 'w' ) as RS_DIAGNOSIS, open( researchsubject_specimen_output_tsv, 'w' ) as RS_SPECIMEN, \
    open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS, open( diagnosis_identifier_output_tsv, 'w' ) as DIAGNOSIS_IDENTIFIER, \
    open( specimen_output_tsv, 'w' ) as SPECIMEN, open( specimen_identifier_output_tsv, 'w' ) as SPECIMEN_IDENTIFIER, open( warning_log, 'w' ) as WARN:
    
    print( *rs_output_column_names, sep='\t', file=RS_OUT )
    print( *[ 'researchsubject_id', 'system', 'field_name', 'value' ], sep='\t', file=RS_IDENTIFIER )
    print( *[ 'researchsubject_id', 'diagnosis_id' ], sep='\t', file=RS_DIAGNOSIS )
    print( *[ 'researchsubject_id', 'specimen_id' ], sep='\t', file=RS_SPECIMEN )
    print( *diagnosis_output_column_names, sep='\t', file=DIAGNOSIS )
    print( *[ 'diagnosis_id', 'system', 'field_name', 'value' ], sep='\t', file=DIAGNOSIS_IDENTIFIER )
    print( *[ 'id', 'associated_project', 'days_to_collection', 'primary_disease_type', 'anatomical_site', 'source_material_type', 'specimen_type', 'derived_from_specimen', 'derived_from_subject' ], sep='\t', file=SPECIMEN )
    print( *[ 'specimen_id', 'system', 'field_name', 'value' ], sep='\t', file=SPECIMEN_IDENTIFIER )

    for case_id in sorted( case_patient_id ):
        
        if case_id not in demographic:
            
            sys.exit( f"FATAL: case_id {case_id} not linked to any demographic record: downstream assumptions violated, aborting." )

        patient_id = ''

        if case_id in case_patient_id:
            
            patient_id = case_patient_id[case_id]

        canine_individual_id = ''

        if case_id in case_canine_individual_id:
            
            canine_individual_id = case_canine_individual_id[case_id]

        enrollment_id = ''

        if case_id in case_enrollment_id:
            
            enrollment_id = case_enrollment_id[case_id]

        registration_strings = set()

        if case_id in case_registration_origin_and_id:
            
            registration_strings = case_registration_origin_and_id[case_id]

        study_id = case_study[case_id]

        if 'case' not in entity_studies_by_type:
            
            entity_studies_by_type['case'] = dict()

        if case_id not in entity_studies_by_type['case']:
            
            entity_studies_by_type['case'][case_id] = set()

        entity_studies_by_type['case'][case_id].add( study_id )

        # Build one record per sample.

        sample_records = dict()

        # CDA ResearchSubject ID: Combination of ICDC clinical_study_designation (here represented as `study_id`) and case_id.

        rs_id = f"{study_id}.{case_id}"

        # CDA Subject ID: Combination of ICDC program acronym and case_id.

        subject_id = f"{study_program[study_id]}.{case_id}"

        if subject_id not in subject_records:
            
            subject_records[subject_id] = dict()
            subject_records[subject_id]['species'] = 'Canis lupus familiaris'
            subject_records[subject_id]['sex'] = '' if demographic[case_id]['sex'] is None else demographic[case_id]['sex']
            subject_records[subject_id]['race'] = ''
            subject_records[subject_id]['ethnicity'] = ''
            subject_records[subject_id]['days_to_birth'] = int( -365.25 * float( demographic[case_id]['patient_age_at_enrollment'] ) ) if demographic[case_id]['patient_age_at_enrollment_unit'] == 'years' and demographic[case_id]['patient_age_at_enrollment'] != '' else ''

            found_necropsy_sample = False

            if case_id in case_sample:
                
                for sample_id in case_sample[case_id]:
                    
                    if 'necropsy_sample' in sample[sample_id] and sample[sample_id]['necropsy_sample'] is not None and sample[sample_id]['necropsy_sample'] == 'Yes':
                        
                        found_necropsy_sample = True

            if found_necropsy_sample:
                
                subject_records[subject_id]['vital_status'] = 'Dead'

            else:
                
                subject_records[subject_id]['vital_status'] = ''

            subject_records[subject_id]['days_to_death'] = ''
            subject_records[subject_id]['cause_of_death'] = ''

            subject_associated_project[subject_id] = set()

            subject_researchsubject[subject_id] = set()

            subject_case_ids[subject_id] = set()
            subject_patient_ids[subject_id] = set()
            subject_canine_individual_ids[subject_id] = set()
            subject_enrollment_ids[subject_id] = set()
            subject_registration_origin_and_ids[subject_id] = set()

        if case_id in case_diagnosis:
            
            for diagnosis_id in sorted( case_diagnosis[case_id] ):
                
                if 'diagnosis' not in entity_studies_by_type:
                    
                    entity_studies_by_type['diagnosis'] = dict()

                if diagnosis_id not in entity_studies_by_type['diagnosis']:
                    
                    entity_studies_by_type['diagnosis'][diagnosis_id] = set()

                entity_studies_by_type['diagnosis'][diagnosis_id].add( study_id )

        subject_associated_project[subject_id].add( study_id )

        subject_researchsubject[subject_id].add( rs_id )

        subject_case_ids[subject_id].add( case_id )
        subject_patient_ids[subject_id].add( patient_id )
        subject_canine_individual_ids[subject_id].add( canine_individual_id )
        subject_enrollment_ids[subject_id].add( enrollment_id )

        for registration_string in registration_strings:
            
            subject_registration_origin_and_ids[subject_id].add( registration_string )

        if case_id in case_file:
            
            for file_id in case_file[case_id]:
                
                if file_id not in file_subject:
                    
                    file_subject[file_id] = set()

                file_subject[file_id].add( subject_id )

        # Cached ICDC identifiers:

        print( *[ rs_id, 'ICDC', 'case.case_id', case_id ], sep='\t', file=RS_IDENTIFIER )

        if patient_id != '':
            
            print( *[ rs_id, 'ICDC', 'case.patient_id', patient_id ], sep='\t', file=RS_IDENTIFIER )

        if canine_individual_id != '':
            
            print( *[ rs_id, 'ICDC', 'canine_individual.canine_individual_id', canine_individual_id ], sep='\t', file=RS_IDENTIFIER )

        if enrollment_id != '':
            
            print( *[ rs_id, 'ICDC', 'enrollment.enrollment_id', enrollment_id ], sep='\t', file=RS_IDENTIFIER )

        if len( registration_strings ) > 0:
            
            for registration_string in registration_strings:
                
                print( *[ rs_id, 'ICDC', 'registration_string', registration_string ], sep='\t', file=RS_IDENTIFIER )

        # If there's a diagnosis record attached to this case, connect it to this ResearchSubject record
        # and save its contents to their CDA analogues.

        if case_id in case_diagnosis:
            
            for diagnosis_id in sorted( case_diagnosis[case_id] ):
                
                diagnosis_cda_id = f"{rs_id}.{diagnosis_id}"

                print( *[ rs_id, diagnosis_cda_id ], sep='\t', file=RS_DIAGNOSIS )

                if diagnosis_cda_id not in printed_diagnosis:
                    
                    print( *[ diagnosis_cda_id, 'ICDC', 'diagnosis.diagnosis_id', diagnosis_id ], sep='\t', file=DIAGNOSIS_IDENTIFIER )

                    diagnosis_record = dict( zip( diagnosis_output_column_names, [ '' ] * len( diagnosis_output_column_names ) ) )

                    diagnosis_record['id'] = diagnosis_cda_id

                    if diagnosis[diagnosis_id]['disease_term'] is not None:
                        
                        diagnosis_record['primary_diagnosis'] = diagnosis[diagnosis_id]['disease_term']

                    if diagnosis[diagnosis_id]['stage_of_disease'] is not None:
                        
                        diagnosis_record['stage'] = diagnosis[diagnosis_id]['stage_of_disease']

                    if diagnosis[diagnosis_id]['histological_grade'] is not None:
                        
                        diagnosis_record['grade'] = diagnosis[diagnosis_id]['histological_grade']

                    print( *[ diagnosis_record[column_name] for column_name in diagnosis_output_column_names ], sep='\t', file=DIAGNOSIS )

                    printed_diagnosis.add( diagnosis_cda_id )

        # If samples are attached to this case, connect them to this ResearchSubject record.

        if case_id in case_sample:
            
            for sample_id in sorted( case_sample[case_id] ):
                
                if 'sample' not in entity_studies_by_type:
                    
                    entity_studies_by_type['sample'] = dict()

                if sample_id not in entity_studies_by_type['sample']:
                    
                    entity_studies_by_type['sample'][sample_id] = set()

                entity_studies_by_type['sample'][sample_id].add( study_id )

                sample_cda_id = f"{rs_id}.{sample_id}"

                print( *[ rs_id, sample_cda_id ], sep='\t', file=RS_SPECIMEN )

                if sample_id not in sample_records:
                    
                    sample_records[sample_id] = dict()

                    sample_records[sample_id]['cda_ids'] = { sample_cda_id }
                    sample_records[sample_id]['associated_project'] = { study_id }
                    sample_records[sample_id]['days_to_collection'] = ''
                    sample_records[sample_id]['primary_disease_type'] = sample[sample_id]['specific_sample_pathology']
                    sample_records[sample_id]['anatomical_site'] = sample[sample_id]['sample_site']
                    sample_records[sample_id]['source_material_type'] = sample[sample_id]['general_sample_pathology']
                    sample_records[sample_id]['specimen_type'] = 'sample'
                    sample_records[sample_id]['derived_from_specimen'] = 'initial specimen'
                    sample_records[sample_id]['derived_from_subject'] = subject_id

                else:
                    
                    sample_records[sample_id]['cda_ids'].add( sample_cda_id )
                    sample_records[sample_id]['associated_project'].add( study_id )

        # Write the main ResearchSubject record.

        output_record = dict()

        output_record['id'] = rs_id
        output_record['member_of_research_project'] = study_id

        output_record['primary_diagnosis_condition'] = ''
        output_record['primary_diagnosis_site'] = ''

        if case_id in case_diagnosis:
            
            for diagnosis_id in sorted( case_diagnosis[case_id] ):
                
                if diagnosis[diagnosis_id]['disease_term'] is not None and diagnosis[diagnosis_id]['disease_term'] != '':
                    
                    primary_diagnosis_condition = diagnosis[diagnosis_id]['disease_term']

                    if output_record['primary_diagnosis_condition'] != '' and output_record['primary_diagnosis_condition'] != primary_diagnosis_condition:
                        
                        print( f"WARNING: RS {rs_id} case_id ({study_id}) {case_id} diagnosis.disease_term value '{primary_diagnosis_condition}' differs from previously-recorded value '{output_record['primary_diagnosis_condition']}'; ignoring new value.", file=WARN )

                    else:
                        
                        output_record['primary_diagnosis_condition'] = primary_diagnosis_condition

                if diagnosis[diagnosis_id]['primary_disease_site'] is not None and diagnosis[diagnosis_id]['primary_disease_site'] != '':
                    
                    primary_diagnosis_site = diagnosis[diagnosis_id]['primary_disease_site']

                    if output_record['primary_diagnosis_site'] != '' and output_record['primary_diagnosis_site'] != primary_diagnosis_site:
                        
                        print( f"WARNING: RS {rs_id} case_id ({study_id}) {case_id} diagnosis.primary_disease_site value '{primary_diagnosis_site}' differs from previously-recorded value '{output_record['primary_diagnosis_site']}'; overwriting with new value.", file=WARN )

                    output_record['primary_diagnosis_site'] = primary_diagnosis_site

        print( *[ output_record[column_name] for column_name in rs_output_column_names ], sep='\t', file=RS_OUT )

        if len( sample_records ) > 0:

            # Write the Specimen records collected for this ResearchSubject.

            seen_specimen_cda_ids = set()

            for sample_id in sorted( sample_records ):
                
                for sample_cda_id in sorted( sample_records[sample_id]['cda_ids'] ):
                    
                    if sample_cda_id in seen_specimen_cda_ids:
                        
                        sys.exit(f"FATAL FOR NOW: Saw {sample_cda_id} more than once, aborting.\n")

                    seen_specimen_cda_ids.add( sample_cda_id )

                    current_row = sample_records[sample_id]

                    print( *[ sample_cda_id, ';'.join( sorted( current_row['associated_project'] ) ), current_row['days_to_collection'], current_row['primary_disease_type'], current_row['anatomical_site'], current_row['source_material_type'], current_row['specimen_type'], current_row['derived_from_specimen'], current_row['derived_from_subject'] ], sep='\t', file=SPECIMEN )

                    print( *[ sample_cda_id, 'ICDC', 'sample.sample_id', sample_id ], sep='\t', file=SPECIMEN_IDENTIFIER )

                    if sample_id in sample_file:
                        
                        for file_id in sample_file[sample_id]:
                            
                            if file_id not in file_specimen:
                                
                                file_specimen[file_id] = set()

                            file_specimen[file_id].add( sample_cda_id )

                            if current_row['derived_from_specimen'] != 'initial specimen':
                                
                                file_specimen[file_id].add( current_row['derived_from_specimen'] )

# Print list of select entities by type with study/program affiliations for inter-DC cross-mapping.

with open( entity_ids_projects_and_types, 'w' ) as ENTITIES_BY_STUDY:
    
    print( *[ 'entity_type', 'entity_id', 'program_acronym', 'study_name', 'study_id' ], sep='\t', file=ENTITIES_BY_STUDY )

    for entity_type in sorted( entity_studies_by_type ):
        
        for entity_id in sorted( entity_studies_by_type[entity_type] ):
            
            for study_id in sorted( entity_studies_by_type[entity_type][entity_id] ):
                
                study_name = study[study_id]['clinical_study_name']

                program_acronym = study_program[study_id]

                print( *[ entity_type, entity_id, program_acronym, study_name, study_id ], sep='\t', file=ENTITIES_BY_STUDY )

# Print collected Subject records.

with open( subject_output_tsv, 'w' ) as SUBJECT, open( subject_identifier_output_tsv, 'w' ) as SUBJECT_IDENTIFIER, \
    open( subject_associated_project_output_tsv, 'w' ) as SUBJECT_ASSOCIATED_PROJECT, open( subject_researchsubject_output_tsv, 'w' ) as SUBJECT_RESEARCHSUBJECT:
    
    print( *subject_output_column_names, sep='\t', file=SUBJECT )

    print( *[ 'subject_id', 'system', 'field_name', 'value' ], sep='\t', file=SUBJECT_IDENTIFIER )
    print( *[ 'subject_id', 'associated_project' ], sep='\t', file=SUBJECT_ASSOCIATED_PROJECT )
    print( *[ 'subject_id', 'researchsubject_id' ], sep='\t', file=SUBJECT_RESEARCHSUBJECT )

    for subject_id in sorted( subject_records ):
        
        current_row = subject_records[subject_id]

        print( *( [ subject_id ] + [ current_row[subject_field_name] for subject_field_name in subject_output_column_names[1:] ] ), sep='\t', file=SUBJECT )

        for case_id in sorted( subject_case_ids[subject_id] ):
            
            print( *[ subject_id, 'ICDC', 'case.case_id', case_id ], sep='\t', file=SUBJECT_IDENTIFIER )

        for patient_id in sorted( subject_patient_ids[subject_id] ):
            
            if patient_id != '':
                
                print( *[ subject_id, 'ICDC', 'case.patient_id', patient_id ], sep='\t', file=SUBJECT_IDENTIFIER )

        for canine_individual_id in sorted( subject_canine_individual_ids[subject_id] ):
            
            if canine_individual_id != '':
                
                print( *[ subject_id, 'ICDC', 'canine_individual.canine_individual_id', canine_individual_id ], sep='\t', file=SUBJECT_IDENTIFIER )

        for enrollment_id in sorted( subject_enrollment_ids[subject_id] ):
            
            if enrollment_id != '':
                
                print( *[ subject_id, 'ICDC', 'enrollment.enrollment_id', enrollment_id ], sep='\t', file=SUBJECT_IDENTIFIER )

        for registration_string in sorted( subject_registration_origin_and_ids[subject_id] ):
            
            print( *[ subject_id, 'ICDC', 'registration_string', registration_string ], sep='\t', file=SUBJECT_IDENTIFIER )

        for associated_project in sorted( subject_associated_project[subject_id] ):
            
            print( *[ subject_id, associated_project ], sep='\t', file=SUBJECT_ASSOCIATED_PROJECT )

        for rs_id in sorted( subject_researchsubject[subject_id] ):
            
            print( *[ subject_id, rs_id ], sep='\t', file=SUBJECT_RESEARCHSUBJECT )

# Print the File->Subject relation.

with open( file_subject_output_tsv, 'w' ) as FILE_SUBJECT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', file=FILE_SUBJECT )

    for file_id in sorted( file_subject ):
        
        for subject_id in sorted( file_subject[file_id] ):
            
            print( *[ file_id, subject_id ], sep='\t', file=FILE_SUBJECT )

# Print the File->Specimen relation.

with open( file_specimen_output_tsv, 'w' ) as FILE_SPECIMEN:
    
    print( *[ 'file_id', 'specimen_id' ], sep='\t', file=FILE_SPECIMEN )

    for file_id in sorted( file_specimen ):
        
        for specimen_id in sorted( file_specimen[file_id] ):
            
            print( *[ file_id, specimen_id ], sep='\t', file=FILE_SPECIMEN )


