#!/usr/bin/env python -u

import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, load_qualified_id_association

# PARAMETERS

# Input files

input_root = 'extracted_data/cds'

participant_input_tsv = path.join( input_root, 'participant', 'participant.tsv' )

participant_diagnosis_input_tsv = path.join( input_root, 'participant', 'participant.diagnosis_id.tsv' )

participant_sample_input_tsv = path.join( input_root, 'participant', 'participant.sample_id.tsv' )

study_input_tsv = path.join( input_root, 'study', 'study.tsv' )

study_participant_input_tsv = path.join( input_root, 'study', 'study.participant_id.tsv' )

sample_input_tsv = path.join( input_root, 'sample', 'sample.tsv' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis', 'diagnosis.tsv' )

file_sample_input_tsv = path.join( input_root, 'file', 'file.sample_id.tsv' )

program_input_tsv = path.join( input_root, 'program', 'program.tsv' )

program_study_name_input_tsv = path.join( input_root, 'program', 'program.study_name.tsv' )

# Initial data structures

study = load_tsv_as_dict( study_input_tsv )

program = load_tsv_as_dict( program_input_tsv )

# result[key_one][key_two] = dict( zip( colnames, values ) )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv, id_column_count=2 )

sample = load_tsv_as_dict( sample_input_tsv, id_column_count=2 )

# result[qualifier][id_one].add( id_two )

participant_diagnosis = load_qualified_id_association( participant_diagnosis_input_tsv, qualifier_field_name='study_name', id_one_field_name='participant_id', id_two_field_name='diagnosis_id' )

participant_sample = load_qualified_id_association( participant_sample_input_tsv, qualifier_field_name='study_name', id_one_field_name='participant_id', id_two_field_name='sample_id' )

sample_participant = load_qualified_id_association( participant_sample_input_tsv, qualifier_field_name='study_name', id_one_field_name='sample_id', id_two_field_name='participant_id' )

participant_study = map_columns_one_to_many( study_participant_input_tsv, 'participant_id', 'study_name' )

file_sample = load_qualified_id_association( file_sample_input_tsv, qualifier_field_name='study_name', id_one_field_name='file_id', id_two_field_name='sample_id' )

sample_file = load_qualified_id_association( file_sample_input_tsv, qualifier_field_name='study_name', id_one_field_name='sample_id', id_two_field_name='file_id' )

study_program = map_columns_one_to_one( program_study_name_input_tsv, 'study_name', 'program_name' )

# Output files

output_root = path.join( 'cda_tsvs', 'cds' )

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

cds_aux_dir = path.join( 'auxiliary_metadata', '__CDS_supplemental_metadata' )

entity_ids_projects_and_types = path.join( cds_aux_dir, 'CDS_entity_submitter_id_to_program_and_study.tsv' )

warning_log = path.join( cds_aux_dir, 'warning_log.txt' )

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

participant_file = dict()

for study_name in file_sample:
    
    for file_id in file_sample[study_name]:
        
        for sample_id in file_sample[study_name][file_id]:
            
            # This should break with a key error if study_name is not found in sample_participant.

            if sample_id in sample_participant[study_name]:
                
                for participant_id in sample_participant[study_name][sample_id]:
                    
                    if study_name not in participant_file:
                        
                        participant_file[study_name] = dict()

                    if participant_id not in participant_file[study_name]:
                        
                        participant_file[study_name][participant_id] = set()

                    participant_file[study_name][participant_id].add( file_id )

for output_dir in [ output_root, cds_aux_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Track one record per Subject.

subject_records = dict()

subject_associated_project = dict()
subject_researchsubject = dict()

subject_participant_ids = dict()

file_subject = dict()

file_specimen = dict()

printed_diagnosis = set()
printed_sample = set()

entity_studies_by_type = dict()

with open( participant_input_tsv ) as PARTICIPANT_IN, open( researchsubject_output_tsv, 'w' ) as RS_OUT, open( researchsubject_identifier_output_tsv, 'w' ) as RS_IDENTIFIER, \
    open( researchsubject_diagnosis_output_tsv, 'w' ) as RS_DIAGNOSIS, open( researchsubject_specimen_output_tsv, 'w' ) as RS_SPECIMEN, \
    open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS, open( diagnosis_identifier_output_tsv, 'w' ) as DIAGNOSIS_IDENTIFIER, \
    open( specimen_output_tsv, 'w' ) as SPECIMEN, open( specimen_identifier_output_tsv, 'w' ) as SPECIMEN_IDENTIFIER, open( warning_log, 'w' ) as WARN:
    
    input_column_names = next( PARTICIPANT_IN ).rstrip( '\n' ).split( '\t' )

    print( *rs_output_column_names, sep='\t', file=RS_OUT )
    print( *[ 'researchsubject_id', 'system', 'field_name', 'value' ], sep='\t', file=RS_IDENTIFIER )
    print( *[ 'researchsubject_id', 'diagnosis_id' ], sep='\t', file=RS_DIAGNOSIS )
    print( *[ 'researchsubject_id', 'specimen_id' ], sep='\t', file=RS_SPECIMEN )
    print( *diagnosis_output_column_names, sep='\t', file=DIAGNOSIS )
    print( *[ 'diagnosis_id', 'system', 'field_name', 'value' ], sep='\t', file=DIAGNOSIS_IDENTIFIER )
    print( *[ 'id', 'associated_project', 'days_to_collection', 'primary_disease_type', 'anatomical_site', 'source_material_type', 'specimen_type', 'derived_from_specimen', 'derived_from_subject' ], sep='\t', file=SPECIMEN )
    print( *[ 'specimen_id', 'system', 'field_name', 'value' ], sep='\t', file=SPECIMEN_IDENTIFIER )

    for line in [ next_line.rstrip( '\n' ) for next_line in PARTICIPANT_IN ]:
        
        input_participant_record = dict( zip( input_column_names, line.split( '\t' ) ) )

        study_name = input_participant_record['study_name']

        participant_id = input_participant_record['participant_id']

        if 'participant' not in entity_studies_by_type:
            
            entity_studies_by_type['participant'] = dict()

        if participant_id not in entity_studies_by_type['participant']:
            
            entity_studies_by_type['participant'][participant_id] = set()

        entity_studies_by_type['participant'][participant_id].add( study_name )

        # Build one record per sample; don't duplicate as we iterate across Studies this Case is in.

        sample_records = dict()

        for study_name in sorted( participant_study[participant_id] ):
            
            # CDA ResearchSubject ID: Combination of CDS study dbGaP PHS accession and participant_id (the latter of which is a submitter ID).

            rs_id = f"{study[study_name]['phs_accession']}.{participant_id}"

            # CDA Subject ID: Combination of CDS program acronym and participant_id.

            subject_id = f"{program[study_program[study_name]]['program_acronym']}.{participant_id}"

            if subject_id not in subject_records:
                
                subject_records[subject_id] = dict()
                subject_records[subject_id]['species'] = '' if study[study_name]['organism_species'] is None else study[study_name]['organism_species']
                subject_records[subject_id]['sex'] = '' if input_participant_record['gender'] is None else input_participant_record['gender']
                subject_records[subject_id]['race'] = '' if input_participant_record['race'] is None else input_participant_record['race']
                subject_records[subject_id]['ethnicity'] = '' if input_participant_record['ethnicity'] is None else input_participant_record['ethnicity']
                subject_records[subject_id]['days_to_birth'] = ''
                subject_records[subject_id]['vital_status'] = ''
                subject_records[subject_id]['days_to_death'] = ''
                subject_records[subject_id]['cause_of_death'] = ''

                subject_associated_project[subject_id] = set()
                subject_researchsubject[subject_id] = set()

                subject_participant_ids[subject_id] = set()

            if participant_id in participant_diagnosis[study_name]:
                
                for diagnosis_id in sorted( participant_diagnosis[study_name][participant_id] ):
                    
                    if 'diagnosis' not in entity_studies_by_type:
                        
                        entity_studies_by_type['diagnosis'] = dict()

                    if diagnosis_id not in entity_studies_by_type['diagnosis']:
                        
                        entity_studies_by_type['diagnosis'][diagnosis_id] = set()

                    entity_studies_by_type['diagnosis'][diagnosis_id].add( study_name )

                    if diagnosis_id in diagnosis[study_name] and diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] is not None and diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] != '':
                        
                        # All CDS values are currently in years.

                        days_to_birth = int( float( diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] ) * 365.25 )

                        if subject_records[subject_id]['days_to_birth'] != '' and subject_records[subject_id]['days_to_birth'] != days_to_birth:
                            
                            sys.exit( f"FATAL FOR NOW: Subject {subject_id} participant_id ({study_name}) {participant_id} derived days_to_birth value '{days_to_birth}' differs from previously-recorded value '{subject_records[subject_id]['days_to_birth']}'; aborting." )

                        subject_records[subject_id]['days_to_birth'] = days_to_birth

                    if diagnosis_id in diagnosis[study_name] and diagnosis[study_name][diagnosis_id]['vital_status'] is not None and diagnosis[study_name][diagnosis_id]['vital_status'] != '':
                        
                        vital_status = diagnosis[study_name][diagnosis_id]['vital_status']

                        if subject_records[subject_id]['vital_status'] != '' and subject_records[subject_id]['vital_status'] != vital_status:
                            
                            sys.exit( f"FATAL FOR NOW: Subject {subject_id} participant_id ({study_name}) {participant_id} derived vital_status value '{vital_status}' differs from previously-recorded value '{subject_records[subject_id]['vital_status']}'; aborting." )

                        subject_records[subject_id]['vital_status'] = vital_status

            subject_associated_project[subject_id].add( study[study_name]['phs_accession'] )

            subject_researchsubject[subject_id].add( rs_id )

            subject_participant_ids[subject_id].add( participant_id )

            if study_name in participant_file:
                
                if participant_id in participant_file[study_name]:
                    
                    for file_id in participant_file[study_name][participant_id]:
                        
                        if file_id not in file_subject:
                            
                            file_subject[file_id] = set()

                        file_subject[file_id].add( subject_id )

            # Cached CDS identifier: the local participant ID (such as it is).

            print( *[ rs_id, 'CDS', 'participant.participant_id', participant_id ], sep='\t', file=RS_IDENTIFIER )

            # If there's a diagnosis record attached to this participant, connect it to this ResearchSubject record
            # and save its contents to their CDA analogues.

            if participant_id in participant_diagnosis[study_name]:
                
                for diagnosis_id in sorted( participant_diagnosis[study_name][participant_id] ):
                    
                    diagnosis_cda_id = f"{rs_id}.{diagnosis_id}"

                    print( *[ rs_id, diagnosis_cda_id ], sep='\t', file=RS_DIAGNOSIS )

                    if diagnosis_cda_id not in printed_diagnosis:
                        
                        print( *[ diagnosis_cda_id, 'CDS', 'diagnosis.diagnosis_id', diagnosis_id ], sep='\t', file=DIAGNOSIS_IDENTIFIER )

                        diagnosis_record = dict( zip( diagnosis_output_column_names, [ '' ] * len( diagnosis_output_column_names ) ) )

                        diagnosis_record['id'] = diagnosis_cda_id

                        if diagnosis[study_name][diagnosis_id]['primary_diagnosis'] is not None:
                            
                            diagnosis_record['primary_diagnosis'] = diagnosis[study_name][diagnosis_id]['primary_diagnosis']

                        if diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] is not None and diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] != '':
                            
                            diagnosis_record['age_at_diagnosis'] = int( diagnosis[study_name][diagnosis_id]['age_at_diagnosis'] )

                        if diagnosis[study_name][diagnosis_id]['morphology'] is not None:
                            
                            diagnosis_record['morphology'] = diagnosis[study_name][diagnosis_id]['morphology']

                        if diagnosis[study_name][diagnosis_id]['tumor_grade'] is not None:
                            
                            diagnosis_record['grade'] = diagnosis[study_name][diagnosis_id]['tumor_grade']

                        print( *[ diagnosis_record[column_name] for column_name in diagnosis_output_column_names ], sep='\t', file=DIAGNOSIS )

                        printed_diagnosis.add( diagnosis_cda_id )

            # If samples are attached to this participant, connect them to this ResearchSubject record.

            if participant_id in participant_sample[study_name]:
                
                for sample_id in sorted( participant_sample[study_name][participant_id] ):
                    
                    if 'sample' not in entity_studies_by_type:
                        
                        entity_studies_by_type['sample'] = dict()

                    if sample_id not in entity_studies_by_type['sample']:
                        
                        entity_studies_by_type['sample'][sample_id] = set()

                    entity_studies_by_type['sample'][sample_id].add( study_name )

                    sample_cda_id = f"{rs_id}.{sample_id}"

                    print( *[ rs_id, sample_cda_id ], sep='\t', file=RS_SPECIMEN )

                    if study_name not in sample_records:
                        
                        sample_records[study_name] = dict()

                    if sample_id not in sample_records[study_name]:
                        
                        sample_records[study_name][sample_id] = dict()

                        sample_records[study_name][sample_id]['cda_ids'] = { sample_cda_id }
                        sample_records[study_name][sample_id]['associated_project'] = { study[study_name]['phs_accession'] }
                        sample_records[study_name][sample_id]['days_to_collection'] = ''
                        sample_records[study_name][sample_id]['primary_disease_type'] = ''
                        sample_records[study_name][sample_id]['anatomical_site'] = sample[study_name][sample_id]['sample_anatomic_site']
                        sample_records[study_name][sample_id]['source_material_type'] = sample[study_name][sample_id]['sample_tumor_status']
                        sample_records[study_name][sample_id]['specimen_type'] = 'sample'
                        sample_records[study_name][sample_id]['derived_from_specimen'] = 'initial specimen'
                        sample_records[study_name][sample_id]['derived_from_subject'] = subject_id

                    else:
                        
                        sample_records[study_name][sample_id]['cda_ids'].add( sample_cda_id )
                        sample_records[study_name][sample_id]['associated_project'].add( study[study_name]['phs_accession'] )

            # Write the main ResearchSubject record.

            output_record = dict()

            output_record['id'] = rs_id
            output_record['member_of_research_project'] = study[study_name]['phs_accession']

            output_record['primary_diagnosis_condition'] = ''
            output_record['primary_diagnosis_site'] = ''

            if participant_id in participant_diagnosis[study_name]:
                
                for diagnosis_id in sorted( participant_diagnosis[study_name][participant_id] ):
                    
                    if diagnosis_id in diagnosis[study_name] and diagnosis[study_name][diagnosis_id]['disease_type'] is not None and diagnosis[study_name][diagnosis_id]['disease_type'] != '':
                        
                        primary_diagnosis_condition = diagnosis[study_name][diagnosis_id]['disease_type']

                        if output_record['primary_diagnosis_condition'] != '' and output_record['primary_diagnosis_condition'] != primary_diagnosis_condition:
                            
                            sys.exit( f"FATAL FOR NOW: RS {rs_id} participant_id ({study_name}) {participant_id} diagnosis.disease_type value '{primary_diagnosis_condition}' differs from previously-recorded value '{output_record['primary_diagnosis_condition']}'; aborting." )

                        output_record['primary_diagnosis_condition'] = primary_diagnosis_condition

                    if diagnosis_id in diagnosis[study_name] and diagnosis[study_name][diagnosis_id]['primary_site'] is not None and diagnosis[study_name][diagnosis_id]['primary_site'] != '':
                        
                        primary_diagnosis_site = diagnosis[study_name][diagnosis_id]['primary_site']

                        if output_record['primary_diagnosis_site'] != '' and output_record['primary_diagnosis_site'] != primary_diagnosis_site:
                            
                            print( f"WARNING: RS {rs_id} participant_id ({study_name}) {participant_id} diagnosis.primary_site value '{primary_diagnosis_site}' differs from previously-recorded value '{output_record['primary_diagnosis_site']}'; overwriting with new value.", file=WARN )

                        output_record['primary_diagnosis_site'] = primary_diagnosis_site

            print( *[ output_record[column_name] for column_name in rs_output_column_names ], sep='\t', file=RS_OUT )

        if study_name in sample_records:

            # Write the Specimen records collected for this ResearchSubject.

            seen_specimen_cda_ids = set()

            for sample_id in sorted( sample_records[study_name] ):
                
                for sample_cda_id in sorted( sample_records[study_name][sample_id]['cda_ids'] ):
                    
                    if sample_cda_id in seen_specimen_cda_ids:
                        
                        sys.exit(f"FATAL FOR NOW: Saw {sample_cda_id} more than once, aborting.\n")

                    seen_specimen_cda_ids.add( sample_cda_id )

                    current_row = sample_records[study_name][sample_id]

                    print( *[ sample_cda_id, ';'.join( sorted( current_row['associated_project'] ) ), current_row['days_to_collection'], current_row['primary_disease_type'], current_row['anatomical_site'], current_row['source_material_type'], current_row['specimen_type'], current_row['derived_from_specimen'], current_row['derived_from_subject'] ], sep='\t', file=SPECIMEN )

                    print( *[ sample_cda_id, 'CDS', 'sample.sample_id', sample_id ], sep='\t', file=SPECIMEN_IDENTIFIER )

                    if sample_id in sample_file[study_name]:
                        
                        for file_id in sample_file[study_name][sample_id]:
                            
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
            
            for study_name in sorted( entity_studies_by_type[entity_type][entity_id] ):
                
                program_acronym = program[study_program[study_name]]['program_acronym']

                study_id = study[study_name]['phs_accession']

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

        for participant_id in sorted( subject_participant_ids[subject_id] ):
            
            print( *[ subject_id, 'CDS', 'participant.participant_id', participant_id ], sep='\t', file=SUBJECT_IDENTIFIER )

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


