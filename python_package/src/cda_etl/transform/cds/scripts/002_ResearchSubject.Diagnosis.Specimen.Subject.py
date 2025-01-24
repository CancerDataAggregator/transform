#!/usr/bin/env python -u

import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, map_columns_one_to_many, load_qualified_id_association

# PARAMETERS

input_root = path.join( 'extracted_data', 'cds' )

diagnosis_input_tsv = path.join( input_root, 'diagnosis.tsv' )

diagnosis_participant_input_tsv = path.join( input_root, 'diagnosis_of_participant.tsv' )

file_input_tsv = path.join( input_root, 'file.tsv' )

file_sample_input_tsv = path.join( input_root, 'file_from_sample.tsv' )

participant_input_tsv = path.join( input_root, 'participant.tsv' )

participant_study_input_tsv = path.join( input_root, 'participant_in_study.tsv' )

program_input_tsv = path.join( input_root, 'program.tsv' )

sample_input_tsv = path.join( input_root, 'sample.tsv' )

sample_participant_input_tsv = path.join( input_root, 'sample_from_participant.tsv' )

study_input_tsv = path.join( input_root, 'study.tsv' )

study_program_input_tsv = path.join( input_root, 'study_in_program.tsv' )

# Initial data structures

study = load_tsv_as_dict( study_input_tsv )

study_name_to_study_uuid = map_columns_one_to_one( study_input_tsv, 'study_name', 'uuid' )

program = load_tsv_as_dict( program_input_tsv )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

sample = load_tsv_as_dict( sample_input_tsv )

file_uuid_to_file_id = map_columns_one_to_one( file_input_tsv, 'uuid', 'file_id' )

file_sample = map_columns_one_to_many( file_sample_input_tsv, 'file_uuid', 'sample_uuid' )
sample_file = map_columns_one_to_many( file_sample_input_tsv, 'sample_uuid', 'file_uuid' )

participant_diagnosis = map_columns_one_to_many( diagnosis_participant_input_tsv, 'participant_uuid', 'diagnosis_uuid' )

participant_sample = map_columns_one_to_many( sample_participant_input_tsv, 'participant_uuid', 'sample_uuid' )
sample_participant = map_columns_one_to_many( sample_participant_input_tsv, 'sample_uuid', 'participant_uuid' )

# Note assumption: each participant is in exactly one study. True at time of writing (2024-06).

participant_study = map_columns_one_to_one( participant_study_input_tsv, 'participant_uuid', 'study_uuid' )

study_program = map_columns_one_to_one( study_program_input_tsv, 'study_uuid', 'program_uuid' )

participant_file = dict()

for file_uuid in file_sample:
    
    for sample_uuid in file_sample[file_uuid]:
        
        if sample_uuid in sample_participant:
            
            for participant_uuid in sample_participant[sample_uuid]:
                
                if participant_uuid not in participant_file:
                    
                    participant_file[participant_uuid] = set()

                participant_file[participant_uuid].add( file_uuid )

# Output files

output_root = path.join( 'cda_tsvs', 'cds_raw_unharmonized' )

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

warning_log = path.join( cds_aux_dir, 'cds_subject_warning_log.txt' )

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

for output_dir in [ output_root, cds_aux_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Track one record per Subject.

subject_records = dict()

subject_associated_project = dict()
subject_researchsubject = dict()

subject_participant_uuids = dict()
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

        participant_uuid = input_participant_record['uuid']

        participant_id = input_participant_record['participant_id']

        study_uuid = participant_study[participant_uuid]

        study_name = study[study_uuid]['study_name']

        if 'participant' not in entity_studies_by_type:
            
            entity_studies_by_type['participant'] = dict()

        if participant_id not in entity_studies_by_type['participant']:
            
            entity_studies_by_type['participant'][participant_id] = set()

        entity_studies_by_type['participant'][participant_id].add( study_name )

        # Build one record per sample.

        sample_records = dict()

        # CDA ResearchSubject ID: Combination of CDS study dbGaP PHS accession and participant_id (the latter of which is a submitter ID).

        rs_id = f"{study[study_uuid]['phs_accession']}.{participant_id}"

        # CDA Subject ID: Combination of CDS program acronym and participant_id.

        subject_id = f"{program[study_program[study_uuid]]['program_acronym']}.{participant_id}"

        if subject_id not in subject_records:
            
            subject_records[subject_id] = dict()
            subject_records[subject_id]['species'] = '' if study[study_uuid]['organism_species'] is None else study[study_uuid]['organism_species']
            subject_records[subject_id]['sex'] = '' if input_participant_record['gender'] is None else input_participant_record['gender']
            subject_records[subject_id]['race'] = '' if input_participant_record['race'] is None else input_participant_record['race']
            subject_records[subject_id]['ethnicity'] = '' if input_participant_record['ethnicity'] is None else input_participant_record['ethnicity']
            subject_records[subject_id]['days_to_birth'] = ''
            subject_records[subject_id]['vital_status'] = ''
            subject_records[subject_id]['days_to_death'] = ''
            subject_records[subject_id]['cause_of_death'] = ''

            subject_associated_project[subject_id] = set()
            subject_researchsubject[subject_id] = set()

            subject_participant_uuids[subject_id] = set()
            subject_participant_ids[subject_id] = set()

        if participant_uuid in participant_diagnosis:
            
            for diagnosis_uuid in sorted( participant_diagnosis[participant_uuid] ):
                
                diagnosis_id = diagnosis[diagnosis_uuid]['diagnosis_id']

                if 'diagnosis' not in entity_studies_by_type:
                    
                    entity_studies_by_type['diagnosis'] = dict()

                if diagnosis_id not in entity_studies_by_type['diagnosis']:
                    
                    entity_studies_by_type['diagnosis'][diagnosis_id] = set()

                entity_studies_by_type['diagnosis'][diagnosis_id].add( study_name )

                if diagnosis[diagnosis_uuid]['age_at_diagnosis'] is not None and diagnosis[diagnosis_uuid]['age_at_diagnosis'] != '':
                    
                    # All CDS values are currently in years.

                    days_to_birth = int( float( diagnosis[diagnosis_uuid]['age_at_diagnosis'] ) * 365.25 )

                    if subject_records[subject_id]['days_to_birth'] != '' and subject_records[subject_id]['days_to_birth'] != days_to_birth:
                        
                        print( f"WARNING: Subject {subject_id} participant_id ({study_name}) {participant_id} derived days_to_birth value '{days_to_birth}' differs from previously-recorded value '{subject_records[subject_id]['days_to_birth']}'; ignoring new value.", file=WARN )

                    else:
                        
                        subject_records[subject_id]['days_to_birth'] = days_to_birth

                if diagnosis[diagnosis_uuid]['vital_status'] is not None and diagnosis[diagnosis_uuid]['vital_status'] != '':
                    
                    vital_status = diagnosis[diagnosis_uuid]['vital_status']

                    if subject_records[subject_id]['vital_status'] != '' and subject_records[subject_id]['vital_status'] != vital_status:
                        
                        print( f"WARNING: Subject {subject_id} participant_id ({study_name}) {participant_id} derived vital_status value '{vital_status}' differs from previously-recorded value '{subject_records[subject_id]['vital_status']}'; ignoring new value.", file=WARN )

                    else:
                        
                        subject_records[subject_id]['vital_status'] = vital_status

        subject_associated_project[subject_id].add( study[study_uuid]['phs_accession'] )

        subject_researchsubject[subject_id].add( rs_id )

        subject_participant_uuids[subject_id].add( participant_uuid )

        subject_participant_ids[subject_id].add( participant_id )

        if participant_uuid in participant_file:
            
            for file_uuid in participant_file[participant_uuid]:
                
                file_id = file_uuid_to_file_id[file_uuid]

                if file_id not in file_subject:
                    
                    file_subject[file_id] = set()

                file_subject[file_id].add( subject_id )

        # Cached CDS identifier: the UUID and the submitter-furnished participant_id (such as it is).

        print( *[ rs_id, 'CDS', 'participant.uuid', participant_uuid ], sep='\t', file=RS_IDENTIFIER )

        print( *[ rs_id, 'CDS', 'participant.participant_id', participant_id ], sep='\t', file=RS_IDENTIFIER )

        # If there's a diagnosis record attached to this participant, connect it to this ResearchSubject record
        # and save its contents to their CDA analogues.

        if participant_uuid in participant_diagnosis:
            
            for diagnosis_uuid in sorted( participant_diagnosis[participant_uuid] ):
                
                diagnosis_id = diagnosis[diagnosis_uuid]['diagnosis_id']

                diagnosis_cda_id = f"{rs_id}.{diagnosis_uuid}"

                print( *[ rs_id, diagnosis_cda_id ], sep='\t', file=RS_DIAGNOSIS )

                if diagnosis_cda_id not in printed_diagnosis:
                    
                    print( *[ diagnosis_cda_id, 'CDS', 'diagnosis.uuid', diagnosis_uuid ], sep='\t', file=DIAGNOSIS_IDENTIFIER )

                    print( *[ diagnosis_cda_id, 'CDS', 'diagnosis.diagnosis_id', diagnosis_id ], sep='\t', file=DIAGNOSIS_IDENTIFIER )

                    diagnosis_record = dict( zip( diagnosis_output_column_names, [ '' ] * len( diagnosis_output_column_names ) ) )

                    diagnosis_record['id'] = diagnosis_cda_id

                    if diagnosis[diagnosis_uuid]['primary_diagnosis'] is not None:
                        
                        diagnosis_record['primary_diagnosis'] = diagnosis[diagnosis_uuid]['primary_diagnosis']

                    if diagnosis[diagnosis_uuid]['age_at_diagnosis'] is not None and diagnosis[diagnosis_uuid]['age_at_diagnosis'] != '':
                        
                        diagnosis_record['age_at_diagnosis'] = int( diagnosis[diagnosis_uuid]['age_at_diagnosis'] )

                    if diagnosis[diagnosis_uuid]['morphology'] is not None:
                        
                        diagnosis_record['morphology'] = diagnosis[diagnosis_uuid]['morphology']

                    if diagnosis[diagnosis_uuid]['tumor_grade'] is not None:
                        
                        diagnosis_record['grade'] = diagnosis[diagnosis_uuid]['tumor_grade']

                    if diagnosis[diagnosis_uuid]['tumor_stage_clinical_m'] is not None:
                        
                        diagnosis_record['stage'] = diagnosis[diagnosis_uuid]['tumor_stage_clinical_m']

                    print( *[ diagnosis_record[column_name] for column_name in diagnosis_output_column_names ], sep='\t', file=DIAGNOSIS )

                    printed_diagnosis.add( diagnosis_cda_id )

        # If samples are attached to this participant, connect them to this ResearchSubject record.

        if participant_uuid in participant_sample:
            
            for sample_uuid in sorted( participant_sample[participant_uuid] ):
                
                sample_id = sample[sample_uuid]['sample_id']

                if 'sample' not in entity_studies_by_type:
                    
                    entity_studies_by_type['sample'] = dict()

                if sample_id not in entity_studies_by_type['sample']:
                    
                    entity_studies_by_type['sample'][sample_id] = set()

                entity_studies_by_type['sample'][sample_id].add( study_name )

                sample_cda_id = f"{rs_id}.{sample_uuid}"

                print( *[ rs_id, sample_cda_id ], sep='\t', file=RS_SPECIMEN )

                if sample_uuid not in sample_records:
                    
                    sample_records[sample_uuid] = dict()

                    sample_records[sample_uuid]['cda_ids'] = { sample_cda_id }
                    sample_records[sample_uuid]['associated_project'] = { study[study_uuid]['phs_accession'] }
                    sample_records[sample_uuid]['days_to_collection'] = ''
                    sample_records[sample_uuid]['primary_disease_type'] = ''
                    sample_records[sample_uuid]['anatomical_site'] = sample[sample_uuid]['sample_anatomic_site']
                    sample_records[sample_uuid]['source_material_type'] = sample[sample_uuid]['sample_tumor_status']
                    sample_records[sample_uuid]['specimen_type'] = 'sample'
                    sample_records[sample_uuid]['derived_from_specimen'] = 'initial specimen'
                    sample_records[sample_uuid]['derived_from_subject'] = subject_id

                else:
                    
                    sample_records[sample_uuid]['cda_ids'].add( sample_cda_id )
                    sample_records[sample_uuid]['associated_project'].add( study[study_uuid]['phs_accession'] )

        # Write the main ResearchSubject record.

        output_record = dict()

        output_record['id'] = rs_id
        output_record['member_of_research_project'] = study[study_uuid]['phs_accession']

        output_record['primary_diagnosis_condition'] = ''
        output_record['primary_diagnosis_site'] = ''

        if participant_uuid in participant_diagnosis:
            
            for diagnosis_uuid in sorted( participant_diagnosis[participant_uuid] ):
                
                if diagnosis[diagnosis_uuid]['disease_type'] is not None and diagnosis[diagnosis_uuid]['disease_type'] != '':
                    
                    # if participant_id == 'C3L-00004':
                    #     
                    #     print( *[ f"'{study_name}'", f"'{diagnosis_uuid}'", f"'{diagnosis[diagnosis_uuid]['disease_type']}'" ], sep=' | ', file=sys.stderr )

                    primary_diagnosis_condition = diagnosis[diagnosis_uuid]['disease_type']

                    if output_record['primary_diagnosis_condition'] != '' and output_record['primary_diagnosis_condition'] != primary_diagnosis_condition:
                        
                        print( f"WARNING: RS {rs_id} participant_id ({study_name}) {participant_id} diagnosis.disease_type value '{primary_diagnosis_condition}' differs from previously-recorded value '{output_record['primary_diagnosis_condition']}'; ignoring new value.", file=WARN )

                    else:
                        
                        output_record['primary_diagnosis_condition'] = primary_diagnosis_condition

                if diagnosis[diagnosis_uuid]['primary_site'] is not None and diagnosis[diagnosis_uuid]['primary_site'] != '':
                    
                    primary_diagnosis_site = diagnosis[diagnosis_uuid]['primary_site']

                    if output_record['primary_diagnosis_site'] != '' and output_record['primary_diagnosis_site'] != primary_diagnosis_site:
                        
                        print( f"WARNING: RS {rs_id} participant_id ({study_name}) {participant_id} diagnosis.primary_site value '{primary_diagnosis_site}' differs from previously-recorded value '{output_record['primary_diagnosis_site']}'; overwriting with new value.", file=WARN )

                    output_record['primary_diagnosis_site'] = primary_diagnosis_site

        print( *[ output_record[column_name] for column_name in rs_output_column_names ], sep='\t', file=RS_OUT )

        if len( sample_records ) > 0:

            # Write the Specimen records collected for this ResearchSubject.

            seen_specimen_cda_ids = set()

            for sample_uuid in sorted( sample_records ):
                
                for sample_cda_id in sorted( sample_records[sample_uuid]['cda_ids'] ):
                    
                    if sample_cda_id in seen_specimen_cda_ids:
                        
                        sys.exit(f"FATAL FOR NOW: Saw {sample_cda_id} more than once, aborting.\n")

                    seen_specimen_cda_ids.add( sample_cda_id )

                    current_row = sample_records[sample_uuid]

                    print( *[ sample_cda_id, ';'.join( sorted( current_row['associated_project'] ) ), current_row['days_to_collection'], current_row['primary_disease_type'], current_row['anatomical_site'], current_row['source_material_type'], current_row['specimen_type'], current_row['derived_from_specimen'], current_row['derived_from_subject'] ], sep='\t', file=SPECIMEN )

                    print( *[ sample_cda_id, 'CDS', 'sample.uuid', sample_uuid ], sep='\t', file=SPECIMEN_IDENTIFIER )

                    print( *[ sample_cda_id, 'CDS', 'sample.sample_id', sample[sample_uuid]['sample_id'] ], sep='\t', file=SPECIMEN_IDENTIFIER )

                    if sample_uuid in sample_file:
                        
                        for file_uuid in sample_file[sample_uuid]:
                            
                            file_id = file_uuid_to_file_id[file_uuid]

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
                
                study_uuid = study_name_to_study_uuid[study_name]

                program_acronym = program[study_program[study_uuid]]['program_acronym']

                phs_accession = study[study_uuid]['phs_accession']

                print( *[ entity_type, entity_id, program_acronym, study_name, phs_accession ], sep='\t', file=ENTITIES_BY_STUDY )

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

        for participant_uuid in sorted( subject_participant_uuids[subject_id] ):
            
            print( *[ subject_id, 'CDS', 'participant.uuid', participant_uuid ], sep='\t', file=SUBJECT_IDENTIFIER )

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


