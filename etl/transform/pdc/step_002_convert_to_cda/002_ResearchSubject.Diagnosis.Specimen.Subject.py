#!/usr/bin/env python -u

import sys

from os import makedirs, path

# SUBROUTINES

def map_columns_one_to_one( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            return_map[current_from] = current_to

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field ):
    
    return_map = dict()

    with open( input_file ) as IN:
        
        header = next(IN).rstrip('\n')

        column_names = header.split('\t')

        if from_field not in column_names or to_field not in column_names:
            
            sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            current_from = ''

            current_to = ''

            for i in range( 0, len( column_names ) ):
                
                if column_names[i] == from_field:
                    
                    current_from = values[i]

                if column_names[i] == to_field:
                    
                    current_to = values[i]

            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add(current_to)

    return return_map

def load_tsv_as_dict( input_file ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next(IN).rstrip('\n').split('\t')

        for line in [ next_line.rstrip('\n') for next_line in IN ]:
            
            values = line.split('\t')

            # Assumes first column is a unique ID column; if this
            # doesn't end up being true, only the last record will
            # be stored for any repeated ID.

            result[values[0]] = dict( zip( colnames, values ) )

    return result

# PARAMETERS

input_root = 'merged_data/pdc'

case_input_tsv = path.join( input_root, 'Case', 'Case.tsv' )

case_demographic_input_tsv = path.join( input_root, 'Case', 'Case.demographic_id.tsv' )

case_diagnosis_input_tsv = path.join( input_root, 'Case', 'Case.diagnosis_id.tsv' )

case_sample_input_tsv = path.join( input_root, 'Case', 'Case.sample_id.tsv' )

case_study_input_tsv = path.join( input_root, 'Case', 'Case.study_id.tsv' )

aliquot_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.tsv' )

aliquot_case_input_tsv = path.join( input_root, 'Aliquot', 'Aliquot.case_id.tsv' )

demographic_input_tsv = path.join( input_root, 'Demographic', 'Demographic.tsv' )

diagnosis_input_tsv = path.join( input_root, 'Diagnosis', 'Diagnosis.tsv' )

file_case_input_tsv = path.join( input_root, 'File', 'File.case_id.tsv' )

file_aliquot_input_tsv = path.join( input_root, 'File', 'File.aliquot_id.tsv' )

project_input_tsv = path.join( input_root, 'Project', 'Project.tsv' )

project_study_input_tsv = path.join( input_root, 'Project', 'Project.study_id.tsv' )

sample_input_tsv = path.join( input_root, 'Sample', 'Sample.tsv' )

sample_aliquot_input_tsv = path.join( input_root, 'Sample', 'Sample.aliquot_id.tsv' )

study_input_tsv = path.join( input_root, 'Study', 'Study.tsv' )

gdc_to_pdc_project_input_tsv = 'naive_GDC-PDC_project_id_map.hand_edited_to_remove_false_positives.tsv'

pdc_study_to_gdc_program = map_columns_one_to_one( gdc_to_pdc_project_input_tsv, 'PDC_pdc_study_id', 'GDC_program_name' )

demographic = load_tsv_as_dict( demographic_input_tsv )

diagnosis = load_tsv_as_dict( diagnosis_input_tsv )

sample = load_tsv_as_dict( sample_input_tsv )

case_demographic = map_columns_one_to_one( case_demographic_input_tsv, 'case_id', 'demographic_id' )

case_diagnosis = map_columns_one_to_one( case_diagnosis_input_tsv, 'case_id', 'diagnosis_id' )

case_sample = map_columns_one_to_many( case_sample_input_tsv, 'case_id', 'sample_id' )

case_study = map_columns_one_to_many( case_study_input_tsv, 'case_id', 'study_id' )

case_file = map_columns_one_to_many( file_case_input_tsv, 'case_id', 'file_id' )

aliquot_file = map_columns_one_to_many( file_aliquot_input_tsv, 'aliquot_id', 'file_id' )

study_project = map_columns_one_to_one( project_study_input_tsv, 'study_id', 'project_id' )

sample_aliquot = map_columns_one_to_many( sample_aliquot_input_tsv, 'sample_id', 'aliquot_id' )

aliquot_sample = map_columns_one_to_one( sample_aliquot_input_tsv, 'aliquot_id', 'sample_id' )

aliquot_submitter_id = map_columns_one_to_one( aliquot_input_tsv, 'aliquot_id', 'aliquot_submitter_id' )

diagnosis_submitter_id = map_columns_one_to_one( diagnosis_input_tsv, 'diagnosis_id', 'diagnosis_submitter_id' )

project_submitter_id = map_columns_one_to_one( project_input_tsv, 'project_id', 'project_submitter_id' )

sample_submitter_id = map_columns_one_to_one( sample_input_tsv, 'sample_id', 'sample_submitter_id' )

pdc_study_id = map_columns_one_to_one( study_input_tsv, 'study_id', 'pdc_study_id' )

output_root = 'transformed_data/pdc'

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

subject_gdc_candidate_output_tsv = path.join( output_root, 'subject_to_GDC_subject_if_latter_exists.tsv' )

output_column_names = [
    'id',
    'member_of_research_project',
    'primary_diagnosis_condition',
    'primary_diagnosis_site'
]

# EXECUTION

for output_dir in [ output_root ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

# Track one record per Subject.

subject_records = dict()

subject_associated_project = dict()
subject_demographic = dict()
subject_researchsubject = dict()

subject_case_ids = dict()
subject_case_submitter_ids = dict()

subject_gdc_candidate_match = dict()

file_subject = dict()

file_specimen = dict()

printed_diagnosis = set()
printed_sample = set()

with open( case_input_tsv ) as CASE_IN, open( researchsubject_output_tsv, 'w' ) as RS_OUT, open( researchsubject_identifier_output_tsv, 'w' ) as RS_IDENTIFIER, \
    open( researchsubject_diagnosis_output_tsv, 'w' ) as RS_DIAGNOSIS, open( researchsubject_specimen_output_tsv, 'w' ) as RS_SPECIMEN, \
    open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS, open( diagnosis_identifier_output_tsv, 'w' ) as DIAGNOSIS_IDENTIFIER, \
    open( specimen_output_tsv, 'w' ) as SPECIMEN, open( specimen_identifier_output_tsv, 'w' ) as SPECIMEN_IDENTIFIER:
    
    header = next(CASE_IN).rstrip('\n')

    input_column_names = header.split('\t')

    print( *output_column_names, sep='\t', end='\n', file=RS_OUT )
    print( *[ 'researchsubject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=RS_IDENTIFIER )
    print( *[ 'researchsubject_id', 'diagnosis_id' ], sep='\t', end='\n', file=RS_DIAGNOSIS )
    print( *[ 'researchsubject_id', 'specimen_id' ], sep='\t', end='\n', file=RS_SPECIMEN )
    print( *[ 'id', 'primary_diagnosis', 'age_at_diagnosis', 'morphology', 'stage', 'grade', 'method_of_diagnosis' ], sep='\t', end='\n', file=DIAGNOSIS )
    print( *[ 'diagnosis_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=DIAGNOSIS_IDENTIFIER )
    print( *[ 'id', 'associated_project', 'days_to_collection', 'primary_disease_type', 'anatomical_site', 'source_material_type', 'specimen_type', 'derived_from_specimen', 'derived_from_subject' ], sep='\t', end='\n', file=SPECIMEN )
    print( *[ 'specimen_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

    for line in [ next_line.rstrip('\n') for next_line in CASE_IN ]:
        
        input_case_record = dict( zip( input_column_names, line.split('\t') ) )

        case_id = input_case_record['case_id']

        case_submitter_id = input_case_record['case_submitter_id']

        # Build one record per sample; don't duplicate as we iterate across Studies this Case is in.

        sample_records = dict()

        aliquot_records = dict()

        for study_id in sorted( case_study[case_id] ):
            
            # CDA ResearchSubject ID: Combination of PDC Study ID and case_submitter_id.

            rs_id = f"{pdc_study_id[study_id]}.{case_submitter_id}"

            # CDA Subject ID: Combination of PDC Project ID and case_submitter_id.

            subject_id = f"{project_submitter_id[study_project[study_id]]}.{case_submitter_id}"

            if pdc_study_id[study_id] in pdc_study_to_gdc_program:
                
                gdc_program = pdc_study_to_gdc_program[pdc_study_id[study_id]]

                if subject_id not in subject_gdc_candidate_match:
                    
                    subject_gdc_candidate_match[subject_id] = set()

                subject_gdc_candidate_match[subject_id].add( f"{gdc_program}.{case_submitter_id}" )

            if subject_id not in subject_records:
                
                subject_records[subject_id] = dict()
                subject_records[subject_id]['species'] = ''
                subject_records[subject_id]['sex'] = ''
                subject_records[subject_id]['race'] = ''
                subject_records[subject_id]['ethnicity'] = ''
                subject_records[subject_id]['days_to_birth'] = ''
                subject_records[subject_id]['vital_status'] = ''
                subject_records[subject_id]['days_to_death'] = ''
                subject_records[subject_id]['cause_of_death'] = ''

                subject_associated_project[subject_id] = set()
                subject_demographic[subject_id] = set()
                subject_researchsubject[subject_id] = set()

                subject_case_ids[subject_id] = set()
                subject_case_submitter_ids[subject_id] = set()

            if case_id in case_sample:
                
                for sample_id in sorted( case_sample[case_id] ):
                    
                    if sample_id in sample and sample[sample_id]['taxon'] is not None and sample[sample_id]['taxon'] != '':
                        
                        if subject_records[subject_id]['species'] != '' and subject_records[subject_id]['species'] != sample[sample_id]['taxon']:
                            
                            sys.exit( f"FATAL FOR NOW: Subject {subject_id} constituent case_id {case_id} labeled '{sample[sample_id]['taxon']}', which does not match previously-recorded taxon for this subject '{subject_records[subject_id]['species']}'; aborting.\n" )

                        subject_records[subject_id]['species'] = sample[sample_id]['taxon']

            subject_associated_project[subject_id].add( pdc_study_id[study_id] )
            subject_demographic[subject_id].add( case_demographic[case_id] )
            subject_researchsubject[subject_id].add( rs_id )

            subject_case_ids[subject_id].add( case_id )
            subject_case_submitter_ids[subject_id].add( case_submitter_id )

            for file_id in case_file[case_id]:
                
                if file_id not in file_subject:
                    
                    file_subject[file_id] = set()

                file_subject[file_id].add( subject_id )

            # Cached PDC identifier: the local PDC case ID.

            print( *[ rs_id, 'PDC', 'Case.case_id', case_id ], sep='\t', end='\n', file=RS_IDENTIFIER )

            # Second cached PDC identifier: the submitter ID.

            print( *[ rs_id, 'PDC', 'Case.case_submitter_id', input_case_record['case_submitter_id'] ], sep='\t', end='\n', file=RS_IDENTIFIER )

            # If there's a diagnosis record attached to this case, connect it to this ResearchSubject record
            # and save its contents to their CDA analogues.

            if case_id in case_diagnosis:
                
                diagnosis_pdc_id = case_diagnosis[case_id]

                diagnosis_cda_id = f"{rs_id}.{diagnosis_submitter_id[diagnosis_pdc_id]}"

                print( *[ rs_id, diagnosis_cda_id ], sep='\t', end='\n', file=RS_DIAGNOSIS )

                if diagnosis_cda_id not in printed_diagnosis:
                    
                    print( *[ diagnosis_cda_id, 'PDC', 'Diagnosis.diagnosis_id', diagnosis_pdc_id ], sep='\t', end='\n', file=DIAGNOSIS_IDENTIFIER )

                    print( *[ diagnosis_cda_id, 'PDC', 'Diagnosis.diagnosis_submitter_id', diagnosis[diagnosis_pdc_id]['diagnosis_submitter_id'] ], sep='\t', end='\n', file=DIAGNOSIS_IDENTIFIER )

                    print( *[ diagnosis_cda_id, diagnosis[diagnosis_pdc_id]['primary_diagnosis'], diagnosis[diagnosis_pdc_id]['age_at_diagnosis'], \
                        diagnosis[diagnosis_pdc_id]['morphology'], diagnosis[diagnosis_pdc_id]['tumor_stage'], diagnosis[diagnosis_pdc_id]['tumor_grade'], diagnosis[diagnosis_pdc_id]['method_of_diagnosis'] ], sep='\t', end='\n', file=DIAGNOSIS )

                    printed_diagnosis.add( diagnosis_cda_id )

            # If samples are attached to this case, connect them and any downstream aliquots to this ResearchSubject record.

            if case_id in case_sample:
                
                for sample_id in sorted( case_sample[case_id] ):
                    
                    sample_cda_id = f"{rs_id}.{sample_submitter_id[sample_id]}"

                    print( *[ rs_id, sample_cda_id ], sep='\t', end='\n', file=RS_SPECIMEN )

                    if sample_id not in sample_records:
                        
                        sample_records[sample_id] = dict()

                        sample_records[sample_id]['cda_ids'] = { sample_cda_id }
                        sample_records[sample_id]['associated_project'] = set()
                        sample_records[sample_id]['associated_project'].add(pdc_study_id[study_id])
                        sample_records[sample_id]['days_to_collection'] = sample[sample_id]['days_to_sample_procurement']

                        if sample_records[sample_id]['days_to_collection'] == '':
                            
                            sample_records[sample_id]['days_to_collection'] = sample[sample_id]['days_to_collection']

                        sample_records[sample_id]['primary_disease_type'] = input_case_record['disease_type']
                        sample_records[sample_id]['anatomical_site'] = sample[sample_id]['biospecimen_anatomic_site']
                        sample_records[sample_id]['source_material_type'] = sample[sample_id]['sample_type']
                        sample_records[sample_id]['specimen_type'] = 'sample'
                        sample_records[sample_id]['derived_from_specimen'] = 'initial specimen'
                        sample_records[sample_id]['derived_from_subject'] = subject_id

                    else:
                        
                        sample_records[sample_id]['cda_ids'].add( sample_cda_id )
                        sample_records[sample_id]['associated_project'].add(pdc_study_id[study_id])

                    if sample_id in sample_aliquot:
                        
                        for aliquot_id in sorted( sample_aliquot[sample_id] ):
                            
                            aliquot_cda_id = f"{rs_id}.{aliquot_submitter_id[aliquot_id]}"

                            print( *[ rs_id, aliquot_cda_id ], sep='\t', end='\n', file=RS_SPECIMEN )

                            if aliquot_id not in aliquot_records:
                                
                                aliquot_records[aliquot_id] = dict()

                                aliquot_records[aliquot_id]['cda_ids'] = { aliquot_cda_id }
                                aliquot_records[aliquot_id]['associated_project'] = set()
                                aliquot_records[aliquot_id]['associated_project'].add(pdc_study_id[study_id])
                                aliquot_records[aliquot_id]['days_to_collection'] = ''
                                aliquot_records[aliquot_id]['primary_disease_type'] = input_case_record['disease_type']
                                aliquot_records[aliquot_id]['anatomical_site'] = ''
                                aliquot_records[aliquot_id]['source_material_type'] = ''
                                aliquot_records[aliquot_id]['specimen_type'] = 'aliquot'
                                aliquot_records[aliquot_id]['derived_from_specimen'] = sample_cda_id
                                aliquot_records[aliquot_id]['derived_from_subject'] = subject_id

                            else:
                                
                                aliquot_records[aliquot_id]['cda_ids'].add( aliquot_cda_id )
                                aliquot_records[aliquot_id]['associated_project'].add(pdc_study_id[study_id])

            # Write the main ResearchSubject record.

            output_record = dict()

            output_record['id'] = rs_id
            output_record['member_of_research_project'] = pdc_study_id[study_id]
            output_record['primary_diagnosis_condition'] = input_case_record['disease_type']
            output_record['primary_diagnosis_site'] = input_case_record['primary_site']

            print( *[ output_record[column_name] for column_name in output_column_names ], sep='\t', end='\n', file=RS_OUT )

        # Write the Specimen records collected for this Case.

        seen_specimen_cda_ids = set()

        for sample_id in sorted( sample_records ):
            
            for sample_cda_id in sorted( sample_records[sample_id]['cda_ids'] ):
                
                if sample_cda_id in seen_specimen_cda_ids:
                    
                    sys.exit(f"FATAL FOR NOW: Saw {sample_cda_id} more than once, aborting.\n")

                seen_specimen_cda_ids.add( sample_cda_id )

                current_row = sample_records[sample_id]

                print( *[ sample_cda_id, ';'.join( sorted( current_row['associated_project'] ) ), current_row['days_to_collection'], current_row['primary_disease_type'], current_row['anatomical_site'], current_row['source_material_type'], current_row['specimen_type'], current_row['derived_from_specimen'], current_row['derived_from_subject'] ], sep='\t', end='\n', file=SPECIMEN )

                print( *[ sample_cda_id, 'PDC', 'Sample.sample_id', sample_id ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

                print( *[ sample_cda_id, 'PDC', 'Sample.sample_submitter_id', sample[sample_id]['sample_submitter_id'] ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

        for aliquot_id in sorted( aliquot_records ):
            
            for aliquot_cda_id in sorted( aliquot_records[aliquot_id]['cda_ids'] ):
                
                if aliquot_cda_id in seen_specimen_cda_ids:
                    
                    sys.exit(f"FATAL FOR NOW: Saw {aliquot_cda_id} more than once, aborting.\n")

                seen_specimen_cda_ids.add( aliquot_cda_id )

                current_row = aliquot_records[aliquot_id]

                print( *[ aliquot_cda_id, ';'.join( sorted( current_row['associated_project'] ) ), current_row['days_to_collection'], current_row['primary_disease_type'], current_row['anatomical_site'], current_row['source_material_type'], current_row['specimen_type'], current_row['derived_from_specimen'], current_row['derived_from_subject'] ], sep='\t', end='\n', file=SPECIMEN )

                print( *[ aliquot_cda_id, 'PDC', 'Aliquot.aliquot_id', aliquot_id ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

                print( *[ aliquot_cda_id, 'PDC', 'Aliquot.aliquot_submitter_id', aliquot_submitter_id[aliquot_id] ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

                for file_id in aliquot_file[aliquot_id]:
                    
                    if file_id not in file_specimen:
                        
                        file_specimen[file_id] = set()

                    file_specimen[file_id].add( aliquot_cda_id )

                    if current_row['derived_from_specimen'] != '':
                        
                        file_specimen[file_id].add( current_row['derived_from_specimen'] )

                    else:
                        
                        sys.exit( f"FATAL FOR NOW: Aliquot {aliquot_id} is orphaned, aborting.\n" )

# Print collected Subject records, scooping up Demographic information on the way.

demographic_field_map = {
    'gender': 'sex',
    'race': 'race',
    'ethnicity': 'ethnicity',
    'days_to_birth': 'days_to_birth',
    'vital_status': 'vital_status',
    'days_to_death': 'days_to_death',
    'cause_of_death': 'cause_of_death'
}

with open( subject_output_tsv, 'w' ) as SUBJECT, open( subject_identifier_output_tsv, 'w' ) as SUBJECT_IDENTIFIER, \
    open( subject_associated_project_output_tsv, 'w' ) as SUBJECT_ASSOCIATED_PROJECT, open( subject_researchsubject_output_tsv, 'w' ) as SUBJECT_RESEARCHSUBJECT, \
    open( subject_gdc_candidate_output_tsv, 'w' ) as SUBJECT_GDC_CANDIDATE:
    
    print( *[ 'id', 'species', 'sex', 'race', 'ethnicity', 'days_to_birth', 'vital_status', 'days_to_death', 'cause_of_death' ], sep='\t', end='\n', file=SUBJECT )
    print( *[ 'subject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=SUBJECT_IDENTIFIER )
    print( *[ 'subject_id', 'associated_project' ], sep='\t', end='\n', file=SUBJECT_ASSOCIATED_PROJECT )
    print( *[ 'subject_id', 'researchsubject_id' ], sep='\t', end='\n', file=SUBJECT_RESEARCHSUBJECT )
    print( *[ 'pdc_subject_id', 'gdc_subject_id' ], sep='\t', end='\n', file=SUBJECT_GDC_CANDIDATE )

    for subject_id in sorted( subject_records ):
        
        for demographic_id in subject_demographic[subject_id]:
            
            for field_name in demographic_field_map:
                
                if demographic[demographic_id][field_name] != '' and subject_records[subject_id][demographic_field_map[field_name]] == '':
                    
                    subject_records[subject_id][demographic_field_map[field_name]] = demographic[demographic_id][field_name]

        current_row = subject_records[subject_id]

        print( *[ subject_id, current_row['species'], current_row['sex'], current_row['race'], current_row['ethnicity'], current_row['days_to_birth'], current_row['vital_status'], current_row['days_to_death'], current_row['cause_of_death'] ], sep='\t', end='\n', file=SUBJECT )

        for case_id in sorted( subject_case_ids[subject_id] ):
            
            print( *[ subject_id, 'PDC', 'Case.case_id', case_id ], sep='\t', end='\n', file=SUBJECT_IDENTIFIER )

        for case_submitter_id in sorted( subject_case_submitter_ids[subject_id] ):
            
            print( *[ subject_id, 'PDC', 'Case.case_submitter_id', case_submitter_id ], sep='\t', end='\n', file=SUBJECT_IDENTIFIER )

        if subject_id in subject_gdc_candidate_match:
            
            for gdc_subject_id in sorted( subject_gdc_candidate_match[subject_id] ):
                
                print( *[ subject_id, gdc_subject_id ], sep='\t', end='\n', file=SUBJECT_GDC_CANDIDATE )

        for associated_project in sorted( subject_associated_project[subject_id] ):
            
            print( *[ subject_id, associated_project ], sep='\t', end='\n', file=SUBJECT_ASSOCIATED_PROJECT )

        for rs_id in sorted( subject_researchsubject[subject_id] ):
            
            print( *[ subject_id, rs_id ], sep='\t', end='\n', file=SUBJECT_RESEARCHSUBJECT )

# Print the File->Subject relation.

with open( file_subject_output_tsv, 'w' ) as FILE_SUBJECT:
    
    print( *[ 'file_id', 'subject_id' ], sep='\t', end='\n', file=FILE_SUBJECT )

    for file_id in sorted( file_subject ):
        
        for subject_id in sorted( file_subject[file_id] ):
            
            print( *[ file_id, subject_id ], sep='\t', end='\n', file=FILE_SUBJECT )

# Print the File->Specimen relation.

with open( file_specimen_output_tsv, 'w' ) as FILE_SPECIMEN:
    
    print( *[ 'file_id', 'specimen_id' ], sep='\t', end='\n', file=FILE_SPECIMEN )

    for file_id in sorted( file_specimen ):
        
        for specimen_id in sorted( file_specimen[file_id] ):
            
            print( *[ file_id, specimen_id ], sep='\t', end='\n', file=FILE_SPECIMEN )


