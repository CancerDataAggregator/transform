#!/usr/bin/env python

import sys

from os import makedirs, path

# ASSUMPTION: GDC entity uuids will never be duplicated across entity types. So e.g. a sample
# with sample_id=X will never clash with some aliquot whose aliquot_id is also X.
# This is true right now, but bears writing down. This script will break (by design) if
# a violation occurs in the future.

# KNOWN ERROR: The current CDA model only allows one value in the Specimen.derived_from_specimen field.
# For samples, analytes and aliquots, this is an unambiguous parent specimen; slides and portions,
# however, can have multiple parents: a slide can be from multiple portions, and a portion can be
# from multiple samples. This will need to be fixed in the CDA model; until then, we're picking
# one parent ID at random in the (relatively rare) case of slide or portion records with multiple
# parent specimens. Since all specimen entity types (including slides and portions) exist in exactly
# one case, this doesn't mask too much critical data loss, but it is an error.

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

# PARAMETERS

input_dir = 'extracted_data/gdc/all_TSV_output'

case_input_tsv = path.join( input_dir, 'case.tsv' )
case_in_project_input_tsv = path.join( input_dir, 'case_in_project.tsv' )

demographic_input_tsv = path.join( input_dir, 'demographic.tsv' )
demographic_of_case_input_tsv = path.join( input_dir, 'demographic_of_case.tsv' )

diagnosis_input_tsv = path.join( input_dir, 'diagnosis.tsv' )
diagnosis_of_case_input_tsv = path.join( input_dir, 'diagnosis_of_case.tsv' )

program_input_tsv = path.join( input_dir, 'program.tsv' )

project_input_tsv = path.join( input_dir, 'project.tsv' )
project_in_program_input_tsv = path.join( input_dir, 'project_in_program.tsv' )

treatment_input_tsv = path.join( input_dir, 'treatment.tsv' )
treatment_of_diagnosis_input_tsv = path.join( input_dir, 'treatment_of_diagnosis.tsv' )

case_maps = {
    
    'aliquot': path.join( input_dir, 'aliquot_from_case.tsv' ),
    'analyte': path.join( input_dir, 'analyte_from_case.tsv' ),
    'portion': path.join( input_dir, 'portion_from_case.tsv' ),
    'sample': path.join( input_dir, 'sample_from_case.tsv' ),
    'slide': path.join( input_dir, 'slide_from_case.tsv' )
}

specimen_provenance_maps = {
    
    'aliquot': path.join( input_dir, 'aliquot_of_analyte.tsv' ),
    'analyte': path.join( input_dir, 'analyte_from_portion.tsv' ),
    'portion': path.join( input_dir, 'portion_from_sample.tsv' ),
    'slide': path.join( input_dir, 'slide_from_portion.tsv' )
}

specimen_tsvs = {
    
    'aliquot': path.join( input_dir, 'aliquot.tsv' ),
    'analyte': path.join( input_dir, 'analyte.tsv' ),
    'portion': path.join( input_dir, 'portion.tsv' ),
    'sample': path.join( input_dir, 'sample.tsv' ),
    'slide': path.join( input_dir, 'slide.tsv' )
}

output_dir = path.join( 'cda_tsvs', 'gdc_raw_unharmonized' )

diagnosis_output_tsv = path.join( output_dir, 'diagnosis.tsv' )
diagnosis_identifier_output_tsv = path.join( output_dir, 'diagnosis_identifier.tsv' )
diagnosis_treatment_output_tsv = path.join( output_dir, 'diagnosis_treatment.tsv' )

researchsubject_output_tsv = path.join( output_dir, 'researchsubject.tsv' )
researchsubject_diagnosis_output_tsv = path.join( output_dir, 'researchsubject_diagnosis.tsv' )
researchsubject_identifier_output_tsv = path.join( output_dir, 'researchsubject_identifier.tsv' )
researchsubject_specimen_output_tsv = path.join( output_dir, 'researchsubject_specimen.tsv' )
researchsubject_treatment_output_tsv = path.join( output_dir, 'researchsubject_treatment.tsv' )

specimen_output_tsv = path.join( output_dir, 'specimen.tsv' )
specimen_identifier_output_tsv = path.join( output_dir, 'specimen_identifier.tsv' )

subject_output_tsv = path.join( output_dir, 'subject.tsv' )
subject_associated_project_output_tsv = path.join( output_dir, 'subject_associated_project.tsv' )
subject_identifier_output_tsv = path.join( output_dir, 'subject_identifier.tsv' )
subject_researchsubject_output_tsv = path.join( output_dir, 'subject_researchsubject.tsv' )

treatment_output_tsv = path.join( output_dir, 'treatment.tsv' )
treatment_identifier_output_tsv = path.join( output_dir, 'treatment_identifier.tsv' )

subject = dict()
researchsubject = dict()
specimen = dict()
diagnosis = dict()
treatment = dict()

subject_output_fields = [
    
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

researchsubject_output_fields = [
    
    'id',
    'member_of_research_project',
    'primary_diagnosis_condition',
    'primary_diagnosis_site'
]

specimen_output_fields = [
    
    'id',
    'associated_project',
    'days_to_collection',
    'primary_disease_type',
    'anatomical_site',
    'source_material_type',
    'specimen_type',
    'derived_from_specimen',
    'derived_from_subject'
]

diagnosis_output_fields = [
    
    'id',
    'primary_diagnosis',
    'age_at_diagnosis',
    'morphology',
    'stage',
    'grade',
    'method_of_diagnosis'
]

treatment_output_fields = [
    
    'id',
    'treatment_type',
    'treatment_outcome',
    'days_to_treatment_start',
    'days_to_treatment_end',
    'therapeutic_agent',
    'treatment_anatomic_site',
    'treatment_effect',
    'treatment_end_reason',
    'number_of_cycles'
]

subject_has_researchsubject = dict()
researchsubject_in_subject = dict()

researchsubject_has_specimen = dict()
specimen_in_researchsubject = dict()

demographic_from_researchsubject = dict()

diagnosis_has_treatment = dict()

researchsubject_has_diagnosis = dict()
diagnosis_of_researchsubject = dict()

# EXECUTION

if not path.exists(output_dir):
    
    makedirs(output_dir)

# Load program names for all projects.

project_id_to_program_id = map_columns_one_to_one( project_in_program_input_tsv, 'project_id', 'program_id' )

program_id_to_program_name = map_columns_one_to_one( program_input_tsv, 'program_id', 'name' )

project_in_program = dict()

for project_id in project_id_to_program_id:
    
    project_in_program[project_id] = program_id_to_program_name[project_id_to_program_id[project_id]]

# Load the submitter_id for each case.

case_id_to_submitter_id = map_columns_one_to_one( case_input_tsv, 'case_id', 'submitter_id' )

# Each case (CDA: ResearchSubject) is in exactly one project. Load this containment map.

case_in_project = map_columns_one_to_one( case_in_project_input_tsv, 'case_id', 'project_id' )

case_id_to_rs_id = dict()

case_id_to_subject_id = dict()

for case_id in case_in_project:
    
    project_id = case_in_project[case_id]

    program_name = project_in_program[project_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = f"{project_id}.{case_submitter_id}"

    case_id_to_rs_id[case_id] = rs_id

    if rs_id not in researchsubject:
        
        researchsubject[rs_id] = dict()

    researchsubject[rs_id]['member_of_research_project'] = project_id

    subject_id = f"{program_name}.{case_submitter_id}"

    case_id_to_subject_id[case_id] = subject_id

    if subject_id not in subject:
        
        subject[subject_id] = dict()
        subject[subject_id]['id'] = subject_id
        subject[subject_id]['species'] = 'Homo sapiens'
        subject[subject_id]['sex'] = None
        subject[subject_id]['race'] = None
        subject[subject_id]['ethnicity'] = None
        subject[subject_id]['days_to_birth'] = None
        subject[subject_id]['vital_status'] = None
        subject[subject_id]['days_to_death'] = None
        subject[subject_id]['cause_of_death'] = None
        subject[subject_id]['identifier'] = { 'system': 'GDC', 'field_name': 'case.submitter_id', 'value': case_submitter_id }
        subject[subject_id]['subject_associated_project'] = set()

        subject_has_researchsubject[subject_id] = set()

    subject[subject_id]['subject_associated_project'].add( project_id )

    subject_has_researchsubject[subject_id].add( rs_id )

    researchsubject_in_subject[rs_id] = subject_id

# Load metadata from case.tsv (including some that will be propagated to child objects),
# and place each case/ResearchSubject into a containing Subject node.

with open( case_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        case_id = record['case_id']

        rs_id = case_id_to_rs_id[case_id]

        # Attach initial metadata to the ResearchSubject record for this case.

        researchsubject[rs_id]['id'] = rs_id

        if 'identifier' not in researchsubject[rs_id]:
            
            researchsubject[rs_id]['identifier'] = dict()

        researchsubject[rs_id]['identifier']['case.case_id'] = case_id
        researchsubject[rs_id]['identifier']['case.submitter_id'] = case_id_to_submitter_id[case_id]

        researchsubject[rs_id]['primary_diagnosis_condition'] = record['disease_type'] if record['disease_type'] != '' else None
        researchsubject[rs_id]['primary_diagnosis_site'] = record['primary_site'] if record['primary_site'] != '' else None

# Start building Specimen objects from samples, portions, slides,
# analytes and aliquots and attach them to their containing case records.

sample_from_case = map_columns_one_to_one( case_maps['sample'], 'sample_id', 'case_id' )

sample_id_to_cda_id = dict()

# Process samples.

with open( specimen_tsvs['sample'] ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        sample_id = record['sample_id']

        case_id = sample_from_case[sample_id]

        case_submitter_id = case_id_to_submitter_id[case_id]

        rs_id = case_id_to_rs_id[case_id]

        # If no sample.submitter_id exists, fall back on the GDC sample UUID to create the CDA Specimen ID.

        sample_cda_id = f"{rs_id}.{sample_id}"

        sample_submitter_id = ''

        if record['submitter_id'] is not None and record['submitter_id'] != '':
            
            # If submitter_id does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

            sample_submitter_id = record['submitter_id']

            sample_cda_id = f"{rs_id}.{sample_submitter_id}"

        sample_id_to_cda_id[sample_id] = sample_cda_id

        if sample_cda_id in specimen:
            
            sys.exit(f"Duplicate entity ID {sample_id} ({sample_cda_id}) found in sample table; aborting.")

        else:
            
            specimen[sample_cda_id] = dict()

            specimen[sample_cda_id]['id'] = sample_cda_id

            specimen[sample_cda_id]['identifier'] = dict()

            specimen[sample_cda_id]['identifier']['case.samples.sample_id'] = sample_id

            if sample_submitter_id != '':
                
                specimen[sample_cda_id]['identifier']['case.samples.submitter_id'] = sample_submitter_id

            specimen[sample_cda_id]['associated_project'] = case_in_project[case_id]

            if record['days_to_sample_procurement'] != '':
                
                specimen[sample_cda_id]['days_to_collection'] = int( record['days_to_sample_procurement'] )

            elif record['days_to_collection'] != '':
                
                specimen[sample_cda_id]['days_to_collection'] = int( record['days_to_collection'] )

            else:
                
                specimen[sample_cda_id]['days_to_collection'] = None

            specimen[sample_cda_id]['primary_disease_type'] = researchsubject[rs_id]['primary_diagnosis_condition']
            specimen[sample_cda_id]['anatomical_site'] = record['biospecimen_anatomic_site'] if record['biospecimen_anatomic_site'] != '' else None
            specimen[sample_cda_id]['source_material_type'] = record['tissue_type'] if record['tissue_type'] != '' else None
            specimen[sample_cda_id]['specimen_type'] = 'sample'
            specimen[sample_cda_id]['derived_from_specimen'] = 'initial specimen'
            specimen[sample_cda_id]['derived_from_subject'] = researchsubject_in_subject[rs_id]

            if rs_id not in researchsubject_has_specimen:
                
                researchsubject_has_specimen[rs_id] = set()

            researchsubject_has_specimen[rs_id].add( sample_cda_id )

            specimen_in_researchsubject[sample_cda_id] = rs_id

# Process portions. See KNOWN ERROR note at the top of this script.

portion_from_sample = map_columns_one_to_one( specimen_provenance_maps['portion'], 'portion_id', 'sample_id' )

portion_id_to_submitter_id = map_columns_one_to_one( specimen_tsvs['portion'], 'portion_id', 'submitter_id' )

portion_id_to_cda_id = dict()

portion_from_case = map_columns_one_to_one( case_maps['portion'], 'portion_id', 'case_id' )

for portion_id in sorted( portion_from_sample ):
    
    sample_id = portion_from_sample[portion_id]

    sample_cda_id = sample_id_to_cda_id[sample_id]

    case_id = portion_from_case[portion_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = case_id_to_rs_id[case_id]

    # If no submitter_id exists, fall back on the GDC UUID to create the CDA Specimen ID.

    portion_cda_id = f"{rs_id}.{portion_id}"

    portion_submitter_id = ''

    if portion_id_to_submitter_id[portion_id] is not None and portion_id_to_submitter_id[portion_id] != '':
        
        # If it does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

        portion_submitter_id = portion_id_to_submitter_id[portion_id]

        portion_cda_id = f"{rs_id}.{portion_submitter_id}"

    portion_id_to_cda_id[portion_id] = portion_cda_id

    if portion_cda_id in specimen:
        
        sys.exit(f"Duplicate entity ID {portion_cda_id} found while building specimen output structure; aborting.")

    else:
        
        specimen[portion_cda_id] = dict()

        specimen[portion_cda_id]['id'] = portion_cda_id

        specimen[portion_cda_id]['identifier'] = dict()

        specimen[portion_cda_id]['identifier']['case.samples.portions.portion_id'] = portion_id

        if portion_submitter_id != '':
            
            specimen[portion_cda_id]['identifier']['case.samples.portions.submitter_id'] = portion_submitter_id

        specimen[portion_cda_id]['associated_project'] = specimen[sample_cda_id]['associated_project']
        specimen[portion_cda_id]['days_to_collection'] = specimen[sample_cda_id]['days_to_collection']
        specimen[portion_cda_id]['primary_disease_type'] = specimen[sample_cda_id]['primary_disease_type']
        specimen[portion_cda_id]['anatomical_site'] = specimen[sample_cda_id]['anatomical_site']
        specimen[portion_cda_id]['source_material_type'] = specimen[sample_cda_id]['source_material_type']
        specimen[portion_cda_id]['specimen_type'] = 'portion'
        specimen[portion_cda_id]['derived_from_specimen'] = sample_cda_id
        specimen[portion_cda_id]['derived_from_subject'] = specimen[sample_cda_id]['derived_from_subject']

        if sample_cda_id not in specimen_in_researchsubject:
            
            sys.exit(f"FATAL: No containing RS ID loaded for portion {portion_id} ({portion_cda_id}) parent sample {sample_id} ({sample_cda_id}); aborting.")

        else:
            
            rs_id = specimen_in_researchsubject[sample_cda_id]

            researchsubject_has_specimen[rs_id].add(portion_cda_id)

            specimen_in_researchsubject[portion_cda_id] = rs_id

# Process slides. See KNOWN ERROR note at the top of this script.

slide_from_portion = map_columns_one_to_one( specimen_provenance_maps['slide'], 'slide_id', 'portion_id' )

slide_id_to_submitter_id = map_columns_one_to_one( specimen_tsvs['slide'], 'slide_id', 'submitter_id' )

slide_id_to_cda_id = dict()

slide_from_case = map_columns_one_to_one( case_maps['slide'], 'slide_id', 'case_id' )

for slide_id in sorted( slide_from_portion ):
    
    portion_id = slide_from_portion[slide_id]

    portion_cda_id = portion_id_to_cda_id[portion_id]

    case_id = slide_from_case[slide_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = case_id_to_rs_id[case_id]

    # If no submitter_id exists, fall back on the GDC UUID to create the CDA Specimen ID.

    slide_cda_id = f"{rs_id}.{slide_id}"

    slide_submitter_id = ''

    if slide_id_to_submitter_id[slide_id] is not None and slide_id_to_submitter_id[slide_id] != '':
        
        # If it does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

        slide_submitter_id = slide_id_to_submitter_id[slide_id]

        slide_cda_id = f"{rs_id}.{slide_submitter_id}"

    slide_id_to_cda_id[slide_id] = slide_cda_id

    if slide_cda_id in specimen:
        
        sys.exit(f"Duplicate entity ID {slide_cda_id} found while building specimen output structure; aborting.")

    else:
        
        specimen[slide_cda_id] = dict()

        specimen[slide_cda_id]['id'] = slide_cda_id

        specimen[slide_cda_id]['identifier'] = dict()

        specimen[slide_cda_id]['identifier']['case.samples.portions.slides.slide_id'] = slide_id

        if slide_submitter_id != '':
            
            specimen[slide_cda_id]['identifier']['case.samples.portions.slides.submitter_id'] = slide_submitter_id

        specimen[slide_cda_id]['associated_project'] = specimen[portion_cda_id]['associated_project']
        specimen[slide_cda_id]['days_to_collection'] = specimen[portion_cda_id]['days_to_collection']
        specimen[slide_cda_id]['primary_disease_type'] = specimen[portion_cda_id]['primary_disease_type']
        specimen[slide_cda_id]['anatomical_site'] = specimen[portion_cda_id]['anatomical_site']
        specimen[slide_cda_id]['source_material_type'] = specimen[portion_cda_id]['source_material_type']
        specimen[slide_cda_id]['specimen_type'] = 'slide'
        specimen[slide_cda_id]['derived_from_specimen'] = portion_cda_id
        specimen[slide_cda_id]['derived_from_subject'] = specimen[portion_cda_id]['derived_from_subject']

        if portion_cda_id not in specimen_in_researchsubject:
            
            sys.exit(f"FATAL: No containing RS ID loaded for slide {slide_id} ({slide_cda_id}) parent portion {portion_id} ({portion_cda_id}); aborting.")

        else:
            
            rs_id = specimen_in_researchsubject[portion_cda_id]

            researchsubject_has_specimen[rs_id].add(slide_cda_id)

            specimen_in_researchsubject[slide_cda_id] = rs_id

# Process analytes.

analyte_from_portion = map_columns_one_to_one( specimen_provenance_maps['analyte'], 'analyte_id', 'portion_id' )

analyte_id_to_submitter_id = map_columns_one_to_one( specimen_tsvs['analyte'], 'analyte_id', 'submitter_id' )

analyte_id_to_cda_id = dict()

analyte_from_case = map_columns_one_to_one( case_maps['analyte'], 'analyte_id', 'case_id' )

for analyte_id in sorted( analyte_from_portion ):
    
    portion_id = analyte_from_portion[analyte_id]

    portion_cda_id = portion_id_to_cda_id[portion_id]

    case_id = analyte_from_case[analyte_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = case_id_to_rs_id[case_id]

    # If no submitter_id exists, fall back on the GDC UUID to create the CDA Specimen ID.

    analyte_cda_id = f"{rs_id}.{analyte_id}"

    analyte_submitter_id = ''

    if analyte_id_to_submitter_id[analyte_id] is not None and analyte_id_to_submitter_id[analyte_id] != '':
        
        # If it does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

        analyte_submitter_id = analyte_id_to_submitter_id[analyte_id]

        analyte_cda_id = f"{rs_id}.{analyte_submitter_id}"

    analyte_id_to_cda_id[analyte_id] = analyte_cda_id

    if analyte_cda_id in specimen:
        
        sys.exit(f"Duplicate entity ID {analyte_cda_id} found while building specimen output structure; aborting.")

    else:
        
        specimen[analyte_cda_id] = dict()

        specimen[analyte_cda_id]['id'] = analyte_cda_id

        specimen[analyte_cda_id]['identifier'] = dict()

        specimen[analyte_cda_id]['identifier']['case.samples.portions.analytes.analyte_id'] = analyte_id

        if analyte_submitter_id != '':
            
            specimen[analyte_cda_id]['identifier']['case.samples.portions.analytes.submitter_id'] = analyte_submitter_id

        specimen[analyte_cda_id]['associated_project'] = specimen[portion_cda_id]['associated_project']
        specimen[analyte_cda_id]['days_to_collection'] = specimen[portion_cda_id]['days_to_collection']
        specimen[analyte_cda_id]['primary_disease_type'] = specimen[portion_cda_id]['primary_disease_type']
        specimen[analyte_cda_id]['anatomical_site'] = specimen[portion_cda_id]['anatomical_site']
        specimen[analyte_cda_id]['source_material_type'] = specimen[portion_cda_id]['source_material_type']
        specimen[analyte_cda_id]['specimen_type'] = 'analyte'
        specimen[analyte_cda_id]['derived_from_specimen'] = portion_cda_id
        specimen[analyte_cda_id]['derived_from_subject'] = specimen[portion_cda_id]['derived_from_subject']

        if portion_cda_id not in specimen_in_researchsubject:
            
            sys.exit(f"FATAL: No containing RS ID loaded for analyte {analyte_id} ({analyte_cda_id}) parent portion {portion_id} ({portion_cda_id}); aborting.")

        else:
            
            rs_id = specimen_in_researchsubject[portion_cda_id]

            researchsubject_has_specimen[rs_id].add(analyte_cda_id)

            specimen_in_researchsubject[analyte_cda_id] = rs_id

# Process aliquots.

aliquot_from_analyte = map_columns_one_to_one( specimen_provenance_maps['aliquot'], 'aliquot_id', 'analyte_id' )

aliquot_id_to_submitter_id = map_columns_one_to_one( specimen_tsvs['aliquot'], 'aliquot_id', 'submitter_id' )

aliquot_id_to_cda_id = dict()

aliquot_from_case = map_columns_one_to_one( case_maps['aliquot'], 'aliquot_id', 'case_id' )

for aliquot_id in sorted( aliquot_from_analyte ):
    
    analyte_id = aliquot_from_analyte[aliquot_id]

    analyte_cda_id = analyte_id_to_cda_id[analyte_id]

    case_id = aliquot_from_case[aliquot_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = case_id_to_rs_id[case_id]

    # If no submitter_id exists, fall back on the GDC UUID to create the CDA Specimen ID.

    aliquot_cda_id = f"{rs_id}.{aliquot_id}"

    aliquot_submitter_id = ''

    if aliquot_id_to_submitter_id[aliquot_id] is not None and aliquot_id_to_submitter_id[aliquot_id] != '':
        
        # If it does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

        aliquot_submitter_id = aliquot_id_to_submitter_id[aliquot_id]

        aliquot_cda_id = f"{rs_id}.{aliquot_submitter_id}"

    aliquot_id_to_cda_id[aliquot_id] = aliquot_cda_id

    if aliquot_cda_id in specimen:
        
        sys.exit(f"Duplicate entity ID {aliquot_cda_id} found while building specimen output structure; aborting.")

    else:
        
        specimen[aliquot_cda_id] = dict()

        specimen[aliquot_cda_id]['id'] = aliquot_cda_id

        specimen[aliquot_cda_id]['identifier'] = dict()

        specimen[aliquot_cda_id]['identifier']['case.samples.portions.analytes.aliquots.aliquot_id'] = aliquot_id

        if aliquot_submitter_id != '':
            
            specimen[aliquot_cda_id]['identifier']['case.samples.portions.analytes.aliquots.submitter_id'] = aliquot_submitter_id

        specimen[aliquot_cda_id]['associated_project'] = specimen[analyte_cda_id]['associated_project']
        specimen[aliquot_cda_id]['days_to_collection'] = specimen[analyte_cda_id]['days_to_collection']
        specimen[aliquot_cda_id]['primary_disease_type'] = specimen[analyte_cda_id]['primary_disease_type']
        specimen[aliquot_cda_id]['anatomical_site'] = specimen[analyte_cda_id]['anatomical_site']
        specimen[aliquot_cda_id]['source_material_type'] = specimen[analyte_cda_id]['source_material_type']
        specimen[aliquot_cda_id]['specimen_type'] = 'aliquot'
        specimen[aliquot_cda_id]['derived_from_specimen'] = analyte_cda_id
        specimen[aliquot_cda_id]['derived_from_subject'] = specimen[analyte_cda_id]['derived_from_subject']

        if analyte_cda_id not in specimen_in_researchsubject:
            
            sys.exit(f"FATAL: No containing RS ID loaded for aliquot {aliquot_id} ({aliquot_cda_id}) parent analyte {analyte_id} ({analyte_cda_id}); aborting.")

        else:
            
            rs_id = specimen_in_researchsubject[analyte_cda_id]

            researchsubject_has_specimen[rs_id].add(aliquot_cda_id)

            specimen_in_researchsubject[aliquot_cda_id] = rs_id

# Load demographic data from case records into their parent Subjects. Note: if there
# are conflicts (i.e. if multiple ResearchSubject records in the same Subject have
# different demographic data, which they shouldn't), this will overwrite conflicting
# values with the last ones seen.

demographic_of_case = map_columns_one_to_one( demographic_of_case_input_tsv, 'demographic_id', 'case_id' )

demographic_id_to_submitter_id = map_columns_one_to_one( demographic_input_tsv, 'demographic_id', 'submitter_id' )

for demographic_id in sorted( demographic_of_case ):
    
    # If no demographic.submitter_id exists, fall back on the GDC demographic UUID.

    demographic_submitter_id = demographic_id

    if demographic_id_to_submitter_id[demographic_id] is not None and demographic_id_to_submitter_id[demographic_id] != '':
        
        # If it does exist, use that for the CDA Specimen ID along with the (unique) containing case's submitter_id.

        demographic_submitter_id = demographic_id_to_submitter_id[demographic_id]

    case_id = demographic_of_case[demographic_id]

    rs_id = case_id_to_rs_id[case_id]

    if demographic_id in demographic_from_researchsubject:
        
        sys.exit(f"FATAL: Demographic record with ID {demographic_id} loaded multiple times from demographic_of_case.tsv; aborting.")

    else:
        
        demographic_from_researchsubject[demographic_id] = rs_id

with open( demographic_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        demographic_id = record['demographic_id']

        if demographic_id not in demographic_from_researchsubject:
            
            sys.exit(f"FATAL: Demographic record with ID {demographic_id} present in demographic.tsv but not in demographic_of_case.tsv; aborting.")

        else:
            
            rs_id = demographic_from_researchsubject[demographic_id]

            subject_id = researchsubject_in_subject[rs_id]

            subject[subject_id]['sex'] = record['gender'] if record['gender'] != '' else None
            subject[subject_id]['race'] = record['race'] if record['race'] != '' else None
            subject[subject_id]['ethnicity'] = record['ethnicity'] if record['ethnicity'] != '' else None
            subject[subject_id]['days_to_birth'] = int(record['days_to_birth']) if record['days_to_birth'] != '' else None
            subject[subject_id]['vital_status'] = record['vital_status'] if record['vital_status'] != '' else None
            subject[subject_id]['days_to_death'] = int(record['days_to_death']) if record['days_to_death'] != '' else None
            subject[subject_id]['cause_of_death'] = record['cause_of_death'] if record['cause_of_death'] != '' else None

# Load diagnosis IDs and attach to containing ResearchSubject records.

diagnosis_id_to_submitter_id = map_columns_one_to_one( diagnosis_input_tsv, 'diagnosis_id', 'submitter_id' )

diagnosis_id_to_cda_id = dict()

diagnosis_of_case = map_columns_one_to_one( diagnosis_of_case_input_tsv, 'diagnosis_id', 'case_id' )

for diagnosis_id in sorted( diagnosis_of_case ):
    
    case_id = diagnosis_of_case[diagnosis_id]
        
    case_submitter_id = case_id_to_submitter_id[case_id]

    rs_id = case_id_to_rs_id[case_id]

    # If no diagnosis.submitter_id exists, fall back on the GDC diagnosis UUID.

    diagnosis_cda_id = f"{rs_id}.{diagnosis_id}"

    diagnosis_submitter_id = ''

    if diagnosis_id_to_submitter_id[diagnosis_id] is not None and diagnosis_id_to_submitter_id[diagnosis_id] != '':
        
        # If it does exist, use that for the CDA Diagnosis ID along with the (unique) containing case's submitter_id.

        diagnosis_submitter_id = diagnosis_id_to_submitter_id[diagnosis_id]

        diagnosis_cda_id = f"{rs_id}.{diagnosis_submitter_id}"

    diagnosis_id_to_cda_id[diagnosis_id] = diagnosis_cda_id

    if diagnosis_cda_id in diagnosis:
        
        sys.exit(f"FATAL: Diagnosis record with ID {diagnosis_id} ({diagnosis_cda_id}) loaded twice from diagnosis_of_case.tsv; aborting.")

    else:
        
        diagnosis[diagnosis_cda_id] = dict()

        diagnosis[diagnosis_cda_id]['id'] = diagnosis_cda_id

        diagnosis[diagnosis_cda_id]['identifier'] = dict()

        diagnosis[diagnosis_cda_id]['identifier']['case.diagnoses.diagnosis_id'] = diagnosis_id

        if diagnosis_submitter_id != '':
            
            diagnosis[diagnosis_cda_id]['identifier']['case.diagnoses.submitter_id'] = diagnosis_submitter_id

        if rs_id not in researchsubject_has_diagnosis:
            
            researchsubject_has_diagnosis[rs_id] = set()

        researchsubject_has_diagnosis[rs_id].add( diagnosis_cda_id )

        diagnosis_of_researchsubject[diagnosis_cda_id] = rs_id

# Load diagnosis metadata.

with open( diagnosis_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        diagnosis_id = record['diagnosis_id']

        diagnosis_cda_id = diagnosis_id_to_cda_id[diagnosis_id]

        if diagnosis_cda_id not in diagnosis:
            
            sys.exit(f"FATAL: Diagnosis record with ID {diagnosis_id} ({diagnosis_cda_id}) present in diagnosis.tsv but not in diagnosis_of_case.tsv; aborting.")

        else:
            
            diagnosis[diagnosis_cda_id]['primary_diagnosis'] = record['primary_diagnosis'] if record['primary_diagnosis'] != '' else None
            diagnosis[diagnosis_cda_id]['age_at_diagnosis'] = int(record['age_at_diagnosis']) if record['age_at_diagnosis'] != '' else None
            diagnosis[diagnosis_cda_id]['morphology'] = record['morphology'] if record['morphology'] != '' else None
            diagnosis[diagnosis_cda_id]['stage'] = record['tumor_stage'] if record['tumor_stage'] != '' else None
            diagnosis[diagnosis_cda_id]['grade'] = record['tumor_grade'] if record['tumor_grade'] != '' else None
            diagnosis[diagnosis_cda_id]['method_of_diagnosis'] = record['method_of_diagnosis'] if record['method_of_diagnosis'] != '' else None

# Load treatment IDs and attach to containing diagnosis records.

diagnosis_id_to_treatment_id = map_columns_one_to_many( treatment_of_diagnosis_input_tsv, 'diagnosis_id', 'treatment_id' )

treatment_id_to_submitter_id = map_columns_one_to_one( treatment_input_tsv, 'treatment_id', 'submitter_id' )

treatment_id_to_cda_id = dict()

for diagnosis_id in diagnosis_id_to_treatment_id:
    
    case_id = diagnosis_of_case[diagnosis_id]

    case_submitter_id = case_id_to_submitter_id[case_id]

    diagnosis_cda_id = diagnosis_id_to_cda_id[diagnosis_id]

    rs_id = diagnosis_of_researchsubject[diagnosis_cda_id]

    for treatment_id in sorted( diagnosis_id_to_treatment_id[diagnosis_id] ):
        
        # If no treatment.submitter_id exists, fall back on the GDC treatment UUID.

        treatment_cda_id = f"{rs_id}.{treatment_id}"

        treatment_submitter_id = ''

        if treatment_id_to_submitter_id[treatment_id] is not None and treatment_id_to_submitter_id[treatment_id] != '':
            
            # If it does exist, use that for the CDA Treatment ID along with the (unique) containing case's submitter_id.

            treatment_submitter_id = treatment_id_to_submitter_id[treatment_id]

            treatment_cda_id = f"{rs_id}.{treatment_submitter_id}"

        treatment_id_to_cda_id[treatment_id] = treatment_cda_id

        if treatment_cda_id in treatment:
            
            sys.exit(f"FATAL: Treatment record with ID {treatment_id} ({treatment_cda_id}) loaded twice from treatment_of_diagnosis.tsv; aborting.")

        else:
            
            treatment[treatment_cda_id] = dict()

            treatment[treatment_cda_id]['id'] = treatment_cda_id

            treatment[treatment_cda_id]['identifier'] = dict()

            treatment[treatment_cda_id]['identifier']['case.diagnoses.treatments.treatment_id'] = treatment_id

            if treatment_submitter_id != '':
                
                treatment[treatment_cda_id]['identifier']['case.diagnoses.treatments.submitter_id'] = treatment_submitter_id

            if diagnosis_cda_id not in diagnosis_has_treatment:
                
                diagnosis_has_treatment[diagnosis_cda_id] = set()

            diagnosis_has_treatment[diagnosis_cda_id].add( treatment_cda_id )

# Load treatment metadata.

with open( treatment_input_tsv ) as IN:
    
    colnames = IN.readline().rstrip('\n').split('\t')

    for line in IN:
        
        record = dict( zip( colnames, line.rstrip('\n').split('\t') ) )

        treatment_id = record['treatment_id']

        treatment_cda_id = treatment_id_to_cda_id[treatment_id]

        if treatment_cda_id not in treatment:
            
            sys.exit(f"FATAL: Treatment record with ID {treatment_id} ({treatment_cda_id}) present in treatment.tsv but not in treatment_of_diagnosis.tsv; aborting.")

        else:
            
            treatment[treatment_cda_id]['treatment_type'] = record['treatment_type'] if record['treatment_type'] != '' else None
            treatment[treatment_cda_id]['treatment_outcome'] = record['treatment_outcome'] if record['treatment_outcome'] != '' else None
            treatment[treatment_cda_id]['days_to_treatment_start'] = int(record['days_to_treatment_start']) if record['days_to_treatment_start'] != '' else None
            treatment[treatment_cda_id]['days_to_treatment_end'] = int(record['days_to_treatment_end']) if record['days_to_treatment_end'] != '' else None
            treatment[treatment_cda_id]['therapeutic_agent'] = record['therapeutic_agents'] if record['therapeutic_agents'] != '' else None
            treatment[treatment_cda_id]['treatment_anatomic_site'] = record['treatment_anatomic_site'] if record['treatment_anatomic_site'] != '' else None
            treatment[treatment_cda_id]['treatment_effect'] = record['treatment_effect'] if record['treatment_effect'] != '' else None
            treatment[treatment_cda_id]['treatment_end_reason'] = record['reason_treatment_ended'] if record['reason_treatment_ended'] != '' else None
            treatment[treatment_cda_id]['number_of_cycles'] = int(record['number_of_cycles']) if record['number_of_cycles'] != '' else None

# Write loaded data to output TSVs.

with open( diagnosis_output_tsv, 'w' ) as DIAGNOSIS, open( diagnosis_identifier_output_tsv, 'w' ) as DIAGNOSIS_IDENTIFIER:
    
    print( *diagnosis_output_fields, sep='\t', end='\n', file=DIAGNOSIS )

    print( *[ 'diagnosis_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=DIAGNOSIS_IDENTIFIER )

    for diagnosis_id in sorted( diagnosis ):
        
        for identifier_name in diagnosis[diagnosis_id]['identifier']:
            
            print( *[ diagnosis_id, 'GDC', identifier_name, diagnosis[diagnosis_id]['identifier'][identifier_name] ], sep='\t', end='\n', file=DIAGNOSIS_IDENTIFIER )

        output_row = list()

        for field_name in diagnosis_output_fields:
            
            if diagnosis[diagnosis_id][field_name] is not None:
                
                output_row.append( diagnosis[diagnosis_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', end='\n', file=DIAGNOSIS )

with open( diagnosis_treatment_output_tsv, 'w' ) as DIAGNOSIS_TREATMENT:
    
    print( *[ 'diagnosis_id', 'treatment_id' ], sep='\t', end='\n', file=DIAGNOSIS_TREATMENT )

    for diagnosis_id in sorted( diagnosis_has_treatment ):
        
        for treatment_id in sorted( diagnosis_has_treatment[diagnosis_id] ):
            
            print( *[ diagnosis_id, treatment_id ], sep='\t', end='\n', file=DIAGNOSIS_TREATMENT )

with open( researchsubject_output_tsv, 'w' ) as RS, open( researchsubject_identifier_output_tsv, 'w' ) as RS_IDENTIFIER:
    
    print( *researchsubject_output_fields, sep='\t', end='\n', file=RS )

    print( *[ 'researchsubject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=RS_IDENTIFIER )

    for rs_id in sorted( researchsubject ):
        
        for identifier_name in sorted( researchsubject[rs_id]['identifier'] ):
            
            print( *[ rs_id, 'GDC', identifier_name, researchsubject[rs_id]['identifier'][identifier_name] ], sep='\t', end='\n', file=RS_IDENTIFIER )

        output_row = list()

        for field_name in researchsubject_output_fields:
            
            if researchsubject[rs_id][field_name] is not None:
                
                output_row.append( researchsubject[rs_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', end='\n', file=RS )

with open( researchsubject_diagnosis_output_tsv, 'w' ) as RS_DIAGNOSIS:
    
    print( *[ 'researchsubject_id', 'diagnosis_id' ], sep='\t', end='\n', file=RS_DIAGNOSIS )

    for rs_id in sorted( researchsubject_has_diagnosis ):
        
        for diagnosis_id in sorted( researchsubject_has_diagnosis[rs_id] ):
            
            print( *[ rs_id, diagnosis_id ], sep='\t', end='\n', file=RS_DIAGNOSIS )

with open( researchsubject_specimen_output_tsv, 'w' ) as RS_SPECIMEN:
    
    print( *[ 'researchsubject_id', 'specimen_id' ], sep='\t', end='\n', file=RS_SPECIMEN )

    for rs_id in sorted( researchsubject_has_specimen ):
        
        for specimen_id in sorted( researchsubject_has_specimen[rs_id] ):
            
            print( *[ rs_id, specimen_id ], sep='\t', end='\n', file=RS_SPECIMEN )

with open( researchsubject_treatment_output_tsv, 'w' ) as RS_TREATMENT:
    
    print( *[ 'researchsubject_id', 'treatment_id' ], sep='\t', end='\n', file=RS_TREATMENT )

    for rs_id in sorted( researchsubject_has_diagnosis ):
        
        for diagnosis_id in sorted( researchsubject_has_diagnosis[rs_id] ):
            
            if diagnosis_id in diagnosis_has_treatment:
                
                for treatment_id in sorted( diagnosis_has_treatment[diagnosis_id] ):
                    
                    print( *[ rs_id, treatment_id ], sep='\t', end='\n', file=RS_TREATMENT )

with open( specimen_output_tsv, 'w' ) as SPECIMEN, open( specimen_identifier_output_tsv, 'w' ) as SPECIMEN_IDENTIFIER:
    
    print( *specimen_output_fields, sep='\t', end='\n', file=SPECIMEN )

    print( *[ 'specimen_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

    for specimen_id in sorted( specimen ):
        
        for identifier_name in specimen[specimen_id]['identifier']:
            
            print( *[ specimen_id, 'GDC', identifier_name, specimen[specimen_id]['identifier'][identifier_name] ], sep='\t', end='\n', file=SPECIMEN_IDENTIFIER )

        output_row = list()

        for field_name in specimen_output_fields:
            
            if specimen[specimen_id][field_name] is not None:
                
                output_row.append( specimen[specimen_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', end='\n', file=SPECIMEN )

with open( subject_output_tsv, 'w' ) as SUBJECT, open( subject_identifier_output_tsv, 'w' ) as SUBJECT_IDENTIFIER:
    
    print( *subject_output_fields, sep='\t', end='\n', file=SUBJECT )

    print( *[ 'subject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=SUBJECT_IDENTIFIER )

    for subject_id in sorted( subject ):
        
        print( *[ subject_id, subject[subject_id]['identifier']['system'], subject[subject_id]['identifier']['field_name'], subject[subject_id]['identifier']['value'] ], sep='\t', end='\n', file=SUBJECT_IDENTIFIER )

        output_row = list()

        for field_name in subject_output_fields:
            
            if subject[subject_id][field_name] is not None:
                
                output_row.append( subject[subject_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', end='\n', file=SUBJECT )

with open( treatment_output_tsv, 'w' ) as TREATMENT, open( treatment_identifier_output_tsv, 'w' ) as TREATMENT_IDENTIFIER:
    
    print( *treatment_output_fields, sep='\t', end='\n', file=TREATMENT )

    print( *[ 'treatment_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=TREATMENT_IDENTIFIER )

    for treatment_id in sorted( treatment ):
        
        for identifier_name in treatment[treatment_id]['identifier']:
            
            print( *[ treatment_id, 'GDC', identifier_name, treatment[treatment_id]['identifier'][identifier_name] ], sep='\t', end='\n', file=TREATMENT_IDENTIFIER )

        output_row = list()

        for field_name in treatment_output_fields:
            
            if treatment[treatment_id][field_name] is not None:
                
                output_row.append( treatment[treatment_id][field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', end='\n', file=TREATMENT )

with open( subject_associated_project_output_tsv, 'w' ) as SUBJECT_ASSOCIATED_PROJECT:
    
    print( *[ 'subject_id', 'associated_project' ], sep='\t', end='\n', file=SUBJECT_ASSOCIATED_PROJECT )

    for subject_id in sorted( subject ):
        
        for associated_project in sorted( subject[subject_id]['subject_associated_project'] ):
            
            print( *[ subject_id, associated_project ], sep='\t', end='\n', file=SUBJECT_ASSOCIATED_PROJECT )

with open( subject_researchsubject_output_tsv, 'w' ) as SUBJECT_RS:
    
    print( *[ 'subject_id', 'researchsubject_id' ], sep='\t', end='\n', file=SUBJECT_RS )

    for subject_id in sorted( subject ):
        
        for rs_id in sorted( subject_has_researchsubject[subject_id] ):
            
            print( *[ subject_id, rs_id ], sep='\t', end='\n', file=SUBJECT_RS )


