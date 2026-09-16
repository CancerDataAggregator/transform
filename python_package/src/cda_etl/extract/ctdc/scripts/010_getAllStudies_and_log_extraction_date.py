#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import get_current_date, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

extraction_date_file = path.join( output_root, 'extraction_date.txt' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_participant_id_tsv = path.join( study_out_dir, 'Study.participant_id.tsv' )
study_associated_link_record_id_tsv = path.join( study_out_dir, 'Study.associated_link_record_id.tsv' )
study_image_collection_record_id_tsv = path.join( study_out_dir, 'Study.image_collection_record_id.tsv' )
study_person_record_id_tsv = path.join( study_out_dir, 'Study.person_record_id.tsv' )
study_digital_object_id_tsv = path.join( study_out_dir, 'Study.digital_object_id.tsv' )
study_consent_group_id_tsv = path.join( study_out_dir, 'Study.consent_group_id.tsv' )

participant_out_dir = path.join( output_root, 'Participant' )
participant_tsv = path.join( participant_out_dir, 'Participant.tsv' )
participant_demographic_record_id_tsv = path.join( participant_out_dir, 'Participant.demographic_record_id.tsv' )
participant_exposure_record_id_tsv = path.join( participant_out_dir, 'Participant.exposure_record_id.tsv' )
participant_diagnosis_record_id_tsv = path.join( participant_out_dir, 'Participant.diagnosis_record_id.tsv' )
participant_targeted_therapy_record_id_tsv = path.join( participant_out_dir, 'Participant.targeted_therapy_record_id.tsv' )
participant_non_targeted_therapy_record_id_tsv = path.join( participant_out_dir, 'Participant.non_targeted_therapy_record_id.tsv' )
participant_surgical_procedure_record_id_tsv = path.join( participant_out_dir, 'Participant.surgical_procedure_record_id.tsv' )
participant_radiological_procedure_record_id_tsv = path.join( participant_out_dir, 'Participant.radiological_procedure_record_id.tsv' )
participant_participant_status_record_id_tsv = path.join( participant_out_dir, 'Participant.participant_status_record_id.tsv' )
participant_specimen_record_id_tsv = path.join( participant_out_dir, 'Participant.specimen_record_id.from_getAllStudies.tsv' )

demographic_out_dir = path.join( output_root, 'Demographic' )
demographic_tsv = path.join( demographic_out_dir, 'Demographic.tsv' )

exposure_out_dir = path.join( output_root, 'Exposure' )
exposure_tsv = path.join( exposure_out_dir, 'Exposure.tsv' )

diagnosis_out_dir = path.join( output_root, 'Diagnosis' )
diagnosis_tsv = path.join( diagnosis_out_dir, 'Diagnosis.from_getAllStudies.tsv' )

targeted_therapy_out_dir = path.join( output_root, 'TargetedTherapy' )
targeted_therapy_tsv = path.join( targeted_therapy_out_dir, 'TargetedTherapy.tsv' )

non_targeted_therapy_out_dir = path.join( output_root, 'NonTargetedTherapy' )
non_targeted_therapy_tsv = path.join( non_targeted_therapy_out_dir, 'NonTargetedTherapy.tsv' )

surgery_out_dir = path.join( output_root, 'Surgery' )
surgery_tsv = path.join( surgery_out_dir, 'Surgery.tsv' )

radiotherapy_out_dir = path.join( output_root, 'Radiotherapy' )
radiotherapy_tsv = path.join( radiotherapy_out_dir, 'Radiotherapy.tsv' )

participant_status_out_dir = path.join( output_root, 'ParticipantStatus' )
participant_status_tsv = path.join( participant_status_out_dir, 'ParticipantStatus.tsv' )

specimen_out_dir = path.join( output_root, 'Specimen' )
specimen_tsv = path.join( specimen_out_dir, 'Specimen.from_getAllStudies.tsv' )

associated_link_out_dir = path.join( output_root, 'AssociatedLink' )
associated_link_tsv = path.join( associated_link_out_dir, 'AssociatedLink.tsv' )

image_collection_out_dir = path.join( output_root, 'ImageCollection' )
image_collection_tsv = path.join( image_collection_out_dir, 'ImageCollection.tsv' )

principal_investigator_out_dir = path.join( output_root, 'PrincipalInvestigator' )
principal_investigator_tsv = path.join( principal_investigator_out_dir, 'PrincipalInvestigator.tsv' )

publication_out_dir = path.join( output_root, 'Publication' )
publication_tsv = path.join( publication_out_dir, 'Publication.from_getAllStudies.tsv' )

consent_group_out_dir = path.join( output_root, 'ConsentGroup' )
consent_group_tsv = path.join( consent_group_out_dir, 'ConsentGroup.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
getAllStudies_json_output_file = path.join( json_out_dir, 'getAllStudies.json' )

### OBJECT DEPTH: 0

# Non-scalar Study fields:
#     participants: [Participant]
#     associated_links: [AssociatedLink]
#     image_collection: [ImageCollection]
#     principal_investigators: [PrincipalInvestigator]
#     publications: [Publication]
#     consent_groups: [ConsentGroup]
scalar_study_fields = [
    'study_id',
    'study_short_name',
    'study_accession',
    'study_name',
    'study_description',
    'study_type',
    'dates_of_conduct',
    'participant_count',
    'image_collection_count',
    'study_file_count',
    'participant_file_count'
]

### OBJECT DEPTH: 1

# Non-scalar Participant fields:
#     demographic: Demographic
#     exposure: [Exposure]
#     diagnosis: [Diagnosis]
#     targeted_therapy: [TargetedTherapy]
#     non_targeted_therapy: [NonTargetedTherapy]
#     surgery: [Surgery]
#     radiotherapy: [Radiotherapy]
#     participant_status: ParticipantStatus
#     specimens: [Specimen]
scalar_participant_fields = [
    'participant_id',
    'biomarker_results_available',
    'histology_images_available',
    'radiology_images_available',
    'radiology_report_available',
    'study_short_name'
]

# Non-scalar AssociatedLink fields:
#     <none>
scalar_associated_link_fields = [
    'associated_link_record_id',
    'associated_link_name',
    'associated_link_url'
]

# Non-scalar ImageCollection fields:
#     <none>
scalar_image_collection_fields = [
    'image_collection_record_id',
    'image_collection_name',
    'image_type_included',
    'image_collection_url',
    'repository_name',
    'collection_access'
]

# Non-scalar PrincipalInvestigator fields:
#     <none>
scalar_principal_investigator_fields = [
    'person_record_id',
    'person_first_name',
    'person_last_name',
    'person_middle_name',
    'person_orcid'
]

# Non-scalar Publication fields:
#     <none>
scalar_publication_fields = [
    # I'm guessing this first one is the field most likely to be both unique and ubiquitous, so it's the key/ID for now until proven otherwise.
    'journal_citation',
    'digital_object_id',
    'pubmed_id',
    'publication_title',
    'authorship',
    'year_of_publication'
]

# Non-scalar ConsentGroup fields:
#     <none>
scalar_consent_group_fields = [
    'consent_group_id',
    'consent_group_name',
    'consent_group_number'
]

### OBJECT DEPTH: 2

# Non-scalar Demographic fields:
#     <none>
scalar_demographic_fields = [
    'demographic_record_id',
    'age_at_enrollment',
    'race',
    'ethnicity',
    'sex',
    'height',
    'weight',
    'body_surface_area',
    'occupation',
    'income',
    'highest_level_of_education',
    'ncbi_taxonomy_id',
    'ncbi_taxonomy_name'
]

# Non-scalar Exposure fields:
#     <none>
scalar_exposure_fields = [
    'exposure_record_id',
    'environmental_exposure_type',
    'carcinogen_exposure'
]

# Non-scalar Diagnosis fields:
#     <none>
scalar_diagnosis_fields = [
    'diagnosis_record_id',
    'primary_diagnosis_disease_group',
    'ctep_disease_term',
    'meddra_disease_code',
    'snomed_disease_term',
    'snomed_disease_code',
    'primary_disease_site',
    'histology',
    'histological_subtype',
    'stage_of_disease',
    'tumor_grade'
]

# Non-scalar TargetedTherapy fields:
#     <none>
scalar_targeted_therapy_fields = [
    'targeted_therapy_record_id',
    'targeted_therapy',
    'targeted_therapy_dose',
    'targeted_therapy_dose_units',
    'targeted_therapy_frequency',
    'targeted_therapy_start_date',
    'targeted_therapy_end_date',
    'best_response_to_targeted_therapy'
]

# Non-scalar NonTargetedTherapy fields:
#     <none>
scalar_non_targeted_therapy_fields = [
    'non_targeted_therapy_record_id',
    'non_targeted_therapy',
    'non_targeted_therapy_dose',
    'non_targeted_therapy_dose_units',
    'non_targeted_therapy_frequency',
    'non_targeted_therapy_start_date',
    'non_targeted_therapy_end_date',
    'best_response_to_non_targeted_therapy'
]

# Non-scalar Surgery fields:
#     <none>
scalar_surgery_fields = [
    'surgical_procedure_record_id',
    'surgical_procedure',
    'surgical_procedure_date',
    'surgical_procedure_anatomical_location',
    'surgical_procedure_therapeutic',
    'surgical_procedure_findings',
    'extent_of_residual_disease'
]

# Non-scalar Radiotherapy fields:
#     <none>
scalar_radiotherapy_fields = [
    'radiological_procedure_record_id',
    'radiological_procedure',
    'radiological_procedure_anatomical_location',
    'radiation_dose',
    'radiation_dose_units',
    'radiation_frequency',
    'radiation_extent',
    'radiotherapy_start_date',
    'radiotherapy_end_date',
    'best_response_to_radiotherapy'
]

# Non-scalar ParticipantStatus fields:
#     <none>
scalar_participant_status_fields = [
    'participant_status_record_id',
    'survival_status',
    'primary_cause_of_death',
    'off_study',
    'off_study_reason'
]

# Non-scalar Specimen fields:
#     <none>
scalar_specimen_fields = [
    'specimen_record_id',
    'specimen_type',
    'specimen_category',
    'anatomical_collection_site',
    'tissue_category',
    'assessment_timepoint',
    'collection_date'
]

getAllStudies_query = '''
query search {
    getAllStudies {
        ''' + '\n        '.join( scalar_study_fields ) + '''
        participants {
            ''' + '\n            '.join( scalar_participant_fields ) + '''
            demographic {
                ''' + '\n                '.join( scalar_demographic_fields ) + '''
            }
            exposure {
                ''' + '\n                '.join( scalar_exposure_fields ) + '''
            }
            diagnosis {
                ''' + '\n                '.join( scalar_diagnosis_fields ) + '''
            }
            targeted_therapy {
                ''' + '\n                '.join( scalar_targeted_therapy_fields ) + '''
            }
            non_targeted_therapy {
                ''' + '\n                '.join( scalar_non_targeted_therapy_fields ) + '''
            }
            surgery {
                ''' + '\n                '.join( scalar_surgery_fields ) + '''
            }
            radiotherapy {
                ''' + '\n                '.join( scalar_radiotherapy_fields ) + '''
            }
            participant_status {
                ''' + '\n                '.join( scalar_participant_status_fields ) + '''
            }
            specimens {
                ''' + '\n                '.join( scalar_specimen_fields ) + '''
            }
        }
        associated_links {
            ''' + '\n            '.join( scalar_associated_link_fields ) + '''
        }
        image_collection {
            ''' + '\n            '.join( scalar_image_collection_fields ) + '''
        }
        principal_investigators {
            ''' + '\n            '.join( scalar_principal_investigator_fields ) + '''
        }
        publications {
            ''' + '\n            '.join( scalar_publication_fields ) + '''
        }
        consent_groups {
            ''' + '\n            '.join( scalar_consent_group_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, study_out_dir, participant_out_dir, demographic_out_dir, exposure_out_dir, diagnosis_out_dir, targeted_therapy_out_dir, non_targeted_therapy_out_dir, surgery_out_dir, radiotherapy_out_dir, participant_status_out_dir, specimen_out_dir, associated_link_out_dir, image_collection_out_dir, principal_investigator_out_dir, publication_out_dir, consent_group_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

with open( extraction_date_file, 'w' ) as OUT:
    print( get_current_date(), file=OUT )

# Send the getAllStudies() query to the API server.
response = requests.post(
    ctdc_api_url,
    json={
        'query': getAllStudies_query,
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( getAllStudies_query, file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

# Save a version of the returned data as raw JSON.
with open( getAllStudies_json_output_file, 'w' ) as JSON:
    print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

# Open handles for output files to save TSVs describing:
# 
#     * the returned Study objects
#     * association TSVs enumerating containment relationships between objects and sub-objects (e.g. Study->Participant)
# 
# We can't always safely use the Python `with` keyword in cases like this because the Python interpreter
# enforces an arbitrary hard-coded limit on the total number of simultaneous indents, and each `with` keyword
# creates another indent, even if you use the
# 
#     with open(A) as A, open(B) as B, ...
# 
# (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
# have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
# indentation, especially when the syntax above should've avoided creating multiple nested indents in the first place.

output_tsv_keywords = [
    'STUDY',
    'STUDY_PARTICIPANT',
    'PARTICIPANT',
    'PARTICIPANT_DEMOGRAPHIC',
    'DEMOGRAPHIC',
    'PARTICIPANT_EXPOSURE',
    'EXPOSURE',
    'PARTICIPANT_DIAGNOSIS',
    'DIAGNOSIS',
    'PARTICIPANT_TARGETED_THERAPY',
    'TARGETED_THERAPY',
    'PARTICIPANT_NON_TARGETED_THERAPY',
    'NON_TARGETED_THERAPY',
    'PARTICIPANT_SURGERY',
    'SURGERY',
    'PARTICIPANT_RADIOTHERAPY',
    'RADIOTHERAPY',
    'PARTICIPANT_PARTICIPANT_STATUS',
    'PARTICIPANT_STATUS',
    'PARTICIPANT_SPECIMEN',
    'SPECIMEN',
    'STUDY_ASSOCIATED_LINK',
    'ASSOCIATED_LINK',
    'STUDY_IMAGE_COLLECTION',
    'IMAGE_COLLECTION',
    'STUDY_PRINCIPAL_INVESTIGATOR',
    'PRINCIPAL_INVESTIGATOR',
    'STUDY_PUBLICATION',
    'PUBLICATION',
    'STUDY_CONSENT_GROUP',
    'CONSENT_GROUP'
]

output_tsv_filenames = [
    study_tsv,
    study_participant_id_tsv,
    participant_tsv,
    participant_demographic_record_id_tsv,
    demographic_tsv,
    participant_exposure_record_id_tsv,
    exposure_tsv,
    participant_diagnosis_record_id_tsv,
    diagnosis_tsv,
    participant_targeted_therapy_record_id_tsv,
    targeted_therapy_tsv,
    participant_non_targeted_therapy_record_id_tsv,
    non_targeted_therapy_tsv,
    participant_surgical_procedure_record_id_tsv,
    surgery_tsv,
    participant_radiological_procedure_record_id_tsv,
    radiotherapy_tsv,
    participant_participant_status_record_id_tsv,
    participant_status_tsv,
    participant_specimen_record_id_tsv,
    specimen_tsv,
    study_associated_link_record_id_tsv,
    associated_link_tsv,
    study_image_collection_record_id_tsv,
    image_collection_tsv,
    study_person_record_id_tsv,
    principal_investigator_tsv,
    study_digital_object_id_tsv,
    publication_tsv,
    study_consent_group_id_tsv,
    consent_group_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_study_fields, sep='\t', end='\n', file=output_tsvs['STUDY'] )
print( *[ 'study_id', 'participant_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_PARTICIPANT'] )
print( *scalar_participant_fields, sep='\t', end='\n', file=output_tsvs['PARTICIPANT'] )
print( *[ 'participant_id', 'demographic_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DEMOGRAPHIC'] )
print( *scalar_demographic_fields, sep='\t', end='\n', file=output_tsvs['DEMOGRAPHIC'] )
print( *[ 'participant_id', 'exposure_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_EXPOSURE'] )
print( *scalar_exposure_fields, sep='\t', end='\n', file=output_tsvs['EXPOSURE'] )
print( *[ 'participant_id', 'diagnosis_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DIAGNOSIS'] )
print( *scalar_diagnosis_fields, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )
print( *[ 'participant_id', 'targeted_therapy_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_TARGETED_THERAPY'] )
print( *scalar_targeted_therapy_fields, sep='\t', end='\n', file=output_tsvs['TARGETED_THERAPY'] )
print( *[ 'participant_id', 'non_targeted_therapy_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_NON_TARGETED_THERAPY'] )
print( *scalar_non_targeted_therapy_fields, sep='\t', end='\n', file=output_tsvs['NON_TARGETED_THERAPY'] )
print( *[ 'participant_id', 'surgical_procedure_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_SURGERY'] )
print( *scalar_surgery_fields, sep='\t', end='\n', file=output_tsvs['SURGERY'] )
print( *[ 'participant_id', 'radiological_procedure_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_RADIOTHERAPY'] )
print( *scalar_radiotherapy_fields, sep='\t', end='\n', file=output_tsvs['RADIOTHERAPY'] )
print( *[ 'participant_id', 'participant_status_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_PARTICIPANT_STATUS'] )
print( *scalar_participant_status_fields, sep='\t', end='\n', file=output_tsvs['PARTICIPANT_STATUS'] )
print( *[ 'participant_id', 'specimen_record_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_SPECIMEN'] )
print( *scalar_specimen_fields, sep='\t', end='\n', file=output_tsvs['SPECIMEN'] )
print( *[ 'study_id', 'associated_link_record_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_ASSOCIATED_LINK'] )
print( *scalar_associated_link_fields, sep='\t', end='\n', file=output_tsvs['ASSOCIATED_LINK'] )
print( *[ 'study_id', 'image_collection_record_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_IMAGE_COLLECTION'] )
print( *scalar_image_collection_fields, sep='\t', end='\n', file=output_tsvs['IMAGE_COLLECTION'] )
print( *[ 'study_id', 'person_record_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_PRINCIPAL_INVESTIGATOR'] )
print( *scalar_principal_investigator_fields, sep='\t', end='\n', file=output_tsvs['PRINCIPAL_INVESTIGATOR'] )
print( *[ 'study_id', 'digital_object_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_PUBLICATION'] )
print( *scalar_publication_fields, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )
print( *[ 'study_id', 'consent_group_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_CONSENT_GROUP'] )
print( *scalar_consent_group_fields, sep='\t', end='\n', file=output_tsvs['CONSENT_GROUP'] )

# Don't print duplicate records.
seen = {
    'participant': set(),
    'demographic': set(),
    'exposure': set(),
    'diagnosis': set(),
    'targeted_therapy': set(),
    'non_targeted_therapy': set(),
    'surgery': set(),
    'radiotherapy': set(),
    'participant_status': set(),
    'specimen': set(),
    'associated_link': set(),
    'image_collection': set(),
    'principal_investigator': set(),
    'publication': set(),
    'consent_group': set()
}

# Parse the returned data and save to TSV.
for study in result['data']['getAllStudies']:
    # Main Study metadata.
    study_row = list()
    for field_name in scalar_study_fields:
        if study[field_name] is not None:
            study_row.append( study[field_name] )
        else:
            study_row.append( '' )
    print( *study_row, sep='\t', end='\n', file=output_tsvs['STUDY'] )

    # Study.participants [array of Participant records].
    if study['participants'] is not None and len( study['participants'] ) > 0:
        
        for participant in study['participants']:
            # Main Participant metadata.
            participant_row = list()
            for field_name in scalar_participant_fields:
                if participant[field_name] is not None:
                    participant_row.append( participant[field_name] )
                else:
                    participant_row.append( '' )
            # Don't print duplicate Participant records. This should break with a KeyError if participant_id isn't present.
            if participant['participant_id'] not in seen['participant']:
                print( *participant_row, sep='\t', end='\n', file=output_tsvs['PARTICIPANT'] )
                seen['participant'].add( participant['participant_id'] )
            print( *[ study['study_id'], participant['participant_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_PARTICIPANT'] )

            # Participant.demographic [a unique (possibly null) Demographic record].
            if participant['demographic'] is not None:
                demographic = participant['demographic']
                demographic_row = list()
                for field_name in scalar_demographic_fields:
                    if demographic[field_name] is not None:
                        demographic_row.append( demographic[field_name] )
                    else:
                        demographic_row.append( '' )
                # Don't print duplicate Demographic records. This should break with a KeyError if demographic_record_id isn't present.
                if demographic['demographic_record_id'] not in seen['demographic']:
                    print( *demographic_row, sep='\t', end='\n', file=output_tsvs['DEMOGRAPHIC'] )
                    seen['demographic'].add( demographic['demographic_record_id'] )
                print( *[ participant['participant_id'], demographic['demographic_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DEMOGRAPHIC'] )

            # Participant.exposure [array of Exposure records].
            if participant['exposure'] is not None and len( participant['exposure'] ) > 0:
                
                for exposure in participant['exposure']:
                    # Main Exposure metadata.
                    exposure_row = list()
                    for field_name in scalar_exposure_fields:
                        if exposure[field_name] is not None:
                            exposure_row.append( exposure[field_name] )
                        else:
                            exposure_row.append( '' )
                    # Don't print duplicate Exposure records. This should break with a KeyError if exposure_record_id isn't present.
                    if exposure['exposure_record_id'] not in seen['exposure']:
                        print( *exposure_row, sep='\t', end='\n', file=output_tsvs['EXPOSURE'] )
                        seen['exposure'].add( exposure['exposure_record_id'] )
                    print( *[ participant['participant_id'], exposure['exposure_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_EXPOSURE'] )

            # Participant.diagnosis [array of Diagnosis records].
            if participant['diagnosis'] is not None and len( participant['diagnosis'] ) > 0:
                
                for diagnosis in participant['diagnosis']:
                    # Main Diagnosis metadata.
                    diagnosis_row = list()
                    for field_name in scalar_diagnosis_fields:
                        if diagnosis[field_name] is not None:
                            diagnosis_row.append( diagnosis[field_name] )
                        else:
                            diagnosis_row.append( '' )
                    # Don't print duplicate Diagnosis records. This should break with a KeyError if diagnosis_record_id isn't present.
                    if diagnosis['diagnosis_record_id'] not in seen['diagnosis']:
                        print( *diagnosis_row, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )
                        seen['diagnosis'].add( diagnosis['diagnosis_record_id'] )
                    print( *[ participant['participant_id'], diagnosis['diagnosis_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DIAGNOSIS'] )

            # Participant.targeted_therapy [array of TargetedTherapy records].
            if participant['targeted_therapy'] is not None and len( participant['targeted_therapy'] ) > 0:
                
                for targeted_therapy in participant['targeted_therapy']:
                    # Main TargetedTherapy metadata.
                    targeted_therapy_row = list()
                    for field_name in scalar_targeted_therapy_fields:
                        if targeted_therapy[field_name] is not None:
                            targeted_therapy_row.append( targeted_therapy[field_name] )
                        else:
                            targeted_therapy_row.append( '' )
                    # Don't print duplicate TargetedTherapy records. This should break with a KeyError if targeted_therapy_record_id isn't present.
                    if targeted_therapy['targeted_therapy_record_id'] not in seen['targeted_therapy']:
                        print( *targeted_therapy_row, sep='\t', end='\n', file=output_tsvs['TARGETED_THERAPY'] )
                        seen['targeted_therapy'].add( targeted_therapy['targeted_therapy_record_id'] )
                    print( *[ participant['participant_id'], targeted_therapy['targeted_therapy_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_TARGETED_THERAPY'] )

            # Participant.non_targeted_therapy [array of NonTargetedTherapy records].
            if participant['non_targeted_therapy'] is not None and len( participant['non_targeted_therapy'] ) > 0:
                
                for non_targeted_therapy in participant['non_targeted_therapy']:
                    # Main NonTargetedTherapy metadata.
                    non_targeted_therapy_row = list()
                    for field_name in scalar_non_targeted_therapy_fields:
                        if non_targeted_therapy[field_name] is not None:
                            non_targeted_therapy_row.append( non_targeted_therapy[field_name] )
                        else:
                            non_targeted_therapy_row.append( '' )
                    # Don't print duplicate NonTargetedTherapy records. This should break with a KeyError if non_targeted_therapy_record_id isn't present.
                    if non_targeted_therapy['non_targeted_therapy_record_id'] not in seen['non_targeted_therapy']:
                        print( *non_targeted_therapy_row, sep='\t', end='\n', file=output_tsvs['NON_TARGETED_THERAPY'] )
                        seen['non_targeted_therapy'].add( non_targeted_therapy['non_targeted_therapy_record_id'] )
                    print( *[ participant['participant_id'], non_targeted_therapy['non_targeted_therapy_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_NON_TARGETED_THERAPY'] )

            # Participant.surgery [array of Surgery records].
            if participant['surgery'] is not None and len( participant['surgery'] ) > 0:
                
                for surgery in participant['surgery']:
                    # Main Surgery metadata.
                    surgery_row = list()
                    for field_name in scalar_surgery_fields:
                        if surgery[field_name] is not None:
                            surgery_row.append( surgery[field_name] )
                        else:
                            surgery_row.append( '' )
                    # Don't print duplicate Surgery records. This should break with a KeyError if surgical_procedure_record_id isn't present.
                    if surgery['surgical_procedure_record_id'] not in seen['surgery']:
                        print( *surgery_row, sep='\t', end='\n', file=output_tsvs['SURGERY'] )
                        seen['surgery'].add( surgery['surgical_procedure_record_id'] )
                    print( *[ participant['participant_id'], surgery['surgical_procedure_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_SURGERY'] )

            # Participant.radiotherapy [array of Radiotherapy records].
            if participant['radiotherapy'] is not None and len( participant['radiotherapy'] ) > 0:
                
                for radiotherapy in participant['radiotherapy']:
                    # Main Radiotherapy metadata.
                    radiotherapy_row = list()
                    for field_name in scalar_radiotherapy_fields:
                        if radiotherapy[field_name] is not None:
                            radiotherapy_row.append( radiotherapy[field_name] )
                        else:
                            radiotherapy_row.append( '' )
                    # Don't print duplicate Radiotherapy records. This should break with a KeyError if radiological_procedure_record_id isn't present.
                    if radiotherapy['radiological_procedure_record_id'] not in seen['radiotherapy']:
                        print( *radiotherapy_row, sep='\t', end='\n', file=output_tsvs['RADIOTHERAPY'] )
                        seen['radiotherapy'].add( radiotherapy['radiological_procedure_record_id'] )
                    print( *[ participant['participant_id'], radiotherapy['radiological_procedure_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_RADIOTHERAPY'] )

            # Participant.participant_status [a unique (possibly null) ParticipantStatus record].
            if participant['participant_status'] is not None:
                participant_status = participant['participant_status']
                participant_status_row = list()
                for field_name in scalar_participant_status_fields:
                    if participant_status[field_name] is not None:
                        participant_status_row.append( participant_status[field_name] )
                    else:
                        participant_status_row.append( '' )
                # Don't print duplicate ParticipantStatus records. This should break with a KeyError if participant_status_record_id isn't present.
                    if participant_status['participant_status_record_id'] not in seen['participant_status']:
                        print( *participant_status_row, sep='\t', end='\n', file=output_tsvs['PARTICIPANT_STATUS'] )
                        seen['participant_status'].add( participant_status['participant_status_record_id'] )
                    print( *[ participant['participant_id'], participant_status['participant_status_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_PARTICIPANT_STATUS'] )

            # Participant.specimens [array of Specimen records].
            if participant['specimens'] is not None and len( participant['specimens'] ) > 0:
                
                for specimen in participant['specimens']:
                    # Main Specimen metadata.
                    specimen_row = list()
                    for field_name in scalar_specimen_fields:
                        if specimen[field_name] is not None:
                            specimen_row.append( specimen[field_name] )
                        else:
                            specimen_row.append( '' )
                    # Don't print duplicate Specimen records. This should break with a KeyError if specimen_record_id isn't present.
                    if specimen['specimen_record_id'] not in seen['specimen']:
                        print( *specimen_row, sep='\t', end='\n', file=output_tsvs['SPECIMEN'] )
                        seen['specimen'].add( specimen['specimen_record_id'] )
                    print( *[ participant['participant_id'], specimen['specimen_record_id'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_SPECIMEN'] )

    # Study.associated_links [array of AssociatedLink records].
    if study['associated_links'] is not None and len( study['associated_links'] ) > 0:
        
        for associated_link in study['associated_links']:
            # Main AssociatedLink metadata. Note: there are stub records here where everything is null.
            associated_link_row = list()
            all_null = True
            for field_name in scalar_associated_link_fields:
                if associated_link[field_name] is not None:
                    associated_link_row.append( associated_link[field_name] )
                    all_null = False
                else:
                    associated_link_row.append( '' )
            # Don't print null or duplicate AssociatedLink records. This should break with a KeyError if associated_link_record_id isn't present.
            if not all_null:
                if associated_link['associated_link_record_id'] not in seen['associated_link']:
                    print( *associated_link_row, sep='\t', end='\n', file=output_tsvs['ASSOCIATED_LINK'] )
                    seen['associated_link'].add( associated_link['associated_link_record_id'] )
                print( *[ study['study_id'], associated_link['associated_link_record_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_ASSOCIATED_LINK'] )

    # Study.image_collection [array of ImageCollection records].
    if study['image_collection'] is not None and len( study['image_collection'] ) > 0:
        
        for image_collection in study['image_collection']:
            # Main ImageCollection metadata.
            image_collection_row = list()
            for field_name in scalar_image_collection_fields:
                if image_collection[field_name] is not None:
                    image_collection_row.append( image_collection[field_name] )
                else:
                    image_collection_row.append( '' )
            # Don't print duplicate ImageCollection records. This should break with a KeyError if image_collection_record_id isn't present.
            if image_collection['image_collection_record_id'] not in seen['image_collection']:
                print( *image_collection_row, sep='\t', end='\n', file=output_tsvs['IMAGE_COLLECTION'] )
                seen['image_collection'].add( image_collection['image_collection_record_id'] )
            print( *[ study['study_id'], image_collection['image_collection_record_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_IMAGE_COLLECTION'] )

    # Study.principal_investigators [array of PrincipalInvestigator records].
    if study['principal_investigators'] is not None and len( study['principal_investigators'] ) > 0:
        
        for principal_investigator in study['principal_investigators']:
            # Main PrincipalInvestigator metadata.
            principal_investigator_row = list()
            for field_name in scalar_principal_investigator_fields:
                if principal_investigator[field_name] is not None:
                    principal_investigator_row.append( principal_investigator[field_name] )
                else:
                    principal_investigator_row.append( '' )
            # Don't print duplicate PrincipalInvestigator records. This should break with a KeyError if person_record_id isn't present.
            if principal_investigator['person_record_id'] not in seen['principal_investigator']:
                print( *principal_investigator_row, sep='\t', end='\n', file=output_tsvs['PRINCIPAL_INVESTIGATOR'] )
                seen['principal_investigator'].add( principal_investigator['person_record_id'] )
            print( *[ study['study_id'], principal_investigator['person_record_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_PRINCIPAL_INVESTIGATOR'] )

    # Study.publications [array of Publication records].
    if study['publications'] is not None and len( study['publications'] ) > 0:
        
        for publication in study['publications']:
            # Main Publication metadata. Note: there are stub records here where everything is null.
            publication_row = list()
            all_null = True
            for field_name in scalar_publication_fields:
                if publication[field_name] is not None:
                    publication_row.append( publication[field_name] )
                    all_null = False
                else:
                    publication_row.append( '' )
            # Don't print null or duplicate Publication records. This should break with a KeyError if digital_object_id isn't present.
            if not all_null:
                if publication['digital_object_id'] not in seen['publication']:
                    print( *publication_row, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )
                    seen['publication'].add( publication['digital_object_id'] )
                print( *[ study['study_id'], publication['digital_object_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_PUBLICATION'] )

    # Study.consent_groups [array of ConsentGroup records].
    if study['consent_groups'] is not None and len( study['consent_groups'] ) > 0:
        
        for consent_group in study['consent_groups']:
            # Main ConsentGroup metadata.
            consent_group_row = list()
            for field_name in scalar_consent_group_fields:
                if consent_group[field_name] is not None:
                    consent_group_row.append( consent_group[field_name] )
                else:
                    consent_group_row.append( '' )
            # Don't print duplicate ConsentGroup records. This should break with a KeyError if consent_group_id isn't present.
            if consent_group['consent_group_id'] not in seen['consent_group']:
                print( *consent_group_row, sep='\t', end='\n', file=output_tsvs['CONSENT_GROUP'] )
                seen['consent_group'].add( consent_group['consent_group_id'] )
            print( *[ study['study_id'], consent_group['consent_group_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_CONSENT_GROUP'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


