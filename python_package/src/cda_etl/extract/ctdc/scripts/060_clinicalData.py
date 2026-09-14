#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import load_tsv_as_dict, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_dict = load_tsv_as_dict( study_tsv )

clinical_data_out_dir = path.join( output_root, 'ClinicalData' )
clinical_data_tsv = path.join( clinical_data_out_dir, 'ClinicalData.tsv' )
clinical_data_unique_node_types_tsv = path.join( clinical_data_out_dir, 'ClinicalData.unique_node_types.tsv' )

clinical_diagnosis_out_dir = path.join( output_root, 'ClinicalDiagnosis' )
clinical_diagnosis_tsv = path.join( clinical_diagnosis_out_dir, 'ClinicalDiagnosis.tsv' )
clinical_diagnosis_study_id_tsv = path.join( clinical_diagnosis_out_dir, 'ClinicalDiagnosis.study_id.tsv' )

clinical_demographic_out_dir = path.join( output_root, 'ClinicalDemographic' )
clinical_demographic_tsv = path.join( clinical_demographic_out_dir, 'ClinicalDemographic.tsv' )
clinical_demographic_study_id_tsv = path.join( clinical_demographic_out_dir, 'ClinicalDemographic.study_id.tsv' )

clinical_exposure_out_dir = path.join( output_root, 'ClinicalExposure' )
clinical_exposure_tsv = path.join( clinical_exposure_out_dir, 'ClinicalExposure.tsv' )
clinical_exposure_study_id_tsv = path.join( clinical_exposure_out_dir, 'ClinicalExposure.study_id.tsv' )

clinical_specimen_out_dir = path.join( output_root, 'ClinicalSpecimen' )
clinical_specimen_tsv = path.join( clinical_specimen_out_dir, 'ClinicalSpecimen.tsv' )
clinical_specimen_study_id_tsv = path.join( clinical_specimen_out_dir, 'ClinicalSpecimen.study_id.tsv' )

clinical_participant_status_out_dir = path.join( output_root, 'ClinicalParticipantStatus' )
clinical_participant_status_tsv = path.join( clinical_participant_status_out_dir, 'ClinicalParticipantStatus.tsv' )
clinical_participant_status_study_id_tsv = path.join( clinical_participant_status_out_dir, 'ClinicalParticipantStatus.study_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
clinicalData_json_output_file = path.join( json_out_dir, 'clinicalData.json' )

### OBJECT DEPTH: 0

# Non-scalar ClinicalData fields:
#     unique_node_types: [String]
#     diagnosisNodeData: [ClinicalDiagnosis]
#     demographicNodeData: [ClinicalDemographic]
#     exposureNodeData: [ClinicalExposure]
#     specimenNodeData: [ClinicalSpecimen]
#     participantStatusNodeData: [ClinicalParticipantStatus]
scalar_clinical_data_fields = [
    'study_id', # String
    'study_short_name', # String
    'study_accession', # String
    'diagnosisNodeCount', # Int
    'diagnosisParticipantCount', # Int
    'demographicNodeCount', # Int
    'demographicParticipantCount', # Int
    'exposureNodeCount', # Int
    'exposureParticipantCount', # Int
    'specimenNodeCount', # Int
    'specimenParticipantCount', # Int
    'participantStatusNodeCount', # Int
    'participantStatusParticipantCount' # Int
]

### OBJECT DEPTH: 1

# Non-scalar ClinicalDiagnosis fields:
#     <none>
scalar_clinical_diagnosis_fields = [
    'UUID', # String
    'diagnosis_record_id', # String
    'participant_ids', # String (?!)
    'ctep_disease_term', # String
    'date_of_diagnosis', # String
    'date_of_diagnosis_original', # String
    'date_of_diagnosis_unit', # String
    'histological_subtype', # String
    'histology', # String
    'meddra_disease_code', # String
    'primary_diagnosis_disease_group', # String
    'primary_disease_site', # String
    'snomed_disease_code', # String
    'snomed_disease_term' # String
]

# Non-scalar ClinicalDemographic fields:
#     <none>
scalar_clinical_demographic_fields = [
    'uuid', # String
    'demographic_record_id', # String
    'participant_ids', # String
    'age_at_enrollment', # String
    'age_at_enrollment_original', # String
    'age_at_enrollment_original_unit', # String
    'age_at_enrollment_unit', # String
    'body_surface_area', # String
    'body_surface_area_original', # String
    'body_surface_area_original_unit', # String
    'body_surface_area_unit', # String
    'ethnicity', # String
    'height', # String
    'height_original', # String
    'height_original_unit', # String
    'height_unit', # String
    'ncbi_taxonomy_id', # String
    'ncbi_taxonomy_name', # String
    'race', # String
    'sex', # String
    'weight', # String
    'weight_original', # String
    'weight_original_unit', # String
    'weight_unit', # String
    'income', # String
    'occupation', # String
    'highest_level_of_education' # String
]

# Non-scalar ClinicalExposure fields:
#     <none>
scalar_clinical_exposure_fields = [
    'exposure_record_id', # String
    'participant_ids', # String
    'carcinogen_exposure' # String
]

# Non-scalar ClinicalSpecimen fields:
#     <none>
scalar_clinical_specimen_fields = [
    'specimen_record_id', # String
    'crdc_id', # String
    'participant_ids', # String
    'specimen_category', # String
    'specimen_type_concept_code', # String
    'anatomical_collection_site', # String
    'assessment_timepoint', # String
    'collection_date', # String
    'collection_date_original', # String
    'collection_date_original_unit', # String
    'collection_date_unit', # String
    'created', # String
    'tissue_category' # String
]

# Non-scalar ClinicalParticipantStatus fields:
#     <none>
scalar_clinical_participant_status_fields = [
    'participant_status_record_id', # String
    'participant_ids', # String
    'off_study', # String
    'off_study_reason', # String
    'survival_status' # String
]

clinicalData_query = '''
query search {
    clinicalData ( study_id: "$STUDY_ID$" ) {
        ''' + '\n        '.join( scalar_clinical_data_fields ) + '''
        unique_node_types
        diagnosisNodeData {
            ''' + '\n            '.join( scalar_clinical_diagnosis_fields ) + '''
        }
        demographicNodeData {
            ''' + '\n            '.join( scalar_clinical_demographic_fields ) + '''
        }
        exposureNodeData {
            ''' + '\n            '.join( scalar_clinical_exposure_fields ) + '''
        }
        specimenNodeData {
            ''' + '\n            '.join( scalar_clinical_specimen_fields ) + '''
        }
        participantStatusNodeData {
            ''' + '\n            '.join( scalar_clinical_participant_status_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, clinical_data_out_dir, clinical_diagnosis_out_dir, clinical_demographic_out_dir, clinical_exposure_out_dir, clinical_specimen_out_dir, clinical_participant_status_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Open handles for output files to save TSVs describing returned data.
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
    'CLINICAL_DATA',
    'CLINICAL_DATA_UNIQUE_NODE_TYPE',
    'CLINICAL_DIAGNOSIS',
    'CLINICAL_DIAGNOSIS_STUDY_ID',
    'CLINICAL_DEMOGRAPHIC',
    'CLINICAL_DEMOGRAPHIC_STUDY_ID',
    'CLINICAL_EXPOSURE',
    'CLINICAL_EXPOSURE_STUDY_ID',
    'CLINICAL_SPECIMEN',
    'CLINICAL_SPECIMEN_STUDY_ID',
    'CLINICAL_PARTICIPANT_STATUS',
    'CLINICAL_PARTICIPANT_STATUS_STUDY_ID'
]

output_tsv_filenames = [
    clinical_data_tsv,
    clinical_data_unique_node_types_tsv,
    clinical_diagnosis_tsv,
    clinical_diagnosis_study_id_tsv,
    clinical_demographic_tsv,
    clinical_demographic_study_id_tsv,
    clinical_exposure_tsv,
    clinical_exposure_study_id_tsv,
    clinical_specimen_tsv,
    clinical_specimen_study_id_tsv,
    clinical_participant_status_tsv,
    clinical_participant_status_study_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_clinical_data_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_DATA'] )
print( *[ 'study_id', 'unique_node_type' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DATA_UNIQUE_NODE_TYPE'] )
print( *scalar_clinical_diagnosis_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_DIAGNOSIS'] )
print( *[ 'diagnosis_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DIAGNOSIS_STUDY_ID'] )
print( *scalar_clinical_demographic_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_DEMOGRAPHIC'] )
print( *[ 'demographic_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DEMOGRAPHIC_STUDY_ID'] )
print( *scalar_clinical_exposure_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_EXPOSURE'] )
print( *[ 'exposure_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_EXPOSURE_STUDY_ID'] )
print( *scalar_clinical_specimen_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_SPECIMEN'] )
print( *[ 'specimen_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_SPECIMEN_STUDY_ID'] )
print( *scalar_clinical_participant_status_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_PARTICIPANT_STATUS'] )
print( *[ 'participant_status_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_PARTICIPANT_STATUS_STUDY_ID'] )

with open( clinicalData_json_output_file, 'w' ) as JSON:
    
    for study_id in sorted( study_dict.keys() ):
        # Send the clinicalData() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_ID\\$', study_id, clinicalData_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_ID\\$', study_id, clinicalData_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for clinical_data_record in result['data']['clinicalData']:
            # Main ClinicalData record.
            clinical_data_row = list()
            for field_name in scalar_clinical_data_fields:
                if field_name in clinical_data_record and clinical_data_record[field_name] is not None:
                    clinical_data_row.append( clinical_data_record[field_name] )
                else:
                    clinical_data_row.append( '' )
            print( *clinical_data_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_DATA'] )
            # ClinicalData.unique_node_types [array of strings]. Associate with this ClinicalData record (which summarizes clinical data for a single Study).
            if 'unique_node_types' in clinical_data_record and len( clinical_data_record['unique_node_types'] ) > 0:
                for unique_node_type in clinical_data_record['unique_node_types']:
                    print( *[ study_id, unique_node_type ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DATA_UNIQUE_NODE_TYPE'] )
            for clinical_diagnosis in clinical_data_record['diagnosisNodeData']:
                if 'diagnosis_record_id' in clinical_diagnosis and clinical_diagnosis['diagnosis_record_id'] is not None and clinical_diagnosis['diagnosis_record_id'] != '':
                    clinical_diagnosis_row = list()
                    for field_name in scalar_clinical_diagnosis_fields:
                        if clinical_diagnosis[field_name] is not None:
                            clinical_diagnosis_row.append( clinical_diagnosis[field_name] )
                        else:
                            clinical_diagnosis_row.append( '' )
                    print( *clinical_diagnosis_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_DIAGNOSIS'] )
                    # Associate this study with this ClinicalDiagnosis record.
                    print( *[ clinical_diagnosis['diagnosis_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DIAGNOSIS_STUDY_ID'] )
            for clinical_demographic in clinical_data_record['demographicNodeData']:
                if 'demographic_record_id' in clinical_demographic and clinical_demographic['demographic_record_id'] is not None and clinical_demographic['demographic_record_id'] != '':
                    clinical_demographic_row = list()
                    for field_name in scalar_clinical_demographic_fields:
                        if clinical_demographic[field_name] is not None:
                            clinical_demographic_row.append( clinical_demographic[field_name] )
                        else:
                            clinical_demographic_row.append( '' )
                    print( *clinical_demographic_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_DEMOGRAPHIC'] )
                    # Associate this study with this ClinicalDemographic record.
                    print( *[ clinical_demographic['demographic_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_DEMOGRAPHIC_STUDY_ID'] )
            for clinical_exposure in clinical_data_record['exposureNodeData']:
                if 'exposure_record_id' in clinical_exposure and clinical_exposure['exposure_record_id'] is not None and clinical_exposure['exposure_record_id'] != '':
                    clinical_exposure_row = list()
                    for field_name in scalar_clinical_exposure_fields:
                        if clinical_exposure[field_name] is not None:
                            clinical_exposure_row.append( clinical_exposure[field_name] )
                        else:
                            clinical_exposure_row.append( '' )
                    print( *clinical_exposure_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_EXPOSURE'] )
                    # Associate this study with this ClinicalExposure record.
                    print( *[ clinical_exposure['exposure_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_EXPOSURE_STUDY_ID'] )
            for clinical_specimen in clinical_data_record['specimenNodeData']:
                if 'specimen_record_id' in clinical_specimen and clinical_specimen['specimen_record_id'] is not None and clinical_specimen['specimen_record_id'] != '':
                    clinical_specimen_row = list()
                    for field_name in scalar_clinical_specimen_fields:
                        if clinical_specimen[field_name] is not None:
                            clinical_specimen_row.append( clinical_specimen[field_name] )
                        else:
                            clinical_specimen_row.append( '' )
                    print( *clinical_specimen_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_SPECIMEN'] )
                    # Associate this study with this ClinicalSpecimen record.
                    print( *[ clinical_specimen['specimen_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_SPECIMEN_STUDY_ID'] )
            for clinical_participant_status in clinical_data_record['participantStatusNodeData']:
                if 'participant_status_record_id' in clinical_participant_status and clinical_participant_status['participant_status_record_id'] is not None and clinical_participant_status['participant_status_record_id'] != '':
                    clinical_participant_status_row = list()
                    for field_name in scalar_clinical_participant_status_fields:
                        if clinical_participant_status[field_name] is not None:
                            clinical_participant_status_row.append( clinical_participant_status[field_name] )
                        else:
                            clinical_participant_status_row.append( '' )
                    print( *clinical_participant_status_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_PARTICIPANT_STATUS'] )
                    # Associate this study with this ClinicalParticipantStatus record.
                    print( *[ clinical_participant_status['participant_status_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_PARTICIPANT_STATUS_STUDY_ID'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


