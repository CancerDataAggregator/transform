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

clinical_trial_data_out_dir = path.join( output_root, 'ClinicalTrialData' )
clinical_trial_data_tsv = path.join( clinical_trial_data_out_dir, 'ClinicalTrialData.tsv' )

clinical_targeted_therapy_out_dir = path.join( output_root, 'ClinicalTargetedTherapy' )
clinical_targeted_therapy_tsv = path.join( clinical_targeted_therapy_out_dir, 'ClinicalTargetedTherapy.tsv' )
clinical_targeted_therapy_study_id_tsv = path.join( clinical_targeted_therapy_out_dir, 'ClinicalTargetedTherapy.study_id.tsv' )

clinical_non_targeted_therapy_out_dir = path.join( output_root, 'ClinicalNonTargetedTherapy' )
clinical_non_targeted_therapy_tsv = path.join( clinical_non_targeted_therapy_out_dir, 'ClinicalNonTargetedTherapy.tsv' )
clinical_non_targeted_therapy_study_id_tsv = path.join( clinical_non_targeted_therapy_out_dir, 'ClinicalNonTargetedTherapy.study_id.tsv' )

clinical_radiotherapy_out_dir = path.join( output_root, 'ClinicalRadiotherapy' )
clinical_radiotherapy_tsv = path.join( clinical_radiotherapy_out_dir, 'ClinicalRadiotherapy.tsv' )
clinical_radiotherapy_study_id_tsv = path.join( clinical_radiotherapy_out_dir, 'ClinicalRadiotherapy.study_id.tsv' )

clinical_surgery_out_dir = path.join( output_root, 'ClinicalSurgery' )
clinical_surgery_tsv = path.join( clinical_surgery_out_dir, 'ClinicalSurgery.tsv' )
clinical_surgery_study_id_tsv = path.join( clinical_surgery_out_dir, 'ClinicalSurgery.study_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
clinicalTrialData_json_output_file = path.join( json_out_dir, 'clinicalTrialData.json' )

### OBJECT DEPTH: 0

# Non-scalar ClinicalTrialData fields:
#     targetedTherapyNodeData: [ClinicalTargetedTherapy]
#     nonTargetedTherapyNodeData: [ClinicalNonTargetedTherapy]
#     radiotherapyNodeData: [ClinicalRadiotherapy]
#     surgeryNodeData: [ClinicalSurgery]
scalar_clinical_trial_data_fields = [
    'study_id', # String
    'study_short_name', # String
    'study_accession', # String
    'targetedTherapyNodeCount', # Int
    'targetedTherapyParticipantCount', # Int
    'nonTargetedTherapyNodeCount', # Int
    'nonTargetedTherapyParticipantCount', # Int
    'radiotherapyNodeCount', # Int
    'radiotherapyParticipantCount', # Int
    'surgeryNodeCount', # Int
    'surgeryParticipantCount' # Int
]

### OBJECT DEPTH: 1

# Non-scalar ClinicalTargetedTherapy fields:
#     <none>
scalar_clinical_targeted_therapy_fields = [
    'targeted_therapy_record_id', # String
    'participant_ids', # String
    'targeted_therapy', # String
    'targeted_therapy_dose', # String
    'targeted_therapy_frequency' # String
]

# Non-scalar ClinicalNonTargetedTherapy fields:
#     <none>
scalar_clinical_non_targeted_therapy_fields = [
    'non_targeted_therapy_record_id', # String
    'participant_ids', # String
    'best_response_to_non_targeted_therapy', # String
    'non_targeted_therapy', # String
    'non_targeted_therapy_dose', # String
    'non_targeted_therapy_frequency' # String
]

# Non-scalar ClinicalRadiotherapy fields:
#     <none>
scalar_clinical_radiotherapy_fields = [
    'radiological_procedure_record_id', # String
    'uuid', # String
    'participant_ids', # String
    'radiation_dose', # String
    'radiation_extent', # String
    'radiation_frequency', # String
    'radiological_procedure', # String
    'radiological_procedure_anatomical_location' # String
]

# Non-scalar ClinicalSurgery fields:
#     <none>
scalar_clinical_surgery_fields = [
    'surgical_procedure_record_id', # String
    'participant_ids', # String
    'extent_of_residual_disease', # String
    'surgical_procedure', # String
    'surgical_procedure_anatomical_location', # String
    'surgical_procedure_date', # String
    'surgical_procedure_findings', # String
    'surgical_procedure_therapeutic' # String
]

clinicalTrialData_query = '''
query search {
    clinicalTrialData ( study_id: "$STUDY_ID$" ) {
        ''' + '\n        '.join( scalar_clinical_trial_data_fields ) + '''
        targetedTherapyNodeData {
            ''' + '\n            '.join( scalar_clinical_targeted_therapy_fields ) + '''
        }
        nonTargetedTherapyNodeData {
            ''' + '\n            '.join( scalar_clinical_non_targeted_therapy_fields ) + '''
        }
        radiotherapyNodeData {
            ''' + '\n            '.join( scalar_clinical_radiotherapy_fields ) + '''
        }
        surgeryNodeData {
            ''' + '\n            '.join( scalar_clinical_surgery_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, clinical_trial_data_out_dir, clinical_targeted_therapy_out_dir, clinical_non_targeted_therapy_out_dir, clinical_radiotherapy_out_dir, clinical_surgery_out_dir ]:
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
    'CLINICAL_TRIAL_DATA',
    'CLINICAL_TARGETED_THERAPY',
    'CLINICAL_TARGETED_THERAPY_STUDY_ID',
    'CLINICAL_NON_TARGETED_THERAPY',
    'CLINICAL_NON_TARGETED_THERAPY_STUDY_ID',
    'CLINICAL_RADIOTHERAPY',
    'CLINICAL_RADIOTHERAPY_STUDY_ID',
    'CLINICAL_SURGERY',
    'CLINICAL_SURGERY_STUDY_ID'
]

output_tsv_filenames = [
    clinical_trial_data_tsv,
    clinical_targeted_therapy_tsv,
    clinical_targeted_therapy_study_id_tsv,
    clinical_non_targeted_therapy_tsv,
    clinical_non_targeted_therapy_study_id_tsv,
    clinical_radiotherapy_tsv,
    clinical_radiotherapy_study_id_tsv,
    clinical_surgery_tsv,
    clinical_surgery_study_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_clinical_trial_data_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_TRIAL_DATA'] )
print( *scalar_clinical_targeted_therapy_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_TARGETED_THERAPY'] )
print( *[ 'targeted_therapy_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_TARGETED_THERAPY_STUDY_ID'] )
print( *scalar_clinical_non_targeted_therapy_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_NON_TARGETED_THERAPY'] )
print( *[ 'non_targeted_therapy_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_NON_TARGETED_THERAPY_STUDY_ID'] )
print( *scalar_clinical_radiotherapy_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_RADIOTHERAPY'] )
print( *[ 'radiological_procedure_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_RADIOTHERAPY_STUDY_ID'] )
print( *scalar_clinical_surgery_fields, sep='\t', end='\n', file=output_tsvs['CLINICAL_SURGERY'] )
print( *[ 'surgical_procedure_record_id', 'study_id' ], sep='\t', end='\n', file=output_tsvs['CLINICAL_SURGERY_STUDY_ID'] )

with open( clinicalTrialData_json_output_file, 'w' ) as JSON:
    
    for study_id in sorted( study_dict.keys() ):
        # Send the clinicalTrialData() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_ID\\$', study_id, clinicalTrialData_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_ID\\$', study_id, clinicalTrialData_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for clinical_trial_data_record in result['data']['clinicalTrialData']:
            # Main ClinicalTrialData record.
            clinical_trial_data_row = list()
            for field_name in scalar_clinical_trial_data_fields:
                if field_name in clinical_trial_data_record and clinical_trial_data_record[field_name] is not None:
                    clinical_trial_data_row.append( clinical_trial_data_record[field_name] )
                else:
                    clinical_trial_data_row.append( '' )
            print( *clinical_trial_data_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_TRIAL_DATA'] )
            for clinical_targeted_therapy in clinical_trial_data_record['targetedTherapyNodeData']:
                if 'targeted_therapy_record_id' in clinical_targeted_therapy and clinical_targeted_therapy['targeted_therapy_record_id'] is not None and clinical_targeted_therapy['targeted_therapy_record_id'] != '':
                    clinical_targeted_therapy_row = list()
                    for field_name in scalar_clinical_targeted_therapy_fields:
                        if clinical_targeted_therapy[field_name] is not None:
                            clinical_targeted_therapy_row.append( clinical_targeted_therapy[field_name] )
                        else:
                            clinical_targeted_therapy_row.append( '' )
                    print( *clinical_targeted_therapy_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_TARGETED_THERAPY'] )
                    # Associate this study with this ClinicalTargetedTherapy record.
                    print( *[ clinical_targeted_therapy['targeted_therapy_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_TARGETED_THERAPY_STUDY_ID'] )
            for clinical_non_targeted_therapy in clinical_trial_data_record['nonTargetedTherapyNodeData']:
                if 'non_targeted_therapy_record_id' in clinical_non_targeted_therapy and clinical_non_targeted_therapy['non_targeted_therapy_record_id'] is not None and clinical_non_targeted_therapy['non_targeted_therapy_record_id'] != '':
                    clinical_non_targeted_therapy_row = list()
                    for field_name in scalar_clinical_non_targeted_therapy_fields:
                        if clinical_non_targeted_therapy[field_name] is not None:
                            clinical_non_targeted_therapy_row.append( clinical_non_targeted_therapy[field_name] )
                        else:
                            clinical_non_targeted_therapy_row.append( '' )
                    print( *clinical_non_targeted_therapy_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_NON_TARGETED_THERAPY'] )
                    # Associate this study with this ClinicalNonTargetedTherapy record.
                    print( *[ clinical_non_targeted_therapy['non_targeted_therapy_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_NON_TARGETED_THERAPY_STUDY_ID'] )
            for clinical_radiotherapy in clinical_trial_data_record['radiotherapyNodeData']:
                if 'radiological_procedure_record_id' in clinical_radiotherapy and clinical_radiotherapy['radiological_procedure_record_id'] is not None and clinical_radiotherapy['radiological_procedure_record_id'] != '':
                    clinical_radiotherapy_row = list()
                    for field_name in scalar_clinical_radiotherapy_fields:
                        if clinical_radiotherapy[field_name] is not None:
                            clinical_radiotherapy_row.append( clinical_radiotherapy[field_name] )
                        else:
                            clinical_radiotherapy_row.append( '' )
                    print( *clinical_radiotherapy_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_RADIOTHERAPY'] )
                    # Associate this study with this ClinicalRadiotherapy record.
                    print( *[ clinical_radiotherapy['radiological_procedure_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_RADIOTHERAPY_STUDY_ID'] )
            for clinical_surgery in clinical_trial_data_record['surgeryNodeData']:
                if 'surgical_procedure_record_id' in clinical_surgery and clinical_surgery['surgical_procedure_record_id'] is not None and clinical_surgery['surgical_procedure_record_id'] != '':
                    clinical_surgery_row = list()
                    for field_name in scalar_clinical_surgery_fields:
                        if clinical_surgery[field_name] is not None:
                            clinical_surgery_row.append( clinical_surgery[field_name] )
                        else:
                            clinical_surgery_row.append( '' )
                    print( *clinical_surgery_row, sep='\t', end='\n', file=output_tsvs['CLINICAL_SURGERY'] )
                    # Associate this study with this ClinicalSurgery record.
                    print( *[ clinical_surgery['surgical_procedure_record_id'], study_id ], sep='\t', end='\n', file=output_tsvs['CLINICAL_SURGERY_STUDY_ID'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


