#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://caninecommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'icdc' )

case_out_dir = path.join( output_root, 'case' )

case_output_tsv = path.join( case_out_dir, 'case.tsv' )

case_cohort_output_tsv = path.join( case_out_dir, 'case.cohort_id.tsv' )
case_study_output_tsv = path.join( case_out_dir, 'case.clinical_study_designation.tsv' )
case_enrollment_output_tsv = path.join( case_out_dir, 'case.enrollment_id.tsv' )
case_demographic_output_tsv = path.join( case_out_dir, 'case.demographic_id.tsv' )
case_study_arm_output_tsv = path.join( case_out_dir, 'case.study_arm_id.tsv' )
case_diagnosis_output_tsv = path.join( case_out_dir, 'case.diagnosis_id.tsv' )
case_sample_output_tsv = path.join( case_out_dir, 'case.sample_id.tsv' )
case_file_output_tsv = path.join( case_out_dir, 'case.file_uuid.tsv' )
case_visit_output_tsv = path.join( case_out_dir, 'case.visit_id.tsv' )

off_study_out_dir = path.join( output_root, 'off_study' )
case_off_study_output_tsv = path.join( off_study_out_dir, 'off_study.tsv' )

off_treatment_out_dir = path.join( output_root, 'off_treatment' )
case_off_treatment_output_tsv = path.join( off_treatment_out_dir, 'off_treatment.tsv' )

canine_individual_out_dir = path.join( output_root, 'canine_individual' )
case_canine_individual_output_tsv = path.join( canine_individual_out_dir, 'canine_individual.tsv' )

follow_up_out_dir = path.join( output_root, 'follow_up' )
case_follow_up_output_tsv = path.join( follow_up_out_dir, 'follow_up.tsv' )

registration_out_dir = path.join( output_root, 'registration' )
case_registration_output_tsv = path.join( registration_out_dir, 'registration.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
case_output_json = path.join( json_out_dir, 'case_metadata.json' )

# type case {
#    case_id: String
#    patient_id: String
#    patient_first_name: String
#    cohort: cohort
#    study: study
#    enrollment: enrollment
#    demographic: demographic
#    study_arm: study_arm
#    adverse_event: adverse_event
#    off_study: off_study
#    off_treatment: off_treatment
#    canine_individual: canine_individual
#    diagnoses(first: Int, offset: Int, orderBy: [_diagnosisOrdering!], filter: _diagnosisFilter): [diagnosis]
#    cycles(first: Int, offset: Int, orderBy: [_cycleOrdering!], filter: _cycleFilter): [cycle]
#    follow_ups(first: Int, offset: Int, orderBy: [_follow_upOrdering!], filter: _follow_upFilter): [follow_up]
#    samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
#    files(first: Int, offset: Int, orderBy: [_fileOrdering!], filter: _fileFilter): [file]
#    visits(first: Int, offset: Int, orderBy: [_visitOrdering!], filter: _visitFilter): [visit]
#    adverse_events(first: Int, offset: Int, orderBy: [_adverse_eventOrdering!], filter: _adverse_eventFilter): [adverse_event]
#    registrations(first: Int, offset: Int, orderBy: [_registrationOrdering!], filter: _registrationFilter): [registration]
# }

scalar_case_fields = [
    'case_id',
    'patient_id',
    'patient_first_name'
]

# type off_study {
#     document_number: String
#     date_off_study: String
#     reason_off_study: String
#     date_of_disease_progression: String
#     date_off_treatment: String
#     best_resp_vet_tx_tp_secondary_response: String
#     date_last_medication_administration: String
#     best_resp_vet_tx_tp_best_response: String
#     date_of_best_response: String
#     case: case
# }

scalar_off_study_fields = [
    'document_number',
    'date_off_study',
    'reason_off_study',
    'date_of_disease_progression',
    'date_off_treatment',
    'best_resp_vet_tx_tp_secondary_response',
    'date_last_medication_administration',
    'best_resp_vet_tx_tp_best_response',
    'date_of_best_response'
]

# type off_treatment {
#     document_number: String
#     date_off_treatment: String
#     reason_off_treatment: String
#     date_of_disease_progression: String
#     best_resp_vet_tx_tp_secondary_response: String
#     date_last_medication_administration: String
#     best_resp_vet_tx_tp_best_response: String
#     date_of_best_response: String
#     case: case
# }

scalar_off_treatment_fields = [
    'document_number',
    'date_off_treatment',
    'reason_off_treatment',
    'date_of_disease_progression',
    'best_resp_vet_tx_tp_secondary_response',
    'date_last_medication_administration',
    'best_resp_vet_tx_tp_best_response',
    'date_of_best_response'
]

# type canine_individual {
#     canine_individual_id: String
#     cases(first: Int, offset: Int, orderBy: [_caseOrdering!], filter: _caseFilter): [case]
# }

scalar_canine_individual_fields = [
    'canine_individual_id'
]

# type follow_up {
#     document_number: String
#     date_of_last_contact: String
#     patient_status: String
#     explain_unknown_status: String
#     contact_type: String
#     treatment_since_last_contact: Boolean
#     physical_exam_performed: Boolean
#     physical_exam_changes: String
#     case: case
# }

scalar_follow_up_fields = [
    'document_number',
    'date_of_last_contact',
    'patient_status',
    'explain_unknown_status',
    'contact_type',
    'treatment_since_last_contact',
    'physical_exam_performed',
    'physical_exam_changes'
]

# type registration {
#     registration_origin: String
#     registration_id: String
#     cases(first: Int, offset: Int, orderBy: [_caseOrdering!], filter: _caseFilter): [case]
# }


scalar_registration_fields = [
    'registration_id',
    'registration_origin'
]


# case(case_id: String, patient_id: String, patient_first_name: String, filter: _caseFilter, first: Int, offset: Int, orderBy: [_caseOrdering!]): [case!]!

case_api_query_json_template = f'''    {{
        
        case( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_case_fields ) + f'''
            cohort {{
                cohort_id
            }}
            study {{
                clinical_study_designation
            }}
            enrollment {{
                enrollment_id
            }}
            demographic {{
                demographic_id
            }}
            study_arm {{
                arm_id
            }}
            off_study {{
                ''' + '''
                '''.join( scalar_off_study_fields ) + f'''
            }}
            off_treatment {{
                ''' + '''
                '''.join( scalar_off_treatment_fields ) + f'''
            }}
            canine_individual {{
                ''' + '''
                '''.join( scalar_canine_individual_fields ) + f'''
            }}
            diagnoses( first: {page_size}, offset: 0, orderBy: [ diagnosis_id_asc ] ) {{
                diagnosis_id
            }}
            follow_ups( first: {page_size}, offset: 0, orderBy: [ document_number_asc ] ) {{
                ''' + '''
                '''.join( scalar_follow_up_fields ) + f'''
            }}
            samples( first: {page_size}, offset: 0, orderBy: [ sample_id_asc ] ) {{
                sample_id
            }}
            files( first: {page_size}, offset: 0, orderBy: [ uuid_asc ] ) {{
                uuid
            }}
            visits( first: {page_size}, offset: 0, orderBy: [ visit_id_asc ] ) {{
                visit_id
            }}
            registrations( first: {page_size}, offset: 0, orderBy: [ registration_id_asc ] ) {{
                ''' + '''
                '''.join( scalar_registration_fields ) + f'''
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ case_out_dir, off_study_out_dir, off_treatment_out_dir, canine_individual_out_dir, follow_up_out_dir, registration_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( case_output_json, 'w' ) as JSON, \
    open( case_output_tsv, 'w' ) as CASE, \
    open( case_cohort_output_tsv, 'w' ) as CASE_COHORT, \
    open( case_study_output_tsv, 'w' ) as CASE_STUDY, \
    open( case_enrollment_output_tsv, 'w' ) as CASE_ENROLLMENT, \
    open( case_demographic_output_tsv, 'w' ) as CASE_DEMOGRAPHIC, \
    open( case_study_arm_output_tsv, 'w' ) as CASE_STUDY_ARM, \
    open( case_diagnosis_output_tsv, 'w' ) as CASE_DIAGNOSIS, \
    open( case_sample_output_tsv, 'w' ) as CASE_SAMPLE, \
    open( case_file_output_tsv, 'w' ) as CASE_FILE, \
    open( case_visit_output_tsv, 'w' ) as CASE_VISIT, \
    open( case_off_study_output_tsv, 'w' ) as CASE_OFF_STUDY, \
    open( case_off_treatment_output_tsv, 'w' ) as CASE_OFF_TREATMENT, \
    open( case_canine_individual_output_tsv, 'w' ) as CASE_CANINE_INDIVIDUAL, \
    open( case_follow_up_output_tsv, 'w' ) as CASE_FOLLOW_UP, \
    open( case_registration_output_tsv, 'w' ) as CASE_REGISTRATION:

    print( *scalar_case_fields, sep='\t', file=CASE )
    print( *[ 'case_id', 'cohort_id' ], sep='\t', file=CASE_COHORT )
    print( *[ 'case_id', 'clinical_study_designation' ], sep='\t', file=CASE_STUDY )
    print( *[ 'case_id', 'enrollment_id' ], sep='\t', file=CASE_ENROLLMENT )
    print( *[ 'case_id', 'demographic_id' ], sep='\t', file=CASE_DEMOGRAPHIC )
    print( *[ 'case_id', 'arm_id' ], sep='\t', file=CASE_STUDY_ARM )
    print( *[ 'case_id', 'diagnosis_id' ], sep='\t', file=CASE_DIAGNOSIS )
    print( *[ 'case_id', 'sample_id' ], sep='\t', file=CASE_SAMPLE )
    print( *[ 'case_id', 'uuid' ], sep='\t', file=CASE_FILE )
    print( *[ 'case_id', 'visit_id' ], sep='\t', file=CASE_VISIT )
    print( *( [ 'case_id' ] + scalar_off_study_fields ), sep='\t', file=CASE_OFF_STUDY )
    print( *( [ 'case_id' ] + scalar_off_treatment_fields ), sep='\t', file=CASE_OFF_TREATMENT )
    print( *( [ 'case_id' ] + scalar_canine_individual_fields ), sep='\t', file=CASE_CANINE_INDIVIDUAL )
    print( *( [ 'case_id' ] + scalar_follow_up_fields ), sep='\t', file=CASE_FOLLOW_UP )
    print( *( [ 'case_id' ] + scalar_registration_fields ), sep='\t', file=CASE_REGISTRATION )

    while not null_result:
    
        case_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), case_api_query_json_template )
        }

        case_response = requests.post( api_url, json=case_api_query_json )

        if not case_response.ok:
            
            print( case_api_query_json['query'], file=sys.stderr )

            case_response.raise_for_status()

        case_result = json.loads( case_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( case_result, indent=4, sort_keys=False ), file=JSON )

        case_count = len( case_result['data']['case'] )

        if case_count == 0:
            
            null_result = True

        else:
            
            # Save case fields and association data as TSVs.

            for case in case_result['data']['case']:
                
                case_id = case['case_id']

                case_row = list()

                for field_name in scalar_case_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( case[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    case_row.append( field_value )

                print( *case_row, sep='\t', file=CASE )

                if 'cohort' in case and case['cohort'] is not None and case['cohort']['cohort_id'] is not None:
                    
                    print( *[ case_id, json.dumps( case['cohort']['cohort_id'] ).strip( '"' ) ], sep='\t', file=CASE_COHORT )

                if 'study' in case and case['study'] is not None and case['study']['clinical_study_designation'] is not None:
                    
                    print( *[ case_id, case['study']['clinical_study_designation'] ], sep='\t', file=CASE_STUDY )

                if 'enrollment' in case and case['enrollment'] is not None and case['enrollment']['enrollment_id'] is not None:
                    
                    print( *[ case_id, case['enrollment']['enrollment_id'] ], sep='\t', file=CASE_ENROLLMENT )

                if 'demographic' in case and case['demographic'] is not None and case['demographic']['demographic_id'] is not None:
                    
                    print( *[ case_id, case['demographic']['demographic_id'] ], sep='\t', file=CASE_DEMOGRAPHIC )

                if 'study_arm' in case and case['study_arm'] is not None and case['study_arm']['arm_id'] is not None:
                    
                    print( *[ case_id, case['study_arm']['arm_id'] ], sep='\t', file=CASE_STUDY_ARM )

                if 'off_study' in case and case['off_study'] is not None:
                    
                    off_study_row = [ case_id ]

                    for field_name in scalar_off_study_fields:
                        
                        if field_name in case['off_study'] and case['off_study'][field_name] is not None:
                            
                            off_study_row.append( case['off_study'][field_name] )

                        else:
                            
                            off_study_row.append( '' )

                    print( *off_study_row, sep='\t', file=CASE_OFF_STUDY )

                if 'off_treatment' in case and case['off_treatment'] is not None:
                    
                    off_treatment_row = [ case_id ]

                    for field_name in scalar_off_treatment_fields:
                        
                        if field_name in case['off_treatment'] and case['off_treatment'][field_name] is not None:
                            
                            off_treatment_row.append( case['off_treatment'][field_name] )

                        else:
                            
                            off_treatment_row.append( '' )

                    print( *off_treatment_row, sep='\t', file=CASE_OFF_TREATMENT )

                if 'canine_individual' in case and case['canine_individual'] is not None:
                    
                    canine_individual_row = [ case_id ]

                    for field_name in scalar_canine_individual_fields:
                        
                        if field_name in case['canine_individual'] and case['canine_individual'][field_name] is not None:
                            
                            canine_individual_row.append( case['canine_individual'][field_name] )

                        else:
                            
                            canine_individual_row.append( '' )

                    print( *canine_individual_row, sep='\t', file=CASE_CANINE_INDIVIDUAL )

                for follow_up in case['follow_ups']:
                    
                    follow_up_row = [ case_id ]

                    for field_name in scalar_follow_up_fields:
                        
                        if field_name in follow_up and follow_up[field_name] is not None:
                            
                            follow_up_row.append( follow_up[field_name] )

                        else:
                            
                            follow_up_row.append( '' )

                    print( *follow_up_row, sep='\t', file=CASE_FOLLOW_UP )

                follow_up_count = len( case['follow_ups'] )

                if follow_up_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} follow_up records: implement paging here or risk data loss.", file=sys.stderr )

                for registration in case['registrations']:
                    
                    registration_row = [ case_id ]

                    for field_name in scalar_registration_fields:
                        
                        if field_name in registration and registration[field_name] is not None:
                            
                            registration_row.append( registration[field_name] )

                        else:
                            
                            registration_row.append( '' )

                    print( *registration_row, sep='\t', file=CASE_REGISTRATION )

                registration_count = len( case['registrations'] )

                if registration_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} registration records: implement paging here or risk data loss.", file=sys.stderr )

                for diagnosis in case['diagnoses']:
                    
                    print( *[ case_id, diagnosis['diagnosis_id'] ], sep='\t', file=CASE_DIAGNOSIS )

                diagnosis_count = len( case['diagnoses'] )

                if diagnosis_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} diagnoses: implement paging here or risk data loss.", file=sys.stderr )

                for sample in case['samples']:
                    
                    print( *[ case_id, sample['sample_id'] ], sep='\t', file=CASE_SAMPLE )

                sample_count = len( case['samples'] )

                if sample_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} samples: implement paging here or risk data loss.", file=sys.stderr )

                for file in case['files']:
                    
                    print( *[ case_id, file['uuid'] ], sep='\t', file=CASE_FILE )

                file_count = len( case['files'] )

                if file_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} files: implement paging here or risk data loss.", file=sys.stderr )

                for visit in case['visits']:
                    
                    print( *[ case_id, visit['visit_id'] ], sep='\t', file=CASE_VISIT )

                visit_count = len( case['visits'] )

                if visit_count == page_size:
                    
                    print( f"WARNING: Case {case_id} has at least {page_size} visits: implement paging here or risk data loss.", visit=sys.stderr )

        offset = offset + page_size


