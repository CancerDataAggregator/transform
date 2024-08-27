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

enrollment_out_dir = path.join( output_root, 'enrollment' )

enrollment_output_tsv = path.join( enrollment_out_dir, 'enrollment.tsv' )

prior_surgery_out_dir = path.join( output_root, 'prior_surgery' )
prior_surgery_output_tsv = path.join( prior_surgery_out_dir, 'prior_surgery.tsv' )

prior_therapy_out_dir = path.join( output_root, 'prior_therapy' )
prior_therapy_output_tsv = path.join( prior_therapy_out_dir, 'prior_therapy.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

enrollment_case_output_tsv = path.join( relationship_validation_out_dir, 'enrollment.case_id.tsv' )
enrollment_physical_exam_output_tsv = path.join( relationship_validation_out_dir, 'enrollment.physical_exam.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
enrollment_output_json = path.join( json_out_dir, 'enrollment_metadata.json' )

# type enrollment {
#     enrollment_id: String
#     date_of_registration: String
#     registering_institution: String
#     initials: String
#     date_of_informed_consent: String
#     site_short_name: String
#     veterinary_medical_center: String
#     patient_subgroup: String
#     case: case
#     prior_therapies(first: Int, offset: Int, orderBy: [_prior_therapyOrdering!], filter: _prior_therapyFilter): [prior_therapy]
#     prior_surgeries(first: Int, offset: Int, orderBy: [_prior_surgeryOrdering!], filter: _prior_surgeryFilter): [prior_surgery]
#     physical_exams(first: Int, offset: Int, orderBy: [_physical_examOrdering!], filter: _physical_examFilter): [physical_exam]
# }

scalar_enrollment_fields = [
    'enrollment_id',
    'date_of_registration',
    'registering_institution',
    'initials',
    'date_of_informed_consent',
    'site_short_name',
    'veterinary_medical_center',
    'patient_subgroup'
]

# type prior_therapy {
#     date_of_first_dose: String
#     date_of_last_dose: String
#     agent_name: String
#     dose_schedule: String
#     total_dose: Float
#     total_dose_unit: String
#     total_dose_original: Float
#     total_dose_original_unit: String
#     agent_units_of_measure: String
#     best_response_to_prior_therapy: String
#     nonresponse_therapy_type: String
#     prior_therapy_type: String
#     prior_steroid_exposure: Boolean
#     number_of_prior_regimens_steroid: Int
#     total_number_of_doses_steroid: Int
#     date_of_last_dose_steroid: String
#     prior_nsaid_exposure: Boolean
#     number_of_prior_regimens_nsaid: Int
#     total_number_of_doses_nsaid: Int
#     date_of_last_dose_nsaid: String
#     tx_loc_geo_loc_ind_nsaid: String
#     min_rsdl_dz_tx_ind_nsaids_treatment_pe: String
#     therapy_type: String
#     any_therapy: Boolean
#     number_of_prior_regimens_any_therapy: Int
#     total_number_of_doses_any_therapy: Int
#     date_of_last_dose_any_therapy: String
#     treatment_performed_at_site: Boolean
#     treatment_performed_in_minimal_residual: Boolean
#     enrollment: enrollment
#     next_prior_therapy: prior_therapy
#     prior_prior_therapy: prior_therapy
# }

scalar_prior_therapy_fields = [
    'date_of_first_dose',
    'date_of_last_dose',
    'agent_name',
    'dose_schedule',
    'total_dose',
    'total_dose_unit',
    'total_dose_original',
    'total_dose_original_unit',
    'agent_units_of_measure',
    'best_response_to_prior_therapy',
    'nonresponse_therapy_type',
    'prior_therapy_type',
    'prior_steroid_exposure',
    'number_of_prior_regimens_steroid',
    'total_number_of_doses_steroid',
    'date_of_last_dose_steroid',
    'prior_nsaid_exposure',
    'number_of_prior_regimens_nsaid',
    'total_number_of_doses_nsaid',
    'date_of_last_dose_nsaid',
    'tx_loc_geo_loc_ind_nsaid',
    'min_rsdl_dz_tx_ind_nsaids_treatment_pe',
    'therapy_type',
    'any_therapy',
    'number_of_prior_regimens_any_therapy',
    'total_number_of_doses_any_therapy',
    'date_of_last_dose_any_therapy',
    'treatment_performed_at_site',
    'treatment_performed_in_minimal_residual'
]

# type prior_surgery {
#     date_of_surgery: String
#     procedure: String
#     anatomical_site_of_surgery: String
#     surgical_finding: String
#     residual_disease: String
#     therapeutic_indicator: String
#     enrollment: enrollment
#     next_prior_surgery: prior_surgery
#     prior_prior_surgery: prior_surgery
# }

scalar_prior_surgery_fields = [
    'date_of_surgery',
    'procedure',
    'anatomical_site_of_surgery',
    'surgical_finding',
    'residual_disease',
    'therapeutic_indicator'
]

# type physical_exam {
#     date_of_examination: String
#     day_in_cycle: Int
#     body_system: String
#     pe_finding: String
#     pe_comment: String
#     phase_pe: String
#     assessment_timepoint: Int
#     enrollment: enrollment
#     visit: visit
# }

scalar_physical_exam_fields = [
    'date_of_examination',
    'day_in_cycle',
    'body_system',
    'pe_finding',
    'pe_comment',
    'phase_pe',
    'assessment_timepoint'
]

# enrollment(enrollment_id: String, date_of_registration: String, registering_institution: String, initials: String, date_of_informed_consent: String, site_short_name: String, veterinary_medical_center: String, patient_subgroup: String, filter: _enrollmentFilter, first: Int, offset: Int, orderBy: [_enrollmentOrdering!]): [enrollment!]!

enrollment_api_query_json_template = f'''    {{
        
        enrollment( first: {page_size}, offset: __OFFSET__, orderBy: [ enrollment_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_enrollment_fields ) + f'''
            case {{
                case_id
            }}
            prior_therapies( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_first_dose_asc ] ) {{
                ''' + '''
                '''.join( scalar_prior_therapy_fields ) + f'''
                enrollment {{
                    enrollment_id
                }}
                next_prior_therapy {{
                    date_of_first_dose
                }}
                prior_prior_therapy {{
                    date_of_first_dose
                }}
            }}
            prior_surgeries( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_surgery_asc ] ) {{
                ''' + '''
                '''.join( scalar_prior_surgery_fields ) + f'''
                enrollment {{
                    enrollment_id
                }}
                next_prior_surgery {{
                    date_of_surgery
                }}
                prior_prior_surgery {{
                    date_of_surgery
                }}
            }}
            physical_exams( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_examination_asc ] ) {{
                ''' + '''
                '''.join( scalar_physical_exam_fields ) + f'''
                enrollment {{
                    enrollment_id
                }}
                visit {{
                    visit_id
                }}
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ enrollment_out_dir, prior_surgery_out_dir, prior_therapy_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( enrollment_output_json, 'w' ) as JSON, \
    open( enrollment_output_tsv, 'w' ) as ENROLLMENT, \
    open( prior_therapy_output_tsv, 'w' ) as PRIOR_THERAPY, \
    open( prior_surgery_output_tsv, 'w' ) as PRIOR_SURGERY, \
    open( enrollment_case_output_tsv, 'w' ) as ENROLLMENT_CASE, \
    open( enrollment_physical_exam_output_tsv, 'w' ) as ENROLLMENT_PHYSICAL_EXAM:

    print( *scalar_enrollment_fields, sep='\t', file=ENROLLMENT )
    print( *( [ 'enrollment_id', 'enrollment_id.sub_field_sanity_check' ] + scalar_prior_therapy_fields + [ 'prior_prior_therapy.date_of_first_dose', 'next_prior_therapy.date_of_first_dose' ] ), sep='\t', file=PRIOR_THERAPY )
    print( *( [ 'enrollment_id', 'enrollment_id.sub_field_sanity_check' ] + scalar_prior_surgery_fields + [ 'prior_prior_surgery.date_of_surgery', 'next_prior_surgery.date_of_surgery' ] ), sep='\t', file=PRIOR_SURGERY )
    print( *[ 'enrollment_id', 'case_id' ], sep='\t', file=ENROLLMENT_CASE )
    print( *( [ 'visit_id', 'enrollment_id', 'enrollment_id.sub_field_sanity_check' ] + scalar_physical_exam_fields ), sep='\t', file=ENROLLMENT_PHYSICAL_EXAM )

    while not null_result:
    
        enrollment_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), enrollment_api_query_json_template )
        }

        enrollment_response = requests.post( api_url, json=enrollment_api_query_json )

        if not enrollment_response.ok:
            
            print( enrollment_api_query_json['query'], file=sys.stderr )

            enrollment_response.raise_for_status()

        enrollment_result = json.loads( enrollment_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( enrollment_result, indent=4, sort_keys=False ), file=JSON )

        enrollment_count = len( enrollment_result['data']['enrollment'] )

        if enrollment_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for enrollment in enrollment_result['data']['enrollment']:
                
                enrollment_id = enrollment['enrollment_id']

                enrollment_row = list()

                for field_name in scalar_enrollment_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( enrollment[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    enrollment_row.append( field_value )

                print( *enrollment_row, sep='\t', file=ENROLLMENT )

                case_id = ''

                if 'case' in enrollment and enrollment['case'] is not None and 'case_id' in enrollment['case'] and enrollment['case']['case_id'] is not None:
                    
                    case_id = enrollment['case']['case_id']

                print( *[ enrollment_id, case_id ], sep='\t', file=ENROLLMENT_CASE )

                for prior_therapy in enrollment['prior_therapies']:
                    
                    enrollment_sub_id = ''

                    if 'enrollment' in prior_therapy and prior_therapy['enrollment'] is not None and 'enrollment_id' in prior_therapy['enrollment'] and prior_therapy['enrollment']['enrollment_id'] is not None:
                        
                        enrollment_sub_id = prior_therapy['enrollment']['enrollment_id']

                    next_prior_therapy_date_of_first_dose = ''

                    if 'next_prior_therapy' in prior_therapy and prior_therapy['next_prior_therapy'] is not None and 'date_of_first_dose' in prior_therapy['next_prior_therapy'] and prior_therapy['next_prior_therapy']['date_of_first_dose'] is not None:
                        
                        next_prior_therapy_date_of_first_dose = prior_therapy['next_prior_therapy']['date_of_first_dose']

                    prior_prior_therapy_date_of_first_dose = ''

                    if 'prior_prior_therapy' in prior_therapy and prior_therapy['prior_prior_therapy'] is not None and 'date_of_first_dose' in prior_therapy['prior_prior_therapy'] and prior_therapy['prior_prior_therapy']['date_of_first_dose'] is not None:
                        
                        prior_prior_therapy_date_of_first_dose = prior_therapy['prior_prior_therapy']['date_of_first_dose']

                    prior_therapy_row = [ enrollment_id, enrollment_sub_id ]

                    for field_name in scalar_prior_therapy_fields:
                        
                        if field_name in prior_therapy and prior_therapy[field_name] is not None:
                            
                            prior_therapy_row.append( json.dumps( prior_therapy[field_name] ).strip( '"' ) )

                        else:
                            
                            prior_therapy_row.append( '' )

                    prior_therapy_row = prior_therapy_row + [ prior_prior_therapy_date_of_first_dose, next_prior_therapy_date_of_first_dose ]

                    print( *prior_therapy_row, sep='\t', file=PRIOR_THERAPY )

                prior_therapy_count = len( enrollment['prior_therapies'] )

                if prior_therapy_count == page_size:
                    
                    print( f"WARNING: Enrollment {enrollment_id} has at least {page_size} prior_therapy records: implement paging here or risk data loss.", file=sys.stderr )

                for prior_surgery in enrollment['prior_surgeries']:
                    
                    enrollment_sub_id = ''

                    if 'enrollment' in prior_surgery and prior_surgery['enrollment'] is not None and 'enrollment_id' in prior_surgery['enrollment'] and prior_surgery['enrollment']['enrollment_id'] is not None:
                        
                        enrollment_sub_id = prior_surgery['enrollment']['enrollment_id']

                    next_prior_surgery_date_of_surgery = ''

                    if 'next_prior_surgery' in prior_surgery and prior_surgery['next_prior_surgery'] is not None and 'date_of_surgery' in prior_surgery['next_prior_surgery'] and prior_surgery['next_prior_surgery']['date_of_surgery'] is not None:
                        
                        next_prior_surgery_date_of_surgery = prior_surgery['next_prior_surgery']['date_of_surgery']

                    prior_prior_surgery_date_of_surgery = ''

                    if 'prior_prior_surgery' in prior_surgery and prior_surgery['prior_prior_surgery'] is not None and 'date_of_surgery' in prior_surgery['prior_prior_surgery'] and prior_surgery['prior_prior_surgery']['date_of_surgery'] is not None:
                        
                        prior_prior_surgery_date_of_surgery = prior_surgery['prior_prior_surgery']['date_of_surgery']

                    prior_surgery_row = [ enrollment_id, enrollment_sub_id ]

                    for field_name in scalar_prior_surgery_fields:
                        
                        if field_name in prior_surgery and prior_surgery[field_name] is not None:
                            
                            prior_surgery_row.append( json.dumps( prior_surgery[field_name] ).strip( '"' ) )

                        else:
                            
                            prior_surgery_row.append( '' )

                    prior_surgery_row = prior_surgery_row + [ prior_prior_surgery_date_of_surgery, next_prior_surgery_date_of_surgery ]

                    print( *prior_surgery_row, sep='\t', file=PRIOR_SURGERY )

                prior_surgery_count = len( enrollment['prior_surgeries'] )

                if prior_surgery_count == page_size:
                    
                    print( f"WARNING: Enrollment {enrollment_id} has at least {page_size} prior_surgery records: implement paging here or risk data loss.", file=sys.stderr )

                for physical_exam in enrollment['physical_exams']:
                    
                    visit_id = ''

                    if 'visit' in physical_exam and physical_exam['visit'] is not None and 'visit_id' in physical_exam['visit'] and physical_exam['visit']['visit_id'] is not None:
                        
                        visit_id = physical_exam['visit']['visit_id']

                    enrollment_sub_id = ''

                    if 'enrollment' in physical_exam and physical_exam['enrollment'] is not None and 'enrollment_id' in physical_exam['enrollment'] and physical_exam['enrollment']['enrollment_id'] is not None:
                        
                        enrollment_sub_id = physical_exam['enrollment']['enrollment_id']

                    physical_exam_row = [ visit_id, enrollment_id, enrollment_sub_id ]

                    for field_name in scalar_physical_exam_fields:
                        
                        if field_name in physical_exam and physical_exam[field_name] is not None:
                            
                            physical_exam_row.append( json.dumps( physical_exam[field_name] ).strip( '"' ) )

                        else:
                            
                            physical_exam_row.append( '' )

                    print( *physical_exam_row, sep='\t', file=ENROLLMENT_PHYSICAL_EXAM )

                physical_exam_count = len( enrollment['physical_exams'] )

                if physical_exam_count == page_size:
                    
                    print( f"WARNING: Enrollment {enrollment_id} has at least {page_size} physical_exam records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


