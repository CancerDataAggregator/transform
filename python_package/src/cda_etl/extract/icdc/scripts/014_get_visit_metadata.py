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

visit_out_dir = path.join( output_root, 'visit' )
visit_output_tsv = path.join( visit_out_dir, 'visit.tsv' )
visit_prior_visit_output_tsv = path.join( visit_out_dir, 'visit.prior_visit_visit_id.tsv' )
visit_next_visit_output_tsv = path.join( visit_out_dir, 'visit.next_visit_visit_id.tsv' )

physical_exam_out_dir = path.join( output_root, 'physical_exam' )
visit_physical_exam_output_tsv = path.join( physical_exam_out_dir, 'physical_exam.tsv' )

disease_extent_out_dir = path.join( output_root, 'disease_extent' )
visit_disease_extent_output_tsv = path.join( disease_extent_out_dir, 'disease_extent.tsv' )

vital_signs_out_dir = path.join( output_root, 'vital_signs' )
visit_vital_signs_output_tsv = path.join( vital_signs_out_dir, 'vital_signs.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

visit_case_output_tsv = path.join( relationship_validation_out_dir, 'visit.case_id.tsv' )
visit_cycle_output_tsv = path.join( relationship_validation_out_dir, 'visit.cycle_case_id_and_cycle_number.tsv' )
visit_agent_administration_output_tsv = path.join( relationship_validation_out_dir, 'visit.agent_administration_document_number_and_medication.tsv' )
visit_sample_output_tsv = path.join( relationship_validation_out_dir, 'visit.sample_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
visit_output_json = path.join( json_out_dir, 'visit_metadata.json' )

# type visit {
#     visit_date: String
#     visit_number: String
#     visit_id: String
#     case: case
#     cycle: cycle
#     agent_administrations(first: Int, offset: Int, orderBy: [_agent_administrationOrdering!], filter: _agent_administrationFilter): [agent_administration]
#     samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
#     physical_exams(first: Int, offset: Int, orderBy: [_physical_examOrdering!], filter: _physical_examFilter): [physical_exam]
#     lab_exams(first: Int, offset: Int, filter: _lab_examFilter): [lab_exam]
#     disease_extents(first: Int, offset: Int, orderBy: [_disease_extentOrdering!], filter: _disease_extentFilter): [disease_extent]
#     vital_signs(first: Int, offset: Int, orderBy: [_vital_signsOrdering!], filter: _vital_signsFilter): [vital_signs]
#     next_visit: visit
#     prior_visit: visit
# }

scalar_visit_fields = [
    'visit_id',
    'visit_date',
    'visit_number'
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

# type disease_extent {
#     lesion_number: String
#     lesion_site: String
#     lesion_description: String
#     previously_irradiated: String
#     previously_treated: String
#     measurable_lesion: String
#     target_lesion: String
#     date_of_evaluation: String
#     measured_how: String
#     longest_measurement: Float
#     longest_measurement_unit: String
#     longest_measurement_original: Float
#     longest_measurement_original_unit: String
#     evaluation_number: String
#     evaluation_code: String
#     visit: visit
# }

scalar_disease_extent_fields = [
    'lesion_number',
    'lesion_site',
    'lesion_description',
    'previously_irradiated',
    'previously_treated',
    'measurable_lesion',
    'target_lesion',
    'date_of_evaluation',
    'measured_how',
    'longest_measurement',
    'longest_measurement_unit',
    'longest_measurement_original',
    'longest_measurement_original_unit',
    'evaluation_number',
    'evaluation_code'
]

# type vital_signs {
#     date_of_vital_signs: String
#     body_temperature: Float
#     body_temperature_unit: String
#     body_temperature_original: Float
#     body_temperature_original_unit: String
#     pulse: Int
#     pulse_unit: String
#     pulse_original: Int
#     pulse_original_unit: String
#     respiration_rate: Int
#     respiration_rate_unit: String
#     respiration_rate_original: Int
#     respiration_rate_original_unit: String
#     respiration_pattern: String
#     systolic_bp: Int
#     systolic_bp_unit: String
#     systolic_bp_original: Int
#     systolic_bp_original_unit: String
#     pulse_ox: Float
#     pulse_ox_unit: String
#     pulse_ox_original: Float
#     pulse_ox_original_unit: String
#     patient_weight: Float
#     patient_weight_unit: String
#     patient_weight_original: Float
#     patient_weight_original_unit: String
#     body_surface_area: Float
#     body_surface_area_unit: String
#     body_surface_area_original: Float
#     body_surface_area_original_unit: String
#     modified_ecog: String
#     ecg: String
#     assessment_timepoint: Int
#     phase: String
#     visit: visit
# }

scalar_vital_signs_fields = [
    'date_of_vital_signs',
    'body_temperature',
    'body_temperature_unit',
    'body_temperature_original',
    'body_temperature_original_unit',
    'pulse',
    'pulse_unit',
    'pulse_original',
    'pulse_original_unit',
    'respiration_rate',
    'respiration_rate_unit',
    'respiration_rate_original',
    'respiration_rate_original_unit',
    'respiration_pattern',
    'systolic_bp',
    'systolic_bp_unit',
    'systolic_bp_original',
    'systolic_bp_original_unit',
    'pulse_ox',
    'pulse_ox_unit',
    'pulse_ox_original',
    'pulse_ox_original_unit',
    'patient_weight',
    'patient_weight_unit',
    'patient_weight_original',
    'patient_weight_original_unit',
    'body_surface_area',
    'body_surface_area_unit',
    'body_surface_area_original',
    'body_surface_area_original_unit',
    'modified_ecog',
    'ecg',
    'assessment_timepoint',
    'phase'
]

# visit(visit_date: String, visit_number: String, visit_id: String, filter: _visitFilter, first: Int, offset: Int, orderBy: [_visitOrdering!]): [visit!]!

visit_api_query_json_template = f'''    {{
        
        visit( first: {page_size}, offset: __OFFSET__, orderBy: [ visit_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_visit_fields ) + f'''
            case {{
                case_id
            }}
            cycle {{
                cycle_number
                case {{
                    case_id
                }}
            }}
            agent_administrations( first: {page_size}, offset: __OFFSET__, orderBy: [ document_number_asc, medication_asc ] ) {{
                document_number
                medication
            }}
            samples( first: {page_size}, offset: __OFFSET__, orderBy: [ sample_id_asc ] ) {{
                sample_id
            }}
            physical_exams( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_examination_asc ] ) {{
                ''' + '''
                '''.join( scalar_physical_exam_fields ) + f'''
                enrollment {{
                    enrollment_id
                }}
            }}
            disease_extents( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_evaluation_asc, evaluation_number_asc, evaluation_code_asc, lesion_site_asc ] ) {{
                ''' + '''
                '''.join( scalar_disease_extent_fields ) + f'''
            }}
            vital_signs( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_vital_signs_asc ] ) {{
                ''' + '''
                '''.join( scalar_vital_signs_fields ) + f'''
            }}
            next_visit {{
                visit_id
            }}
            prior_visit {{
                visit_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ visit_out_dir, physical_exam_out_dir, disease_extent_out_dir, vital_signs_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( visit_output_json, 'w' ) as JSON, \
    open( visit_output_tsv, 'w' ) as VISIT, \
    open( visit_prior_visit_output_tsv, 'w' ) as VISIT_PRIOR, \
    open( visit_next_visit_output_tsv, 'w' ) as VISIT_NEXT, \
    open( visit_physical_exam_output_tsv, 'w' ) as VISIT_PHYSICAL_EXAM, \
    open( visit_disease_extent_output_tsv, 'w' ) as VISIT_DISEASE_EXTENT, \
    open( visit_vital_signs_output_tsv, 'w' ) as VISIT_VITAL_SIGNS, \
    open( visit_case_output_tsv, 'w' ) as VISIT_CASE, \
    open( visit_cycle_output_tsv, 'w' ) as VISIT_CYCLE, \
    open( visit_agent_administration_output_tsv, 'w' ) as VISIT_AGENT_ADMINISTRATION, \
    open( visit_sample_output_tsv, 'w' ) as VISIT_SAMPLE:

    print( *scalar_visit_fields, sep='\t', file=VISIT )
    print( *[ 'visit_id', 'prior_visit.visit_id' ], sep='\t', file=VISIT_PRIOR )
    print( *[ 'visit_id', 'next_visit.visit_id' ], sep='\t', file=VISIT_NEXT )
    print( *( [ 'visit_id', 'enrollment_id' ] + scalar_physical_exam_fields ), sep='\t', file=VISIT_PHYSICAL_EXAM )
    print( *( [ 'visit_id' ] + scalar_disease_extent_fields ), sep='\t', file=VISIT_DISEASE_EXTENT )
    print( *( [ 'visit_id' ] + scalar_vital_signs_fields ), sep='\t', file=VISIT_VITAL_SIGNS )
    print( *[ 'visit_id', 'case_id' ], sep='\t', file=VISIT_CASE )
    print( *[ 'visit_id', 'case_id', 'cycle_number' ], sep='\t', file=VISIT_CYCLE )
    print( *[ 'visit_id', 'document_number', 'medication' ], sep='\t', file=VISIT_AGENT_ADMINISTRATION )
    print( *[ 'visit_id', 'sample_id' ], sep='\t', file=VISIT_SAMPLE )

    while not null_result:
    
        visit_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), visit_api_query_json_template )
        }

        visit_response = requests.post( api_url, json=visit_api_query_json )

        if not visit_response.ok:
            
            print( visit_api_query_json['query'], file=sys.stderr )

            visit_response.raise_for_status()

        visit_result = json.loads( visit_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( visit_result, indent=4, sort_keys=False ), file=JSON )

        visit_count = len( visit_result['data']['visit'] )

        if visit_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for visit in visit_result['data']['visit']:
                
                visit_id = visit['visit_id']

                visit_row = list()

                for field_name in scalar_visit_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( visit[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    visit_row.append( field_value )

                print( *visit_row, sep='\t', file=VISIT )

                for physical_exam in visit['physical_exams']:
                    
                    enrollment_id = ''

                    if 'enrollment' in physical_exam and physical_exam['enrollment'] is not None:
                        
                        enrollment_id = physical_exam['enrollment']['enrollment_id']

                    physical_exam_row = [ visit_id, enrollment_id ]

                    for field_name in scalar_physical_exam_fields:
                        
                        if field_name in physical_exam and physical_exam[field_name] is not None:
                            
                            physical_exam_row.append( json.dumps( physical_exam[field_name] ).strip( '"' ) )

                        else:
                            
                            physical_exam_row.append( '' )

                    print( *physical_exam_row, sep='\t', file=VISIT_PHYSICAL_EXAM )

                physical_exam_count = len( visit['physical_exams'] )

                if physical_exam_count == page_size:
                    
                    print( f"WARNING: Visit {visit_id} has at least {page_size} physical_exam records: implement paging here or risk data loss.", file=sys.stderr )

                for disease_extent in visit['disease_extents']:
                    
                    disease_extent_row = [ visit_id ]

                    for field_name in scalar_disease_extent_fields:
                        
                        if field_name in disease_extent and disease_extent[field_name] is not None:
                            
                            disease_extent_row.append( json.dumps( disease_extent[field_name] ).strip( '"' ) )

                        else:
                            
                            disease_extent_row.append( '' )

                    print( *disease_extent_row, sep='\t', file=VISIT_DISEASE_EXTENT )

                disease_extent_count = len( visit['disease_extents'] )

                if disease_extent_count == page_size:
                    
                    print( f"WARNING: Visit {visit_id} has at least {page_size} disease_extent records: implement paging here or risk data loss.", file=sys.stderr )

                for vital_signs in visit['vital_signs']:
                    
                    vital_signs_row = [ visit_id ]

                    for field_name in scalar_vital_signs_fields:
                        
                        if field_name in vital_signs and vital_signs[field_name] is not None:
                            
                            vital_signs_row.append( json.dumps( vital_signs[field_name] ).strip( '"' ) )

                        else:
                            
                            vital_signs_row.append( '' )

                    print( *vital_signs_row, sep='\t', file=VISIT_VITAL_SIGNS )

                vital_signs_count = len( visit['vital_signs'] )

                if vital_signs_count == page_size:
                    
                    print( f"WARNING: Visit {visit_id} has at least {page_size} vital_signs records: implement paging here or risk data loss.", file=sys.stderr )

                if 'case' in visit and visit['case'] is not None and visit['case']['case_id'] is not None:
                    
                    print( *[ visit_id, visit['case']['case_id'] ], sep='\t', file=VISIT_CASE )

                if 'cycle' in visit and visit['cycle'] is not None:
                    
                    case_id = visit['cycle']['case']['case_id']

                    cycle_number = ''

                    if 'cycle_number' in visit['cycle'] and visit['cycle']['cycle_number'] is not None:
                        
                        cycle_number = visit['cycle']['cycle_number']
                    
                    print( *[ visit_id, case_id, cycle_number ], sep='\t', file=VISIT_CYCLE )

                for agent_administration in visit['agent_administrations']:
                    
                    document_number = ''

                    if 'document_number' in agent_administration and agent_administration['document_number'] is not None:
                        
                        document_number = agent_administration['document_number']

                    medication = ''

                    if 'medication' in agent_administration and agent_administration['medication'] is not None:
                        
                        medication = agent_administration['medication']

                    print( *[ visit_id, document_number, medication ], sep='\t', file=VISIT_AGENT_ADMINISTRATION )

                agent_administration_count = len( visit['agent_administrations'] )

                if agent_administration_count == page_size:
                    
                    print( f"WARNING: Visit {visit_id} has at least {page_size} agent_administration records: implement paging here or risk data loss.", file=sys.stderr )

                for sample in visit['samples']:
                    
                    # sample_id should never be null, and should throw an error if a null is encountered.

                    print( *[ visit_id, sample['sample_id'] ], sep='\t', file=VISIT_SAMPLE )

                sample_count = len( visit['samples'] )

                if sample_count == page_size:
                    
                    print( f"WARNING: Visit {visit_id} has at least {page_size} sample records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


