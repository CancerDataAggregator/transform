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

study_arm_out_dir = path.join( output_root, 'study_arm' )
study_arm_output_tsv = path.join( study_arm_out_dir, 'study_arm.tsv' )
study_arm_study_output_tsv = path.join( study_arm_out_dir, 'study_arm.clinical_study_designation.tsv' )
study_arm_cohort_output_tsv = path.join( study_arm_out_dir, 'study_arm.cohort_id.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

study_arm_case_output_tsv = path.join( relationship_validation_out_dir, 'study_arm.case_id.tsv' )
agent_adverse_event_output_tsv = path.join( relationship_validation_out_dir, 'agent.adverse_events.adverse_event_term.from_study_arm_query.tsv' )
agent_agent_administration_output_tsv = path.join( relationship_validation_out_dir, 'agent.agent_administrations.medication_and_document_number.from_study_arm_query.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
study_arm_output_json = path.join( json_out_dir, 'study_arm_metadata.json' )

# type study_arm {
#     arm: String
#     ctep_treatment_assignment_code: String
#     arm_description: String
#     arm_id: String
#     study {
#         clinical_study_designation
#     }
#     cohorts( first: {page_size}, offset: __OFFSET__, orderBy: [ cohort_id_asc ] ) {
#         cohort_id
#     }
#     cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {
#         # Just for validation.
#         case_id
#     }
#     agents( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {
#         medication
#         document_number
#         adverse_events( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {
#             # Exploration only; there are no reliable record IDs here
#             adverse_event_term
#         }
#         agent_administrations( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {
#             medication
#             document_number
#         }
#     }
# }

scalar_study_arm_fields = [
    'arm',
    'ctep_treatment_assignment_code',
    'arm_description',
    'arm_id'
]

# type agent {
#     medication: String
#     document_number: String
#     study_arms(first: Int, offset: Int, orderBy: [_study_armOrdering!], filter: _study_armFilter): [study_arm]
#     agent_administrations(first: Int, offset: Int, orderBy: [_agent_administrationOrdering!], filter: _agent_administrationFilter): [agent_administration]
#     adverse_events(first: Int, offset: Int, orderBy: [_adverse_eventOrdering!], filter: _adverse_eventFilter): [adverse_event]
# }

scalar_agent_fields = [
    'medication',
    'document_number'
]

# study_arm(arm: String, ctep_treatment_assignment_code: String, arm_description: String, arm_id: String, filter: _study_armFilter, first: Int, offset: Int, orderBy: [_study_armOrdering!]): [study_arm!]!

study_arm_api_query_json_template = f'''    {{
        
        study_arm( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_study_arm_fields ) + f'''
            agents( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
                ''' + '''
                '''.join( scalar_agent_fields ) + f'''
                adverse_events( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {{
                    adverse_event_term
                }}
                agent_administrations( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
                    medication
                    document_number
                }}
            }}
            study {{
                clinical_study_designation
            }}
            cohorts( first: {page_size}, offset: __OFFSET__, orderBy: [ cohort_id_asc ] ) {{
                cohort_id
            }}
            cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {{
                case_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ study_arm_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( study_arm_output_json, 'w' ) as JSON, \
    open( study_arm_output_tsv, 'w' ) as STUDY_ARM, \
    open( study_arm_study_output_tsv, 'w' ) as STUDY_ARM_STUDY, \
    open( study_arm_cohort_output_tsv, 'w' ) as STUDY_ARM_COHORT, \
    open( study_arm_case_output_tsv, 'w' ) as STUDY_ARM_CASE, \
    open( agent_adverse_event_output_tsv, 'w' ) as AGENT_ADVERSE_EVENT, \
    open( agent_agent_administration_output_tsv, 'w' ) as AGENT_AGENT_ADMINISTRATION:

    print( *scalar_study_arm_fields, sep='\t', file=STUDY_ARM )

    print( *[ 'arm_id', 'arm', 'clinical_study_designation' ], sep='\t', file=STUDY_ARM_STUDY )
    print( *[ 'arm_id', 'cohort_id' ], sep='\t', file=STUDY_ARM_COHORT )
    print( *[ 'arm_id', 'case_id' ], sep='\t', file=STUDY_ARM_CASE )
    print( *[ 'agent.medication', 'agent.document_number', 'adverse_event.adverse_event_term' ], sep='\t', file=AGENT_ADVERSE_EVENT )
    print( *[ 'agent.medication', 'agent.document_number', 'agent_administration.medication', 'agent_administration.document_number' ], sep='\t', file=AGENT_AGENT_ADMINISTRATION )

    while not null_result:
    
        study_arm_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), study_arm_api_query_json_template )
        }

        study_arm_response = requests.post( api_url, json=study_arm_api_query_json )

        if not study_arm_response.ok:
            
            print( study_arm_api_query_json['query'], file=sys.stderr )

            study_arm_response.raise_for_status()

        study_arm_result = json.loads( study_arm_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( study_arm_result, indent=4, sort_keys=False ), file=JSON )

        study_arm_count = len( study_arm_result['data']['study_arm'] )

        if study_arm_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for study_arm in study_arm_result['data']['study_arm']:
                
                arm_id = study_arm['arm_id']

                arm = study_arm['arm']

                # This shouldn't happen, but it does.

                if arm_id is None:
                    
                    arm_id = ''

                if arm is None:
                    
                    arm = ''

                study_arm_row = list()

                for field_name in scalar_study_arm_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( study_arm[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    study_arm_row.append( field_value )

                print( *study_arm_row, sep='\t', file=STUDY_ARM )

                if 'agents' in study_arm and study_arm['agents'] is not None and len( study_arm['agents'] ) > 0:
                    
                    for agent in study_arm['agents']:
                        
                        agent_medication = ''
                        agent_document_number = ''

                        if 'medication' in agent and agent['medication'] is not None:
                            
                            agent_medication = agent['medication']

                        if 'document_number' in agent and agent['document_number'] is not None:
                            
                            agent_document_number = agent['document_number']

                        if 'adverse_events' in agent and agent['adverse_events'] is not None and len( agent['adverse_events'] ) > 0:
                            
                            for adverse_event in agent['adverse_events']:
                                
                                print( *[ agent_medication, agent_document_number, adverse_event['adverse_event_term'] ], sep='\t', file=AGENT_ADVERSE_EVENT )

                            adverse_event_count = len( agent['adverse_events'] )

                            if adverse_event_count == page_size:
                                
                                print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} adverse_event records: implement paging here or risk data loss.", file=sys.stderr )

                        if 'agent_administrations' in agent and agent['agent_administrations'] is not None and len( agent['agent_administrations'] ) > 0:
                            
                            for agent_administration in agent['agent_administrations']:
                                
                                aa_medication = ''
                                aa_document_number = ''

                                if 'medication' in agent_administration and agent_administration['medication'] is not None:
                                    
                                    aa_medication = agent_administration['medication']

                                if 'document_number' in agent_administration and agent_administration['document_number'] is not None:
                                    
                                    aa_document_number = agent_administration['document_number']

                                print( *[ agent_medication, agent_document_number, aa_medication, aa_document_number ], sep='\t', file=AGENT_AGENT_ADMINISTRATION )

                            agent_administration_count = len( agent['agent_administrations'] )

                            if agent_administration_count == page_size:
                                
                                print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} agent_administration records: implement paging here or risk data loss.", file=sys.stderr )

                    agent_count = len( study_arm['agents'] )

                    if agent_count == page_size:
                        
                        print( f"WARNING: study_arm {arm_id} has at least {page_size} agent records: implement paging here or risk data loss.", file=sys.stderr )

                clinical_study_designation = ''

                if 'study' in study_arm and study_arm['study'] is not None and 'clinical_study_designation' in study_arm['study'] and study_arm['study']['clinical_study_designation'] is not None:
                    
                    clinical_study_designation = study_arm['study']['clinical_study_designation']

                print( *[ arm_id, arm, clinical_study_designation ], sep='\t', file=STUDY_ARM_STUDY )

                if 'cohorts' in study_arm and study_arm['cohorts'] is not None and len( study_arm['cohorts'] ) > 0:
                    
                    for cohort in study_arm['cohorts']:
                        
                        cohort_id = cohort['cohort_id']

                        if cohort_id is None:
                            
                            cohort_id = ''

                        print( *[ arm_id, cohort_id ], sep='\t', file=STUDY_ARM_COHORT )

                    cohort_count = len( study_arm['cohorts'] )

                    if cohort_count == page_size:
                        
                        print( f"WARNING: study_arm {arm_id} has at least {page_size} cohort records: implement paging here or risk data loss.", file=sys.stderr )

                if 'cases' in study_arm and study_arm['cases'] is not None and len( study_arm['cases'] ) > 0:
                    
                    for case in study_arm['cases']:
                        
                        case_id = case['case_id']

                        print( *[ arm_id, case_id ], sep='\t', file=STUDY_ARM_CASE )

                    case_count = len( study_arm['cases'] )

                    if case_count == page_size:
                        
                        print( f"WARNING: study_arm {arm_id} has at least {page_size} case records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


