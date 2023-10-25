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

adverse_event_out_dir = path.join( output_root, 'adverse_event' )
adverse_event_output_tsv = path.join( adverse_event_out_dir, 'adverse_event.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

# We're saving adverse_event.agent inline in the adverse_event table, but cache agent data for
# later comparison to other sources for agent information.

adverse_event_agent_study_arms_output_tsv = path.join( relationship_validation_out_dir, 'agent.study_arms.arm_id.from_adverse_event_query.tsv' )
adverse_event_agent_agent_administrations_output_tsv = path.join( relationship_validation_out_dir, 'agent.agent_administrations.medication_and_document_number.from_adverse_event_query.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
adverse_event_output_json = path.join( json_out_dir, 'adverse_event_metadata.json' )

# type adverse_event {
#     day_in_cycle: Int
#     date_of_onset: String
#     existing_adverse_event: String
#     date_of_resolution: String
#     ongoing_adverse_event: String
#     adverse_event_term: String
#     adverse_event_description: String
#     adverse_event_grade: String
#     adverse_event_grade_description: String
#     adverse_event_agent_name: String
#     adverse_event_agent_dose: String
#     attribution_to_research: String
#     attribution_to_ind: String
#     attribution_to_disease: String
#     attribution_to_commercial: String
#     attribution_to_other: String
#     other_attribution_description: String
#     dose_limiting_toxicity: String
#     unexpected_adverse_event: String
#     case {
#         case_id
#     }
#     agent {
#         medication
#         document_number
#     }
#     cases( first: {page_size}, offset: 0, orderBy: [ case_id_asc ] ) {
#         case_id
#     }
#     next_adverse_event {
#         # Exploration only; there are no reliable record IDs here
#         adverse_event_term
#     }
#     prior_adverse_event {
#         # Exploration only; there are no reliable record IDs here
#         adverse_event_term
#     }
# }

scalar_adverse_event_fields = [
    'day_in_cycle',
    'date_of_onset',
    'existing_adverse_event',
    'date_of_resolution',
    'ongoing_adverse_event',
    'adverse_event_term',
    'adverse_event_description',
    'adverse_event_grade',
    'adverse_event_grade_description',
    'adverse_event_agent_name',
    'adverse_event_agent_dose',
    'attribution_to_research',
    'attribution_to_ind',
    'attribution_to_disease',
    'attribution_to_commercial',
    'attribution_to_other',
    'other_attribution_description',
    'dose_limiting_toxicity',
    'unexpected_adverse_event'
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

# adverse_event(day_in_cycle: Int, date_of_onset: String, existing_adverse_event: String, date_of_resolution: String, ongoing_adverse_event: String, adverse_event_term: String, adverse_event_description: String, adverse_event_grade: String, adverse_event_grade_description: String, adverse_event_agent_name: String, adverse_event_agent_dose: String, attribution_to_research: String, attribution_to_ind: String, attribution_to_disease: String, attribution_to_commercial: String, attribution_to_other: String, other_attribution_description: String, dose_limiting_toxicity: String, unexpected_adverse_event: String, filter: _adverse_eventFilter, first: Int, offset: Int, orderBy: [_adverse_eventOrdering!]): [adverse_event!]!

adverse_event_api_query_json_template = f'''    {{
        
        adverse_event( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {{
            ''' + '''
            '''.join( scalar_adverse_event_fields ) + f'''
            case {{
                case_id
            }}
            agent {{
                ''' + '''
                '''.join( scalar_agent_fields ) + f'''
                study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {{
                    arm_id
                }}
                agent_administrations( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
                    medication
                    document_number
                }}
            }}
            cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {{
                case_id
            }}
            prior_adverse_event {{
                adverse_event_term
            }}
            next_adverse_event {{
                adverse_event_term
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ adverse_event_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( adverse_event_output_json, 'w' ) as JSON, \
    open( adverse_event_output_tsv, 'w' ) as ADVERSE_EVENT, \
    open( adverse_event_agent_study_arms_output_tsv, 'w' ) as ADVERSE_EVENT_AGENT_STUDY_ARMS, \
    open( adverse_event_agent_agent_administrations_output_tsv, 'w' ) as ADVERSE_EVENT_AGENT_AGENT_ADMINISTRATIONS:

    print( *( scalar_adverse_event_fields + [ 'prior_adverse_event.adverse_event_term', 'next_adverse_event.adverse_event_term', 'case_id', 'cases.case_id', 'agent.medication', 'agent.document_number' ] ), sep='\t', file=ADVERSE_EVENT )
    print( *[ 'agent.medication', 'agent.document_number', 'study_arm.arm_id' ], sep='\t', file=ADVERSE_EVENT_AGENT_STUDY_ARMS )
    print( *[ 'agent.medication', 'agent.document_number', 'agent_administration.medication', 'agent_administration.document_number' ], sep='\t', file=ADVERSE_EVENT_AGENT_AGENT_ADMINISTRATIONS )

    while not null_result:
    
        adverse_event_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), adverse_event_api_query_json_template )
        }

        adverse_event_response = requests.post( api_url, json=adverse_event_api_query_json )

        if not adverse_event_response.ok:
            
            print( adverse_event_api_query_json['query'], file=sys.stderr )

            adverse_event_response.raise_for_status()

        adverse_event_result = json.loads( adverse_event_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( adverse_event_result, indent=4, sort_keys=False ), file=JSON )

        adverse_event_count = len( adverse_event_result['data']['adverse_event'] )

        if adverse_event_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for adverse_event in adverse_event_result['data']['adverse_event']:
                
                adverse_event_row = list()

                for field_name in scalar_adverse_event_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( adverse_event[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    adverse_event_row.append( field_value )

                prior_adverse_event_term = ''

                if 'prior_adverse_event' in adverse_event and adverse_event['prior_adverse_event'] is not None and 'adverse_event_term' in adverse_event['prior_adverse_event'] and adverse_event['prior_adverse_event']['adverse_event_term'] is not None:
                    
                    prior_adverse_event_term = adverse_event['prior_adverse_event']['adverse_event_term']

                adverse_event_row.append( prior_adverse_event_term )

                next_adverse_event_term = ''

                if 'next_adverse_event' in adverse_event and adverse_event['next_adverse_event'] is not None and 'adverse_event_term' in adverse_event['next_adverse_event'] and adverse_event['next_adverse_event']['adverse_event_term'] is not None:
                    
                    next_adverse_event_term = adverse_event['next_adverse_event']['adverse_event_term']

                adverse_event_row.append( next_adverse_event_term )

                case_id = ''

                if 'case' in adverse_event and adverse_event['case'] is not None and 'case_id' in adverse_event['case'] and adverse_event['case']['case_id'] is not None:
                    
                    case_id = adverse_event['case']['case_id']

                adverse_event_row.append( case_id )

                cases_ids = list()

                if 'cases' in adverse_event and adverse_event['cases'] is not None:
                    
                    for case in adverse_event['cases']:
                        
                        cases_ids.append( case['case_id'] )

                adverse_event_row.append( ','.join( cases_ids ) )

                agent_medication = ''
                agent_document_number = ''

                if 'agent' in adverse_event and adverse_event['agent'] is not None:
                    
                    if 'medication' in adverse_event['agent'] and adverse_event['agent']['medication'] is not None:
                        
                        agent_medication = adverse_event['agent']['medication']

                    if 'document_number' in adverse_event['agent'] and adverse_event['agent']['document_number'] is not None:
                        
                        agent_document_number = adverse_event['agent']['document_number']

                    if 'study_arms' in adverse_event['agent'] and adverse_event['agent']['study_arms'] is not None and len( adverse_event['agent']['study_arms'] ) > 0:
                        
                        for study_arm in adverse_event['agent']['study_arms']:
                            
                            print( *[ agent_medication, agent_document_number, study_arm['arm_id'] ], sep='\t', file=ADVERSE_EVENT_AGENT_STUDY_ARMS )

                        study_arm_count = len( adverse_event['agent']['study_arms'] )

                        if study_arm_count == page_size:
                            
                            print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} study_arm records: implement paging here or risk data loss.", file=sys.stderr )

                    if 'agent_administrations' in adverse_event['agent'] and adverse_event['agent']['agent_administrations'] is not None and len( adverse_event['agent']['agent_administrations'] ) > 0:
                        
                        for agent_administration in adverse_event['agent']['agent_administrations']:
                            
                            admin_medication = ''

                            if 'medication' in agent_administration and agent_administration['medication'] is not None:
                                
                                admin_medication = agent_administration['medication']

                            admin_document_number = ''

                            if 'document_number' in agent_administration and agent_administration['document_number'] is not None:
                                
                                admin_document_number = agent_administration['document_number']

                            print( *[ agent_medication, agent_document_number, admin_medication, admin_document_number ], sep='\t', file=ADVERSE_EVENT_AGENT_AGENT_ADMINISTRATIONS )

                        agent_administration_count = len( adverse_event['agent']['agent_administrations'] )

                        if agent_administration_count == page_size:
                            
                            print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} agent_administration records: implement paging here or risk data loss.", file=sys.stderr )

                adverse_event_row.append( agent_medication )
                adverse_event_row.append( agent_document_number )

                print( *adverse_event_row, sep='\t', file=ADVERSE_EVENT )

        offset = offset + page_size


