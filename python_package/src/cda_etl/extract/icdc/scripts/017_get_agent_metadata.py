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

agent_out_dir = path.join( output_root, 'agent' )
agent_output_tsv = path.join( agent_out_dir, 'agent.tsv' )
agent_adverse_events_output_tsv = path.join( agent_out_dir, 'agent.adverse_events.adverse_event_term.tsv' )
agent_agent_administrations_output_tsv = path.join( agent_out_dir, 'agent.agent_administrations.medication_and_document_number.tsv' )
agent_study_arms_output_tsv = path.join( agent_out_dir, 'agent.study_arms.arm_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
agent_output_json = path.join( json_out_dir, 'agent_metadata.json' )

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

# agent(medication: String, document_number: String, filter: _agentFilter, first: Int, offset: Int, orderBy: [_agentOrdering!]): [agent!]!

agent_api_query_json_template = f'''    {{
        
        agent( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
            ''' + '''
            '''.join( scalar_agent_fields ) + f'''
            study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {{
                arm_id
            }}
            agent_administrations( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
                medication
                document_number
            }}
            adverse_events( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {{
                adverse_event_term
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ agent_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( agent_output_json, 'w' ) as JSON, \
    open( agent_output_tsv, 'w' ) as AGENT, \
    open( agent_adverse_events_output_tsv, 'w' ) as AGENT_ADVERSE_EVENTS, \
    open( agent_agent_administrations_output_tsv, 'w' ) as AGENT_AGENT_ADMINISTRATIONS, \
    open( agent_study_arms_output_tsv, 'w' ) as AGENT_STUDY_ARMS:

    print( *scalar_agent_fields, sep='\t', file=AGENT )
    print( *[ 'agent.medication', 'agent.document_number', 'adverse_event.adverse_event_term' ], sep='\t', file=AGENT_ADVERSE_EVENTS )
    print( *[ 'agent.medication', 'agent.document_number', 'agent_administration.medication', 'agent_administration.document_number' ], sep='\t', file=AGENT_AGENT_ADMINISTRATIONS )
    print( *[ 'agent.medication', 'agent.document_number', 'study_arm.arm_id' ], sep='\t', file=AGENT_STUDY_ARMS )

    while not null_result:
    
        agent_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), agent_api_query_json_template )
        }

        agent_response = requests.post( api_url, json=agent_api_query_json )

        if not agent_response.ok:
            
            print( agent_api_query_json['query'], file=sys.stderr )

            agent_response.raise_for_status()

        agent_result = json.loads( agent_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( agent_result, indent=4, sort_keys=False ), file=JSON )

        agent_count = len( agent_result['data']['agent'] )

        if agent_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for agent in agent_result['data']['agent']:
                
                agent_row = list()

                agent_medication = ''

                agent_document_number = ''

                for field_name in scalar_agent_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( agent[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    agent_row.append( field_value )

                    if field_name == 'medication':
                        
                        agent_medication = field_value

                    if field_name == 'document_number':
                        
                        agent_document_number = field_value

                print( *agent_row, sep='\t', file=AGENT )

                if 'adverse_events' in agent and agent['adverse_events'] is not None and len( agent['adverse_events'] ) > 0:
                    
                    for adverse_event in agent['adverse_events']:
                        
                        adverse_event_term = ''

                        if 'adverse_event_term' in adverse_event and adverse_event['adverse_event_term'] is not None:
                            
                            adverse_event_term = adverse_event['adverse_event_term']

                        print( *[ agent_medication, agent_document_number, adverse_event_term ], sep='\t', file=AGENT_ADVERSE_EVENTS )

                    adverse_event_count = len( agent['adverse_events'] )

                    if adverse_event_count == page_size:
                        
                        print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} adverse_event records: implement paging here or risk data loss.", file=sys.stderr )

                if 'agent_administrations' in agent and agent['agent_administrations'] is not None and len( agent['agent_administrations'] ) > 0:
                    
                    for agent_administration in agent['agent_administrations']:
                        
                        admin_medication = ''

                        if 'medication' in agent_administration and agent_administration['medication'] is not None:
                            
                            admin_medication = agent_administration['medication']

                        admin_document_number = ''

                        if 'document_number' in agent_administration and agent_administration['document_number'] is not None:
                            
                            admin_document_number = agent_administration['document_number']

                        print( *[ agent_medication, agent_document_number, admin_medication, admin_document_number ], sep='\t', file=AGENT_AGENT_ADMINISTRATIONS )

                    agent_administration_count = len( agent['agent_administrations'] )

                    if agent_administration_count == page_size:
                        
                        print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} agent_administration records: implement paging here or risk data loss.", file=sys.stderr )

                if 'study_arms' in agent and agent['study_arms'] is not None and len( agent['study_arms'] ) > 0:
                    
                    for study_arm in agent['study_arms']:
                        
                        print( *[ agent_medication, agent_document_number, study_arm['arm_id'] ], sep='\t', file=AGENT_STUDY_ARMS )

                    study_arm_count = len( agent['study_arms'] )

                    if study_arm_count == page_size:
                        
                        print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} study_arm records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


