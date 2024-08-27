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

agent_administration_out_dir = path.join( output_root, 'agent_administration' )
agent_administration_output_tsv = path.join( agent_administration_out_dir, 'agent_administration.tsv' )
agent_administration_visit_output_tsv = path.join( agent_administration_out_dir, 'agent_administration.visit_id.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

# We're saving agent_administration.agent inline in the agent_administration table, but cache agent data for
# later comparison to other sources for agent information.

agent_administration_agent_adverse_events_output_tsv = path.join( relationship_validation_out_dir, 'agent.adverse_events.adverse_event_term.from_agent_administration_query.tsv' )
agent_administration_agent_study_arms_output_tsv = path.join( relationship_validation_out_dir, 'agent.study_arms.arm_id.from_agent_administration_query.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
agent_administration_output_json = path.join( json_out_dir, 'agent_administration_metadata.json' )

# type agent_administration {
#     document_number: String
#     medication: String
#     route_of_administration: String
#     medication_lot_number: String
#     medication_vial_id: String
#     medication_actual_units_of_measure: String
#     medication_duration: Float
#     medication_duration_unit: String
#     medication_duration_original: Float
#     medication_duration_original_unit: String
#     medication_units_of_measure: String
#     medication_actual_dose: Float
#     medication_actual_dose_unit: String
#     medication_actual_dose_original: Float
#     medication_actual_dose_original_unit: String
#     phase: String
#     start_time: String
#     stop_time: String
#     dose_level: Float
#     dose_level_unit: String
#     dose_level_original: Float
#     dose_level_original_unit: String
#     dose_units_of_measure: String
#     date_of_missed_dose: String
#     medication_missed_dose: String
#     missed_dose_amount: Float
#     missed_dose_amount_unit: String
#     missed_dose_amount_original: Float
#     missed_dose_amount_original_unit: String
#     missed_dose_units_of_measure: String
#     medication_course_number: String
#     comment: String
#     agent {
#         medication
#         document_number
#         adverse_events( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {
#             # Exploration only; there are no reliable record IDs here
#             adverse_event_term
#         }
#         study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {
#             # Just for validation.
#             arm_id
#         }
#     }
#     visit {
#         visit_id
#     }
# }

scalar_agent_administration_fields = [
    'document_number',
    'medication',
    'route_of_administration',
    'medication_lot_number',
    'medication_vial_id',
    'medication_actual_units_of_measure',
    'medication_duration',
    'medication_duration_unit',
    'medication_duration_original',
    'medication_duration_original_unit',
    'medication_units_of_measure',
    'medication_actual_dose',
    'medication_actual_dose_unit',
    'medication_actual_dose_original',
    'medication_actual_dose_original_unit',
    'phase',
    'start_time',
    'stop_time',
    'dose_level',
    'dose_level_unit',
    'dose_level_original',
    'dose_level_original_unit',
    'dose_units_of_measure',
    'date_of_missed_dose',
    'medication_missed_dose',
    'missed_dose_amount',
    'missed_dose_amount_unit',
    'missed_dose_amount_original',
    'missed_dose_amount_original_unit',
    'missed_dose_units_of_measure',
    'medication_course_number',
    'comment'
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

agent_administration_api_query_json_template = f'''    {{
        
        agent_administration( first: {page_size}, offset: __OFFSET__, orderBy: [ medication_asc, document_number_asc ] ) {{
            ''' + '''
            '''.join( scalar_agent_administration_fields ) + f'''
            agent {{
                ''' + '''
                '''.join( scalar_agent_fields ) + f'''
                adverse_events( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_onset_asc ] ) {{
                    adverse_event_term
                }}
                study_arms( first: {page_size}, offset: __OFFSET__, orderBy: [ arm_id_asc ] ) {{
                    arm_id
                }}
            }}
            visit {{
                visit_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ agent_administration_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( agent_administration_output_json, 'w' ) as JSON, \
    open( agent_administration_output_tsv, 'w' ) as AGENT_ADMINISTRATION, \
    open( agent_administration_agent_adverse_events_output_tsv, 'w' ) as AGENT_ADMINISTRATION_AGENT_ADVERSE_EVENTS, \
    open( agent_administration_agent_study_arms_output_tsv, 'w' ) as AGENT_ADMINISTRATION_AGENT_STUDY_ARMS, \
    open( agent_administration_visit_output_tsv, 'w' ) as AGENT_ADMINISTRATION_VISIT:

    print( *( scalar_agent_administration_fields + [ 'agent.medication', 'agent.document_number' ] ), sep='\t', file=AGENT_ADMINISTRATION )
    print( *[ 'agent_administration.medication', 'agent_administration.document_number', 'agent.medication', 'agent.document_number', 'agent.adverse_event.adverse_event_term' ], sep='\t', file=AGENT_ADMINISTRATION_AGENT_ADVERSE_EVENTS )
    print( *[ 'agent_administration.medication', 'agent_administration.document_number', 'agent.medication', 'agent.document_number', 'agent.study_arm.arm_id' ], sep='\t', file=AGENT_ADMINISTRATION_AGENT_STUDY_ARMS )
    print( *[ 'medication', 'document_number', 'visit_id' ], sep='\t', file=AGENT_ADMINISTRATION_VISIT )

    while not null_result:
    
        agent_administration_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), agent_administration_api_query_json_template )
        }

        agent_administration_response = requests.post( api_url, json=agent_administration_api_query_json )

        if not agent_administration_response.ok:
            
            print( agent_administration_api_query_json['query'], file=sys.stderr )

            agent_administration_response.raise_for_status()

        agent_administration_result = json.loads( agent_administration_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( agent_administration_result, indent=4, sort_keys=False ), file=JSON )

        agent_administration_count = len( agent_administration_result['data']['agent_administration'] )

        if agent_administration_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for agent_administration in agent_administration_result['data']['agent_administration']:
                
                agent_administration_row = list()

                agent_administration_medication = ''
                agent_administration_document_number = ''

                for field_name in scalar_agent_administration_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( agent_administration[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    agent_administration_row.append( field_value )

                    if field_name == 'medication':
                        
                        agent_administration_medication = field_value

                    if field_name == 'document_number':
                        
                        agent_administration_document_number = field_value

                agent_medication = ''
                agent_document_number = ''

                if 'agent' in agent_administration and agent_administration['agent'] is not None:
                    
                    if 'medication' in agent_administration['agent'] and agent_administration['agent']['medication'] is not None:
                        
                        agent_medication = agent_administration['agent']['medication']

                    if 'document_number' in agent_administration['agent'] and agent_administration['agent']['document_number'] is not None:
                        
                        agent_document_number = agent_administration['agent']['document_number']

                    if 'adverse_events' in agent_administration['agent'] and agent_administration['agent']['adverse_events'] is not None and len( agent_administration['agent']['adverse_events'] ) > 0:
                        
                        for adverse_event in agent_administration['agent']['adverse_events']:
                            
                            print( *[ agent_administration_medication, agent_administration_document_number, agent_medication, agent_document_number, adverse_event['adverse_event_term'] ], sep='\t', file=AGENT_ADMINISTRATION_AGENT_ADVERSE_EVENTS )

                        adverse_event_count = len( agent_administration['agent']['adverse_events'] )

                        if adverse_event_count == page_size:
                            
                            print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} adverse_event records: implement paging here or risk data loss.", file=sys.stderr )

                    if 'study_arms' in agent_administration['agent'] and agent_administration['agent']['study_arms'] is not None and len( agent_administration['agent']['study_arms'] ) > 0:
                        
                        for study_arm in agent_administration['agent']['study_arms']:
                            
                            print( *[ agent_administration_medication, agent_administration_document_number, agent_medication, agent_document_number, study_arm['arm_id'] ], sep='\t', file=AGENT_ADMINISTRATION_AGENT_STUDY_ARMS )

                        study_arm_count = len( agent_administration['agent']['study_arms'] )

                        if study_arm_count == page_size:
                            
                            print( f"WARNING: Agent with medication {agent_medication} and document_number {agent_document_number} has at least {page_size} study_arm records: implement paging here or risk data loss.", file=sys.stderr )

                visit_id = ''

                if 'visit' in agent_administration and agent_administration['visit'] is not None and 'visit_id' in agent_administration['visit'] and agent_administration['visit']['visit_id'] is not None:
                    
                    visit_id = agent_administration['visit']['visit_id']

                    print( *[ agent_administration_medication, agent_administration_document_number, visit_id ], sep='\t', file=AGENT_ADMINISTRATION_VISIT )

                agent_administration_row.append( agent_medication )
                agent_administration_row.append( agent_document_number )

                print( *agent_administration_row, sep='\t', file=AGENT_ADMINISTRATION )

        offset = offset + page_size


