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

cycle_out_dir = path.join( output_root, 'cycle' )
cycle_output_tsv = path.join( cycle_out_dir, 'cycle.tsv' )
cycle_case_output_tsv = path.join( cycle_out_dir, 'cycle.case_id.tsv' )
cycle_case_and_visit_output_tsv = path.join( cycle_out_dir, 'cycle.case_id_and_visit_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
cycle_output_json = path.join( json_out_dir, 'cycle_metadata.json' )

# type cycle {
#     cycle_number: Int
#     date_of_cycle_start: String
#     date_of_cycle_end: String
#     case {
#         # Just for validation
#         case_id
#     }
#     visits( first: {page_size}, offset: __OFFSET__, orderBy: [ visit_id_asc ] ) {
#         # Just for validation
#         visit_id
#     }
# }

scalar_cycle_fields = [
    'cycle_number',
    'date_of_cycle_start',
    'date_of_cycle_end'
]

# cycle(cycle_number: Int, date_of_cycle_start: String, date_of_cycle_end: String, filter: _cycleFilter, first: Int, offset: Int, orderBy: [_cycleOrdering!]): [cycle!]!

cycle_api_query_json_template = f'''    {{
        
        cycle( first: {page_size}, offset: __OFFSET__, orderBy: [ date_of_cycle_start_asc, cycle_number_asc ] ) {{
            ''' + '''
            '''.join( scalar_cycle_fields ) + f'''
            case {{
                case_id
            }}
            visits( first: {page_size}, offset: __OFFSET__, orderBy: [ visit_id_asc ] ) {{
                visit_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ cycle_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( cycle_output_json, 'w' ) as JSON, \
    open( cycle_output_tsv, 'w' ) as CYCLE, \
    open( cycle_case_output_tsv, 'w' ) as CYCLE_CASE, \
    open( cycle_case_and_visit_output_tsv, 'w' ) as CYCLE_CASE_AND_VISIT:

    print( *scalar_cycle_fields, sep='\t', file=CYCLE )
    print( *( scalar_cycle_fields + [ 'case_id' ] ), sep='\t', file=CYCLE_CASE )
    print( *( scalar_cycle_fields + [ 'case_id', 'visit_id' ] ), sep='\t', file=CYCLE_CASE_AND_VISIT )

    while not null_result:
    
        cycle_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), cycle_api_query_json_template )
        }

        cycle_response = requests.post( api_url, json=cycle_api_query_json )

        if not cycle_response.ok:
            
            print( cycle_api_query_json['query'], file=sys.stderr )

            cycle_response.raise_for_status()

        cycle_result = json.loads( cycle_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( cycle_result, indent=4, sort_keys=False ), file=JSON )

        cycle_count = len( cycle_result['data']['cycle'] )

        if cycle_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for cycle in cycle_result['data']['cycle']:
                
                cycle_row = list()

                cycle_number = ''
                date_of_cycle_start = ''
                date_of_cycle_end = ''

                for field_name in scalar_cycle_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( cycle[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    cycle_row.append( field_value )

                    if field_name == 'cycle_number':
                        
                        cycle_number = field_value

                    if field_name == 'date_of_cycle_start':
                        
                        date_of_cycle_start = field_value

                    if field_name == 'date_of_cycle_end':
                        
                        date_of_cycle_end = field_value

                print( *cycle_row, sep='\t', file=CYCLE )

                case_id = ''

                if 'case' in cycle and cycle['case'] is not None and 'case_id' in cycle['case'] and cycle['case']['case_id'] is not None:
                    
                    case_id = cycle['case']['case_id']

                print( *( cycle_row + [ case_id ] ), sep='\t', file=CYCLE_CASE )

                if 'visits' in cycle and cycle['visits'] is not None and len( cycle['visits'] ) > 0:
                    
                    for visit in cycle['visits']:
                        
                        print( *( cycle_row + [ case_id, visit['visit_id'] ] ), sep='\t', file=CYCLE_CASE_AND_VISIT )

                    visit_count = len( cycle['visits'] )

                    if visit_count == page_size:
                        
                        print( f"WARNING: Cycle (number \"{cycle_number}\", start date \"date_of_cycle_start\", end date \"{date_of_cycle_end}\") has at least {page_size} visit records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


