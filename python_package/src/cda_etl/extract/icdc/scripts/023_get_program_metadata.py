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

program_out_dir = path.join( output_root, 'program' )
program_output_tsv = path.join( program_out_dir, 'program.tsv' )
program_study_output_tsv = path.join( program_out_dir, 'program.clinical_study_designation.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
program_output_json = path.join( json_out_dir, 'program_metadata.json' )

# type program {
#     program_name: String
#     program_acronym: String
#     program_short_description: String
#     program_full_description: String
#     program_external_url: String
#     program_sort_order: Int
#     studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {
#         clinical_study_designation
#     }
# }

scalar_program_fields = [
    'program_name',
    'program_acronym',
    'program_short_description',
    'program_full_description',
    'program_external_url',
    'program_sort_order'
]

# program(program_name: String, program_acronym: String, program_short_description: String, program_full_description: String, program_external_url: String, program_sort_order: Int, filter: _programFilter, first: Int, offset: Int, orderBy: [_programOrdering!]): [program!]!

program_api_query_json_template = f'''    {{
        
        program( first: {page_size}, offset: __OFFSET__, orderBy: [ program_acronym_asc ] ) {{
            ''' + '''
            '''.join( scalar_program_fields ) + f'''
            studies( first: {page_size}, offset: __OFFSET__, orderBy: [ clinical_study_designation_asc ] ) {{
                clinical_study_designation
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ program_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( program_output_json, 'w' ) as JSON, \
    open( program_output_tsv, 'w' ) as PROGRAM, \
    open( program_study_output_tsv, 'w' ) as PROGRAM_STUDY:

    print( *scalar_program_fields, sep='\t', file=PROGRAM )
    print( *[ 'program_acronym', 'clinical_study_designation' ], sep='\t', file=PROGRAM_STUDY )

    while not null_result:
    
        program_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), program_api_query_json_template )
        }

        program_response = requests.post( api_url, json=program_api_query_json )

        if not program_response.ok:
            
            print( program_api_query_json['query'], file=sys.stderr )

            program_response.raise_for_status()

        program_result = json.loads( program_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( program_result, indent=4, sort_keys=False ), file=JSON )

        program_count = len( program_result['data']['program'] )

        if program_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for program in program_result['data']['program']:
                
                program_row = list()

                program_acronym = ''

                for field_name in scalar_program_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( program[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    program_row.append( field_value )

                    if field_name == 'program_acronym':
                        
                        program_acronym = field_value

                print( *program_row, sep='\t', file=PROGRAM )

                if 'studies' in program and program['studies'] is not None and len( program['studies'] ) > 0:
                    
                    for study in program['studies']:
                        
                        print( *[ program_acronym, study['clinical_study_designation'] ], sep='\t', file=PROGRAM_STUDY )

                    study_count = len( program['studies'] )

                    if study_count == page_size:
                        
                        print( f"WARNING: Program \"{program_acronym}\" has at least {page_size} study records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


