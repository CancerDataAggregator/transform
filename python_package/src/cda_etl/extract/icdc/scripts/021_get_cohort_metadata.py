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

cohort_out_dir = path.join( output_root, 'cohort' )
cohort_output_tsv = path.join( cohort_out_dir, 'cohort.tsv' )
cohort_study_output_tsv = path.join( cohort_out_dir, 'cohort.clinical_study_designation.tsv' )

relationship_validation_out_dir = path.join( output_root, '__redundant_relationship_validation' )

cohort_study_arm_output_tsv = path.join( relationship_validation_out_dir, 'cohort.study_arm.tsv' )
cohort_case_output_tsv = path.join( relationship_validation_out_dir, 'cohort.case_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
cohort_output_json = path.join( json_out_dir, 'cohort_metadata.json' )

# type cohort {
#     cohort_description: String
#     cohort_dose: String
#     cohort_id: String
#     cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {
#         # Just for validation.
#         case_id
#     }
#     study_arm {
#         # Just for validation.
#         arm_id
#         arm
#     }
#     study {
#         clinical_study_designation
#     }
# }

scalar_cohort_fields = [
    'cohort_id',
    'cohort_description',
    'cohort_dose'
]

# cohort(cohort_description: String, cohort_dose: String, cohort_id: String, filter: _cohortFilter, first: Int, offset: Int, orderBy: [_cohortOrdering!]): [cohort!]!

cohort_api_query_json_template = f'''    {{
        
        cohort( first: {page_size}, offset: __OFFSET__, orderBy: [ cohort_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_cohort_fields ) + f'''
            cases( first: {page_size}, offset: __OFFSET__, orderBy: [ case_id_asc ] ) {{
                case_id
            }}
            study_arm {{
                arm_id
                arm
            }}
            study {{
                clinical_study_designation
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ cohort_out_dir, relationship_validation_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

#cohort_case_output_tsv = path.join( relationship_validation_out_dir, 'cohort.case_id.tsv' )


with open( cohort_output_json, 'w' ) as JSON, \
    open( cohort_output_tsv, 'w' ) as COHORT, \
    open( cohort_study_output_tsv, 'w' ) as COHORT_STUDY, \
    open( cohort_study_arm_output_tsv, 'w' ) as COHORT_STUDY_ARM, \
    open( cohort_case_output_tsv, 'w' ) as COHORT_CASE:

    print( *scalar_cohort_fields, sep='\t', file=COHORT )
    print( *[ 'cohort_id', 'clinical_study_designation' ], sep='\t', file=COHORT_STUDY )
    print( *[ 'cohort_id', 'arm_id', 'arm' ], sep='\t', file=COHORT_STUDY_ARM )
    print( *[ 'cohort_id', 'case_id' ], sep='\t', file=COHORT_CASE )

    while not null_result:
    
        cohort_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), cohort_api_query_json_template )
        }

        cohort_response = requests.post( api_url, json=cohort_api_query_json )

        if not cohort_response.ok:
            
            print( cohort_api_query_json['query'], file=sys.stderr )

            cohort_response.raise_for_status()

        cohort_result = json.loads( cohort_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( cohort_result, indent=4, sort_keys=False ), file=JSON )

        cohort_count = len( cohort_result['data']['cohort'] )

        if cohort_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for cohort in cohort_result['data']['cohort']:
                
                cohort_id = cohort['cohort_id']

                # This shouldn't happen, but it does.

                if cohort_id is None:
                    
                    cohort_id = ''

                cohort_row = list()

                for field_name in scalar_cohort_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( cohort[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    cohort_row.append( field_value )

                print( *cohort_row, sep='\t', file=COHORT )

                clinical_study_designation = ''

                if 'study' in cohort and cohort['study'] is not None and 'clinical_study_designation' in cohort['study'] and cohort['study']['clinical_study_designation'] is not None:
                    
                    clinical_study_designation = cohort['study']['clinical_study_designation']

                print( *[ cohort_id, clinical_study_designation ], sep='\t', file=COHORT_STUDY )

                arm_id = ''

                if 'study_arm' in cohort and cohort['study_arm'] is not None and 'arm_id' in cohort['study_arm'] and cohort['study_arm']['arm_id'] is not None:
                    
                    arm_id = cohort['study_arm']['arm_id']

                arm = ''

                if 'study_arm' in cohort and cohort['study_arm'] is not None and 'arm' in cohort['study_arm'] and cohort['study_arm']['arm'] is not None:
                    
                    arm = cohort['study_arm']['arm']

                print( *[ cohort_id, arm_id, arm ], sep='\t', file=COHORT_STUDY_ARM )

                if 'cases' in cohort and cohort['cases'] is not None and len( cohort['cases'] ) > 0:
                    
                    for case in cohort['cases']:
                        
                        case_id = case['case_id']

                        print( *[ cohort_id, case_id ], sep='\t', file=COHORT_CASE )

                    case_count = len( cohort['cases'] )

                    if case_count == page_size:
                        
                        print( f"WARNING: Cohort {cohort_id} has at least {page_size} case records: implement paging here or risk data loss.", file=sys.stderr )

        offset = offset + page_size


