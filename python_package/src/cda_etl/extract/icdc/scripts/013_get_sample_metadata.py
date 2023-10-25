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

sample_out_dir = path.join( output_root, 'sample' )
sample_output_tsv = path.join( sample_out_dir, 'sample.tsv' )
sample_case_output_tsv = path.join( sample_out_dir, 'sample.case_id.tsv' )
sample_file_output_tsv = path.join( sample_out_dir, 'sample.file_uuid.tsv' )
sample_visit_output_tsv = path.join( sample_out_dir, 'sample.visit_id.tsv' )
sample_prior_sample_output_tsv = path.join( sample_out_dir, 'sample.prior_sample_sample_id.tsv' )
sample_next_sample_output_tsv = path.join( sample_out_dir, 'sample.next_sample_sample_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
sample_output_json = path.join( json_out_dir, 'sample_metadata.json' )

# type sample {
#     sample_id: String
#     sample_site: String
#     physical_sample_type: String
#     general_sample_pathology: String
#     tumor_sample_origin: String
#     summarized_sample_type: String
#     molecular_subtype: String
#     specific_sample_pathology: String
#     date_of_sample_collection: String
#     sample_chronology: String
#     necropsy_sample: String
#     tumor_grade: String
#     length_of_tumor: Float
#     length_of_tumor_unit: String
#     length_of_tumor_original: Float
#     length_of_tumor_original_unit: String
#     width_of_tumor: Float
#     width_of_tumor_unit: String
#     width_of_tumor_original: Float
#     width_of_tumor_original_unit: String
#     volume_of_tumor: Float
#     volume_of_tumor_unit: String
#     volume_of_tumor_original: Float
#     volume_of_tumor_original_unit: String
#     percentage_tumor: String
#     sample_preservation: String
#     comment: String
#     case {
#         # Just for validation
#         case_id
#     }
#     visit {
#         visit_id
#     }
#     assays {
#         [nothing]
#     }
#     files( first: {page_size}, offset: 0, orderBy: [ uuid_asc ] ) {
#         # Just for validation
#         uuid
#     }
#     next_sample {
#         sample_id
#     }
#     prior_sample {
#         sample_id
#     }
# }

scalar_sample_fields = [
    'sample_id',
    'sample_site',
    'physical_sample_type',
    'general_sample_pathology',
    'tumor_sample_origin',
    'summarized_sample_type',
    'molecular_subtype',
    'specific_sample_pathology',
    'date_of_sample_collection',
    'sample_chronology',
    'necropsy_sample',
    'tumor_grade',
    'length_of_tumor',
    'length_of_tumor_unit',
    'length_of_tumor_original',
    'length_of_tumor_original_unit',
    'width_of_tumor',
    'width_of_tumor_unit',
    'width_of_tumor_original',
    'width_of_tumor_original_unit',
    'volume_of_tumor',
    'volume_of_tumor_unit',
    'volume_of_tumor_original',
    'volume_of_tumor_original_unit',
    'percentage_tumor',
    'sample_preservation',
    'comment'
]

# sample(sample_id: String, sample_site: String, physical_sample_type: String, general_sample_pathology: String, tumor_sample_origin: String, summarized_sample_type: String, molecular_subtype: String, specific_sample_pathology: String, date_of_sample_collection: String, sample_chronology: String, necropsy_sample: String, tumor_grade: String, length_of_tumor: Float, length_of_tumor_unit: String, length_of_tumor_original: Float, length_of_tumor_original_unit: String, width_of_tumor: Float, width_of_tumor_unit: String, width_of_tumor_original: Float, width_of_tumor_original_unit: String, volume_of_tumor: Float, volume_of_tumor_unit: String, volume_of_tumor_original: Float, volume_of_tumor_original_unit: String, percentage_tumor: String, sample_preservation: String, comment: String, filter: _sampleFilter, first: Int, offset: Int, orderBy: [_sampleOrdering!]): [sample!]!

sample_api_query_json_template = f'''    {{
        
        sample( first: {page_size}, offset: __OFFSET__, orderBy: [ sample_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_sample_fields ) + f'''
            case {{
                case_id
            }}
            visit {{
                visit_id
            }}
            files( first: {page_size}, offset: 0, orderBy: [ uuid_asc ] ) {{
                uuid
            }}
            next_sample {{
                sample_id
            }}
            prior_sample {{
                sample_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ sample_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( sample_output_json, 'w' ) as JSON, \
    open( sample_output_tsv, 'w' ) as SAMPLE, \
    open( sample_case_output_tsv, 'w' ) as SAMPLE_CASE, \
    open( sample_file_output_tsv, 'w' ) as SAMPLE_FILE, \
    open( sample_visit_output_tsv, 'w' ) as SAMPLE_VISIT, \
    open( sample_prior_sample_output_tsv, 'w' ) as SAMPLE_PRIOR, \
    open( sample_next_sample_output_tsv, 'w' ) as SAMPLE_NEXT:

    print( *scalar_sample_fields, sep='\t', file=SAMPLE )
    print( *[ 'sample_id', 'case_id' ], sep='\t', file=SAMPLE_CASE )
    print( *[ 'sample_id', 'file.uuid' ], sep='\t', file=SAMPLE_FILE )
    print( *[ 'sample_id', 'visit_id' ], sep='\t', file=SAMPLE_VISIT )
    print( *[ 'sample_id', 'prior_sample.sample_id' ], sep='\t', file=SAMPLE_PRIOR )
    print( *[ 'sample_id', 'next_sample.sample_id' ], sep='\t', file=SAMPLE_NEXT )

    while not null_result:
    
        sample_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), sample_api_query_json_template )
        }

        sample_response = requests.post( api_url, json=sample_api_query_json )

        if not sample_response.ok:
            
            print( sample_api_query_json['query'], file=sys.stderr )

            sample_response.raise_for_status()

        sample_result = json.loads( sample_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( sample_result, indent=4, sort_keys=False ), file=JSON )

        sample_count = len( sample_result['data']['sample'] )

        if sample_count == 0:
            
            null_result = True

        else:
            
            # Save fields and association data as TSVs.

            for sample in sample_result['data']['sample']:
                
                sample_id = sample['sample_id']

                sample_row = list()

                for field_name in scalar_sample_fields:
                    
                    # There are newlines, carriage returns, quotes and/or nonprintables in some text fields,
                    # hence the json.dumps() wrap here and throughout the rest of this code.

                    field_value = json.dumps( sample[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    sample_row.append( field_value )

                print( *sample_row, sep='\t', file=SAMPLE )

                if 'case' in sample and sample['case'] is not None and sample['case']['case_id'] is not None:
                    
                    print( *[ sample_id, sample['case']['case_id'] ], sep='\t', file=SAMPLE_CASE )

                for file in sample['files']:
                    
                    print( *[ sample_id, file['uuid'] ], sep='\t', file=SAMPLE_FILE )

                file_count = len( sample['files'] )

                if file_count == page_size:
                    
                    print( f"WARNING: Sample {sample_id} has at least {page_size} files: implement paging here or risk data loss.", file=sys.stderr )

                if 'visit' in sample and sample['visit'] is not None and sample['visit']['visit_id'] is not None:
                    
                    print( *[ sample_id, sample['visit']['visit_id'] ], sep='\t', file=SAMPLE_VISIT )

                if 'prior_sample' in sample and sample['prior_sample'] is not None and sample['prior_sample']['sample_id'] is not None:
                    
                    print( *[ sample_id, sample['prior_sample']['sample_id'] ], sep='\t', file=SAMPLE_PRIOR )

                if 'next_sample' in sample and sample['next_sample'] is not None and sample['next_sample']['sample_id'] is not None:
                    
                    print( *[ sample_id, sample['next_sample']['sample_id'] ], sep='\t', file=SAMPLE_NEXT )

        offset = offset + page_size


