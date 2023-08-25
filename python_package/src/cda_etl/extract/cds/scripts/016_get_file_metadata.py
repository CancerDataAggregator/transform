#!/usr/bin/env python -u

import requests
import json
import re
import sys

from cda_etl.lib import sort_file_with_header

from os import makedirs, path, rename

# PARAMETERS

page_size = 1000

api_url = 'https://dataservice.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'cds' )

file_out_dir = path.join( output_root, 'file' )

file_output_tsv = path.join( file_out_dir, 'file.tsv' )

file_samples_output_tsv = path.join( file_out_dir, 'file.sample_id.tsv' )

file_genomic_info_output_tsv = path.join( file_out_dir, 'file.genomic_info_library_id.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )

file_output_json = path.join( json_out_dir, 'file_metadata.json' )

#     type file {
#       file_id: String
#       file_name: String
#       file_type: String
#       file_description: String
#       file_size: Float
#       md5sum: String
#       file_url_in_cds: String
#       experimental_strategy_and_data_subtypes: String
#       study: study
#       samples(first: Int, offset: Int, orderBy: [_sampleOrdering!], filter: _sampleFilter): [sample]
#       genomic_info: genomic_info
#     }

scalar_file_fields = [
    'file_id',
    'file_name',
    'file_type',
    'file_description',
    'file_size',
    'md5sum',
    'file_url_in_cds',
    'experimental_strategy_and_data_subtypes'
]

file_api_query_json_template = f'''    {{
        
        file( first: {page_size}, offset: __OFFSET__, orderBy: [ file_id_asc ] ) {{
            ''' + '''
            '''.join( scalar_file_fields ) + f'''
            study {{
                study_name
            }}
            samples( first: {page_size}, offset: 0, orderBy: [ sample_id_asc ] ) {{
                sample_id
            }}
            genomic_info {{
                library_id
            }}
        }}
    }}'''

# EXECUTION

for output_dir in [ file_out_dir, json_out_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

null_result = False

offset = 0

with open( file_output_json, 'w' ) as JSON, open( file_output_tsv, 'w' ) as FILE, open( file_samples_output_tsv, 'w' ) as FILE_SAMPLES, open( file_genomic_info_output_tsv, 'w' ) as FILE_GENOMIC_INFO:
    
    print( *scalar_file_fields, sep='\t', file=FILE )
    print( *[ 'study_name', 'file_id', 'sample_id' ], sep='\t', file=FILE_SAMPLES )
    print( *[ 'study_name', 'file_id', 'library_id' ], sep='\t', file=FILE_GENOMIC_INFO )

    while not null_result:
    
        file_api_query_json = {
            'query': re.sub( r'__OFFSET__', str( offset ), file_api_query_json_template )
        }

        file_response = requests.post( api_url, json=file_api_query_json )

        if not file_response.ok:
            
            print( file_api_query_json['query'], file=sys.stderr )

            file_response.raise_for_status()

        file_result = json.loads( file_response.content )

        # Save a version of the returned data as raw JSON.

        print( json.dumps( file_result, indent=4, sort_keys=False ), file=JSON )

        # Save sample data as a TSV.

        file_count = len( file_result['data']['file'] )

        if file_count == 0:
            
            null_result = True

        else:
            
            for file in file_result['data']['file']:
                
                file_id = file['file_id']

                file_row = list()

                for field_name in scalar_file_fields:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.

                    field_value = json.dumps( file[field_name] ).strip( '"' )

                    if field_value == 'null':
                        
                        field_value = ''

                    file_row.append( field_value )

                print( *file_row, sep='\t', file=FILE )

                # There are newlines, carriage returns, quotes and nonprintables in some CDS text fields, hence the json.dumps() wrap here.
                # 
                # Also, we want this to break with a key error if it doesn't exist.

                study_name = json.dumps( file['study']['study_name'] ).strip( '"' )

                for sample in file['samples']:
                    
                    print( *[ study_name, file_id, sample['sample_id'] ], sep='\t', file=FILE_SAMPLES )

                sample_count = len( file['samples'] )

                if sample_count == page_size:
                    
                    print( f"WARNING: File {file_id} in study '{study_name}' has at least {page_size} samples: implement paging here or risk data loss.", file=sys.stderr )

                if file['genomic_info'] is not None:
                    
                    if file['genomic_info']['library_id'] is not None:
                        
                        print( *[ study_name, file_id, file['genomic_info']['library_id'] ], sep='\t', file=FILE_GENOMIC_INFO )

        offset = offset + page_size


