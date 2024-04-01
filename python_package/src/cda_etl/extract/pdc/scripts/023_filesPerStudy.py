#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import get_unique_values_from_tsv_column, sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

file_per_study_out_dir = f"{output_root}/FilePerStudy"

file_per_study_tsv = f"{file_per_study_out_dir}/FilePerStudy.tsv"

filesPerStudy_json_output_file = f"{json_out_dir}/filesPerStudy.json"

scalar_file_per_study_fields = (
    'file_id',
    'file_submitter_id',
    'file_name',
    'file_type',
    'data_category',
    'file_size',
    'md5sum',
    'file_location',
    'file_format',
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'study_name'
)

pdc_study_ids = get_unique_values_from_tsv_column( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, file_per_study_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Open handles for output files to save TSVs describing the returned objects and (as needed) association TSVs
# enumerating containment relationships between objects and sub-objects as well as association TSVs enumerating
# relationships between objects and keyword-style dictionaries.
# 
# We can't always safely use the Python `with` keyword in cases like this because the Python interpreter
# enforces an arbitrary hard-coded limit on the total number of simultaneous indents, and each `with` keyword
# creates another indent, even if you use the
# 
#     with open(A) as A, open(B) as B, ...
# 
# (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
# have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
# indentation, especially when the syntax above should've avoided creating multiple nested indents in the first place.

### OLD COMMENT: (none at this level)

output_tsv_keywords = [
    'FILE_PER_STUDY'
]

output_tsv_filenames = [
    file_per_study_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open( filesPerStudy_json_output_file, 'w' ) as JSON:
    
    # Table header.

    print( *scalar_file_per_study_fields, sep='\t', end='\n', file=output_tsvs['FILE_PER_STUDY'] )

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                filesPerStudy( ''' + f'pdc_study_id: "{pdc_study_id}", acceptDUA: true' + ''' ) {
                    ''' + '\n                    '.join(scalar_file_per_study_fields) + '''
                }
            }'''
        }

        # Send the filesPerStudy() query to the API server.

        response = requests.post(api_url, json=api_query_json)

        # If the HTTP response code is not OK (200), dump the query, print the http
        # error result and exit.

        if not response.ok:
            
            print( api_query_json['query'], file=sys.stderr )

            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.

        result = json.loads(response.content)

        # Save a version of the returned data as JSON (caching the PDC Study ID ahead of each block).

        print( pdc_study_id, file=JSON )

        print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for file_per_study in result['data']['filesPerStudy']:
            
            file_per_study_row = list()

            for field_name in scalar_file_per_study_fields:
                
                if file_per_study[field_name] is not None:
                    
                    file_per_study_row.append(file_per_study[field_name])

                else:
                    
                    file_per_study_row.append('')

            print( *file_per_study_row, sep='\t', end='\n', file=output_tsvs['FILE_PER_STUDY'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


