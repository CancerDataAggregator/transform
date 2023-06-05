#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

# SUBROUTINES

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

reference_out_dir = f"{output_root}/Reference"

reference_tsv = f"{reference_out_dir}/Reference.tsv"

reference_json_output_file = f"{json_out_dir}/reference.json"

scalar_reference_fields = (
    'reference_id',
    'entity_type',
    'entity_id',
    'entity_submitter_id',
    'reference_type',
    'reference_entity_type',
    'reference_entity_alias',
    'reference_entity_id',
    'reference_resource_name',
    'reference_resource_shortname',
    'reference_entity_location',
    'annotation'
)

api_query_json = {
    'query': '''    {
        reference( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_reference_fields) + '''
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, reference_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the reference() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(reference_json_output_file, 'w') as JSON:
    
    print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

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

### OLD COMMENT: Also save a version of the returned Reference objects in TSV form.

output_tsv_keywords = [
    'REFERENCE'
]

output_tsv_filenames = [
    reference_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table header.

print( *scalar_reference_fields, sep='\t', end='\n', file=output_tsvs['REFERENCE'] )

# Parse the returned data and save to TSV.

for reference in result['data']['reference']:
    
    reference_row = list()

    for field_name in scalar_reference_fields:
        
        if reference[field_name] is not None:
            
            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

            reference_row.append(json.dumps(reference[field_name]).strip('"'))

        else:
            
            reference_row.append('')

    print( *reference_row, sep='\t', end='\n', file=output_tsvs['REFERENCE'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


