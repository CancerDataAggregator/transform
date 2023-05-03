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

uihuman_body_out_dir = f"{output_root}/UIHumanBody"

uihuman_body_tsv = f"{uihuman_body_out_dir}/UIHumanBody.tsv"
uihuman_body_primary_sites_tsv = f"{uihuman_body_out_dir}/UIHumanBody.primarySites.tsv"

uiPrimarySiteCaseCount_json_output_file = f"{json_out_dir}/uiPrimarySiteCaseCount.json"

scalar_uihuman_body_fields = (
    'major_primary_site',
    'cases_count'
)

api_query_json = {
    'query': '''    {
        uiPrimarySiteCaseCount {
            ''' + '\n            '.join(scalar_uihuman_body_fields) + '''
            primarySites
        }
    }'''
}

# EXECUTION

for output_dir in ( json_out_dir, uihuman_body_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the uiPrimarySiteCaseCount() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(uiPrimarySiteCaseCount_json_output_file, 'w') as JSON:
    
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

### OLD COMMENT: Also save a version of the returned UIHumanBody objects in TSV form.

output_tsv_keywords = [
    'UIHUMAN_BODY',
    'PRIMARY_SITES'
]

output_tsv_filenames = [
    uihuman_body_tsv,
    uihuman_body_primary_sites_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_uihuman_body_fields, sep='\t', end='\n', file=output_tsvs['UIHUMAN_BODY'] )
print( *['primary_site', 'major_primary_site'], sep='\t', end='\n', file=output_tsvs['PRIMARY_SITES'] )

# Parse the returned data and save to TSV.

for uihuman_body in result['data']['uiPrimarySiteCaseCount']:
    
    uihuman_body_row = list()

    for field_name in scalar_uihuman_body_fields:
        
        if uihuman_body[field_name] is not None:
            
            uihuman_body_row.append(uihuman_body[field_name])

        else:
            
            uihuman_body_row.append('')

    print( *uihuman_body_row, sep='\t', end='\n', file=output_tsvs['UIHUMAN_BODY'] )

    if uihuman_body['primarySites'] is not None and len(uihuman_body['primarySites']) > 0:
        
        for primary_site in uihuman_body['primarySites']:
            
            print( *[primary_site, uihuman_body['major_primary_site']], sep='\t', end='\n', file=output_tsvs['PRIMARY_SITES'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


