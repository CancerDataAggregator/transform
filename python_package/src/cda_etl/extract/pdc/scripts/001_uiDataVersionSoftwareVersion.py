#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path

from cda_etl.lib import get_current_date

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = path.join( 'extracted_data', 'pdc' )

extraction_date_file = path.join( output_root, 'extraction_date.txt' )

version_output_dir = path.join( output_root, '__version_metadata' )

version_json_output_file = path.join( version_output_dir, 'uiDataVersionSoftwareVersion.json' )

api_query_json = {
    'query': '''    {
        uiDataVersionSoftwareVersion {
            software_release: build_tag
            data_release
        }
    }'''
}

# EXECUTION

if not path.exists(version_output_dir):
    
    makedirs(version_output_dir)

with open( extraction_date_file, 'w' ) as OUT:
    
    print( get_current_date(), file=OUT )

# Send the uiDataVersionSoftwareVersion() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save the returned data as JSON.

with open(version_json_output_file, 'w') as OUT:
    
    print (json.dumps(result, indent=4, sort_keys=False), file=OUT)


