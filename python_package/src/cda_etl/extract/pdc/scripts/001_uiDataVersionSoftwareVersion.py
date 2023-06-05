#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

version_output_dir = f"{output_root}/__version_metadata"

version_json_output_file = f"{version_output_dir}/uiDataVersionSoftwareVersion.json"

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


