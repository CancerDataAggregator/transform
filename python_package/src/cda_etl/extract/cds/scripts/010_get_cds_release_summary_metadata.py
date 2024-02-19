#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

# PARAMETERS

api_url = 'https://dataservice.datacommons.cancer.gov/v1/graphql/'

output_dir = path.join( 'extracted_data', 'cds', '__release_summary_metadata' )

output_file = path.join( output_dir, 'release_metadata.tsv' )

# schemaVersion: String
# numberOfStudies: Int
# numberOfSubjects: Int
# numberOfSamples: Int
# numberOfFiles: Int
# numberOfDiseaseSites: Int

named_stats = [ 'schemaVersion', 'numberOfStudies', 'numberOfSubjects', 'numberOfSamples', 'numberOfFiles', 'numberOfDiseaseSites' ]
api_query_json = {
    
    'query': '''    {
        
        ''' + '''
        '''.join( named_stats ) + '''
    }'''
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Send the query to the API server.

response = requests.post( api_url, json=api_query_json )

# If the HTTP response code is not OK (200), dump the query, print the
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads( response.content )

# Save a version of the returned data as raw JSON.

with open( output_file, 'w' ) as OUT:
    
    print( *named_stats, sep='\t', file=OUT )

    row = list()

    for named_stat in named_stats:
        
        row.append( result['data'][named_stat] )

    print( *row, sep='\t', file=OUT )


