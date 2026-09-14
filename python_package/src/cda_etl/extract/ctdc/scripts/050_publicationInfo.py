#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import map_columns_one_to_one, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_short_name_to_study_id = map_columns_one_to_one( study_tsv, 'study_short_name', 'study_id' )

publication_out_dir = path.join( output_root, 'Publication' )
publication_tsv = path.join( publication_out_dir, 'Publication.from_publicationInfo.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
publicationInfo_json_output_file = path.join( json_out_dir, 'publicationInfo.json' )

# Non-scalar Publication fields:
#     <none>
scalar_publication_fields = [
    'digital_object_id',
    'pubmed_id',
    'publication_title',
    'authorship',
    'year_of_publication',
    'journal_citation'
]

publicationInfo_query = '''
query search {
    publicationInfo ( study_short_name: "$STUDY_SHORT_NAME$" ) {
        ''' + '\n            '.join( scalar_publication_fields ) + '''
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, publication_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Open handles for output files to save TSVs describing returned data.
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

output_tsv_keywords = [
    'PUBLICATION'
]

output_tsv_filenames = [
    publication_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_publication_fields, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )

with open( publicationInfo_json_output_file, 'w' ) as JSON:
    
    for study_short_name in sorted( study_short_name_to_study_id ):
        # Send the publicationInfo() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, publicationInfo_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, publicationInfo_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for publication in result['data']['publicationInfo']:
            # Note: there may be stub records here whose fields are all null.
            publication_row = list()
            all_null = True
            for field_name in scalar_publication_fields:
                if publication[field_name] is not None:
                    publication_row.append( publication[field_name] )
                    all_null = False
                else:
                    publication_row.append( '' )
            # Don't process all-null Publication records.
            if not all_null:
                print( *publication_row, sep='\t', end='\n', file=output_tsvs['PUBLICATION'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


