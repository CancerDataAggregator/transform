#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

file_overview_out_dir = path.join( output_root, 'FileOverview' )
file_overview_tsv = path.join( file_overview_out_dir, 'FileOverview.from_fileOverview.tsv' )
file_overview_association_tsv = path.join( file_overview_out_dir, 'FileOverview.association.from_fileOverview.tsv' )

data_file_out_dir = path.join( output_root, 'DataFile' )
data_file_specimen_record_id_tsv = path.join( data_file_out_dir, 'DataFile.specimen_record_id.from_fileOverview.tsv' )
data_file_participant_id_tsv = path.join( data_file_out_dir, 'DataFile.participant_id.from_fileOverview.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
fileOverview_json_output_file = path.join( json_out_dir, 'fileOverview.json' )

# Non-scalar FileOverview fields:
#     association: [String]
scalar_file_overview_fields = [
    'data_file_uuid',
    'participant_id',
    'specimen_record_id',
    'specimen_id', # This can be null.
    'data_file_name',
    'data_file_format',
    'data_file_type',
    'data_file_size',
    'data_file_description',
    'data_file_checksum_value',
    'data_file_checksum_type',
    'data_file_location',
    'data_file_compression_status',
    'ctep_disease_term',
    'primary_diagnosis_disease_group',
    'meddra_disease_code',
    'histology',
    'stage_of_disease',
    'tumor_grade',
    'survival_status',
    'sex',
    'race',
    'ethnicity',
    'age_at_enrollment',
    'carcinogen_exposure',
    'targeted_therapy',
    'targeted_therapy_string',
    'anatomical_collection_site',
    'specimen_type',
    'tissue_category',
    'assessment_timepoint',
    'primary_disease_site',
    'drs_uri',
    'study_id',
    'study_short_name',
    'study_accession'
]

fileOverview_query = '''
query search {
    fileOverview ( first: $FILE_COUNT$ ) {
        ''' + '\n        '.join( scalar_file_overview_fields ) + '''
        association
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, file_overview_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Ask the API how many files there are. The default number of returned records for fileOverview() searches with no search filter applied is way too low to cover all files.
response = requests.post(
    ctdc_api_url,
    json={
        'query': 'query search { searchParticipants { numberOfFiles } }',
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( 'query search { searchParticipants { numberOfFiles } }', file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

file_count = result['data']['searchParticipants']['numberOfFiles']

# Send the fileOverview() query to the API server.
response = requests.post(
    ctdc_api_url,
    json={
        'query': re.sub( '\\$FILE_COUNT\\$', str( file_count ), fileOverview_query ),
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( re.sub( '\\$FILE_COUNT\\$', str( file_count ), fileOverview_query ), file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

# Save a version of the returned data as raw JSON.
with open( fileOverview_json_output_file, 'w' ) as JSON:
    print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

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
    'FILE_OVERVIEW',
    'FILE_OVERVIEW_ASSOCIATION',
    'FILE_SPECIMEN',
    'FILE_PARTICIPANT'
]

output_tsv_filenames = [
    file_overview_tsv,
    file_overview_association_tsv,
    data_file_specimen_record_id_tsv,
    data_file_participant_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_file_overview_fields, sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW'] )
print( *[ 'data_file_uuid', 'association' ], sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW_ASSOCIATION'] )
print( *[ 'data_file_uuid', 'specimen_record_id' ], sep='\t', end='\n', file=output_tsvs['FILE_SPECIMEN'] )
print( *[ 'data_file_uuid', 'participant_id' ], sep='\t', end='\n', file=output_tsvs['FILE_PARTICIPANT'] )

# Parse the returned data and save to TSV.
for file_overview in result['data']['fileOverview']:
    # Main FileOverview metadata.
    file_overview_row = list()
    for field_name in scalar_file_overview_fields:
        if file_overview[field_name] is not None:
            file_overview_row.append( file_overview[field_name] )
        else:
            file_overview_row.append( '' )
    print( *file_overview_row, sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW'] )

    # FileOverview.association [array of strings].
    # At time of writing (2026-03-11), possible values for elements of this array are 'biospecimen' and 'participant'.
    if file_overview['association'] is not None and len( file_overview['association'] ) > 0:
        for associated_entity in file_overview['association']:
            print( *[ file_overview['data_file_uuid'], associated_entity ], sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW_ASSOCIATION'] )
            if associated_entity == 'biospecimen':
                # We want this to break with a KeyError if the association array asserts an association but no ID is to be found.
                print( *[ file_overview['data_file_uuid'], file_overview['specimen_record_id'] ], sep='\t', end='\n', file=output_tsvs['FILE_SPECIMEN'] )
            elif associated_entity == 'participant':
                # We want this to break with a KeyError if the association array asserts an association but no ID is to be found.
                print( *[ file_overview['data_file_uuid'], file_overview['participant_id'] ], sep='\t', end='\n', file=output_tsvs['FILE_PARTICIPANT'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


