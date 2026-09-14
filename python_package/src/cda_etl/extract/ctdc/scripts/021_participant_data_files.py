#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import get_unique_values_from_tsv_column, load_tsv_as_dict, sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = path.join( 'extracted_data', 'ctdc' )

study_out_dir = path.join( output_root, 'Study' )
study_tsv = path.join( study_out_dir, 'Study.tsv' )
study_dict = load_tsv_as_dict( study_tsv )

participant_out_dir = path.join( output_root, 'Participant' )
participant_data_file_uuid_tsv = path.join( participant_out_dir, 'Participant.data_file_uuid.from_participant_data_files.tsv' )
participant_specimen_id_tsv = path.join( participant_out_dir, 'Participant.specimen_id.from_participant_data_files.tsv' )

data_file_out_dir = path.join( output_root, 'DataFile' )
data_file_specimen_id_tsv = path.join( data_file_out_dir, 'DataFile.specimen_id.from_participant_data_files.tsv' )
data_file_association_tsv = path.join( data_file_out_dir, 'DataFile.association.from_participant_data_files.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
participant_data_files_json_output_file = path.join( json_out_dir, 'participant_data_files.json' )

# What we actually receive:
# 
# type FileParticipant {
#   participant_id: String
#   study_short_name: String
#   study_id: String
#   study_accession: String
#   data_file_name: String
#   data_file_format: String
#   data_file_type: String
#   data_file_size: Float
#   association: [String]
#   data_file_description: String
#   specimen_id: String
#   ctep_disease_term: String
#   primary_diagnosis_disease_group: String
#   survival_status: String
#   data_file_uuid: String
# }

participant_data_files_query = '''
query search {
    participant_data_files ( study_id: "$STUDY_ID$", first: $FILE_COUNT$ ) {
        participant_id
        specimen_id
        data_file_uuid
        association
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, participant_out_dir, data_file_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Ask the API how many files there are. We don't want our results truncated by an invisible default record limit, which exists and is of unknown value.
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
    'PARTICIPANT_DATA_FILE',
    'PARTICIPANT_SPECIMEN',
    'DATA_FILE_SPECIMEN',
    'DATA_FILE_ASSOCIATION'
]

output_tsv_filenames = [
    participant_data_file_uuid_tsv,
    participant_specimen_id_tsv,
    data_file_specimen_id_tsv,
    data_file_association_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *[ 'participant_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DATA_FILE'] )
print( *[ 'participant_id', 'specimen_id' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_SPECIMEN'] )
print( *[ 'data_file_uuid', 'specimen_id' ], sep='\t', end='\n', file=output_tsvs['DATA_FILE_SPECIMEN'] )
print( *[ 'data_file_uuid', 'association' ], sep='\t', end='\n', file=output_tsvs['DATA_FILE_ASSOCIATION'] )

with open( participant_data_files_json_output_file, 'w' ) as JSON:
    
    for study_id in sorted( study_dict.keys() ):
        # Send the participant_data_files() query to the API server.
        final_query = re.sub( '\\$FILE_COUNT\\$', str( file_count ), re.sub( '\\$STUDY_ID\\$', study_id, participant_data_files_query ) )
        response = requests.post(
            ctdc_api_url,
            json={
                'query': final_query,
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( final_query, file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for file_participant in result['data']['participant_data_files']:
            # This endpoint will give us FileOverview records for files not associated with any specimen. We don't need those in this context.
            if 'participant_id' in file_participant and file_participant['participant_id'] is not None:
                participant_id = file_participant['participant_id']
                if file_participant['data_file_uuid'] is not None and file_participant['data_file_uuid'] != '':
                    print( *[ participant_id, file_participant['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DATA_FILE'] )
                    if file_participant['specimen_id'] is not None and file_participant['specimen_id'] != '':
                        print( *[ file_participant['data_file_uuid'], file_participant['specimen_id'] ], file=output_tsvs['DATA_FILE_SPECIMEN'] )
                    if file_participant['association'] is not None and len( file_participant['association'] ) > 0:
                        for association in file_participant['association']:
                            print( *[ file_participant['data_file_uuid'], association ], sep='\t', end='\n', file=output_tsvs['DATA_FILE_ASSOCIATION'] )
                elif file_participant['specimen_id'] is not None and file_participant['specimen_id'] != '':
                    print( *[ participant_id, file_participant['specimen_id'] ], file=output_tsvs['PARTICIPANT_SPECIMEN'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


