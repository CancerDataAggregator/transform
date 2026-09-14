#!/usr/bin/env python -u

import re
import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

ctdc_api_url = 'https://clinical.datacommons.cancer.gov/v1/graphql/'

output_root = 'extracted_data/ctdc'

participant_overview_out_dir = f"{output_root}/ParticipantOverview"
participant_overview_tsv = f"{participant_overview_out_dir}/ParticipantOverview.tsv"

participant_out_dir = f"{output_root}/Participant"
participant_data_file_uuid_tsv = f"{participant_out_dir}/Participant.data_file_uuid.from_participantOverview.tsv"

data_file_out_dir = f"{output_root}/DataFile"
data_file_tsv = f"{data_file_out_dir}/DataFile.from_participantOverview.tsv"

json_out_dir = f"{output_root}/__API_result_json"
participantOverview_json_output_file = f"{json_out_dir}/participantOverview.json"

# Non-scalar ParticipantOverview fields:
#     data_files: [DataFile]
scalar_participant_overview_fields = [
    'participant_id',
    'study_id',
    'study_accession',
    'study_short_name',
    'study_name',
    'study_type',
    'study_description',
    'best_response_to_targeted_therapy',
    'surgical_procedure',
    'surgical_procedure_anatomical_location',
    'surgical_procedure_date',
    'surgical_procedure_findings',
    'surgical_procedure_id',
    'surgical_procedure_therapeutic',
    'extent_of_residual_disease',
    'dates_of_conduct',
    'ctep_disease_term',
    'primary_diagnosis_disease_group',
    'primary_disease_site',
    'stage_of_disease',
    'tumor_grade',
    'survival_status',
    'age_at_enrollment',
    'sex',
    'race',
    'ethnicity',
    'carcinogen_exposure',
    'targeted_therapy',
    'targeted_therapy_string',
    'specimen_id',
    'anatomical_collection_site',
    'parent_specimen_id',
    'parent_specimen_type',
    'tissue_category',
    'assessment_timepoint',
    'data_file_uuid' # string encoding an array
]

# Non-scalar DataFile fields:
#     <none>
scalar_data_file_fields = [
    'data_file_uuid',
    'data_file_name',
    'data_file_type',
    'data_file_description',
    'data_file_format',
    'data_file_size',
    'data_file_checksum_value',
    'data_file_checksum_type',
    'data_file_compression_status',
    'data_file_location'
]

participantOverview_query = '''
query search {
    participantOverview ( first: $PARTICIPANT_COUNT$ ) {
        ''' + '\n        '.join( scalar_participant_overview_fields ) + '''
        data_files {
            ''' + '\n            '.join( scalar_data_file_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, participant_out_dir, participant_overview_out_dir, data_file_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Ask the API how many participants there are. The default number of returned records for participantOverview() searches with no search filter applied is way too low to cover all participants.
response = requests.post(
    ctdc_api_url,
    json={
        'query': 'query search { searchParticipants { numberOfParticipants } }',
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( 'query search { searchParticipants { numberOfParticipants } }', file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

participant_count = result['data']['searchParticipants']['numberOfParticipants']

# Send the participantOverview() query to the API server.
response = requests.post(
    ctdc_api_url,
    json={
        'query': re.sub( '\\$PARTICIPANT_COUNT\\$', str( participant_count ), participantOverview_query ),
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( re.sub( '\\$PARTICIPANT_COUNT\\$', str( participant_count ), participantOverview_query ), file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

# Save a version of the returned data as raw JSON.
with open( participantOverview_json_output_file, 'w' ) as JSON:
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
    'PARTICIPANT_OVERVIEW',
    'PARTICIPANT_DATA_FILE',
    'DATA_FILE'
]

output_tsv_filenames = [
    participant_overview_tsv,
    participant_data_file_uuid_tsv,
    data_file_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_participant_overview_fields, sep='\t', end='\n', file=output_tsvs['PARTICIPANT_OVERVIEW'] )
print( *[ 'participant_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DATA_FILE'] )
print( *scalar_data_file_fields, sep='\t', end='\n', file=output_tsvs['DATA_FILE'] )

# Don't print duplicate records.
seen = {
    'data_file': set()
}

# Parse the returned data and save to TSV.
for participant_overview in result['data']['participantOverview']:
    # Main ParticipantOverview metadata.
    participant_overview_row = list()
    for field_name in scalar_participant_overview_fields:
        if participant_overview[field_name] is not None:
            participant_overview_row.append( participant_overview[field_name] )
        else:
            participant_overview_row.append( '' )
    print( *participant_overview_row, sep='\t', end='\n', file=output_tsvs['PARTICIPANT_OVERVIEW'] )

    # ParticipantOverview.data_files [array of DataFile records].
    if participant_overview['data_files'] is not None and len( participant_overview['data_files'] ) > 0:
        
        for data_file in participant_overview['data_files']:
            # This can happen.
            if data_file['data_file_uuid'] is not None and data_file['data_file_uuid'] != '':
                # Main DataFile metadata.
                data_file_row = list()
                for field_name in scalar_data_file_fields:
                    if data_file[field_name] is not None:
                        data_file_row.append( data_file[field_name] )
                    else:
                        data_file_row.append( '' )
                # Don't print duplicate DataFile records. This should break with a KeyError if data_file_uuid isn't present.
                if data_file['data_file_uuid'] not in seen['data_file']:
                    print( *data_file_row, sep='\t', end='\n', file=output_tsvs['DATA_FILE'] )
                    seen['data_file'].add( data_file['data_file_uuid'] )
                print( *[ participant_overview['participant_id'], data_file['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['PARTICIPANT_DATA_FILE'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


