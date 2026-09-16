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

biospecimen_overview_out_dir = path.join( output_root, 'BiospecimenOverview' )
biospecimen_overview_tsv = path.join( biospecimen_overview_out_dir, 'BiospecimenOverview.tsv' )

specimen_out_dir = path.join( output_root, 'Specimen' )
specimen_data_file_uuid_tsv = path.join( specimen_out_dir, 'Specimen.data_file_uuid.from_biospecimenOverview.tsv' )
specimen_participant_id_tsv = path.join( specimen_out_dir, 'Specimen.participant_id.from_biospecimenOverview.tsv' )

data_file_out_dir = path.join( output_root, 'DataFile' )
data_file_tsv = path.join( data_file_out_dir, 'DataFile.from_biospecimenOverview.tsv' )
data_file_study_id_tsv = path.join( data_file_out_dir, 'DataFile.study_id.from_biospecimenOverview.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
biospecimenOverview_json_output_file = path.join( json_out_dir, 'biospecimenOverview.json' )

# Non-scalar BiospecimenOverview fields:
#     data_file_uuid: [String]
#     data_files: [DataFile]
scalar_biospecimen_overview_fields = [
    'specimen_record_id',
    'specimen_id', # This can be null.
    'participant_id',
    'study_short_name',
    'study_id',
    'study_accession',
    'ctep_disease_term',
    'primary_diagnosis_disease_group',
    'stage_of_disease',
    'primary_disease_site',
    'anatomical_collection_site',
    'specimen_type',
    'sex',
    'race',
    'specimen_category',
    'tumor_grade',
    'survival_status',
    'targeted_therapy',
    'surgical_procedure',
    'age_at_enrollment',
    'tissue_category',
    'assessment_timepoint',
    'targeted_therapy_string'
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

# Clarification from CTDC TPM: `order_by` is required; an update is planned but will happen at at unknown future date (as of 2026-09-10).
biospecimenOverview_query = '''
query search {
    biospecimenOverview ( first: $BIOSPECIMEN_COUNT$, order_by: "specimen_record_id" ) {
        ''' + '\n        '.join( scalar_biospecimen_overview_fields ) + '''
        data_file_uuid
        data_files {
            ''' + '\n            '.join( scalar_data_file_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, specimen_out_dir, biospecimen_overview_out_dir, data_file_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Ask the API how many biospecimens there are. The default number of returned records for biospecimenOverview() searches with no search filter applied is way too low to cover all biospecimens.
response = requests.post(
    ctdc_api_url,
    json={
        'query': 'query search { searchParticipants { numberOfSpecimens } }',
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( 'query search { searchParticipants { numberOfSpecimens } }', file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

biospecimen_count = result['data']['searchParticipants']['numberOfSpecimens']

# Send the biospecimenOverview() query to the API server.
response = requests.post(
    ctdc_api_url,
    json={
        'query': re.sub( '\\$BIOSPECIMEN_COUNT\\$', str( biospecimen_count ), biospecimenOverview_query ),
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( re.sub( '\\$BIOSPECIMEN_COUNT\\$', str( biospecimen_count ), biospecimenOverview_query ), file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

# Save a version of the returned data as raw JSON.
with open( biospecimenOverview_json_output_file, 'w' ) as JSON:
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
    'BIOSPECIMEN_OVERVIEW',
    'BIOSPECIMEN_DATA_FILE',
    'BIOSPECIMEN_PARTICIPANT',
    'DATA_FILE',
    'DATA_FILE_STUDY_ID'
]

output_tsv_filenames = [
    biospecimen_overview_tsv,
    specimen_data_file_uuid_tsv,
    specimen_participant_id_tsv,
    data_file_tsv,
    data_file_study_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_biospecimen_overview_fields, sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_OVERVIEW'] )
print( *[ 'specimen_record_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_DATA_FILE'] )
print( *[ 'specimen_record_id', 'participant_id' ], sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_PARTICIPANT'] )
print( *scalar_data_file_fields, sep='\t', end='\n', file=output_tsvs['DATA_FILE'] )
print( *[ 'data_file_uuid', 'study_id' ], sep='\t', end='\n', file=output_tsvs['DATA_FILE_STUDY_ID'] )

# Don't print duplicate records.
seen = {
    'data_file': set()
}

# Parse the returned data and save to TSV.
for biospecimen_overview in result['data']['biospecimenOverview']:
    # Main BiospecimenOverview metadata.
    biospecimen_overview_row = list()
    for field_name in scalar_biospecimen_overview_fields:
        if biospecimen_overview[field_name] is not None:
            biospecimen_overview_row.append( biospecimen_overview[field_name] )
        else:
            biospecimen_overview_row.append( '' )
    print( *biospecimen_overview_row, sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_OVERVIEW'] )

    # BiospecimenOverview.participant_id [one ID string].
    if biospecimen_overview['participant_id'] is not None and biospecimen_overview['participant_id'] != '':
        print( *[ biospecimen_overview['specimen_record_id'], biospecimen_overview['participant_id'] ], sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_PARTICIPANT'] )

    # BiospecimenOverview.data_files [array of DataFile records].
    if biospecimen_overview['data_files'] is not None and len( biospecimen_overview['data_files'] ) > 0:
        for data_file in biospecimen_overview['data_files']:
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
                    print( *[ data_file['data_file_uuid'], biospecimen_overview['study_id'] ], sep='\t', end='\n', file=output_tsvs['DATA_FILE_STUDY_ID'] )
                    seen['data_file'].add( data_file['data_file_uuid'] )
                print( *[ biospecimen_overview['specimen_record_id'], data_file['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['BIOSPECIMEN_DATA_FILE'] )

    # BiospecimenOverview.data_file_uuid [array of uuid strings]: use as sanity check only.
    if biospecimen_overview['data_file_uuid'] is not None and len( biospecimen_overview['data_file_uuid'] ) > 0:
        for uuid in biospecimen_overview['data_file_uuid']:
            if uuid not in seen['data_file']:
                print( f"Warning: DataFile uuid '{uuid}' listed in Biospecimen.data_file_uuid for specimen {biospecimen_overview['specimen_record_id']} but not found in BiospecimenOverview.data_files: please investigate, this shouldn't happen.", file=sys.stderr )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


