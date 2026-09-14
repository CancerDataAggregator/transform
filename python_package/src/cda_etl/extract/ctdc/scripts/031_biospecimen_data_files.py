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

specimen_out_dir = path.join( output_root, 'Specimen' )
specimen_tsv = path.join( specimen_out_dir, 'Specimen.from_StudySpecimenByStudyShortName.tsv' )
specimen_data_file_uuid_tsv = path.join( specimen_out_dir, 'Specimen.data_file_uuid.from_biospecimen_data_files.tsv' )
specimen_participant_id_tsv = path.join( specimen_out_dir, 'Specimen.participant_id.from_biospecimen_data_files.tsv' )

file_overview_out_dir = path.join( output_root, 'FileOverview' )
file_overview_tsv = path.join( file_overview_out_dir, 'FileOverview.from_biospecimen_data_files.tsv' )
file_overview_association_tsv = path.join( file_overview_out_dir, 'FileOverview.association.from_biospecimen_data_files.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
biospecimen_data_files_json_output_file = path.join( json_out_dir, 'biospecimen_data_files.json' )

# Non-scalar FileOverview fields:
#     association: [String]
scalar_file_overview_fields = [
    'data_file_uuid',
    'participant_id',
    'specimen_id',
    'specimen_record_id',
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

biospecimen_data_files_query = '''
query search {
    biospecimen_data_files ( study_id: "$STUDY_ID$", first: $FILE_COUNT$ ) {
        ''' + '\n        '.join( scalar_file_overview_fields ) + '''
        association
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, file_overview_out_dir, specimen_out_dir ]:
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
    'FILE_OVERVIEW',
    'FILE_OVERVIEW_ASSOCIATION',
    'SPECIMEN_DATA_FILE',
    'SPECIMEN_PARTICIPANT'
]

output_tsv_filenames = [
    file_overview_tsv,
    file_overview_association_tsv,
    specimen_data_file_uuid_tsv,
    specimen_participant_id_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_file_overview_fields, sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW'] )
print( *[ 'data_file_uuid', 'association' ], sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW_ASSOCIATION'] )
print( *[ 'specimen_record_id', 'data_file_uuid' ], sep='\t', end='\n', file=output_tsvs['SPECIMEN_DATA_FILE'] )
print( *[ 'specimen_record_id', 'participant_id' ], sep='\t', end='\n', file=output_tsvs['SPECIMEN_PARTICIPANT'] )

with open( biospecimen_data_files_json_output_file, 'w' ) as JSON:
    seen_file_overviews = set()
    seen_specimen_files = dict()
    seen_specimen_participants = dict()

    for study_id in sorted( study_dict.keys() ):
        # Send the biospecimen_data_files() query to the API server.
        final_query = re.sub( '\\$FILE_COUNT\\$', str( file_count ), re.sub( '\\$STUDY_ID\\$', study_id, biospecimen_data_files_query ) )
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
        for file_overview in result['data']['biospecimen_data_files']:
            # This endpoint will give us FileOverview records for files not associated with any specimen. We don't need those in this context.
            if ( 'specimen_id' in file_overview and file_overview['specimen_id'] is not None ) or ( 'specimen_record_id' in file_overview and file_overview['specimen_record_id'] is not None ):
                # Main FileOverview metadata. Don't write duplicate records.
                if file_overview['data_file_uuid'] not in seen_file_overviews:
                    file_overview_row = list()
                    for field_name in scalar_file_overview_fields:
                        if file_overview[field_name] is not None:
                            file_overview_row.append( file_overview[field_name] )
                        else:
                            file_overview_row.append( '' )
                    print( *file_overview_row, sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW'] )
                    if 'association' in file_overview and file_overview['association'] is not None and len( file_overview['association'] ) > 0:
                        for association in file_overview['association']:
                            print( *[ file_overview['data_file_uuid'], association ], sep='\t', end='\n', file=output_tsvs['FILE_OVERVIEW_ASSOCIATION'] )
                    seen_file_overviews.add( file_overview['data_file_uuid'] )
                # We want this to break with a KeyError if no ID is to be found. Don't write duplicate records.
                if file_overview['specimen_record_id'] not in seen_specimen_files or file_overview['data_file_uuid'] not in seen_specimen_files[file_overview['specimen_record_id']]:
                    if file_overview['specimen_record_id'] not in seen_specimen_files:
                        seen_specimen_files[file_overview['specimen_record_id']] = set()
                    seen_specimen_files[file_overview['specimen_record_id']].add( file_overview['data_file_uuid'] )
                    print( *[ file_overview['specimen_record_id'], file_overview['data_file_uuid'] ], sep='\t', end='\n', file=output_tsvs['SPECIMEN_DATA_FILE'] )
                # We want this to break with a KeyError if no ID is to be found. Dont' write duplicate records.
                if file_overview['specimen_record_id'] not in seen_specimen_participants or file_overview['participant_id'] not in seen_specimen_participants[file_overview['specimen_record_id']]:
                    if file_overview['specimen_record_id'] not in seen_specimen_participants:
                        seen_specimen_participants[file_overview['specimen_record_id']] = set()
                    seen_specimen_participants[file_overview['specimen_record_id']].add( file_overview['participant_id'] )
                    print( *[ file_overview['specimen_record_id'], file_overview['participant_id'] ], sep='\t', end='\n', file=output_tsvs['SPECIMEN_PARTICIPANT'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


