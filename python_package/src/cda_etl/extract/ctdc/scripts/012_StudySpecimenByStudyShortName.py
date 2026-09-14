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
study_specimen_count_tsv = path.join( study_out_dir, 'Study.specimen_count.tsv' )
study_specimen_record_id_tsv = path.join( study_out_dir, 'Study.specimen_record_id.tsv' )
study_specimen_timepoint_tsv = path.join( study_out_dir, 'Study.SpecimenTimepoint.tsv' )
study_specimen_type_tsv = path.join( study_out_dir, 'Study.SpecimenType.tsv' )

study_short_name_to_study_id = map_columns_one_to_one( study_tsv, 'study_short_name', 'study_id' )

specimen_out_dir = path.join( output_root, 'Specimen' )
specimen_tsv = path.join( specimen_out_dir, 'Specimen.from_StudySpecimenByStudyShortName.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
StudySpecimenByStudyShortName_json_output_file = path.join( json_out_dir, 'StudySpecimenByStudyShortName.json' )

# What we actually receive:
# 
# type StudySpecimen {
#   study_short_name: String
#   specimen_types: [SpecimenType]
#   specimen_timepoints: [SpecimenTimepoint]
#   specimen_count: String
#   specimen: [Specimen]
# }

# Non-scalar SpecimenType fields:
#     <none>
scalar_specimen_type_fields = [
    'group',
    'count'
]

# Non-scalar SpecimenTimepoint fields:
#     <none>
scalar_specimen_timepoint_fields = [
    'group',
    'count'
]

# Non-scalar Specimen fields:
#     <none>
scalar_specimen_fields = [
    'specimen_record_id',
    'specimen_type',
    'specimen_category',
    'anatomical_collection_site',
    'tissue_category',
    'assessment_timepoint',
    'collection_date'
]

StudySpecimenByStudyShortName_query = '''
query search {
    StudySpecimenByStudyShortName ( study_short_name: "$STUDY_SHORT_NAME$" ) {
        study_short_name
        specimen_types {
            ''' + '\n            '.join( scalar_specimen_type_fields ) + '''
        }
        specimen_timepoints {
            ''' + '\n            '.join( scalar_specimen_timepoint_fields ) + '''
        }
        specimen_count
        specimen {
            ''' + '\n            '.join( scalar_specimen_fields ) + '''
        }
    }
}
'''

# EXECUTION

for output_dir in [ json_out_dir, study_out_dir, specimen_out_dir ]:
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
    'SPECIMEN',
    'STUDY_SPECIMEN',
    'STUDY_SPECIMEN_COUNT',
    'STUDY_SPECIMEN_TIMEPOINT',
    'STUDY_SPECIMEN_TYPE'
]

output_tsv_filenames = [
    specimen_tsv,
    study_specimen_record_id_tsv,
    study_specimen_count_tsv,
    study_specimen_timepoint_tsv,
    study_specimen_type_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

# Table headers.
print( *scalar_specimen_fields, sep='\t', end='\n', file=output_tsvs['SPECIMEN'] )
print( *[ 'study_id', 'specimen_record_id' ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN'] )
print( *[ 'study_id', 'specimen_count' ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_COUNT'] )
print( *[ 'study_id', 'SpecimenTimepoint.group', 'SpecimenTimepoint.count' ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_TIMEPOINT'] )
print( *[ 'study_id', 'SpecimenType.group', 'SpecimenType.count' ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_TYPE'] )

with open( StudySpecimenByStudyShortName_json_output_file, 'w' ) as JSON:
    
    for study_short_name in sorted( study_short_name_to_study_id ):
        # Send the StudySpecimenByStudyShortName() query to the API server.
        response = requests.post(
            ctdc_api_url,
            json={
                'query': re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, StudySpecimenByStudyShortName_query ),
                'variables': {}
            },
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        )

        # If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
        if not response.ok:
            print( re.sub( '\\$STUDY_SHORT_NAME\\$', study_short_name, StudySpecimenByStudyShortName_query ), file=sys.stderr )
            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.
        result = json.loads( response.content )

        # Save a version of the returned data as raw JSON.
        print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

        # Parse the returned data and save to TSV.
        for study_specimen in result['data']['StudySpecimenByStudyShortName']:
            study_id = study_short_name_to_study_id[study_specimen['study_short_name']]

            for specimen in study_specimen['specimen']:
                specimen_row = list()
                for field_name in scalar_specimen_fields:
                    if specimen[field_name] is not None:
                        specimen_row.append( specimen[field_name] )
                    else:
                        specimen_row.append( '' )
                print( *specimen_row, sep='\t', end='\n', file=output_tsvs['SPECIMEN'] )

                # Associate this study with this Specimen record. ID access attempts should fail with a KeyError if the needed ID isn't present.
                print( *[ study_id, specimen['specimen_record_id'] ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN'] )

            # StudySpecimen.specimen_count [string]. The number of Specimens associated with this Study.
            if 'specimen_count' in study_specimen and study_specimen['specimen_count'] != '':
                print( *[ study_id, study_specimen['specimen_count'] ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_COUNT'] )

            # StudySpecimen.specimen_types [array of SpecimenTypes]. Associate with the Study record referenced in this StudySpecimen object.
            if 'specimen_types' in study_specimen and len( study_specimen['specimen_types'] ) > 0:
                for specimen_type in study_specimen['specimen_types']:
                    print( *[ study_id, specimen_type['group'], specimen_type['count'] ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_TYPE'] )

            # StudySpecimen.specimen_timepoints [array of SpecimenTimepoints]. Associate with the Study record referenced in this StudySpecimen object.
            if 'specimen_timepoints' in study_specimen and len( study_specimen['specimen_timepoints'] ) > 0:
                for specimen_timepoint in study_specimen['specimen_timepoints']:
                    print( *[ study_id, specimen_timepoint['group'], specimen_timepoint['count'] ], sep='\t', end='\n', file=output_tsvs['STUDY_SPECIMEN_TIMEPOINT'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


