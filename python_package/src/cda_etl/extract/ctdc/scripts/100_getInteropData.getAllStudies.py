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

interop_link_out_dir = path.join( output_root, 'InteropLink' )
interop_link_tsv = path.join( interop_link_out_dir, 'InteropLink.tsv' )

study_out_dir = path.join( output_root, 'Study' )
study_unique_repository_tsv = path.join( study_out_dir, 'Study.unique_repository.tsv' )
study_associated_link_url_tsv = path.join( study_out_dir, 'Study.associated_link_url.tsv' )

json_out_dir = path.join( output_root, '__API_result_json' )
getInteropData_getAllStudies_json_output_file = path.join( json_out_dir, 'getInteropData.getAllStudies.json' )

### OBJECT DEPTH: 0

# We will be fetching an array of Interop objects, each of which has exactly one
# field called 'data' referring to exactly one InteropData object, each of
# which has exactly one field called 'getAllStudies' referring to exactly one 
# AllStudies object.

# Non-scalar AllStudies fields:
#     unique_repository: [String]
#     associated_links: [InteropLink]
scalar_all_studies_fields = [
    'study_id',
    'study_short_name',
    'image_collection_count'
]

### OBJECT DEPTH: 1

# Non-scalar InteropLink fields:
#     metadataIDC: MetadataIDC
#     metadataTCIA: MetadataTCIA
scalar_interop_link_fields = [
    'associated_link_url',
    'associated_link_name' # not unique!
]

### OBJECT DEPTH: 2

# Non-scalar MetadataIDC fields:
#     <none>
scalar_metadata_idc_fields = [
    'collection_id',
    'cancer_type',
    'date_updated',
    'description',
    'doi',
    'image_types',
    'location',
    'species',
    'subject_count',
    'supporting_data'
]

# Non-scalar MetadataTCIA fields:
#     aggregate_BodyPartExamined: [String]
scalar_metadata_tcia_fields = [
    'collection',
    'aggregate_PatientID',
    'aggregate_Modality',
    'aggregate_ImageBool',
    'aggregate_ImageCount'
]

getInteropData_getAllStudies_query = '''
query search {
    getInteropData {
        data {
            getAllStudies {
                ''' + '\n                '.join( scalar_all_studies_fields ) + '''
                unique_repository
                associated_links {
                    ''' + '\n                    '.join( scalar_interop_link_fields ) + '''
                    metadataIDC {
                        ''' + '\n                        '.join( scalar_metadata_idc_fields ) + '''
                    }
                    metadataTCIA {
                        ''' + '\n                        '.join( scalar_metadata_tcia_fields ) + '''
                        aggregate_BodyPartExamined
                    }
                }
            }
        }
    }
}
'''

# Newlines are encoded in some free-text fields.
make_safe = {
    'metadataIDC': {
        'description'
    },
    'metadataTCIA': {
    }
}

# EXECUTION

for output_dir in [ json_out_dir, interop_link_out_dir, study_out_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Send the getInteropData() query to the API server.
response = requests.post(
    ctdc_api_url,
    json={
        'query': getInteropData_getAllStudies_query,
        'variables': {}
    },
    headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
)

# If the HTTP response code is not OK (200), dump the query, print the http error result and exit.
if not response.ok:
    print( getInteropData_getAllStudies_query, file=sys.stderr )
    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.
result = json.loads( response.content )

# Save a version of the returned data as raw JSON.
with open( getInteropData_getAllStudies_json_output_file, 'w' ) as JSON:
    print( json.dumps( result, indent=4, sort_keys=False ), file=JSON )

# Open handles for output files to save TSVs describing:
# 
#     * the returned Study objects
#     * association TSVs enumerating containment relationships between objects and sub-objects (e.g. Study->Participant)
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
    'STUDY_UNIQUE_REPOSITORY',
    'STUDY_INTEROP_LINK',
    'INTEROP_LINK'
]

output_tsv_filenames = [
    study_unique_repository_tsv,
    study_associated_link_url_tsv,
    interop_link_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open( file_name, 'w' ) for file_name in output_tsv_filenames ] ) )

interop_link_tsv_fields = scalar_interop_link_fields + [ f"MetadataIDC.{field_name}" for field_name in scalar_metadata_idc_fields ] + [ f"MetadataTCIA.{field_name}" for field_name in scalar_metadata_tcia_fields ] + [ 'MetadataTCIA.aggregate_BodyPartExamined' ]

# Table headers.
print( *[ 'study_id', 'unique_repository' ], sep='\t', end='\n', file=output_tsvs['STUDY_UNIQUE_REPOSITORY'] )
print( *[ 'study_id', 'associated_link_url' ], sep='\t', end='\n', file=output_tsvs['STUDY_INTEROP_LINK'] )
print( *interop_link_tsv_fields, sep='\t', end='\n', file=output_tsvs['INTEROP_LINK'] )

# Don't print duplicate records.
seen = {
    'associated_link_url': set()
}

for interop_object in result['data']['getInteropData']:
    interop_data_object = interop_object['data']
    all_studies_object = interop_data_object['getAllStudies']
    # All accesses at this level should break with a KeyError if the expected fields aren't present.
    study_id = all_studies_object['study_id']

    # Write associations between the current study and any values in AllStudies.unique_repository.
    if 'unique_repository' in all_studies_object and all_studies_object['unique_repository'] is not None and len( all_studies_object['unique_repository'] ) > 0:
        for unique_repository in all_studies_object['unique_repository']:
            print( *[ study_id, unique_repository ], sep='\t', end='\n', file=output_tsvs['STUDY_UNIQUE_REPOSITORY'] )

    
    # Digest AllStudies.associated_links [array of InteropLink objects], if it exists.
    if 'associated_links' in all_studies_object and all_studies_object['associated_links'] is not None and len( all_studies_object['associated_links'] ) > 0:
        for interop_link_object in all_studies_object['associated_links']:
            # Don't print duplicate InteropLink records. This should break with a KeyError if associated_link_url isn't present.
            if interop_link_object['associated_link_url'] not in seen['associated_link_url']:
                interop_link_row = list()
                for field_name in scalar_interop_link_fields:
                    if field_name in interop_link_object and interop_link_object[field_name] is not None:
                        interop_link_row.append( interop_link_object[field_name] )
                    else:
                        interop_link_row.append( '' )
                for field_name in scalar_metadata_idc_fields:
                    if 'metadataIDC' in interop_link_object and interop_link_object['metadataIDC'] is not None:
                        if field_name in interop_link_object['metadataIDC'] and interop_link_object['metadataIDC'][field_name] is not None:
                            if field_name in make_safe['metadataIDC']:
                                interop_link_row.append( json.dumps( interop_link_object['metadataIDC'][field_name].strip( '"' ) ).strip( '"' ) )
                            else:
                                interop_link_row.append( interop_link_object['metadataIDC'][field_name] )
                        else:
                            interop_link_row.append( '' )
                    else:
                        interop_link_row.append( '' )
                for field_name in scalar_metadata_tcia_fields:
                    if 'metadataTCIA' in interop_link_object and interop_link_object['metadataTCIA'] is not None:
                        if field_name in interop_link_object['metadataTCIA'] and interop_link_object['metadataTCIA'][field_name] is not None:
                            if field_name in make_safe['metadataTCIA']:
                                interop_link_row.append( json.dumps( interop_link_object['metadataTCIA'][field_name].strip( '"' ) ).strip( '"' ) )
                            else:
                                interop_link_row.append( interop_link_object['metadataTCIA'][field_name] )
                        else:
                            interop_link_row.append( '' )
                    else:
                        interop_link_row.append( '' )
                # MetadataTCIA.aggregate_BodyPartExamined is not a scalar field, but we're flattening it here to avoid truly stupid levels of indirection.
                if 'metadataTCIA' in interop_link_object and \
                            interop_link_object['metadataTCIA'] is not None and \
                            'aggregate_BodyPartExamined' in interop_link_object['metadataTCIA'] and \
                            interop_link_object['metadataTCIA']['aggregate_BodyPartExamined'] is not None and \
                            len( interop_link_object['metadataTCIA']['aggregate_BodyPartExamined'] ) > 0:
                    interop_link_row.append( str( interop_link_object['metadataTCIA']['aggregate_BodyPartExamined'] ) )
                else:
                    interop_link_row.append( '' )
                print( *interop_link_row, sep='\t', end='\n', file=output_tsvs['INTEROP_LINK'] )
                seen['associated_link_url'].add( interop_link_object['associated_link_url'] )
            print( *[ study_id, interop_link_object['associated_link_url'] ], sep='\t', end='\n', file=output_tsvs['STUDY_INTEROP_LINK'] )

# Close the output TSVs.
for keyword in output_tsv_keywords:
    output_tsvs[keyword].close()

# Sort the rows in the TSV output files.
for file in output_tsv_filenames:
    sort_file_with_header( file )


