#!/usr/bin/env python -u

import requests
import json
import sys
import time

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

file_metadata_out_dir = f"{output_root}/FileMetadata"

file_metadata_tsv = f"{file_metadata_out_dir}/FileMetadata.tsv"
file_metadata_aliquots_tsv = f"{file_metadata_out_dir}/FileMetadata.aliquots.tsv"

# We pull these from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store these in the FileMetadata directory
# to indicate that's where this version came from. This is half-normalization; will
# merge later during aggregation across (in this case) FileMetadata and Aliquot.

aliquot_tsv = f"{file_metadata_out_dir}/Aliquot.tsv"

scalar_file_metadata_fields = (
    'file_id',
    'file_submitter_id',
    'file_name',
    'file_type',
    'data_category',
    'file_size',
    'md5sum',
    'file_location',
    'file_format',
    'analyte',
    'instrument',
    'plex_or_dataset_name',
    'fraction_number',
    'experiment_type',
    'study_run_metadata_id',
    'study_run_metadata_submitter_id'
)

scalar_aliquot_fields = (
    'aliquot_id',
    'aliquot_submitter_id',
    'status',
    'aliquot_is_ref',
    'pool',
    'aliquot_quantity',
    'aliquot_volume',
    'amount',
    'analyte_type',
    'concentration',
    'sample_id',
    'sample_submitter_id',
    'case_id',
    'case_submitter_id'
)

offset = 0

# Conservative paging, plus pre-tested sleep intervals between pages and between
# batches of pages, is now (Feb 2024) necessary to avoid triggering server-side throttling
# which ends up quadrupling retrieval time (for this script, from 25 minutes to nearly 2 hours).

# Pre-fix for above: offset_increment = 500

offset_increment = 10000

returned_nothing = False

# EXECUTION

for output_dir in ( json_out_dir, file_metadata_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Open handles for output files to save TSVs describing the returned objects and (as needed) association TSVs
# enumerating containment relationships between objects and sub-objects as well as association TSVs enumerating
# relationships between objects and keyword-style dictionaries.
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

### OLD COMMENT: (none at this level)

output_tsv_keywords = [
    'FILE_METADATA',
    'FILE_METADATA_ALIQUOTS',
    'ALIQUOT'
]

output_tsv_filenames = [
    file_metadata_tsv,
    file_metadata_aliquots_tsv,
    aliquot_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_file_metadata_fields, sep='\t', end='\n', file=output_tsvs['FILE_METADATA'] )
print( *scalar_aliquot_fields, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )

print( *('file_id', 'aliquot_id'), sep='\t', end='\n', file=output_tsvs['FILE_METADATA_ALIQUOTS'] )

# These repeat. Save only one copy of each record in `aliquot_tsv`.

seen_aliquot_IDs = set()

# It's actually worse than that: some Aliquot records are repeatedly returned within _the_same_ FileMetadata object.

seen_aliquot_IDs_by_fileMetadata = dict()

while not returned_nothing:
    
    api_query_json = {
        'query': '''            {
            fileMetadata( ''' + f"offset: {offset}, limit: {offset_increment}" + ''', acceptDUA: true ) {
                ''' + '\n                    '.join(scalar_file_metadata_fields) + '''
                aliquots {
                    ''' + '\n                        '.join(scalar_aliquot_fields) + '''
                }
            }
        }'''
    }

    # Help a bored user out.

    print( f"Running `fileMetadata( offset: {offset}, limit: {offset_increment}, acceptDUA: true )`...", file=sys.stderr )

    # Send the fileMetadata() query to the API server.

    response = requests.post(api_url, json=api_query_json)

    # If the HTTP response code is not OK (200), dump the query, print the http
    # error result and exit.

    if not response.ok:
        
        print( api_query_json['query'], file=sys.stderr )

        response.raise_for_status()

    # Retrieve the server's JSON response as a Python object.

    result = json.loads(response.content)

    if result['data']['fileMetadata'] is None or len(result['data']['fileMetadata']) == 0:
        
        returned_nothing = True

    else:
        
        # Save a version of the returned data as JSON.

        upper_limit = offset + (len(result['data']['fileMetadata']) - 1)

        fileMetadata_json_output_file = f"{json_out_dir}/fileMetadata.{offset:06}-{upper_limit:06}.json"

        with open(fileMetadata_json_output_file, 'w') as JSON:
            
            print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for file_metadata in result['data']['fileMetadata']:
            
            file_metadata_row = list()

            for field_name in scalar_file_metadata_fields:
                
                if file_metadata[field_name] is not None:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                    file_metadata_row.append(json.dumps(file_metadata[field_name]).strip('"'))

                else:
                    
                    file_metadata_row.append('')

            print( *file_metadata_row, sep='\t', end='\n', file=output_tsvs['FILE_METADATA'] )

            if file_metadata['aliquots'] is not None and len(file_metadata['aliquots']) > 0:
                
                for aliquot in file_metadata['aliquots']:
                    
                    if aliquot['aliquot_id'] not in seen_aliquot_IDs:
                        
                        seen_aliquot_IDs.add(aliquot['aliquot_id'])

                        aliquot_row = list()

                        for field_name in scalar_aliquot_fields:
                            
                            if aliquot[field_name] is not None:
                                
                                aliquot_row.append(aliquot[field_name])

                            else:
                                
                                aliquot_row.append('')

                        print( *aliquot_row, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )

                    # Associate each Aliquot record with each of its containing FileMetadata record exactly once (despite the fact that
                    # this query returns multiple identical Aliquot records within the same FileMetadata record).

                    if file_metadata['file_id'] not in seen_aliquot_IDs_by_fileMetadata or aliquot['aliquot_id'] not in seen_aliquot_IDs_by_fileMetadata[file_metadata['file_id']]:
                        
                        if file_metadata['file_id'] not in seen_aliquot_IDs_by_fileMetadata:
                            
                            seen_aliquot_IDs_by_fileMetadata[file_metadata['file_id']] = set()

                        seen_aliquot_IDs_by_fileMetadata[file_metadata['file_id']].add(aliquot['aliquot_id'])

                        print( *(file_metadata['file_id'], aliquot['aliquot_id']), sep='\t', end='\n', file=output_tsvs['FILE_METADATA_ALIQUOTS'] )

        # Increment the paging offset in advance of the next query iteration.

        offset = offset + offset_increment

        # Empirically figuring out how best to self-regulate request size over time to avoid getting bandwidth-throttled has proven somewhat cumbersome (Feb 2024), but this seems to work pretty reliably. Today.

        # Update 2024-05: this bit was crafted for when we had to page in batches of 500; leave as is (1m41s delay between pulls) unless a reason appears.

        if offset % 10000 == 0:
            
            time.sleep( 101 )

        else:
            
            time.sleep( 3 )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


