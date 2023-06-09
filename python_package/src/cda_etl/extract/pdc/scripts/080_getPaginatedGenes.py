#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

# SUBROUTINES

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

gene_out_dir = f"{output_root}/Gene"

gene_tsv = f"{gene_out_dir}/Gene.tsv"
gene_spectral_counts_tsv = f"{gene_out_dir}/Gene.spectral_counts.tsv"

scalar_gene_fields = (
    'gene_id',
    'gene_name',
    'NCBI_gene_id',
    'authority',
    'description',
    'organism',
    'chromosome',
    'locus',
    'proteins',
    'assays'
)

scalar_spectral_count_fields = (
    'study_id',
    'plex',
    'spectral_count',
    'distinct_peptide',
    'unshared_peptide',
    'study_submitter_id',
    'pdc_study_id',
    'project_id',
    'project_submitter_id'
)

offset = 0

offset_increment = 10000

returned_nothing = False

# EXECUTION

for output_dir in ( json_out_dir, gene_out_dir ):
    
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
    'GENE',
    'GENE_SPECTRAL_COUNTS'
]

output_tsv_filenames = [
    gene_tsv,
    gene_spectral_counts_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_gene_fields, sep='\t', end='\n', file=output_tsvs['GENE'] )
print( *(['gene_id'] + list(scalar_spectral_count_fields)), sep='\t', end='\n', file=output_tsvs['GENE_SPECTRAL_COUNTS'] )

# getPaginatedGenes() sometimes returns Spectral_count sub-objects with no Study data.
# These are sometimes duplicated within a single gene. Don't print duplicates to `gene_spectral_counts_tsv`.

seen_broken_spectral_count_rows = set()

while not returned_nothing:
    
    # This now breaks the PDC GraphQL server. Makes it cry, vomit, complain. Poor server.

    # api_query_json = {
    #     'query': '''            {
    #         getPaginatedGenes( ''' + f"offset: {offset}, limit: {offset_increment}" + ''' ) {
    #             genesProper {
    #                 ''' + '\n                        '.join(scalar_gene_fields) + '''
    #                 spectral_counts {
    #                     ''' + '\n                            '.join(scalar_spectral_count_fields) + '''
    #                 }
    #             }
    #         }
    #     }'''
    # }

    # This works.

    api_query_json = {
        'query': '''            {
            getPaginatedGenes( ''' + f"offset: {offset}, limit: {offset_increment}, acceptDUA: true" + ''' ) {
                genesProper {
                    ''' + '\n                        '.join(scalar_gene_fields) + '''
                }
            }
        }'''
    }

    # Send the getPaginatedGenes() query to the API server.

    response = requests.post(api_url, json=api_query_json)

    # If the HTTP response code is not OK (200), dump the query, print the http
    # error result and exit.

    if not response.ok:
        
        print( api_query_json['query'], file=sys.stderr )

        response.raise_for_status()

    # Retrieve the server's JSON response as a Python object.

    result = json.loads(response.content)

    if result['data']['getPaginatedGenes']['genesProper'] is None or len(result['data']['getPaginatedGenes']['genesProper']) == 0:
        
        returned_nothing = True

    else:
        
        # Save a version of the returned data as JSON.

        upper_limit = offset + (len(result['data']['getPaginatedGenes']['genesProper']) - 1)

        getPaginatedGenes_json_output_file = f"{json_out_dir}/getPaginatedGenes.{offset:06}-{upper_limit:06}.json"

        with open(getPaginatedGenes_json_output_file, 'w') as JSON:
            
            print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for gene in result['data']['getPaginatedGenes']['genesProper']:
            
            gene_row = list()

            for field_name in scalar_gene_fields:
                
                if gene[field_name] is not None:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                    gene_row.append(json.dumps(gene[field_name]).strip('"'))

                else:
                    
                    gene_row.append('')

            print( *gene_row, sep='\t', end='\n', file=output_tsvs['GENE'] )

            if 'spectral_counts' in gene and gene['spectral_counts'] is not None and len(gene['spectral_counts']) > 0:
                
                for spectral_count in gene['spectral_counts']:
                    
                    spectral_count_row = [gene['gene_id']]

                    for field_name in scalar_spectral_count_fields:
                        
                        if spectral_count[field_name] is not None:
                            
                            spectral_count_row.append(spectral_count[field_name])

                        else:
                            
                            spectral_count_row.append('')

                    # getPaginatedGenes() sometimes returns Spectral_count sub-objects with no Study data.
                    # These are sometimes duplicated within a single gene. Don't print duplicates to `gene_spectral_counts_tsv`.

                    if spectral_count['study_id'] is not None:
                        
                        print( *spectral_count_row, sep='\t', end='\n', file=output_tsvs['GENE_SPECTRAL_COUNTS'] )

                    else:
                        
                        spectral_count_row_string = '\t'.join(str(value) for value in spectral_count_row)

                        if spectral_count_row_string not in seen_broken_spectral_count_rows:
                            
                            seen_broken_spectral_count_rows.add(spectral_count_row_string)

                            print( *spectral_count_row, sep='\t', end='\n', file=output_tsvs['GENE_SPECTRAL_COUNTS'] )

        # Increment the paging offset in advance of the next query iteration.

        offset = offset + offset_increment

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


