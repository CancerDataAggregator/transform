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

def get_unique_values( tsv_path, column_name ):
    
    if not path.exists( tsv_path ):
        
        sys.exit(f"FATAL: Can't find specified TSV \"{tsv_path}\"; aborting.\n")

    with open(tsv_path) as IN:
        
        headers = IN.readline().rstrip('\n').split('\t')

        if column_name not in headers:
            
            sys.exit(f"FATAL: TSV \"{tsv_path}\" has no column named \"{column_name}\"; aborting.\n")

        scanned_values = set()

        for line in [ nextLine.rstrip() for nextLine in IN.readlines() ]:
            
            current_record = dict( zip( headers, line.split('\t') ) )

            scanned_values.add(current_record[column_name])

        return sorted(scanned_values)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

protocol_out_dir = f"{output_root}/Protocol"

protocol_tsv = f"{protocol_out_dir}/Protocol.tsv"
protocol_study_tsv = f"{protocol_out_dir}/Protocol.study.tsv"

protocolPerStudy_json_output_file = f"{json_out_dir}/protocolPerStudy.json"

scalar_protocol_fields = (
    'protocol_id',
    'protocol_submitter_id',
    'protocol_name',
    'protocol_date',
    'document_name',
    'experiment_type',
    'quantitation_strategy',
    'label_free_quantitation',
    'labeled_quantitation',
    'isobaric_labeling_reagent',
    'reporter_ion_ms_level',
    'starting_amount',
    'starting_amount_uom',
    'digestion_reagent',
    'alkylation_reagent',
    'enrichment_strategy',
    'enrichment',
    'chromatography_dimensions_count',
    'one_d_chromatography_type',
    'two_d_chromatography_type',
    'fractions_analyzed_count',
    'column_type',
    'amount_on_column',
    'amount_on_column_uom',
    'column_length',
    'column_length_uom',
    'column_inner_diameter',
    'column_inner_diameter_uom',
    'particle_size',
    'particle_size_uom',
    'particle_type',
    'gradient_length',
    'gradient_length_uom',
    'instrument_make',
    'instrument_model',
    'dissociation_type',
    'ms1_resolution',
    'ms2_resolution',
    'dda_topn',
    'normalized_collision_energy',
    'acquistion_type',
    'dia_multiplexing',
    'dia_ims',
    'auxiliary_data',
    'cud_label',
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'project_submitter_id',
    'program_id',
    'program_submitter_id'
)

pdc_study_ids = get_unique_values( f"{output_root}/Study/Study.tsv", 'pdc_study_id' )

# EXECUTION

for output_dir in ( json_out_dir, protocol_out_dir ):
    
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
    'PROTOCOL',
    'PROTOCOL_STUDY'
]

output_tsv_filenames = [
    protocol_tsv,
    protocol_study_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open(protocolPerStudy_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_protocol_fields, sep='\t', end='\n', file=output_tsvs['PROTOCOL'] )
    print( *('protocol_id', 'study_id'), sep='\t', end='\n', file=output_tsvs['PROTOCOL_STUDY'] )

    for pdc_study_id in pdc_study_ids:
        
        api_query_json = {
            'query': '''            {
                protocolPerStudy( ''' + f'pdc_study_id: "{pdc_study_id}", acceptDUA: true' + ''' ) {
                    ''' + '\n                    '.join(scalar_protocol_fields) + '''
                    study {
                        study_id
                    }
                }
            }'''
        }

        # Send the protocolPerStudy() query to the API server.

        response = requests.post(api_url, json=api_query_json)

        # If the HTTP response code is not OK (200), dump the query, print the http
        # error result and exit.

        if not response.ok:
            
            print( api_query_json['query'], file=sys.stderr )

            response.raise_for_status()

        # Retrieve the server's JSON response as a Python object.

        result = json.loads(response.content)

        # Save a version of the returned data as JSON (caching the PDC Study ID ahead of each block).

        print( pdc_study_id, file=JSON )

        print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for protocol in result['data']['protocolPerStudy']:
            
            protocol_row = list()

            for field_name in scalar_protocol_fields:
                
                if protocol[field_name] is not None:
                    
                    # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                    protocol_row.append(json.dumps(protocol[field_name]).strip('"'))

                else:
                    
                    protocol_row.append('')

            print( *protocol_row, sep='\t', end='\n', file=output_tsvs['PROTOCOL'] )

            if protocol['study'] is not None:
                
                print( *(protocol['protocol_id'], protocol['study']['study_id']), sep='\t', end='\n', file=output_tsvs['PROTOCOL_STUDY'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


