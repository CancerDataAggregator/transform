#!/usr/bin/env python3 -u

import re

from os import path

# PARAMETERS

icd_dir = path.join( 'auxiliary_metadata', '__ontology_reference', 'ICD-O-3' )

input_file = path.join( icd_dir, 'icd_morphology.tsv' )

output_file = path.join( icd_dir, 'icd_o_3.codes_and_preferred_names.tsv' )

# EXECUTION

with open( input_file ) as IN, open( output_file, 'w' ) as OUT:
    
    display_header = next( IN )

    input_colnames = next( IN ).rstrip( '\n' ).split( '\t' )

    print( *[ 'icd_o_3_code', 'icd_o_3_preferred_name' ], sep='\t', file=OUT )

    for next_line in IN:
        
        record = dict( zip( input_colnames, next_line.rstrip( '\n' ).split( '\t' ) ) )

        code = record['ICDO3.2']

        if record['Level'].lower() == 'preferred':
            
            name = record['Term']

            print( *[ code, name ], sep='\t', file=OUT )


