#!/usr/bin/env python3 -u

from os import path, makedirs

# PARAMETERS

input_dir = path.join( 'cda_tsvs', 'last_merge' )

subject_input_tsv = path.join( input_dir, 'subject.tsv' )

identifiers_input_tsv = path.join( input_dir, 'upstream_identifiers.tsv' )

output_dir = path.join( 'auxiliary_metadata', '__aggregation_logs', 'subjects' )

subject_provenance_output_tsv = path.join( output_dir, 'CDA_subject_provenance.tsv' )

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Load IDs for aliases.

alias_to_id = dict()

with open( subject_input_tsv ) as IN:
    
    colnames = next( IN ).rstrip( '\n' ).split( '\t' )

    for next_line in IN:
        
        record = dict( zip( colnames, next_line.rstrip( '\n' ).split( '\t' ) ) )

        alias_to_id[record['id_alias']] = record['id']

# Create provenance file.

with open( identifiers_input_tsv ) as IN, open( subject_provenance_output_tsv, 'w' ) as OUT:
    
    header = next( IN )

    print( *[ 'cda_subject_id', 'upstream_source', 'upstream_field', 'upstream_id' ], sep='\t', file=OUT )

    for next_line in IN:
        
        [ table, id_alias, upstream_source, upstream_field, upstream_id ] = next_line.rstrip( '\n' ).split( '\t' )

        if table == 'subject':
            
            print( *[ alias_to_id[id_alias], upstream_source, upstream_field, upstream_id ], sep='\t', file=OUT )


