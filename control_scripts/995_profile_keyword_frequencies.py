#!/usr/bin/env python3 -u

import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

input_dir = sys.argv[1]

if not path.isdir( input_dir ):
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

# PARAMETERS

output_dir = path.join( 'auxiliary_metadata', '__harmonization_logs', '__global_keyword_frequencies' )

# EXECUTION

if not path.exists( output_dir ):
    makedirs( output_dir )

for entity in [ 'file', 'subject' ]:
    keywords = map_columns_one_to_one( path.join( input_dir, f"{entity}_keywords.tsv" ), 'id_alias', 'keyword' )
    keyword_describes_entity_file = path.join( input_dir, f"keyword_describes_{entity}.tsv" )

    frequency = dict()

    with open( keyword_describes_entity_file ) as IN:
        header = next( IN )
        for next_line in IN:
            ( keyword_alias, entity_alias ) = next_line.rstrip( '\n' ).split( '\t' )
            if keyword_alias not in frequency:
                frequency[keyword_alias] = 1
            else:
                frequency[keyword_alias] = frequency[keyword_alias] + 1

    output_file = path.join( output_dir, f"keyword_describes_{entity}.keyword_occurrence_counts.tsv" )

    with open( output_file, 'w' ) as OUT:
        print( *[ 'id_alias', 'keyword', 'count' ], sep='\t', file=OUT )
        for record in sorted( frequency.items(), key=lambda item: item[1], reverse=True ):
            keyword_alias = record[0]
            keyword = keywords[keyword_alias]
            count = record[1]
            print( *[ keyword_alias, keyword, count ], sep='\t', file=OUT )


