#!/usr/bin/env python3 -u

import gzip
import jsonlines
import re
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <a gzipped Neo4j JSONL dump file>\n" )

input_file = sys.argv[1]

# PARAMETERS

field_list_dir = path.join( 'extracted_data', 'cds', '__CDS_release_metadata' )

output_basename = 'all_CDS_fields_by_entity_type.tsv'

output_file = path.join( field_list_dir, output_basename )

for output_dir in [ field_list_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

fields = dict()

with gzip.open( input_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        neo4j_type = record['type']

        if neo4j_type == 'node':
            
            # Assumes 'labels', which is currently an array, only ever has one element. True at time of writing.

            record_type = record['labels'][0]

            if record_type not in fields:
                
                fields[record_type] = dict()

            props = record['properties']

            for key in sorted( props ):
                
                prop_type = 'UNKNOWN_TYPE__ALL_VALUES_NULL'

                if props[key] is not None:
                    
                    if isinstance( props[key], str ):
                        
                        prop_value = props[key]

                        if re.search( r'^\[', prop_value ) is not None:
                            
                            prop_type = 'array'

                        else:
                            
                            prop_type = 'string'

                    elif isinstance( props[key], int ):
                        
                        prop_type = 'integer'

                    elif isinstance( props[key], float ):
                        
                        prop_type = 'float'

                    else:
                        
                        sys.exit( f"NO. {props[key]}" )

                if key not in fields[record_type] or fields[record_type][key] == 'UNKNOWN_TYPE__ALL_VALUES_NULL':
                    
                    fields[record_type][key] = prop_type

                elif fields[record_type][key] != prop_type:
                    
                    sys.exit( f"NOOOO. {fields[record_type][key]} != {prop_type}" )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'entity_type', 'value_type', 'field_name' ], sep='\t', file=OUT )

    for record_type in sorted( fields ):
        
        if 'uuid' in fields[record_type]:
            
            print( *[ record_type, fields[record_type]['uuid'], 'uuid' ], sep='\t', file=OUT )

        for field_name in sorted( [ key for key in fields[record_type] if key != 'uuid' ] ):
            
            print( *[ record_type, fields[record_type][field_name], field_name ], sep='\t', file=OUT )


