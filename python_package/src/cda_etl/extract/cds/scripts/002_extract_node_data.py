#!/usr/bin/env python3 -u

import gzip
import json
import jsonlines
import re
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <a gzipped Neo4j JSONL dump file>\n" )

dump_file = sys.argv[1]

# PARAMETERS

field_list_dir = path.join( 'extracted_data', 'cds', '__CDS_release_metadata' )

field_list_basename = 'all_CDS_fields_by_entity_type.tsv'

field_list_file = path.join( field_list_dir, field_list_basename )

output_dir = path.join( 'extracted_data', 'cds' )

for target_dir in [ output_dir ]:
    
    if not path.exists( target_dir ):
        
        makedirs( target_dir )

# EXECUTION

field_list = dict()

field_value_type = dict()

with open( field_list_file ) as IN:
    
    header = next( IN )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        [ entity_type, value_type, field_name ] = line.split( '\t' )

        if entity_type not in field_list:
            
            field_list[entity_type] = list()

        if entity_type not in field_value_type:
            
            field_value_type[entity_type] = dict()

        field_list[entity_type].append( field_name )

        field_value_type[entity_type][field_name] = value_type

output_data = dict()

with gzip.open( dump_file ) as IN:
    
    reader = jsonlines.Reader( IN )

    for record in reader:
        
        neo4j_type = record['type']

        if neo4j_type == 'node':
            
            # Assumes 'labels', which is currently an array, only ever has one element. True at time of writing.

            entity_type = record['labels'][0]

            props = record['properties']

            output_record = list()

            for field_name in field_list[entity_type]:
                
                if field_name in props and props[field_name] is not None and props[field_name] != '':
                    
                    if field_value_type[entity_type][field_name] == 'string':
                        
                        output_record.append( json.dumps( props[field_name] ).strip( '"' ) )

                    elif field_value_type[entity_type][field_name] == 'array':
                        
                        array_string = re.sub( r"(?<!\\)'", r'"', props[field_name] )

                        my_array = sorted( json.loads( array_string ) )

                        output_record.append( json.dumps( my_array ) )

                    else:
                        
                        output_record.append( props[field_name] )

                else:
                    
                    output_record.append( '' )

            if entity_type not in output_data:
                
                output_data[entity_type] = list()

            output_data[entity_type].append( output_record )

for entity_type in sorted( output_data ):
    
    output_file = path.join( output_dir, f"{entity_type}.tsv" )

    with open( output_file, 'w' ) as OUT:
        
        print( *field_list[entity_type], sep='\t', file=OUT )

        for record in output_data[entity_type]:
            
            print( *record, sep='\t', file=OUT )


