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

field_list_dir = path.join( 'extracted_data', 'gc', '__GC_release_metadata' )

field_list_basename = 'all_GC_fields_by_entity_type.tsv'

field_list_file = path.join( field_list_dir, field_list_basename )

output_dir = path.join( 'extracted_data', 'gc' )

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
                    
                    if field_value_type[entity_type][field_name] == 'array':
                        
                        # print( f"props[{field_name}] == {props[field_name]}", file=sys.stderr )

                        array_string = re.sub( r"(?<!\\)'", r'"', props[field_name] )

                        if re.search( r'^\[', array_string ) is None:
                            # Some (2025-08-25) sample.sample_anatomic_site and (2025-10-21) pdx.implantation_type values are coming in as e.g. "Cervix"; others as "[]" (always an empty array). Handle the former as one-element arrays.
                            array_string = f'["{array_string}"]'

                        my_array = sorted( json.loads( array_string ) )

                        output_record.append( json.dumps( my_array ) )

                    else:
                        
                        value = props[field_name]
                        # Make strings safe for TSVs and Unicode-unfriendly environments.
                        control_char_or_non_ascii_pattern = r'[^\x20-\x7F]'
                        if isinstance( value, str) and ( re.search( r'[\t\n]', value ) is not None or re.search( control_char_or_non_ascii_pattern, value ) is not None ):
                            value = json.dumps( value ).strip( '"' )
                        output_record.append( value )

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


