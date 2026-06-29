#!/usr/bin/env python3 -u

import gzip
import json
import re
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <a memgraph .cypherl dump file>\n" )

input_file = sys.argv[1]

# PARAMETERS

field_list_dir = path.join( 'extracted_data', 'gc', '__GC_release_metadata' )
output_file = path.join( field_list_dir, 'all_GC_fields_by_entity_type.tsv' )

for output_dir in [ field_list_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

fields = dict()
current_line = 1
bracket_anomalies = 0
# Assume node block appears first at the top of the file.
in_node_block = True

with open( input_file ) as IN:
    for next_line in IN:
        if in_node_block:
            # CREATE (:__mg_vertex__:`genomic_info` {__mg_id__: 9806845, `uuid`: "431c6110-07d9-5d20-a4d1-feae5c5ffbcd", `created`: "2026-03-16T15:59:45.872745+00:00[Etc/UTC]", `library_selection`: "Unspecified", `library_layout`: "Not Reported", `instrument_model`: "Illumina HiSeq X Ten", `reference_genome_assembly`: "Not specified in data", `platform`: "Illumina", `genomic_info_id`: "id_9996", `library_strategy`: "WGS", `platform_concept_code`: "C146817", `library_strategy_concept_code`: "C101294"});
            node_block_result = re.search( r'^CREATE\s+\(:__mg_vertex__:`([^`]+)`\s+{__mg_id__:\s+(\S+),\s+`(\S.*)}\);$', next_line.rstrip( '\n' ) )
            if node_block_result is None:
                in_node_block = False
            else:
                node_type = node_block_result.group( 1 )
                node_id = node_block_result.group( 2 )
                node_data_line = node_block_result.group( 3 )

                if node_type not in fields:
                    fields[node_type] = dict()

                for key_value_string in node_data_line.split( r', `' ):
                    value_type = 'UNKNOWN_TYPE__ALL_VALUES_NULL'
                    ( key, value ) = key_value_string.split( r'`: ' )
                    if value is not None and value != '' and value != '""' and value != "''":
                        # Unpack array-encoded strings.
                        if re.search( r'^"\[.*\]"$', value ) is not None:
                            value = value.strip( '"' )
                            # Because -- NO JOKE -- ... `file_types_and_format`: "[\"[\\\"FASTQ\\\"]\"]", ...
                            if re.search( r'\[.*\[', value ) is not None:
                                # WE REFUSE.
                                value = ''
                                bracket_anomalies = bracket_anomalies + 1
                            # Because ... `sample_anatomic_site`: "[]", ...
                            if value == '[]':
                                value = ''
                            # Because ... `authz`: "[\'/programs/phs002431\']", ...
                            value = re.sub( r"\\'", r'\\"', value )
                            # Because experimental_strategy_and_data_subtypes:[\"Targeted Sequencing\"]
                            value = re.sub( r'\\' , r'', value )
                        # Because ... `coverage`: "30.259247", ...
                        if key == 'coverage':
                            value = value.strip( '"' )
                        if re.search( r'^"', value ) is not None:
                            value = json.dumps( value.strip( '"' ) )
                        # Because ... `updated`: DATETIME("2025-05-23T20:06:48.050461+00:00[Etc/UTC]"), ...
                        value = re.sub( r'^DATETIME\((.*)\)$', r'\1', value )
                        # print( f"{current_line} {key}:{value}" )
                        # We might've zeroed the thing.
                        if value is not None and value != '':
                            value = json.loads( value )
                            if isinstance( value, list ):
                                value_type = 'array'
                            elif isinstance( value, str ):
                                if re.search( r'^\[.*\]$', value ) is not None:
                                    value_type = 'array'
                                else:
                                    value_type = 'string'
                            elif isinstance( value, int ):
                                value_type = 'integer'
                            elif isinstance( value, float ):
                                value_type = 'float'
                            else:
                                sys.exit( f"NO. {value}" )

                            if key not in fields[node_type] or fields[node_type][key] == 'UNKNOWN_TYPE__ALL_VALUES_NULL':
                                fields[node_type][key] = value_type
                            elif fields[node_type][key] != value_type:
                                # New 2025-08-25: Some sample.sample_anatomic_site values are coming in as e.g. "Cervix"; others as "[]" (always an empty array).
                                # The following sets up a parse hack in a subsequent extraction script to force treatment of these values as arrays, assuming proper downstream format case checking. Ech.
                                # New 2025-10-21: Apparently the same thing is happening now for pdx.implantation_type.
                                # New 2026-02-17: And now for participant.ethnicity, which only ever has one value.
                                #                 And now also for participant.race, which sometimes now has multiple values.
                                # New 2026-04-21: Sometimes numeric fields (e.g. avg_read_length) are encoded inconsistently as both floats and integers in different records.
                                if ( node_type == 'sample' and key == 'sample_anatomic_site' ) or \
                                    ( node_type == 'pdx' and key == 'implantation_type' ) or \
                                    ( node_type == 'participant' and key in { 'ethnicity', 'race' } ):
                                    fields[node_type][key] = 'array'
                                elif ( value_type == 'integer' and fields[node_type][key] == 'float' ) or \
                                    ( value_type == 'float' and fields[node_type][key] == 'integer' ):
                                    fields[node_type][key] = 'float'
                                else:
                                    sys.exit( f"[line {current_line}]: Eek! [node_type={node_type}; key={key}] ( fields[node_type][key] == {fields[node_type][key]} ) != ( value_type == {value_type} )" )

        current_line = current_line + 1

print( f"Bracket anomalies encountered: {bracket_anomalies}", file=sys.stderr )

with open( output_file, 'w' ) as OUT:
    
    print( *[ 'entity_type', 'value_type', 'field_name' ], sep='\t', file=OUT )

    for record_type in sorted( fields ):
        
        if 'uuid' in fields[record_type]:
            
            print( *[ record_type, fields[record_type]['uuid'], 'uuid' ], sep='\t', file=OUT )

        for field_name in sorted( [ key for key in fields[record_type] if key != 'uuid' ] ):
            
            print( *[ record_type, fields[record_type][field_name], field_name ], sep='\t', file=OUT )


