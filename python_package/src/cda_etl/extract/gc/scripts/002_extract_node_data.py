#!/usr/bin/env python3 -u

import gzip
import json
import re
import sys

from os import path, makedirs

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <a memgraph .cypherl dump file>\n" )

dump_file = sys.argv[1]

# PARAMETERS

field_list_dir = path.join( 'extracted_data', 'gc', '__GC_release_metadata' )
field_list_file = path.join( field_list_dir, 'all_GC_fields_by_entity_type.tsv' )

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
bracket_anomalies = 0
current_line = 1

with open( dump_file ) as IN:
    for next_line in IN:
        # CREATE (:__mg_vertex__:`genomic_info` {__mg_id__: 9806845, `uuid`: "431c6110-07d9-5d20-a4d1-feae5c5ffbcd", `created`: "2026-03-16T15:59:45.872745+00:00[Etc/UTC]", `library_selection`: "Unspecified", `library_layout`: "Not Reported", `instrument_model`: "Illumina HiSeq X Ten", `reference_genome_assembly`: "Not specified in data", `platform`: "Illumina", `genomic_info_id`: "id_9996", `library_strategy`: "WGS", `platform_concept_code`: "C146817", `library_strategy_concept_code`: "C101294"});
        node_block_result = re.search( r'^CREATE\s+\(:__mg_vertex__:`([^`]+)`\s+{__mg_id__:\s+(\S+),\s+`(\S.*)}\);$', next_line.rstrip( '\n' ) )

        if node_block_result is not None:
            entity_type = node_block_result.group( 1 )
            node_id = node_block_result.group( 2 )
            node_data_line = node_block_result.group( 3 )

            input_record = dict()

            for key_value_string in node_data_line.split( r', `' ):
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
                    # We might've zeroed the thing.
                    if value is not None and value != '':
                        value = json.loads( value )
                        input_record[key] = value

            output_record = list()

            for field_name in field_list[entity_type]:
                
                if field_name in input_record and input_record[field_name] is not None and input_record[field_name] != '' and input_record[field_name] != '[]':
                    if field_value_type[entity_type][field_name] == 'array':
                        if isinstance( input_record[field_name], str ):
                            # Some (2025-08-25) sample.sample_anatomic_site and (2025-10-21) pdx.implantation_type values are coming in as e.g. "Cervix"; others as "[]" (always an empty array). Handle the former as one-element arrays.
                            # New 2026-02-17: Add participant.ethnicity and participant.race (the latter of which now sometimes has multiple elements, while the former does not) to this list.
                            output_record.append( json.dumps( [ input_record[field_name] ] ) )
                        else:
                            output_record.append( json.dumps( input_record[field_name] ) )
                    else:
                        value = input_record[field_name]
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

    current_line = current_line + 1

print( f"Bracket anomalies encountered: {bracket_anomalies}", file=sys.stderr )

for entity_type in sorted( output_data ):
    output_file = path.join( output_dir, f"{entity_type}.tsv" )
    with open( output_file, 'w' ) as OUT:
        print( *field_list[entity_type], sep='\t', file=OUT )
        for record in output_data[entity_type]:
            print( *record, sep='\t', file=OUT )


