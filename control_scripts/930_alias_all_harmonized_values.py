#!/usr/bin/env python3 -u

import gzip
import re
import shutil
import subprocess
import sys

from os import listdir, makedirs, path

from cda_etl.lib import get_column_metadata

# PARAMETERS

input_dir = path.join( 'cda_tsvs', 'last_merge' )
output_dir = path.join( 'cda_tsvs', 'final_merge__decorated_harmonized_aliased' )

for dir_name in { output_dir }:
    if not path.exists( dir_name ):
        makedirs( dir_name )

term_dir = path.join( 'harmonization_maps', 'zz03_term_tables' )

for file_name in listdir( term_dir ):
    shutil.copy2( path.join( term_dir, file_name ), path.join( output_dir, file_name ) )

controlled_term_tsv = path.join( output_dir, 'controlled_term.tsv' )

core_ontology_sources = {
    'CDA',
    'UBERON',
    'ICD-O-3',
    'NCBI'
}

offensive_suffixes = {
    r',\s+nos$',
    r',\s+not\s+otherwise\s+specified$',
    r'-\s+not\s+otherwise\s+specified\s+\(nos\)$'
}

# EXECUTION

# Hack the controlled_term dictionary for the current build: remove offensive suffixes from harmonized terms.
temp_tsv = path.join( output_dir, 'temp.tsv' )
with open( controlled_term_tsv ) as IN, open( temp_tsv, 'w' ) as OUT:
    columns = next( IN ).rstrip( '\n' ).split( '\t' )
    print( *columns, sep='\t', file=OUT )
    for next_line in IN:
        current_record = dict( zip( columns, next_line.rstrip( '\n' ).split( '\t' ) ) )
        for offensive_suffix in offensive_suffixes:
            current_record['name'] = re.sub( offensive_suffix, r'', current_record['name'], flags=re.IGNORECASE )
        print( *[ current_record[column] for column in columns ], sep='\t', file=OUT )
shutil.move( temp_tsv, controlled_term_tsv )

# Find out which columns are harmonized.
column_metadata = get_column_metadata()
harmonized_fields = dict()

for table_name in sorted( column_metadata ):
    # Python 3 preserves insert order for dicts. That means column data will be displayed
    # in the order in which columns are listed in the definition (in lib.py) of
    # get_column_metadata(). Handy. Also worth noting because it's not obvious.
    for column_name in column_metadata[table_name]:
        current_record = column_metadata[table_name][column_name]
        if 'concept' in current_record and current_record['concept'] is not None and current_record['concept'] != '':
            if table_name not in harmonized_fields:
                harmonized_fields[table_name] = dict()
            harmonized_fields[table_name][column_name] = current_record['concept']

# Load controlled-term data.
name_to_alias = dict()

with open( controlled_term_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
        
        if record['data_source'] is not None and record['data_source'] in core_ontology_sources:
            if record['concept'] not in name_to_alias:
                name_to_alias[record['concept']] = dict()
            if record['name'] is None or record['name'] == '':
                sys.exit( f"FATAL: No name for canonical '{record['concept']}' term with alias {record['id_alias']}; cannot continue, please handle." )
            name_to_alias[record['concept']][record['name']] = record['id_alias']

# Track unknown input values so we don't spam the error stream with more than one warning per value.
unknown_term_values = set()

# Transcode, aliasing as we go.
for file_name in listdir( input_dir ):
    file_match = re.search( r'^(\S+)\.tsv', file_name )

    if file_match is not None:
        print( f"Transcoding/copying {file_name}...", end='', file=sys.stderr )
        table_name = file_match.group(1)

        if table_name not in harmonized_fields:
            shutil.copy2( path.join( input_dir, file_name ), path.join( output_dir, file_name ) )

        else:
            input_file = path.join( input_dir, file_name )
            output_file = path.join( output_dir, file_name )
            IN = open( input_file )
            OUT = open( output_file, 'w' )

            if re.search( r'\.tsv\.gz$', file_name ) is not None:
                IN.close()
                IN = gzip.open( input_file, 'rt' )
                OUT.close()
                OUT = gzip.open( output_file, 'wt' )
            header_line = next( IN )
            print( header_line, end='', file=OUT )
            column_names = header_line.rstrip( '\n' ).split( '\t' )

            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                for column_name in harmonized_fields[table_name]:

                    if record[column_name] is not None and record[column_name] != '':
                        # Leverage the hack to the controlled_term dictionary for the current build: remove offensive suffixes from harmonized terms.
                        for offensive_suffix in offensive_suffixes:
                            record[column_name] = re.sub( offensive_suffix, r'', record[column_name], flags=re.IGNORECASE )
                        if record[column_name] not in name_to_alias[harmonized_fields[table_name][column_name]]:
                            # Print one warning to stderr for each unknown value.
                            if record[column_name] not in unknown_term_values:
                                unknown_term_values.add( record[column_name] )
                                print( f"WARNING: {record[column_name]} not found in '{harmonized_fields[table_name][column_name]}' term dictionary; skipping and nulling.", file=sys.stderr )
                            # Null unknown values.
                            record[column_name] = ''
                        else:
                            record[column_name] = name_to_alias[harmonized_fields[table_name][column_name]][record[column_name]]

                print( *[ record[column_name] for column_name in column_names ], sep='\t', file=OUT )

            IN.close()
            OUT.close()

        print( 'done.', file=sys.stderr )


