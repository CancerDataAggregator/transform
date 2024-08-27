#!/usr/bin/env python -u

import re, sys

from os import listdir, makedirs, path

from cda_etl.lib import get_current_timestamp

# SUBROUTINE

def find_input_tsvs( scan_dir ):
    
    found_tsvs = set()

    for basename in listdir( scan_dir ):
        
        # Ignore files and directories beginning with '__' by convention.

        if re.search( r'^__', basename ) is None:
            
            usable_path = path.join( scan_dir, basename )

            if path.isdir( usable_path ):
                
                found_tsvs = found_tsvs | find_input_tsvs( usable_path )

            elif re.search( r'\.tsv', basename ) is not None:
                
                found_tsvs.add( path.join( scan_dir, basename ) )

    return found_tsvs

# ARGUMENTS

if ( len( sys.argv ) < 8 ) or ( ( len( sys.argv ) - 2 ) % 3 != 0 ):
    
    sys.exit( f"\n   [{len( sys.argv )}] Usage: {sys.argv[0]} <TSV directory to scan> <desired output file> <CDA release version> <current date, e.g. 2024-08-19> [<data source, e.g. 'gdc'> <extraction_date, e.g. '2024-08-18'> <version string, e.g. \"Data Release 40.0\">]+\n" )

input_root = sys.argv[1]

output_file = sys.argv[2]

version_string = dict()

version_string['CDA'] = sys.argv[3]

extraction_date = dict()

extraction_date['CDA'] = sys.argv[4]

arg_index = 5

while arg_index < len( sys.argv ):
    
    data_source = sys.argv[arg_index].upper()

    extraction_date[data_source] = sys.argv[arg_index + 1]

    version_string[data_source] = sys.argv[arg_index + 2]

    arg_index = arg_index + 3

match_list = re.findall( r'^(.*)\/[^\/]+$', output_file )

output_dir = '.'

if len( match_list ) > 0:
    
    output_dir = match_list[0]

# PARAMETERS

# Don't scan CDA release-metadata files if they've been built out of sequence.

skip_files = {
    
    'column_metadata.tsv',
    'release_metadata.tsv'
}

# EXECUTION

if not path.exists( output_dir ):
    
    makedirs( output_dir )

# Locate input TSVs.

input_files = find_input_tsvs( input_root )

display_filename = dict()

for input_file in input_files:
    
    if input_file not in skip_files:
        
        display_filename[input_file] = re.sub( r'^' + re.escape( input_root ) + r'\/?', r'', input_file )

with open( output_file, 'w' ) as OUT:
    
    print( *['table_name', 'column_name', 'data_source', 'data_source_version', 'data_source_extraction_date', 'distinct_non_null_values', 'number_of_data_rows', 'has_nulls', 'number_of_rows_with_nulls', 'numeric_column' ], sep='\t', end='\n', file=OUT )

    for input_file_path in sorted( input_files ):
        
        file_label = display_filename[input_file_path]

        if file_label not in skip_files and re.search( r'\.tsv$', file_label ) is not None:
            
            print( f"   [{get_current_timestamp()}] Processing {file_label}...", end='', file=sys.stderr )

            with open( input_file_path ) as IN:
                
                column_names = next( IN ).rstrip( '\n' ).split( '\t' )

                data_source_column_to_source = dict()

                for column_name in column_names:
                    
                    # Identify data_at_* columns and hook each up with its associated data_source.

                    match_list = re.findall( r'^data_at_(.+)$', column_name )

                    if len( match_list ) > 0:
                        
                        data_source = match_list[0].upper()

                        data_source_column_to_source[column_name] = data_source

                column_has_nulls = dict()

                column_all_numbers = dict()

                column_null_count = dict()

                column_distinct_values = dict()

                row_count = dict()

                column_has_nulls['CDA'] = dict( zip( column_names, [ False ] * len( column_names ) ) )

                column_all_numbers['CDA'] = dict( zip( column_names, [ True ] * len( column_names ) ) )

                column_null_count['CDA'] = dict( zip( column_names, [ 0 ] * len( column_names ) ) )

                column_distinct_values['CDA'] = dict( zip( column_names, [ set() for column_name in column_names ] ) )

                row_count['CDA'] = 0

                # Track data sources actually observed.

                saw_data_from = set()

                for data_source in extraction_date:
                    
                    column_has_nulls[data_source] = dict( zip( column_names, [ False ] * len( column_names ) ) )

                    column_all_numbers[data_source] = dict( zip( column_names, [ True ] * len( column_names ) ) )

                    column_null_count[data_source] = dict( zip( column_names, [ 0 ] * len( column_names ) ) )

                    column_distinct_values[data_source] = dict( zip( column_names, [ set() for column_name in column_names ] ) )

                    row_count[data_source] = 0

                for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                    
                    record = dict( zip( column_names, line.split( '\t' ) ) )

                    # Identify which upstream data sources contributed to this record (of the ones we're tracking based on the version and extraction-date information we received as arguments).

                    current_data_sources = set()

                    for data_source_column in data_source_column_to_source:
                        
                        if record[data_source_column].lower() == 'true' and data_source_column_to_source[data_source_column] in row_count:
                            
                            saw_data_from.add( data_source_column_to_source[data_source_column] )

                            current_data_sources.add( data_source_column_to_source[data_source_column] )

                    # Data source 'CDA' counts everything.

                    saw_data_from.add( 'CDA' )

                    current_data_sources.add( 'CDA' )

                    for data_source in current_data_sources:
                        
                        row_count[data_source] = row_count[data_source] + 1

                    for column_name in record:
                        
                        value = record[column_name]

                        for data_source in current_data_sources:
                            
                            if value == '':
                                
                                column_has_nulls[data_source][column_name] = True

                                column_null_count[data_source][column_name] = column_null_count[data_source][column_name] + 1

                            else:
                                
                                column_distinct_values[data_source][column_name].add( value )

                                if re.search( r'^-?\d+(\.\d+([eE]-?\d+)?)?$', value ) is None:
                                    
                                    column_all_numbers[data_source][column_name] = False

                for column_name in column_names:
                    
                    for data_source in sorted( saw_data_from ):
                        
                        has_nulls = str( column_has_nulls[data_source][column_name] )

                        is_all_numbers = str( column_all_numbers[data_source][column_name] )

                        if row_count[data_source] == 0:
                            
                            has_nulls = 'NA'
                            is_all_numbers = 'NA'

                        if column_null_count[data_source][column_name] == row_count:
                            
                            is_all_numbers = 'NA'

                        print( *[ f"{file_label}", column_name, data_source, version_string[data_source], extraction_date[data_source], str( len( column_distinct_values[data_source][column_name] ) ), str( row_count[data_source] ), has_nulls, column_null_count[data_source][column_name], is_all_numbers ], sep='\t', end='\n', file=OUT )

            print( 'done.', file=sys.stderr )


