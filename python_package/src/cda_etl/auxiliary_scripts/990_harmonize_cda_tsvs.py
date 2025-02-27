#!/usr/bin/env python3 -u

import gzip
import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import get_current_timestamp, get_universal_value_deletion_patterns, deduplicate_sorted_file_with_header, deduplicate_and_sort_unsorted_file_with_header

# ARGUMENT

if len( sys.argv ) != 4:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <unharmonized CDA TSV dir> <target output directory for harmonized CDA TSVs> <substitution log directory specific to this pass>\n" )

cda_tsv_dir = sys.argv[1]

harmonized_cda_tsv_dir = sys.argv[2]

substitution_log_dir = sys.argv[3]

# PARAMETERS

harmonization_map_dir = path.join( '.', 'harmonization_maps' )

harmonization_field_map_file = path.join( harmonization_map_dir, '000_cda_column_targets.tsv' )

debug=False

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found, except for tables listed
# in `exclude_tables` (some CDS submitter IDs, for example, are "Not Applicable",
# hence the need to exclude certain files from value alteration).

delete_everywhere = get_universal_value_deletion_patterns()

delete_row_if_nulled = {
    
    'dicom_series_anatomic_site',
    'dicom_series_tumor_vs_normal',
    'file_anatomic_site',
    'file_tumor_vs_normal'
}

remove_possible_sorted_dupes = {
    
    'dicom_series_anatomic_site',
    'dicom_series_tumor_vs_normal',
    'file_anatomic_site',
    'file_tumor_vs_normal'
}

# These need id_alias-blind dupe checking or they'll repeat data in different rows whether or not it's the same.

remove_possible_unsorted_dupes = {
    
    'mutation',
    'observation',
    'treatment'
}

delete_row_if_all_null_but = {
    
    'observation': {
        
        'id',
        'subject_id'
    }
}

exclude_tables = {
    
    'column_metadata',
    'dicom_series_describes_subject',
    'dicom_series_in_project',
    'file_describes_subject',
    'file_in_project',
    'project',
    'project_in_project',
    'release_metadata',
    'subject_in_project',
    'upstream_identifiers'
}

# EXECUTION

for output_dir in [ harmonized_cda_tsv_dir, substitution_log_dir ]:
    
    if not path.exists( output_dir ):
        
        makedirs( output_dir )

harmonized_value = dict()

print( f"[{get_current_timestamp()}] Loading harmonization maps...", end='', file=sys.stderr )

with open( harmonization_field_map_file ) as IN:
    
    colnames = next( IN ).rstrip( '\n' ).split( '\t' )

    for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
        
        row_dict = dict( zip( colnames, line.split( '\t' ) ) )

        table = row_dict['cda_table']

        column = row_dict['cda_column']

        concept_map_name = row_dict['concept_map_name']

        map_file = path.join( harmonization_map_dir, f"{concept_map_name}.tsv" )

        if table not in harmonized_value:
            
            harmonized_value[table] = dict()

        if column not in harmonized_value[table]:
            
            harmonized_value[table][column] = dict()

        with open( map_file ) as MAP:
            
            header_line = next( MAP ).rstrip( '\n' )

            if concept_map_name == 'anatomic_site':
                
                for ( old_value, uberon_id, uberon_name ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                    
                    if uberon_name != '__CDA_UNASSIGNED__':
                        
                        harmonized_value[table][column][old_value] = uberon_name

            elif concept_map_name == 'disease':
                
                for ( old_value, icd_code, icd_name, do_id, do_name, ncit_codes ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                    
                    if icd_name != '__CDA_UNASSIGNED__':
                        
                        harmonized_value[table][column][old_value] = icd_name

            elif concept_map_name == 'species':
                
                for ( old_value, ncbi_tax_id, ncbi_tax_sci_name, cda_common_name ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                    
                    if cda_common_name != '__CDA_UNASSIGNED__':
                        
                        harmonized_value[table][column][old_value] = cda_common_name

            else:
                
                for ( old_value, new_value ) in [ next_term_pair.rstrip( '\n' ).split( '\t' ) for next_term_pair in MAP ]:
                    
                    if new_value != '__CDA_UNASSIGNED__':
                        
                        harmonized_value[table][column][old_value] = new_value

print( 'done.', file=sys.stderr )

print( f"[{get_current_timestamp()}] Harmonizing tables...", end='\n\n', file=sys.stderr )

# Track all substitutions.

all_subs_performed = dict()

for file_basename in sorted( listdir( cda_tsv_dir ) ):
    
    print( f"   [{get_current_timestamp()}] ...{file_basename}...", file=sys.stderr )

    gzipped = False

    if re.search( r'\.tsv', file_basename ) is not None:
        
        table = re.sub( r'^(.*)\.tsv.*$', r'\1', file_basename )

        old_table_file = path.join( cda_tsv_dir, file_basename )

        new_table_file = path.join( harmonized_cda_tsv_dir, file_basename )

        if debug:
            
            print( f"table: {table}", file=sys.stderr )

            print( f"old file: {old_table_file}", file=sys.stderr )

            print( f"new file: {new_table_file}", end='\n\n', file=sys.stderr )

            for column in sorted( harmonized_value[table] ):
                
                print( f"   {column}", file=sys.stderr )

            print( file=sys.stderr )

        IN = open( old_table_file )

        OUT = open( new_table_file, 'w' )

        if re.search( r'\.gz$', old_table_file ) is not None:
            
            # Sometimes things are gzipped.

            gzipped = True

            IN.close()

            OUT.close()

            IN = gzip.open( old_table_file, 'rt' )

            OUT = gzip.open( new_table_file, 'wt' )

        all_subs_performed[table] = dict()

        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        print( *colnames, sep='\t', file=OUT )

        for next_line in IN:
            
            line = next_line.rstrip( '\n' )
            
            input_row_dict = dict( zip( colnames, line.split( '\t' ) ) )

            output_row = list()

            print_row = True

            for column in colnames:
                
                old_value = input_row_dict[column]

                if table in harmonized_value and column in harmonized_value[table]:
                    
                    new_value = ''

                    if old_value is not None and old_value.lower() in harmonized_value[table][column] and harmonized_value[table][column][old_value.lower()] != 'null':
                        
                        new_value = harmonized_value[table][column][old_value.lower()]

                    elif old_value is not None and old_value.lower() not in harmonized_value[table][column]:
                        
                        # Preserve values we haven't seen or mapped yet.

                        new_value = old_value

                    # Check target values for global cleanup: `delete_everywhere` takes precedence.

                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        
                        new_value = ''

                    output_row.append( new_value )

                    if column not in all_subs_performed[table]:
                        
                        all_subs_performed[table][column] = dict()

                    if old_value not in all_subs_performed[table][column]:
                        
                        all_subs_performed[table][column][old_value] = {
                            new_value : 1
                        }

                    elif new_value not in all_subs_performed[table][column][old_value]:
                        
                        all_subs_performed[table][column][old_value][new_value] = 1

                    else:
                        
                        all_subs_performed[table][column][old_value][new_value] = all_subs_performed[table][column][old_value][new_value] + 1

                    if table in delete_row_if_nulled and new_value == '':
                        
                        print_row = False

                elif table not in exclude_tables:
                    
                    if re.sub( r'\s', r'', old_value.strip().lower() ) in delete_everywhere:
                            
                        if column not in all_subs_performed[table]:
                            
                            all_subs_performed[table][column] = dict()

                        if old_value not in all_subs_performed[table][column]:
                            
                            all_subs_performed[table][column][old_value] = {
                                '' : 1
                            }

                        elif '' not in all_subs_performed[table][column][old_value]:
                            
                            all_subs_performed[table][column][old_value][''] = 1

                        else:
                            
                            all_subs_performed[table][column][old_value][''] = all_subs_performed[table][column][old_value][''] + 1

                        output_row.append( '' )

                        if table in delete_row_if_nulled:
                            
                            print_row = False

                    else:
                        
                        output_row.append( old_value )

                else:
                    
                    output_row.append( old_value )

            if table in delete_row_if_all_null_but:
                
                print_row = False

                row_dict = dict( zip( colnames, output_row ) )

                for colname in colnames:
                    
                    if colname not in delete_row_if_all_null_but[table] and row_dict[colname] is not None and row_dict[colname] != '':
                        
                        print_row = True

            if print_row:
                
                print( *output_row, sep='\t', file=OUT )

        IN.close()

        OUT.close()

        if table in remove_possible_sorted_dupes:
            
            print( f"   [{get_current_timestamp()}]    ...deduplicating {new_table_file}...", file=sys.stderr )

            deduplicate_sorted_file_with_header( new_table_file, gzipped )

        elif table in remove_possible_unsorted_dupes:
            
            print( f"   [{get_current_timestamp()}]    ...deduplicating {new_table_file}...", file=sys.stderr )

            deduplicate_and_sort_unsorted_file_with_header( new_table_file, gzipped, ignore_primary_id_field=True )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

# Dump substitution logs.

print( f"[{get_current_timestamp()}] Dumping harmonization substitution logs to {substitution_log_dir} ...", end='', file=sys.stderr )

for table in sorted( all_subs_performed ):
    
    for column in sorted( all_subs_performed[table] ):
        
        log_file = path.join( substitution_log_dir, f"{table}.{column}.substitution_log.tsv" )

        with open( log_file, 'w' ) as OUT:
            
            print( *[ 'raw_value', 'harmonized_value', 'number_of_substitutions' ], sep='\t', file=OUT )

            for old_value in sorted( all_subs_performed[table][column] ):
                
                for new_value in sorted( all_subs_performed[table][column][old_value] ):
                    
                    print( *[ old_value, new_value, all_subs_performed[table][column][old_value][new_value] ], sep='\t', file=OUT )

print( 'done.', file=sys.stderr )


