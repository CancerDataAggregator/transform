#!/usr/bin/env python3 -u

import gzip
import re
import sys

from os import listdir, makedirs, path, remove

from cda_etl.lib import get_current_timestamp, get_universal_value_deletion_patterns, deduplicate_and_sort_unsorted_file_with_header

# ARGUMENT

if len( sys.argv ) != 4:
    sys.exit( f"\n   Usage: {sys.argv[0]} <unharmonized CDA TSV dir> <target output directory for harmonized CDA TSVs> <substitution log directory specific to this pass>\n" )

cda_tsv_dir = sys.argv[1]
harmonized_cda_tsv_dir = sys.argv[2]
substitution_log_dir = sys.argv[3]

# PARAMETERS

debug=False

harmonization_map_dir = 'harmonization_maps'

harmonization_field_map_file = path.join( harmonization_map_dir, '000_cda_column_targets.tsv' )

# Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
# should be deleted wherever they are found, except for tables listed
# in `exclude_tables` (some CDS submitter IDs, for example, are "Not Applicable",
# hence the need to exclude certain files from value alteration).
delete_everywhere = get_universal_value_deletion_patterns()

# Keep only one distinct copy of each harmonized/QC'd (id, value) association row.
remove_possible_dupes_including_ids = {
    'file_anatomic_site',
    'file_tumor_vs_normal'
}

# These need id-blind dupe checking or they'll repeat identical data records across
# different rows that differ only by their own row IDs (e.g. observation_id might differ
# because two observation source records harmonized to the same identical result record,
# leaving distinct IDs but all else the same).
remove_possible_dupes_ignoring_ids = {
    'mutation',
    'observation',
    'treatment'
}

# Don't keep harmonizedi/QC'd versions of single-value rows if their values are nulled.
delete_row_if_nulled = {
    'file_anatomic_site',
    'file_tumor_vs_normal'
}

# Don't keep harmonized/QC'd versions of multi-value rows if their (non-ID) values are all nulled.
delete_row_if_all_null_but = {
    'observation': {
        'id',
        'subject_id'
    },
    'treatment': {
        'id',
        'subject_id'
    }
}

# We don't harmonize or QC anything in these tables: everything in them* came from us (and
# is validated as part of our software development life cycle), not from upstream data
# sources (which would require explicit value validation permitting no up-front assumptions).
# 
# *Except the ID values in `upstream_identifiers`, which we never alter and so there's
# nothing to validate.
exclude_tables = {
    'column_metadata',
    'file_describes_subject',
    'file_in_project',
    'project',
    'project_in_project',
    'release_metadata',
    'subject_in_project',
    'upstream_identifiers'
}

# Store explicit "this came from that" maps for harmonized and QCd fields. Necessary
# to be this explicit because (for example) sometimes multiple GDC treatment rows,
# when harmonized, collapse into a single CDA treatment row -- asking "what was
# the value that came before this CDA harmonized version?" can thus produce complex
# answers, and it's not always a matter of a direct one-to-one lookup. This enables
# the recovery of all possible value-ancestry patterns.
make_unharmonized_tsvs = {
    'file',
    'file_anatomic_site',
    'file_tumor_vs_normal',
    'subject',
    'observation',
    'treatment'
}

# IDs never get harmonized or QC'd, but in addition, these fields also never get
# harmonized or QC'd, so we don't have to track "before and after" versions of them.
# Which is especially good for fields like `file.drs_uri`, which contains a
# massive volume of data, making it really helpful not to have to duplicate it
# in a before/after map.
fields_to_ignore_for_unharmonized_tsvs = {
    'file': {
        'crdc_id',
        'name',
        'description',
        'drs_uri',
        'size'
    },
    'file_anatomic_site': {
    },
    'file_tumor_vs_normal': {
    },
    'subject': {
        'crdc_id',
    },
    'observation': {
    },
    'treatment': {
    }
}

# EXECUTION

for output_dir in [ harmonized_cda_tsv_dir, substitution_log_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

# Load harmonization maps for all concept-harmonized CDA fields.
print( f"[{get_current_timestamp()}] Loading harmonization maps...", end='', file=sys.stderr )

harmonized_value = dict()

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


# Apply harmonization maps to all concept-harmonized CDA fields, and remove
# 'delete these everywhere' values (like 'Unknown') wherever we find them (including
# in non-harmonized data columns). Save "before and after" value maps for things
# that might be changed by these transformations, as guided by the rules laid out
# in the data structures in the PARAMETERS section at the top of this script.
print( f"[{get_current_timestamp()}] Harmonizing tables...", end='\n\n', file=sys.stderr )

# Track counts of all distinct A-to-B value substitutions.
all_subs_performed = dict()

# Scan the input directory for files.
for file_basename in sorted( listdir( cda_tsv_dir ) ):
    
    print( f"   [{get_current_timestamp()}] ...{file_basename}...", file=sys.stderr )

    gzipped = False

    # Is this a TSV (gzipped or otherwise)?
    if re.search( r'\.tsv', file_basename ) is not None:
        
        # Recover the table name from the filename.
        table = re.sub( r'^(.*)\.tsv.*$', r'\1', file_basename )

        # Assign locations for harmonized versions and "before and after" value map files.
        old_table_file = path.join( cda_tsv_dir, file_basename )
        new_table_file = path.join( harmonized_cda_tsv_dir, file_basename )
        new_unharmonized_table_file = ''
        if table in make_unharmonized_tsvs:
            new_unharmonized_table_file = path.join( harmonized_cda_tsv_dir, re.sub( r'\.tsv', r'_harmonization_value_map.tsv', file_basename ) )

        if debug:
            # Some extra information should anyone want to see it. (Flip the `debug` variable in the PARAMETERS section above to True to enable.)
            print( f"table: {table}", file=sys.stderr )
            print( f"old file: {old_table_file}", file=sys.stderr )
            print( f"new file: {new_table_file}", file=sys.stderr )
            if table in make_unharmonized_tsvs:
                print( f"new unharmonized file: {new_unharmonized_table_file}", file=sys.stderr )
            print( file=sys.stderr )
            for column in sorted( harmonized_value[table] ):
                print( f"   {column}", file=sys.stderr )
            print( file=sys.stderr )

        # Create and activate file handles for reading and writing.
        IN = open( old_table_file )
        OUT = open( new_table_file, 'w' )
        UNH = None
        if table in make_unharmonized_tsvs:
            UNH = open( new_unharmonized_table_file, 'w' )

        # Is this file gzipped? If so, its transformed version and "before and after" value map
        # will also be gzipped -- switch to the right library for reading and writing such files
        # using the same filehandles we just made.
        if re.search( r'\.gz$', old_table_file ) is not None:
            gzipped = True

            IN.close()
            OUT.close()
            if table in make_unharmonized_tsvs:
                UNH.close()

            IN = gzip.open( old_table_file, 'rt' )
            OUT = gzip.open( new_table_file, 'wt' )
            if table in make_unharmonized_tsvs:
                UNH = gzip.open( new_unharmonized_table_file, 'wt' )

        # Initialize the transformation counter for this table.
        all_subs_performed[table] = dict()

        # Pass all columns from the input TSV to its harmonized version.
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )
        print( *colnames, sep='\t', file=OUT )

        # If we're creating a "before and after" value map for this table,
        # only work on columns that actually get transformed -- in other words,
        # don't bother mapping "before and after" data for columns whose
        # "before" and "after" values   will always be identical.
        unharmonized_tsv_colnames = list()
        if table in make_unharmonized_tsvs:
            for colname in colnames:
                if colname not in fields_to_ignore_for_unharmonized_tsvs[table]:
                    if colname in { 'id', 'file_id', 'subject_id' }:
                        unharmonized_tsv_colnames.append( colname )
                    else:
                        unharmonized_tsv_colnames.append( f"{colname}_unharmonized" )
                        unharmonized_tsv_colnames.append( f"{colname}_harmonized" )
            print( *unharmonized_tsv_colnames, sep='\t', file=UNH )

        # Now go through the input TSV record by record, doing our transformations
        # and writing results to our output file(s).
        for next_line in IN:
            line = next_line.rstrip( '\n' )
            input_row_dict = dict( zip( colnames, line.split( '\t' ) ) )

            output_row = list()
            unharmonized_output_row = list()

            # We might not end up forwarding this record to the output files if
            # it ends up with its values all removed by harmonization and/or QC.
            # Track that.
            print_row = True

            for column in colnames:
                
                old_value = input_row_dict[column]

                # At no point does it help us to use Python `None` values for
                # these transformation processes. Convert all such immediately to
                # empty strings.
                if old_value is None:
                    old_value = ''

                if table in harmonized_value and column in harmonized_value[table]:
                    
                    # This is a table column that has an associated CDA harmonization map.
                    new_value = ''

                    if old_value.strip().lower() in harmonized_value[table][column] and harmonized_value[table][column][old_value.strip().lower()] != 'null':
                        # We have a match in the harmonization map to the original value. Convert the value
                        # to its harmonized version unless that version is labeled 'null', in which case we
                        # need do nothing, because we already initialized `new_value` to an empty string above.
                        new_value = harmonized_value[table][column][old_value.strip().lower()]
                    elif old_value.strip().lower() not in harmonized_value[table][column]:
                        # Preserve original values we haven't seen or mapped yet.
                        new_value = old_value

                    # Check the resulting `new_value` data against global QC cleanup rules: `delete_everywhere` takes precedence.
                    if re.sub( r'\s', r'', new_value.strip().lower() ) in delete_everywhere:
                        new_value = ''

                    # Save whatever we've got now for output to the appropriate files.
                    output_row.append( new_value )
                    if table in make_unharmonized_tsvs and column not in fields_to_ignore_for_unharmonized_tsvs[table]:
                        unharmonized_output_row.append( old_value )
                        unharmonized_output_row.append( new_value )

                    # Count distinct A-to-B substitutions for this table.
                    if column not in all_subs_performed[table]:
                        all_subs_performed[table][column] = dict()
                    if old_value not in all_subs_performed[table][column]:
                        all_subs_performed[table][column][old_value] = { new_value : 1 }
                    elif new_value not in all_subs_performed[table][column][old_value]:
                        all_subs_performed[table][column][old_value][new_value] = 1
                    else:
                        all_subs_performed[table][column][old_value][new_value] = all_subs_performed[table][column][old_value][new_value] + 1

                    # Did we delete the only value we care about? If so, don't print the (degenerate) results to the output files.
                    if table in delete_row_if_nulled and new_value == '':
                        print_row = False

                elif table not in exclude_tables:
                    # This isn't a table column that has an associated CDA harmonization map, but it might need cleanup QC.
                    if re.sub( r'\s', r'', old_value.strip().lower() ) in delete_everywhere:
                        # The original value needs deleting.
                        new_value = ''
                        output_row.append( new_value )
                        if table in make_unharmonized_tsvs and column not in fields_to_ignore_for_unharmonized_tsvs[table]:
                            if column in { 'id', 'file_id', 'subject_id' }:
                                unharmonized_output_row.append( old_value )
                            else:
                                unharmonized_output_row.append( old_value )
                                unharmonized_output_row.append( new_value )

                        if column not in all_subs_performed[table]:
                            all_subs_performed[table][column] = dict()
                        if old_value not in all_subs_performed[table][column]:
                            all_subs_performed[table][column][old_value] = { new_value : 1 }
                        elif new_value not in all_subs_performed[table][column][old_value]:
                            all_subs_performed[table][column][old_value][new_value] = 1
                        else:
                            all_subs_performed[table][column][old_value][new_value] = all_subs_performed[table][column][old_value][new_value] + 1

                        # Did we delete the only value we care about? If so, don't print the (degenerate) results to the output files.
                        if table in delete_row_if_nulled:
                            print_row = False

                    else:
                        # This value was not deleted due to a delete-everywhere rule, and it's not a harmonized column. Keep the old value.
                        output_row.append( old_value )
                        if table in make_unharmonized_tsvs and column not in fields_to_ignore_for_unharmonized_tsvs[table]:
                            if column in { 'id', 'file_id', 'subject_id' }:
                                unharmonized_output_row.append( old_value )
                            else:
                                unharmonized_output_row.append( old_value )
                                unharmonized_output_row.append( old_value )

                else:
                    # We're not analyzing values in this table for harmonization at all. Preserve all old values.
                    output_row.append( old_value )
                    if table in make_unharmonized_tsvs and column not in fields_to_ignore_for_unharmonized_tsvs[table]:
                        if column in { 'id', 'file_id', 'subject_id' }:
                            unharmonized_output_row.append( old_value )
                        else:
                            unharmonized_output_row.append( old_value )
                            unharmonized_output_row.append( old_value )

            # Check transformed rows to see if all their data was removed, and if it was, don't forward them to the output files.
            if table in delete_row_if_all_null_but:
                print_row = False
                row_dict = dict( zip( colnames, output_row ) )
                for colname in colnames:
                    if colname not in delete_row_if_all_null_but[table] and row_dict[colname] is not None and row_dict[colname] != '':
                        print_row = True

            if print_row:
                print( *output_row, sep='\t', file=OUT )
                if table in make_unharmonized_tsvs:
                    print( *unharmonized_output_row, sep='\t', file=UNH )

        IN.close()
        OUT.close()
        if table in make_unharmonized_tsvs:
            UNH.close()

        # If this is a table for which we might generate identical rows, remove duplicates.
        if table in remove_possible_dupes_including_ids:
            print( f"   [{get_current_timestamp()}]    ...deduplicating {new_table_file}...", file=sys.stderr )
            deduplicate_and_sort_unsorted_file_with_header( new_table_file, gzipped, ignore_primary_id_field=False )
            if table in make_unharmonized_tsvs:
                deduplicate_and_sort_unsorted_file_with_header( new_unharmonized_table_file, gzipped, ignore_primary_id_field=False )

        # If this is a table for which we might generate rows identical except for their row IDs, remove duplicates.
        elif table in remove_possible_dupes_ignoring_ids:
            print( f"   [{get_current_timestamp()}]    ...deduplicating {new_table_file}...", file=sys.stderr )
            deduplicate_and_sort_unsorted_file_with_header( new_table_file, gzipped, ignore_primary_id_field=True )
            if table in make_unharmonized_tsvs:
                deduplicate_and_sort_unsorted_file_with_header( new_unharmonized_table_file, gzipped, ignore_primary_id_field=True )

print( f"\n[{get_current_timestamp()}] ...done.", file=sys.stderr )

# Dump substitution count logs.
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


