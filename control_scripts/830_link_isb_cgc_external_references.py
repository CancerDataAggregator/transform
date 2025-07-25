#!/usr/bin/env python3 -u

import json

import re
import sys

from os import listdir, makedirs, path

from cda_etl.lib import deduplicate_and_sort_unsorted_file_with_header, load_tsv_as_dict, map_columns_one_to_one
from google.cloud import bigquery

# ARGUMENT

if len( sys.argv ) != 2:
    
    sys.exit( f"\n   Usage: {sys.argv[0]} <TSV describing derived-data BQ tables from ISB-CGC>\n" )

input_list_file = sys.argv[1]

# PARAMETERS

cda_tsv_dir = path.join( 'cda_tsvs', 'last_merge' )

if not path.exists( cda_tsv_dir ):
    sys.exit( f"\n   FATAL: Required CDA TSV build directory '{cda_tsv_dir}' does not exist. Cannot continue.\n" )

external_reference_tsv = path.join( cda_tsv_dir, 'external_reference.tsv' )
project_tsv = path.join( cda_tsv_dir, 'project.tsv' )
subject_external_reference_tsv = path.join( cda_tsv_dir, 'subject_external_reference.tsv' )
subject_in_project_tsv = path.join( cda_tsv_dir, 'subject_in_project.tsv' )
upstream_identifiers_tsv = path.join( cda_tsv_dir, 'upstream_identifiers.tsv' )

source_short_name = 'ISB-CGC'
source_url = 'https://www.isb-cgc.org'
external_reference_type = 'BigQuery derived-data table'

aux_root = path.join( 'auxiliary_metadata', '__external_references', source_short_name )

input_cache_dir = path.join( aux_root, '000_input_tsv_from_isb_cgc' )
submitter_id_output_dir = path.join( aux_root, '002_submitter_IDs_of_represented_subjects_by_table' )
cda_alias_output_dir = path.join( aux_root, '003_CDA_aliases_of_represented_subjects_by_table' )

input_cache_file = path.join( input_cache_dir, path.basename( input_list_file ) )
bq_metadata_file = path.join( aux_root, '001_current_bq_table_metadata.tsv' )

# The following loader function is only used to look up PDC studies: it is
# not safe to use in this way for other purposes, because there are multiple
# project records with the same short_name when the area of inquiry is not
# restricted only to PDC studies.

project_short_name_to_alias = map_columns_one_to_one( project_tsv, 'short_name', 'id_alias' )

# ...a counterexample, done safely:

gdc_program_short_name_to_alias = dict()

with open( project_tsv ) as IN:
    column_names = next( IN ).rstrip( '\n' ).split( '\t' )
    for next_line in IN:
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
        if record['type'] == 'program' and record['data_at_gdc'] == 'True':
            program_short_name = record['short_name']
            if program_short_name in gdc_program_short_name_to_alias:
                sys.exit( f"AAAAARGH! {program_short_name}" )
            else:
                gdc_program_short_name_to_alias[program_short_name] = record['id_alias']

# These don't match exactly. We help.

gdc_program_code_to_cda_short_name = {
    'BEATAML1_0' : 'BEATAML1.0',
    'EXC_RESPONDERS' : 'EXCEPTIONAL_RESPONDERS'
}

external_reference_columns = [
    'id_alias',
    'type',
    'name',
    'short_name',
    'last_updated',
    'uri',
    'description',
    'source_short_name',
    'source_url'
]

subject_external_reference_columns = [
    'subject_alias',
    'external_reference_alias'
]

# EXECUTION

for output_dir in [ input_cache_dir, submitter_id_output_dir, cda_alias_output_dir ]:
    if not path.exists( output_dir ):
        makedirs( output_dir )

tables_to_scan = set()
table_link = dict()

input_lines = list()

with open( input_list_file ) as IN:
    for next_line in IN:
        input_lines.append( next_line.rstrip( '\n' ) )
        ( isb_program, bq_table_name, desc, bq_table_link ) = next_line.rstrip( '\n' ).split( '\t' )
        tables_to_scan.add( bq_table_name )
        table_link[bq_table_name] = bq_table_link

# Cache the input data in a standard location.
with open( input_cache_file, 'w' ) as OUT:
    for line in input_lines:
        print( line, file=OUT )

# Construct a BigQuery client object.
client = bigquery.Client()

print( 'Caching BQ table metadata...', file=sys.stderr )

# Save table metadata direct from BigQuery.
with open( bq_metadata_file, 'w' ) as OUT:
    print( *[ 'table_id', 'friendly_name', 'last_modified', 'bigquery_web_url', 'description', 'pdc_study_id' ], sep='\t', file=OUT )
    for table_id in sorted( tables_to_scan ):
        table = client.get_table( table_id )  # Make an API request.
        pdc_study_id = ''
        pdc_study_id_match = re.search( r'(PDC[0-9]+)', table.description )
        if pdc_study_id_match is not None:
            pdc_study_id = pdc_study_id_match.group(1)
        print( *[ table_id, json.dumps(table.friendly_name).strip('"'), table.modified, table_link[table_id], json.dumps(table.description).strip('"'), pdc_study_id ], sep='\t', file=OUT )
        print( f"   ...{table_id}...", file=sys.stderr )

print( '...done.', file=sys.stderr )

table_metadata = load_tsv_as_dict( bq_metadata_file )

# We don't know how to connect records in these tables to CDA subjects.
tables_to_ignore = {
    'isb-cgc-bq.CCLE.copy_number_segment_hg19_current',
    'isb-cgc-bq.DEPMAP.CCLE_gene_expression_DepMapPublic_current'
}

print( 'Getting subject submitter IDs by table...', file=sys.stderr )

for table_id in sorted( table_metadata ):
    if table_id not in tables_to_ignore:
        if table_metadata[table_id]['pdc_study_id'] != '':
            print( f"   ...PDC derived data {table_id} ({table_metadata[table_id]['friendly_name']})...", end='', file=sys.stderr )
            sql_query = f"SELECT DISTINCT( case_id ) FROM {table_id} ORDER BY case_id"
            rows = client.query_and_wait( sql_query )
            output_file = path.join( submitter_id_output_dir, f"{table_id}.pdc_case_ids.txt" )
            with open( output_file, 'w' ) as OUT:
                print( 'case_id', file=OUT )
                for row in rows:
                    # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.Row
                    print( row.get( 'case_id' ), file=OUT )
            print( 'done.', file=sys.stderr )
        else:
            print( f"   ...GDC derived data {table_id} ({table_metadata[table_id]['friendly_name']})...", end='', file=sys.stderr )
            sql_query = f"SELECT DISTINCT( case_barcode ) FROM {table_id} ORDER BY case_barcode"
            rows = client.query_and_wait( sql_query )
            output_file = path.join( submitter_id_output_dir, f"{table_id}.gdc_case_submitter_ids.txt" )
            with open( output_file, 'w' ) as OUT:
                print( 'submitter_id', file=OUT )
                for row in rows:
                    # https://cloud.google.com/python/docs/reference/bigquery/latest/google.cloud.bigquery.table.Row
                    print( row.get( 'case_barcode' ), file=OUT )
            print( 'done.', file=sys.stderr )

print( '...done.', file=sys.stderr )

# Convert subject submitter IDs & PDC case IDs to CDA subject aliases.
print( 'Converting submitter and case IDs to CDA subject aliases...', file=sys.stderr )

subject_alias_to_table_id = dict()
table_ids_to_include = set()

for subject_list_basename in sorted( listdir( submitter_id_output_dir ) ):
    pdc_study_match_result = re.search( r'^(.+)\.pdc_case_ids\.txt$', subject_list_basename )
    if pdc_study_match_result is not None:
        table_id = pdc_study_match_result.group(1)
        pdc_study_id = table_metadata[table_id]['pdc_study_id']
        print( f"   ...({pdc_study_id}) {table_id}...", end='', file=sys.stderr )
        # Load PDC case IDs from this study with data represented in this table.
        target_case_ids = set()
        with open( path.join( submitter_id_output_dir, subject_list_basename ) ) as IN:
            header = next( IN )
            for next_line in IN:
                target_case_ids.add( next_line.rstrip( '\n' ) )
        print( f"loaded {len( target_case_ids )} target case IDs...", end='', file=sys.stderr )
        # Load candidate subject CDA aliases present in this PDC study.
        project_alias = project_short_name_to_alias[pdc_study_id]
        potential_subject_aliases = set( map_columns_one_to_one( subject_in_project_tsv, 'subject_alias', 'project_alias', where_field='project_alias', where_value=project_alias ).keys() )
        # Identify matching subjects.
        matched_target_case_ids = set()
        matching_subject_aliases = set()
        with open( upstream_identifiers_tsv ) as IN:
            header = next( IN )
            column_names = header.rstrip( '\n' ).split( '\t' )
            for next_line in IN:
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                if record['cda_table'] == 'subject' \
                    and record['id_alias'] in potential_subject_aliases \
                    and record['upstream_source'] == 'PDC' \
                    and record['upstream_field'] == 'Case.case_id' \
                    and record['upstream_id'] in target_case_ids:
                        matched_target_case_ids.add( record['upstream_id'] )
                        matching_subject_aliases.add( record['id_alias'] )
        print( f"matched {len(matched_target_case_ids)} of those", end='', file=sys.stderr )
        # Report any unmatched target IDs.
        missing_case_ids = set()
        for target_case_id in sorted( target_case_ids ):
            if target_case_id not in matched_target_case_ids:
                missing_case_ids.add( target_case_id )
        if len( missing_case_ids ) == 0:
            print( '...', end='', file=sys.stderr )
        else:
            print( f" (missing:{','.join( sorted( missing_case_ids ) )})...", end='', file=sys.stderr )
        # Save results.
        output_file = path.join( cda_alias_output_dir, re.sub( r'pdc_case_ids\.txt$', r'cda_subject_aliases.txt', subject_list_basename ) )
        with open( output_file, 'w' ) as OUT:
            print( 'subject.id_alias', file=OUT )
            for subject_alias in sorted( matching_subject_aliases ):
                print( subject_alias, file=OUT )
                if subject_alias not in subject_alias_to_table_id:
                    subject_alias_to_table_id[subject_alias] = set()
                subject_alias_to_table_id[subject_alias].add( table_id )
                table_ids_to_include.add( table_id )
        print( 'done.', file=sys.stderr )
    else:
        gdc_table_match_result = re.search( r'^(isb-cgc-bq\.)([^\.]+)(\..*)\.gdc_case_submitter_ids\.txt$', subject_list_basename )
        if gdc_table_match_result is not None:
            table_id = f"{gdc_table_match_result.group(1)}{gdc_table_match_result.group(2)}{gdc_table_match_result.group(3)}"
            gdc_program_name = gdc_table_match_result.group(2)
            if gdc_program_name in gdc_program_code_to_cda_short_name:
                gdc_program_name = gdc_program_code_to_cda_short_name[gdc_program_name]
            print( f"   ...({gdc_program_name}) {table_id}...", end='', file=sys.stderr )
            # Load GDC submitter_id values from this program with data represented in this table.
            target_submitter_ids = set()
            with open( path.join( submitter_id_output_dir, subject_list_basename ) ) as IN:
                header = next( IN )
                for next_line in IN:
                    target_submitter_ids.add( next_line.rstrip( '\n' ) )
            print( f"loaded {len( target_submitter_ids )} target submitter IDs...", end='', file=sys.stderr )
            program_alias = ''
            if gdc_program_name in gdc_program_short_name_to_alias:
                # Load candidate subject CDA aliases present in this GDC program.
                program_alias = gdc_program_short_name_to_alias[gdc_program_name]
                potential_subject_aliases = set( map_columns_one_to_one( subject_in_project_tsv, 'subject_alias', 'project_alias', where_field='project_alias', where_value=program_alias ).keys() )
                # Identify matching subjects. This time, we'll have to guard against possible multiple matches.
                matching_subject_aliases = dict()
                with open( upstream_identifiers_tsv ) as IN:
                    header = next( IN )
                    column_names = header.rstrip( '\n' ).split( '\t' )
                    for next_line in IN:
                        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
                        if record['cda_table'] == 'subject' \
                            and record['id_alias'] in potential_subject_aliases \
                            and record['upstream_source'] == 'GDC' \
                            and record['upstream_field'] == 'case.submitter_id' \
                            and record['upstream_id'] in target_submitter_ids:
                                submitter_id = record['upstream_id']
                                if submitter_id not in matching_subject_aliases:
                                    matching_subject_aliases[submitter_id] = set()
                                matching_subject_aliases[submitter_id].add( record['id_alias'] )
                print( f"matched {len(matching_subject_aliases)} of those", end='', file=sys.stderr )
                # Report any unmatched target IDs.
                missing_submitter_ids = set()
                for target_submitter_id in sorted( target_submitter_ids ):
                    if target_submitter_id not in matching_subject_aliases:
                        missing_submitter_ids.add( target_submitter_id )
                if len( missing_submitter_ids ) == 0:
                    print( '...', end='', file=sys.stderr )
                else:
                    print( f" (missing:{','.join(sorted(missing_submitter_ids))})...", end='', file=sys.stderr )
                # Validate results: make sure each ID resolves to exactly one subject.
                validated_subject_aliases = set()
                for submitter_id in matching_subject_aliases:
                    if len( matching_subject_aliases[submitter_id] ) != 1:
                        print( f"SKIPPING: submitter_id {submitter_id} matches multiple CDA subjects in program {gdc_program_name}; cannot process this ID.", file=sys.stderr )
                    else:
                        validated_subject_aliases.add( sorted( matching_subject_aliases[submitter_id] )[0] )
                # Save results.
                output_file = path.join( cda_alias_output_dir, re.sub( r'gdc_case_submitter_ids\.txt$', r'cda_subject_aliases.txt', subject_list_basename ) )
                with open( output_file, 'w' ) as OUT:
                    print( 'subject.id_alias', file=OUT )
                    for subject_alias in sorted( validated_subject_aliases ):
                        print( subject_alias, file=OUT )
                        if subject_alias not in subject_alias_to_table_id:
                            subject_alias_to_table_id[subject_alias] = set()
                        subject_alias_to_table_id[subject_alias].add( table_id )
                        table_ids_to_include.add( table_id )
            else:
                print( f"SKIPPING: Can't find GDC program '{gdc_program_name}'.", file=sys.stderr )
            print( 'done.', file=sys.stderr )
        else:
            print( f"SKIPPING: Case/subject list '{subject_list_basename}' is of an unrecognized type: don't know how to process it.", file=sys.stderr )

print( '...done.', file=sys.stderr )

# Update CDA external_reference data.
print( 'Updating CDA external_reference data...', end='', file=sys.stderr )

external_reference_alias = 0
table_id_to_external_reference_alias = dict()

# Write external_reference.tsv if none exists. If it does exist, merge our new data
# into whatever's there.

if not path.exists( external_reference_tsv ):
    with open( external_reference_tsv, 'w' ) as OUT:
        print( *external_reference_columns, sep='\t', file=OUT )
        for table_id in sorted( table_ids_to_include ):
            last_updated = ''
            if table_metadata[table_id]['last_modified'] is not None:
                last_updated = re.sub( r'^\s*(\d\d\d\d-\d\d-\d\d)\s.*$', r'\1', table_metadata[table_id]['last_modified'] )
            print( *[ external_reference_alias, external_reference_type, table_metadata[table_id]['friendly_name'], table_id, last_updated, table_metadata[table_id]['bigquery_web_url'], table_metadata[table_id]['description'], source_short_name, source_url ], sep='\t', file=OUT )
            table_id_to_external_reference_alias[table_id] = external_reference_alias
            external_reference_alias = external_reference_alias + 1
else:
    existing_lines = list()
    with open( external_reference_tsv ) as IN:
        column_names = next( IN ).rstrip( '\n' ).split( '\t' )
        for next_line in IN:
            record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
            current_alias = int( record['id_alias'] )
            if current_alias >= external_reference_alias:
                external_reference_alias = current_alias + 1
            existing_lines.append( next_line.rstrip( '\n' ) )
    with open( external_reference_tsv, 'w' ) as OUT:
        print( *external_reference_columns, sep='\t', file=OUT )
        for existing_line in existing_lines:
            print( existing_line, file=OUT )
        for table_id in sorted( table_ids_to_include ):
            last_updated = ''
            if table_metadata[table_id]['last_modified'] is not None:
                last_updated = re.sub( r'^\s*(\d\d\d\d-\d\d-\d\d)\s.*$', r'\1', table_metadata[table_id]['last_modified'] )
            print( *[ external_reference_alias, external_reference_type, table_metadata[table_id]['friendly_name'], table_id, last_updated, table_metadata[table_id]['bigquery_web_url'], table_metadata[table_id]['description'], source_short_name, source_url ], sep='\t', file=OUT )
            external_reference_alias = external_reference_alias + 1
    # Remove dupes. Also makes this script robust across multiple successive testing runs: only one copy of each record should survive.
    deduplicate_and_sort_unsorted_file_with_header( external_reference_tsv, gzipped=False, ignore_primary_id_field=True )
    # Load (possibly updated) table_id_to_external_reference_alias values.
    external_reference = load_tsv_as_dict( external_reference_tsv )
    for id_alias in external_reference:
        table_id = external_reference[id_alias]['short_name']
        table_id_to_external_reference_alias[table_id] = id_alias

# Write subject_external_reference.tsv: Link CDA subjects to external_reference records.

if not path.exists( subject_external_reference_tsv ):
    with open( subject_external_reference_tsv, 'w' ) as OUT:
        print( *subject_external_reference_columns, sep='\t', file=OUT )
        for subject_alias in sorted( subject_alias_to_table_id ):
            for table_id in sorted( subject_alias_to_table_id[subject_alias] ):
                table_alias = table_id_to_external_reference_alias[table_id]
                print( *[ subject_alias, table_alias ], sep='\t', file=OUT )
else:
    # In the possibly unlikely event of anyone wondering why we can manage the merging and
    # deduplication of record lines in the following (simpler) way for this TSV but cannot
    # do the same for the previous one, it's because the previous one has a primary
    # key (id_alias) that will (misleadingly) distinguish between multiple copies of the
    # "same" record and so has to be handled directly (via deduplicate_and_sort_unsorted_file_with_header()).
    # Since this TSV doesn't have any such field, we can just maintain a unique bag of all
    # lines, old and new, and then dump them all out when we're done.
    all_output_lines = set()
    with open( subject_external_reference_tsv ) as IN:
        column_names = next( IN ).rstrip( '\n' ).split( '\t' )
        for next_line in IN:
            all_output_lines.add( next_line.rstrip( '\n' ) )
    for subject_alias in sorted( subject_alias_to_table_id ):
        for table_id in sorted( subject_alias_to_table_id[subject_alias] ):
            table_alias = table_id_to_external_reference_alias[table_id]
            all_output_lines.add( f"{subject_alias}\t{table_alias}" )
    with open( subject_external_reference_tsv, 'w' ) as OUT:
        print( *subject_external_reference_columns, sep='\t', file=OUT )
        for output_line in sorted( all_output_lines ):
            print( output_line, file=OUT )

print( 'done.', file=sys.stderr )


