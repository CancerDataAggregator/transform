#!/usr/bin/env python3 -u

import gzip
import json
import re
import sys

from datetime import date
from google.cloud import bigquery
from os import path, makedirs
from cda_etl.lib import get_current_timestamp, get_idc_extraction_fields

def remove_nested_nulls( source_dict ):
    result_dict = dict()

    for key in source_dict:
        value = source_dict[key]
        if value is not None:
            if isinstance( value, list ):
                result_list = list()
                for element in value:
                    if element is not None:
                        if isinstance( element, dict ):
                            result_list.append( remove_nested_nulls( element ) )
                        elif isinstance( element, list):
                            # We don't handle lists of lists. At time of writing (2025-08-12), this is safe.
                            sys.exit( f"FATAL: List element is of type 'list', assumptions broken; please fix." )
                        else:
                            result_list.append( element )
                result_dict[key] = result_list
            elif isinstance( value, dict ):
                result_dict[key] = remove_nested_nulls( value )
            else:
                result_dict[key] = value

    return result_dict

project_id = 'cda-bigquery-pipeline'

extraction_root = path.join( 'extracted_data', 'idc' )
output_root = path.join( extraction_root, '__raw_BigQuery_JSONL' )

idc_version_file = path.join( extraction_root, 'data_version.txt' )
idc_version_string = ''
with open( idc_version_file, 'r' ) as IN:
    idc_version_string = next(IN).rstrip( '\n' )

target_table = 'tcga_biospecimen_rel9'
target_table_path = f"bigquery-public-data.idc_{idc_version_string}.{target_table}"

output_file = path.join( output_root, f"{target_table}.jsonl.gz" )

target_field_lists = get_idc_extraction_fields()
fields_to_pull = target_field_lists[target_table]
field_list_string = ', '.join( fields_to_pull )

# This is an ugly hack to remove STRUCT fields from the ORDER BY clause when querying `target_table`.
order_by_fields = list()
for field in fields_to_pull:
    if re.search( r'Sequence$', field ) is None:
        order_by_fields.append( field )
order_by_field_sequence = ', '.join( order_by_fields )

selection_query = f"SELECT DISTINCT {field_list_string} FROM {target_table_path} ORDER BY {order_by_field_sequence}"
print( f"Querying\n\n    {selection_query}\n\ninto\n\n    {output_file} ...", end='\n\n', file=sys.stderr )

# google.cloud.bigquery.job.query.QueryJobConfig
job_config = bigquery.QueryJobConfig( allow_large_results=True ) # , use_query_cache=True )
# google.cloud.bigquery.client.Client
bq_client = bigquery.Client( project_id )
# google.cloud.bigquery.job.query.QueryJob
selection_result = bq_client.query( selection_query, job_config=job_config )
# google.cloud.bigquery.table.RowIterator or google.cloud.bigquery.table._EmptyRowIterator
result_rows = selection_result.result()

row_count = 0

with gzip.open( output_file, 'wt' ) as OUT:
    for row in result_rows:
        row_count = row_count + 1
        if row_count % 10000 == 0:
            if selection_result.done():
                print( f"[{get_current_timestamp()}] Processed {row_count} rows", file=sys.stderr )
        row_dict = { key : value for key, value in row.items() if value is not None }
        for key in row_dict:
            if isinstance( row_dict[key], date ):
                row_dict[key] = str( row_dict[key] )
        row_dict = remove_nested_nulls( row_dict )
        json.dump( row_dict, OUT )
        OUT.write( '\n' )


