import gzip
import jsonlines
import re
import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, sort_file_with_header

class mutation_transformer:
    
    def __init__(
        self,
        columns_to_keep = [],
        source_datasets = [], # e.g. TCGA, TARGET, HCMI
        source_version = None # ISB-CGC-assigned version suffix on mutation table names, e.g. source_version == 'hg38_gdc_current' --> TCGA mutations table address inside BQ isb-cgc-bq project space == 'TCGA.masked_somatic_mutation_hg38_gdc_current'
    ):

        self.columns_to_keep = columns_to_keep
        self.source_datasets = source_datasets
        self.source_version = source_version

        self.cda_data_sources = [
            'CDS',
            'GDC',
            'ICDC',
            'IDC',
            'PDC'
        ]

        self.gdc_entity_map = path.join( 'auxiliary_metadata', '__GDC_supplemental_metadata', 'GDC_entity_submitter_id_to_program_name_and_project_id.tsv' )

        self.input_dir = path.join( 'extracted_data', 'mutation', source_version )
        self.merged_cda_dir = path.join( 'cda_tsvs', 'merged_icdc_cds_idc_gdc_and_pdc_tables' )

        self.cda_table_inputs = {
            
            'mutation' : path.join( self.merged_cda_dir, 'mutation.tsv.gz' ),
            'subject_integer_aliases' : path.join( self.merged_cda_dir, 'subject_integer_aliases.tsv.gz' ),
            'subject_mutation' : path.join( self.merged_cda_dir, 'subject_mutation.tsv.gz' )
        }

        self.sql_root = 'SQL_data'
        self.sql_table_output_dir = path.join( self.sql_root, 'new_table_data' )

        self.sql_outputs = {
            
            'mutation' : path.join( self.sql_table_output_dir, 'mutation.sql.gz' ),
            'subject_mutation' : path.join( self.sql_table_output_dir, 'subject_mutation.sql.gz' )
        }

        # Spit out a logger message every `self.display_increment` lines while transcoding.

        self.display_increment = 500000

        for target_dir in [ self.merged_cda_dir, self.sql_table_output_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

    def make_mutation_and_subject_mutation_TSVs( self ):
        
        cda_subject_id_to_integer_alias = dict()

        with gzip.open( self.cda_table_inputs['subject_integer_aliases'], 'rt' ) as IN:
            
            for line in IN:
                
                [ subject_id, subject_alias ] = line.rstrip( '\n' ).split( '\t' )

                cda_subject_id_to_integer_alias[subject_id] = subject_alias

        gdc_project_and_case_submitter_id_to_cda_subject_id = dict()

        gdc_project_to_containing_program = dict()

        with open( self.gdc_entity_map ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            column_names = header.split( '\t' )

            for next_line in [ line.rstrip( '\n' ) for line in IN ]:
                
                record = dict( zip( column_names, next_line.split( '\t' ) ) )

                if record['entity_type'] == 'case':
                    
                    if record['GDC_project_id'] not in gdc_project_and_case_submitter_id_to_cda_subject_id:
                        
                        gdc_project_and_case_submitter_id_to_cda_subject_id[record['GDC_project_id']] = dict()

                    gdc_project_and_case_submitter_id_to_cda_subject_id[record['GDC_project_id']][record['entity_submitter_id']] = f"{record['GDC_program_name']}.{record['entity_submitter_id']}"

                    gdc_project_to_containing_program[record['GDC_project_id']] = record['GDC_program_name']

        with gzip.open( self.cda_table_inputs['mutation'], 'wt' ) as OUT, gzip.open( self.cda_table_inputs['subject_mutation'], 'wt' ) as SUBJECT_ASSOC:
            
            mutation_output_columns = [ 'id', 'integer_id_alias' ] + self.columns_to_keep

            print( *mutation_output_columns, sep='\t', file=OUT )

            subject_assoc_output_columns = [ 'subject_alias', 'mutation_alias' ]

            print( *subject_assoc_output_columns, sep='\t', file=SUBJECT_ASSOC )

            current_mutation_alias = 0

            for source_dataset in self.source_datasets:
                
                mutation_jsonl = path.join( self.input_dir, f"{source_dataset}.masked_somatic_mutation_{self.source_version}.jsonl.gz" )

                line_count = 0

                skipped_null = 0

                skipped_unfound = 0

                print( f"Transcoding mutation JSONL for {source_dataset} into CDA-formatted mutation.tsv and subject_mutation.tsv and linking CDA records...", end='', file=sys.stderr )

                with gzip.open( mutation_jsonl, 'rt' ) as IN:
                    
                    reader = jsonlines.Reader( IN )

                    for old_record in reader:
                        
                        record = dict()

                        for field_name in old_record:
                            
                            record[field_name.lower()] = old_record[field_name]

                        case_barcode = ''

                        project_short_name = ''

                        if 'case_barcode' in record and record['case_barcode'] is not None and record['case_barcode'] != '':
                            
                            case_barcode = record['case_barcode']

                        if 'project_short_name' in record and record['project_short_name'] is not None and record['project_short_name'] != '':
                            
                            project_short_name = record['project_short_name']

                        if case_barcode != '' and project_short_name != '':
                            
                            if project_short_name in gdc_project_and_case_submitter_id_to_cda_subject_id and case_barcode in gdc_project_and_case_submitter_id_to_cda_subject_id[project_short_name]:
                                
                                cda_subject_id = gdc_project_and_case_submitter_id_to_cda_subject_id[project_short_name][case_barcode]

                                cda_subject_alias = cda_subject_id_to_integer_alias[cda_subject_id]

                                print( *[ cda_subject_alias, current_mutation_alias ], sep='\t', file=SUBJECT_ASSOC )

                                cda_mutation_id = f"{cda_subject_id}.mutation_{current_mutation_alias}"

                                kept_columns = list()

                                for column_name in self.columns_to_keep:
                                    
                                    if column_name in record:
                                        
                                        value = record[column_name]

                                        # Nitpick about capitalization for boolean literals in postgresql INSERTs (while refusing to capitalize "Boolean")

                                        if str(value) == 'False':
                                            
                                            value = 'false'

                                        elif str(value) == 'True':
                                            
                                            value = 'true'

                                        kept_columns.append( value )

                                    else:
                                        
                                        kept_columns.append( '' )

                                result_row = [ cda_mutation_id, current_mutation_alias ] + kept_columns

                                print( *result_row, sep='\t', file=OUT )

                            else:
                                
                                skipped_unfound = skipped_unfound + 1

                        else:
                            
                            skipped_null = skipped_null + 1

                        current_mutation_alias = current_mutation_alias + 1

                        line_count = line_count + 1

                        if line_count == self.display_increment:
                            
                            print( file=sys.stderr )

                        if line_count % self.display_increment == 0:
                            
                            print( f"   ...processed {line_count} lines...", file=sys.stderr )

                print( f"done. processed {line_count} lines; skipped {skipped_null} due to missing records and {skipped_unfound} due to unmatched case barcodes.", end='\n\n', file=sys.stderr )

    def transform_mutation_and_subject_mutation_to_SQL( self ):
        
        print( 'Transcoding CDA mutation and subject_mutation TSVs to SQL...', file=sys.stderr )

        for target_table in self.cda_table_inputs.keys():
            
            # Transcode TSV rows into the body of a prepared SQL COPY statement, to populate `target_table` in postgres.

            print( f"   ...{self.cda_table_inputs[target_table]} -> {self.sql_outputs[target_table]}...", file=sys.stderr )

            with gzip.open( self.cda_table_inputs[target_table], 'rt' ) as IN, gzip.open( self.sql_outputs[target_table], 'wt' ) as OUT:
                
                ##########################################
                # Add all the new rows using a COPY block.

                colnames = next( IN ).rstrip( '\n' ).split( '\t' )

                print( f"COPY {target_table} ( {', '.join( colnames )} ) FROM stdin;", file=OUT )

                line_count = 0

                for next_line in IN:
                    
                    record = dict( zip( colnames, [ value for value in next_line.rstrip( '\n' ).split( '\t' ) ] ) )

                    print( '\t'.join( [ r'\N' if len( record[colname] ) == 0 else record[colname] for colname in colnames ] ), file=OUT )

                    line_count = line_count + 1

                    if line_count % self.display_increment == 0:
                        
                        print( f"      ...processed {line_count} lines...", file=sys.stderr )

                print( r'\.', end='\n\n', file=OUT )

                print( f"   ...done with {target_table}: processed {line_count} data rows total.", end='\n\n', file=sys.stderr )


