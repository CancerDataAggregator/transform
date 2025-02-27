import gzip
import jsonlines
import re
import sys

from os import makedirs, path, rename

from cda_etl.lib import deduplicate_and_sort_unsorted_file_with_header, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_one, sort_file_with_header

class mutation_transformer:
    
    def __init__(
        self,
        columns_to_keep = [],
        source_datasets = [], # e.g. TCGA, TARGET, HCMI
        source_version = None, # ISB-CGC-assigned version suffix on mutation table names, e.g. source_version == 'hg38_gdc_current' --> TCGA mutations table address inside BQ isb-cgc-bq project space == 'TCGA.masked_somatic_mutation_hg38_gdc_current'
        substitution_log_dir = None
    ):

        self.columns_to_keep = columns_to_keep
        self.source_datasets = source_datasets
        self.source_version = source_version

        self.substitution_log_dir = substitution_log_dir

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

        # Load any relevant value-harmonization maps.

        harmonization_map_dir = path.join( '.', 'harmonization_maps' )

        harmonization_map_index = path.join( harmonization_map_dir, '000_cda_column_targets.tsv' )

        self.delete_everywhere = get_universal_value_deletion_patterns()

        ### EXAMINE OUTPUTS: remove_possible_unsorted_dupes = True

        # Load harmonization value maps by column.

        self.harmonized_value = dict()

        with open( harmonization_map_index )  as IN:
            
            colnames = next( IN ).rstrip( '\n' ).split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                row_dict = dict( zip( colnames, line.split( '\t' ) ) )

                table = row_dict['cda_table']

                if table == 'mutation':
                    
                    column = row_dict['cda_column']

                    concept_map_name = row_dict['concept_map_name']

                    map_file = path.join( harmonization_map_dir, f"{concept_map_name}.tsv" )

                    if column not in self.harmonized_value:
                        
                        self.harmonized_value[column] = dict()

                    with open( map_file ) as MAP:
                        
                        header_line = next( MAP ).rstrip( '\n' )

                        if concept_map_name == 'anatomic_site':
                            
                            for ( old_value, uberon_id, uberon_name ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                                
                                if uberon_name != '__CDA_UNASSIGNED__':
                                    
                                    self.harmonized_value[column][old_value] = uberon_name

                        elif concept_map_name == 'disease':
                            
                            for ( old_value, icd_code, icd_name, do_id, do_name, ncit_codes ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                                
                                if icd_name != '__CDA_UNASSIGNED__':
                                    
                                    self.harmonized_value[column][old_value] = icd_name

                        elif concept_map_name == 'species':
                            
                            for ( old_value, ncbi_tax_id, ncbi_tax_sci_name, cda_common_name ) in [ next_term_tuple.rstrip( '\n' ).split( '\t' ) for next_term_tuple in MAP ]:
                                
                                if cda_common_name != '__CDA_UNASSIGNED__':
                                    
                                    self.harmonized_value[column][old_value] = cda_common_name

                        else:
                            
                            for ( old_value, new_value ) in [ next_term_pair.rstrip( '\n' ).split( '\t' ) for next_term_pair in MAP ]:
                                
                                self.harmonized_value[column][old_value] = new_value

        # Track all substitutions.

        self.all_subs_performed = dict()

        # Spit out a logger message every `self.display_increment` lines while transcoding.

        self.display_increment = 500000

        for target_dir in [ self.merged_cda_dir, self.sql_table_output_dir, self.substitution_log_dir ]:
            
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

                skipped_nulled = 0

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

                                result_row = [ cda_mutation_id, current_mutation_alias ]

                                current_mutation_alias = current_mutation_alias + 1

                                for column in self.columns_to_keep:
                                    
                                    if column in record:
                                        
                                        old_value = record[column]

                                        if old_value is None:
                                            
                                            old_value = ''

                                        new_value = ''

                                        # Nitpick about capitalization for boolean literals in postgresql INSERTs (while refusing to capitalize "Boolean")

                                        if str( old_value ) == 'False':
                                            
                                            new_value = 'false'

                                        elif str( old_value ) == 'True':
                                            
                                            new_value = 'true'

                                        # Harmonize values as directed by the contents of the `harmonization_map_index` file (cf. __init__() definition above).

                                        if column in self.harmonized_value:
                                            
                                            if old_value is not None and old_value.lower() in self.harmonized_value[column] and self.harmonized_value[column][old_value.lower()] != 'null':
                                                
                                                new_value = self.harmonized_value[column][old_value.lower()]

                                            elif old_value is not None and old_value.lower() not in self.harmonized_value[column]:
                                                
                                                # Preserve values we haven't seen or mapped yet.

                                                new_value = old_value

                                            # Check target values for global cleanup: `self.delete_everywhere` takes precedence.

                                            if re.sub( r'\s', r'', new_value.strip().lower() ) in self.delete_everywhere:
                                                
                                                new_value = ''

                                            if column not in self.all_subs_performed:
                                                
                                                self.all_subs_performed[column] = dict()

                                            if old_value not in self.all_subs_performed[column]:
                                                
                                                self.all_subs_performed[column][old_value] = {
                                                    new_value : 1
                                                }

                                            elif new_value not in self.all_subs_performed[column][old_value]:
                                                
                                                self.all_subs_performed[column][old_value][new_value] = 1

                                            else:
                                                
                                                self.all_subs_performed[column][old_value][new_value] = self.all_subs_performed[column][old_value][new_value] + 1

                                        elif not isinstance( old_value, bool ) and re.sub( r'\s', r'', old_value.strip().lower() ) in self.delete_everywhere:
                                            
                                            # This column is not harmonized, but this value is slated for global deletion.

                                            if column not in self.all_subs_performed:
                                                
                                                self.all_subs_performed[column] = dict()

                                            if old_value not in self.all_subs_performed[column]:
                                                
                                                self.all_subs_performed[column][old_value] = {
                                                    '' : 1
                                                }

                                            elif '' not in self.all_subs_performed[column][old_value]:
                                                
                                                self.all_subs_performed[column][old_value][''] = 1

                                            else:
                                                
                                                self.all_subs_performed[column][old_value][''] = self.all_subs_performed[column][old_value][''] + 1

                                        else:
                                            
                                            # Column not harmonized; value not slated for deletion. Preserve value as is.

                                            new_value = old_value

                                        result_row.append( new_value )

                                    else:
                                        
                                        # `column` didn't appear at all in the loaded record.

                                        result_row.append( '' )

                                # end for ( column in self.columns_to_keep )

                                # Did we delete everything of possible use?

                                values_all_null = True

                                # Fast-forward past the two ID values at the beginning of each row.

                                row_index = 2

                                for column in self.columns_to_keep:
                                    
                                    if result_row[row_index] != '':
                                        
                                        values_all_null = False

                                    row_index = row_index + 1

                                if not values_all_null:
                                    
                                    print( *result_row, sep='\t', file=OUT )

                                else:
                                    
                                    skipped_nulled = skipped_nulled + 1

                            else:
                                
                                skipped_unfound = skipped_unfound + 1

                        else:
                            
                            skipped_null = skipped_null + 1

                        line_count = line_count + 1

                        if line_count == self.display_increment:
                            
                            print( file=sys.stderr )

                        if line_count % self.display_increment == 0:
                            
                            print( f"   ...processed {line_count} lines...", file=sys.stderr )

                print( f"done. Processed {line_count} lines; skipped {skipped_null} due to missing records; skipped {skipped_nulled} due to harmonization deleting all row data; and skipped {skipped_unfound} due to unmatched case barcodes.", end='\n\n', file=sys.stderr )

        # Remove any duplicate rows created by value harmonization.

        print( 'Deduplicating output...', end='', file=sys.stderr )

        removed_mutation_aliases = deduplicate_and_sort_unsorted_file_with_header( self.cda_table_inputs['mutation'], gzipped=True, ignore_primary_id_fields=True )

        temp_file = f"{self.cda_table_inputs['subject_mutation']}.tmp"

        with gzip.open( self.cda_table_inputs['subject_mutation'], 'rt' ) as IN, gzip.open( temp_file, 'wt' ) as OUT:
            
            header = next( IN ).rstrip( '\n' )

            columns = header.split( '\t' )

            print( header, file=OUT )

            for next_line in IN:
                
                line = next_line.rstrip( '\n' )

                record = dict( zip( columns, line.split( '\t' ) ) )

                current_mutation_alias = record['mutation_alias']

                if current_mutation_alias not in removed_mutation_aliases:
                    
                    print( line, file=OUT )

        rename( temp_file, self.cda_table_inputs['subject_mutation'] )

        print( f"done. Removed {len( removed_mutation_aliases )} duplicate mutation records and associated subject-mutation links.", end='\n\n', file=sys.stderr )

        # Dump substitution logs.

        print( 'Dumping substitution logs...', end='', file=sys.stderr )

        for column in sorted( self.all_subs_performed ):
            
            log_file = path.join( self.substitution_log_dir, f"mutation.{column}.substitution_log.tsv" )

            with open( log_file, 'w' ) as OUT:
                
                print( *[ 'raw_value', 'harmonized_value', 'number_of_substitutions' ], sep='\t', file=OUT )

                for old_value in sorted( self.all_subs_performed[column] ):
                    
                    for new_value in sorted( self.all_subs_performed[column][old_value] ):
                        
                        print( *[ old_value, new_value, self.all_subs_performed[column][old_value][new_value] ], sep='\t', file=OUT )

        print( 'done.', end='\n\n', file=sys.stderr )

    def transform_mutation_and_subject_mutation_to_SQL( self ):
        
        print( 'Transcoding CDA mutation and subject_mutation TSVs to SQL...', file=sys.stderr )

        for target_table in self.sql_outputs.keys():
            
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


