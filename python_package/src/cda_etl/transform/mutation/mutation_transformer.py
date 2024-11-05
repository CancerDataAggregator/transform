import gzip
import jsonlines
import re
import sys

from os import makedirs, path

from cda_etl.lib import deduplicate_and_sort_unsorted_file_with_header, get_universal_value_deletion_patterns, load_tsv_as_dict, map_columns_one_to_many, map_columns_one_to_one, sort_file_with_header

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

        if not path.exists( self.substitution_log_dir ):
            
            makedirs( self.substitution_log_dir )

        self.cda_data_sources = [
            'CDS',
            'GDC',
            'ICDC',
            'IDC',
            'PDC'
        ]

        self.input_dir = path.join( 'extracted_data', 'mutation' )
        self.merged_cda_dir = path.join( 'cda_tsvs', 'last_merge' )

        self.cda_table_files = {
            
            'mutation' : path.join( self.merged_cda_dir, 'mutation.tsv.gz' ),
            'subject' : path.join( self.merged_cda_dir, 'subject.tsv' ),
            'subject_in_project': path.join( self.merged_cda_dir, 'subject_in_project.tsv' ),
            'upstream_identifiers': path.join( self.merged_cda_dir, 'upstream_identifiers.tsv' )
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

                    map_file = path.join( harmonization_map_dir, f"{row_dict['concept_map_name']}.tsv" )

                    if column not in self.harmonized_value:
                        
                        self.harmonized_value[column] = dict()

                    with open( map_file ) as MAP:
                        
                        for next_term_pair in MAP:
                            
                            ( old_value, new_value ) = next_term_pair.rstrip( '\n' ).split( '\t' )
                            
                            self.harmonized_value[column][old_value] = new_value

        # Track all substitutions.

        self.all_subs_performed = dict()

        # Spit out a logger message every `self.display_increment` lines while transcoding.

        self.display_increment = 500000

    def make_mutation_TSV( self ):
        
        cda_subject_alias_to_project_alias = map_columns_one_to_many( self.cda_table_files['subject_in_project'], 'subject_alias', 'project_alias' )

        cda_subject_alias_to_gdc_case_submitter_id = dict()

        cda_project_alias_to_gdc_project_id = dict()

        with open( self.cda_table_files['upstream_identifiers'] ) as IN:
            
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                [ cda_table, entity_alias, data_source, source_field, value ] = line.split( '\t' )

                if cda_table == 'subject' and data_source == 'GDC' and source_field == 'case.submitter_id':
                    
                    if entity_alias not in cda_subject_alias_to_gdc_case_submitter_id:
                        
                        cda_subject_alias_to_gdc_case_submitter_id[entity_alias] = set()

                    cda_subject_alias_to_gdc_case_submitter_id[entity_alias].add( value )

                elif cda_table == 'project' and data_source == 'GDC' and source_field == 'project.project_id':
                    
                    cda_project_alias_to_gdc_project_id[entity_alias] = value

        gdc_project_id_and_case_submitter_id_to_cda_subject_alias = dict()

        for project_alias in cda_project_alias_to_gdc_project_id:
            
            project_id = cda_project_alias_to_gdc_project_id[project_alias]

            for subject_alias in cda_subject_alias_to_project_alias:
                
                if project_alias in cda_subject_alias_to_project_alias[subject_alias]:
                    
                    # This is meant to break with a KeyError if no submitter_id was loaded for this subject_alias.

                    for case_submitter_id in cda_subject_alias_to_gdc_case_submitter_id[subject_alias]:
                        
                        if project_id not in gdc_project_id_and_case_submitter_id_to_cda_subject_alias:
                            
                            gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_id] = dict()

                        if case_submitter_id not in gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_id]:
                            
                            gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_id][case_submitter_id] = subject_alias

                        elif gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_id][case_submitter_id] != subject_alias:
                            
                            sys.exit( f"FATAL: GDC project_id {project_id} and case_submitter_id {case_submitter_id} mapped to multiple CDA subjects, including aliases {subject_alias} and {gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_id][case_submitter_id]}; assumptions violated, cannot continue. Aborting." )

        # Load provenance for target subjects.

        subject_provenance = dict()

        provenance_column_set = set()

        with open( self.cda_table_files['subject'] ) as IN:
            
            column_names = next( IN ).rstrip( '\n' ).split( '\t' )

            for next_line in IN:
                
                record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

                subject_alias = record['id_alias']

                subject_provenance[subject_alias] = dict()

                for column in column_names:
                    
                    if column == 'data_source_count' or re.search( r'^data_at_', column ) is not None:
                        
                        if column != 'data_source_count':
                            
                            provenance_column_set.add( column )

                        subject_provenance[subject_alias][column] = record[column]

        provenance_columns = sorted( provenance_column_set ) + [ 'data_source_count' ]

        with gzip.open( self.cda_table_files['mutation'], 'wt' ) as OUT:
            
            mutation_output_columns = [ 'id_alias', 'subject_alias' ] + self.columns_to_keep + provenance_columns

            print( *mutation_output_columns, sep='\t', file=OUT )

            current_mutation_alias = 0

            for source_dataset in self.source_datasets:
                
                mutation_jsonl = path.join( self.input_dir, f"{source_dataset}.masked_somatic_mutation_{self.source_version}.jsonl.gz" )

                line_count = 0

                skipped_null = 0

                skipped_unfound = 0

                skipped_nulled = 0

                print( f"Transcoding mutation JSONL for {source_dataset} into CDA-formatted mutation.tsv and linking CDA subject records...", end='', file=sys.stderr )

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
                            
                            if project_short_name in gdc_project_id_and_case_submitter_id_to_cda_subject_alias and case_barcode in gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_short_name]:
                                
                                cda_mutation_alias = current_mutation_alias

                                current_mutation_alias = current_mutation_alias + 1

                                cda_subject_alias = gdc_project_id_and_case_submitter_id_to_cda_subject_alias[project_short_name][case_barcode]

                                result_row = [ cda_mutation_alias, cda_subject_alias ]

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
                                            
                                            if old_value in self.harmonized_value[column]:
                                                
                                                new_value = self.harmonized_value[column][old_value]

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

                                for provenance_column in provenance_columns:
                                    
                                    result_row.append( subject_provenance[cda_subject_alias][provenance_column] )

                                # Did we delete everything of possible use?

                                values_all_null = True

                                # Fast-forward past the two ID aliases at the beginning of each row.

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

        deduplicate_and_sort_unsorted_file_with_header( self.cda_table_files['mutation'], gzipped=True, ignore_primary_id_field=True )

        print( 'done.', end='\n\n', file=sys.stderr )

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


