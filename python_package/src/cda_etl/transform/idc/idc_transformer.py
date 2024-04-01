import gzip
import jsonlines
import re
import sys

from os import makedirs, path

from cda_etl.lib import load_tsv_as_dict, map_columns_one_to_one, sort_file_with_header

class IDC_transformer:
    
    def __init__( self, source_version ):
        
        self.source_version = source_version

        self.extract_dir = path.join( 'extracted_data', 'idc', self.source_version )

        self.output_dir = path.join( 'cda_tsvs', 'idc_raw_unharmonized', self.source_version )

        self.aux_root = 'auxiliary_metadata'
        self.supp_dir = path.join( self.aux_root, '__IDC_supplemental_metadata' )
        self.merge_log_dir = path.join( self.aux_root, '__merge_logs' )

        # \/ \/ \/ \/ \/ \/ \/ \/ MOVE ME \/ \/ \/ \/ \/ \/ \/ \/
        self.map_dir = 'package_root/extract/idc/dicom_code_maps'
        # /\ /\ /\ /\ /\ /\ /\ /\ MOVE ME /\ /\ /\ /\ /\ /\ /\ /\

        self.input_files = {
            
            'dicom_all' : path.join( self.extract_dir, 'dicom_all.jsonl.gz' ),
            'auxiliary_metadata' : path.join( self.extract_dir, 'auxiliary_metadata.jsonl.gz' ),
            'original_collections_metadata' : path.join( self.extract_dir, 'original_collections_metadata.jsonl.gz' ),
            'tcga_biospecimen_rel9' : path.join( self.extract_dir, 'tcga_biospecimen_rel9.jsonl.gz' ),
            'tcga_clinical_rel9' : path.join( self.extract_dir, 'tcga_clinical_rel9.jsonl.gz' )
        }

        self.output_files = {
            
            'file' : path.join( self.output_dir, 'file.tsv.gz' ),
            'file_associated_project' : path.join( self.output_dir, 'file_associated_project.tsv.gz' ),
            'file_identifier' : path.join( self.output_dir, 'file_identifier.tsv.gz' ),
            'file_specimen' : path.join( self.output_dir, 'file_specimen.tsv.gz' ),
            'file_subject' : path.join( self.output_dir, 'file_subject.tsv.gz' ),
            'researchsubject' : path.join( self.output_dir, 'researchsubject.tsv.gz' ),
            'rs_identifier' : path.join( self.output_dir, 'researchsubject_identifier.tsv.gz' ),
            'rs_specimen' : path.join( self.output_dir, 'researchsubject_specimen.tsv.gz' ),
            'specimen' : path.join( self.output_dir, 'specimen.tsv.gz' ),
            'specimen_identifier' : path.join( self.output_dir, 'specimen_identifier.tsv.gz' ),
            'subject' : path.join( self.output_dir, 'subject.tsv.gz' ),
            'subject_associated_project' : path.join( self.output_dir, 'subject_associated_project.tsv.gz' ),
            'subject_identifier' : path.join( self.output_dir, 'subject_identifier.tsv.gz' ),
            'subject_researchsubject' : path.join( self.output_dir, 'subject_researchsubject.tsv.gz' )
        }

        self.aux_files = {
            
            'collection_id_to_entity_id' : path.join( self.supp_dir, 'IDC_entity_submitter_id_to_collection_id.tsv' ),
            'file_researchsubject' : path.join( self.supp_dir, 'file_researchsubject.tsv.gz' ),
            'idc_case_id_to_submitter_case_id' : path.join( self.supp_dir, 'idc_case_id_to_submitter_case_id.tsv' ),
            'subject_idc_webapp_collection_id' : path.join( self.supp_dir, 'subject_idc_webapp_collection_id.tsv' ),
            'tcga_biospecimen_sample_barcode_to_gdc_id' : path.join( self.supp_dir, 'tcga_biospecimen_rel9.submitter_sample_id_to_gdc_id.tsv' ),
            'tcga_biospecimen_submitter_case_id_to_gdc_id' : path.join( self.supp_dir, 'tcga_biospecimen_rel9.submitter_case_id_to_gdc_id.tsv' )
        }

        self.log_files = {
            
            'no_subject_specimens' : path.join( self.supp_dir, 'IDC_skipped_specimens_with_missing_subjects.tsv' )
        }

        self.map_files = {
            
            'SOPClassUID' : path.join( self.map_dir, 'SOPClassUID.tsv' ),
            'DICOM_controlled_terms' : path.join( self.map_dir, 'DICOM_controlled_terms.tsv' )
        }

        for target_dir in [ self.output_dir, self.supp_dir, self.merge_log_dir ]:
            
            if not path.isdir( target_dir ):
                
                makedirs( target_dir )

        self.cda_headers = {
            
            'file' : [
                
                'id',
                'label',
                'data_category',
                'data_type',
                'file_format',
                'drs_uri',
                'byte_size',
                'checksum',
                'data_modality',
                'imaging_modality',
                'dbgap_accession_number',
                'imaging_series'
            ],

            'specimen' : [
                
                'id',
                'days_to_collection',
                'primary_disease_type',
                'anatomical_site',
                'source_material_type',
                'specimen_type',
                'associated_project',
                'derived_from_specimen',
                'derived_from_subject'
            ],

            'subject' : [
                
                'id',
                'species',
                'sex',
                'race',
                'ethnicity',
                'days_to_birth',
                'vital_status',
                'days_to_death',
                'cause_of_death'
            ],

            'researchsubject': [
                
                'id',
                'primary_diagnosis_condition',
                'primary_diagnosis_site',
                'member_of_research_project'
            ]
        }

    def extract_submitter_case_IDs_from_auxiliary_metadata( self ):
        
        input_file = self.input_files['auxiliary_metadata']

        id_map_out = self.aux_files['idc_case_id_to_submitter_case_id']

        submitter_case_id = dict()

        line_count = 0

        display_increment = 1000000

        print( "Scanning auxiliary_metadata map...", file=sys.stderr )

        with gzip.open( input_file ) as IN:
            
            reader = jsonlines.Reader( IN )

            for record in reader:
                
                idc_case_id = record['idc_case_id']

                if idc_case_id not in submitter_case_id:
                    
                    submitter_case_id[idc_case_id] = set()

                target_id = record['submitter_case_id']

                submitter_case_id[idc_case_id].add( target_id )

                line_count = line_count + 1

                if line_count % display_increment == 0:
                    
                    print( f"   ...scanned {line_count} lines...", file=sys.stderr )

            print( f"...done. Scanned {line_count} lines total.", file=sys.stderr )

        with open( id_map_out, 'w' ) as OUT:

            print( *[ 'idc_case_id', 'submitter_case_id' ], sep='\t', end='\n', file=OUT )

            for idc_case_id in sorted( submitter_case_id ):
                
                for target_id in sorted( submitter_case_id[idc_case_id] ):
                    
                    print( *[ idc_case_id, target_id ], sep='\t', end='\n', file=OUT )

    def extract_file_subject_and_rs_data_from_dicom_all( self ):
        
        transform_log = path.join( self.merge_log_dir, 'IDC_inconsistent_records_substitution_counts.tsv' )
        transform_block_log = path.join( self.merge_log_dir, 'IDC_inconsistent_records_blocked_substitutions.tsv' )

        subject_identifiers = dict()
        rs_identifiers = dict()

        subject_collection_ids = dict()
        subject_idc_webapp_collection_ids = dict()

        subject_researchsubjects = dict()

        subject_rows = dict()
        rs_rows = dict()

        transform_counts = {
            
            'file' : dict(),
            'subject' : dict(),
            'researchsubject' : dict()
        }

        blocked_ids = {
            
            'file' : dict(),
            'subject' : dict(),
            'researchsubject' : dict()
        }

        class_uid_map = map_columns_one_to_one( self.map_files['SOPClassUID'], 'SOP Class UID', 'SOP Class Name' )

        imaging_modality_map = map_columns_one_to_one( self.map_files['DICOM_controlled_terms'], 'Code Value', 'Code Meaning' )

        idc_case_id_to_submitter_case_id = map_columns_one_to_one( self.aux_files['idc_case_id_to_submitter_case_id'], 'idc_case_id', 'submitter_case_id' )

        bad_modality_codes = set()

        line_count = 0

        display_increment = 1000000

        print( "Scanning dicom_all...", file=sys.stderr )

        with gzip.open( self.input_files['dicom_all'] ) as IN, gzip.open( self.output_files['file'], 'wt' ) as FILE_OUT, \
            gzip.open( self.output_files['file_identifier'], 'wt' ) as FILE_ID, gzip.open( self.output_files['file_associated_project'], 'wt' ) as FILE_PROJECT, \
            gzip.open( self.output_files['file_subject'], 'wt' ) as FILE_SUBJECT, gzip.open( self.aux_files['file_researchsubject'], 'wt' ) as FILE_RS:
            
            print( *self.cda_headers['file'], sep='\t', end='\n', file=FILE_OUT )

            print( *[ 'file_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=FILE_ID )

            print( *[ 'file_id', 'associated_project' ], sep='\t', end='\n', file=FILE_PROJECT )

            print( *[ 'file_id', 'subject_id' ], sep='\t', end='\n', file=FILE_SUBJECT )

            print( *[ 'file_id', 'researchsubject_id' ], sep='\t', end='\n', file=FILE_RS )

            reader = jsonlines.Reader( IN )

            for record in reader:
                
                if record['collection_id'] == '' or record['PatientID'] == '':
                    
                    sys.exit( f"FATAL: One of PatientID '{record['PatientID']}' or collection_id '{record['collection_id']}' is null; aborting.\n" )

                # Log basic file ID and collection information.

                file_id = record['SOPInstanceUID']

                print( *[ file_id, 'IDC', 'SOPInstanceUID', file_id ], sep='\t', end='\n', file=FILE_ID )

                print( *[ file_id, record['collection_id'] ], sep='\t', end='\n', file=FILE_PROJECT )
                
                # Manage CDA subject IDs and association metadata.

                subject_id = record['collection_id'] + '.' + record['PatientID']

                print( *[ file_id, subject_id ], sep='\t', end='\n', file=FILE_SUBJECT )

                if subject_id not in subject_identifiers:
                    
                    subject_identifiers[subject_id] = {
                        
                        'PatientID' : set(),
                        'idc_case_id' : set(),
                        'submitter_case_id' : set()
                    }

                subject_identifiers[subject_id]['PatientID'].add( record['PatientID'] )
                subject_identifiers[subject_id]['idc_case_id'].add( record['idc_case_id'] )
                subject_identifiers[subject_id]['submitter_case_id'].add( idc_case_id_to_submitter_case_id[record['idc_case_id']] )

                if subject_id not in subject_collection_ids:
                    
                    subject_collection_ids[subject_id] = set()

                subject_collection_ids[subject_id].add( record['collection_id'] )

                if subject_id not in subject_idc_webapp_collection_ids:
                    
                    subject_idc_webapp_collection_ids[subject_id] = set()

                subject_idc_webapp_collection_ids[subject_id].add( record['idc_webapp_collection_id'] )

                # Manage CDA researchsubject IDs and association metadata.

                rs_id = subject_id + '.RS'

                print( *[ file_id, rs_id ], sep='\t', end='\n', file=FILE_RS )

                if rs_id not in rs_identifiers:
                    
                    rs_identifiers[rs_id] = {
                        
                        'PatientID' : set(),
                        'idc_case_id' : set(),
                        'submitter_case_id' : set()
                    }

                rs_identifiers[rs_id]['PatientID'].add( record['PatientID'] )
                rs_identifiers[rs_id]['idc_case_id'].add( record['idc_case_id'] )
                rs_identifiers[rs_id]['submitter_case_id'].add( idc_case_id_to_submitter_case_id[record['idc_case_id']] )

                if subject_id not in subject_researchsubjects:
                    
                    subject_researchsubjects[subject_id] = set()

                subject_researchsubjects[subject_id].add( rs_id )

                # Load file metadata and cache for the file table.

                file_values = dict()

                for field_name in self.cda_headers['file']:
                    
                    file_values[field_name] = ''

                file_values['id'] = file_id

                # All of the following are either fixed constant values or never null in the source table.

                file_values['label'] = record['gcs_url']

                file_values['label'] = re.sub( r'^.*\/([^\/]+)$', r'\1', file_values['label'] )

                file_values['data_category'] = 'Imaging'

                class_uid = record['SOPClassUID']

                if class_uid not in class_uid_map:
                    
                    print( f"WARNING: SOPClassUID '{class_uid}' not found in reference map; skipping and saving data_type:NULL for file {file_id}.", sep='', end='\n', file=sys.stderr )

                    file_values['data_type'] = ''

                else:
                    
                    file_values['data_type'] = class_uid_map[class_uid]

                file_values['file_format'] = 'DICOM'

                file_values['drs_uri'] = 'drs://dg.4DFC:' + record['crdc_instance_uuid']

                file_values['byte_size'] = record['instance_size']

                file_values['checksum'] = record['instance_hash']

                file_values['data_modality'] = 'Imaging'

                modality_code = record['Modality']

                if modality_code not in imaging_modality_map:
                    
                    if modality_code not in bad_modality_codes:
                        
                        print( f"WARNING: Modality code '{modality_code}' not found in reference list of DICOM controlled terms; saving raw code as imaging_modality:{modality_code} for file {file_id} (and doing the same and suppressing this message for future instances of the same unfound code '{modality_code}').", sep='', end='\n', file=sys.stderr )

                        bad_modality_codes.add( modality_code )

                    file_values['imaging_modality'] = modality_code

                else:
                    
                    file_values['imaging_modality'] = imaging_modality_map[modality_code]

                file_values['dbgap_accession_number'] = ''

                file_values['imaging_series'] = record['crdc_series_uuid']

                row_result = [ file_values[field_name] for field_name in self.cda_headers['file'] ]

                print( *row_result, sep='\t', end='\n', file=FILE_OUT )

                # Load atomic subject metadata and cache for the subject table.

                subject_values = dict()

                for field_name in self.cda_headers['subject']:
                    
                    subject_values[field_name] = ''

                subject_values['id'] = subject_id

                subject_values['species'] = 'Homo sapiens'

                if 'PatientSpeciesDescription' in record and record['PatientSpeciesDescription'] != '':
                    
                    subject_values['species'] = record['PatientSpeciesDescription']

                if 'PatientSex' in record and record['PatientSex'] != '':
                    
                    subject_values['sex'] = record['PatientSex']

                if 'EthnicGroup' in record and record['EthnicGroup'] != '':
                    
                    subject_values['ethnicity'] = record['EthnicGroup']

                row_result = [ subject_values[field_name] for field_name in self.cda_headers['subject'] ]

                if subject_id not in subject_rows:
                    
                    subject_rows[subject_id] = row_result

                else:
                    
                    for i in range( len( subject_rows[subject_id] ) ):
                        
                        if row_result[i] != subject_rows[subject_id][i]:
                            
                            if subject_rows[subject_id][i] == '' or row_result[i] != '':
                                
                                # Either
                                #     Sub a non-null value in for a previously-recorded null value for this same subject.
                                #     ( subject_rows[subject_id][i] == '' )
                                # or
                                #     Sub a non-null value for another, different non-null value previously recorded for this same subject.
                                #     ( subject_rows[subject_id][i] != '' and row_result[i] != '' )

                                field_name = self.cda_headers['subject'][i]

                                old_value = subject_rows[subject_id][i]

                                new_value = row_result[i]

                                if old_value == '' and ( field_name not in blocked_ids['subject'] or subject_id not in blocked_ids['subject'][field_name] ):
                                    
                                    # New value; previously blank. Update record.

                                    subject_rows[subject_id][i] = new_value

                                    # Log inconsistencies by swapped value.

                                    if field_name not in transform_counts['subject']:
                                        
                                        transform_counts['subject'][field_name] = dict()

                                    if old_value not in transform_counts['subject'][field_name]:
                                        
                                        transform_counts['subject'][field_name][old_value] = dict()

                                    if new_value not in transform_counts['subject'][field_name][old_value]:
                                        
                                        transform_counts['subject'][field_name][old_value][new_value] = 1

                                    else:
                                        
                                        transform_counts['subject'][field_name][old_value][new_value] = transform_counts['subject'][field_name][old_value][new_value] + 1

                                elif old_value == '':
                                    
                                    # Already blocked; record the observed value and do not update the subject record.

                                    blocked_ids['subject'][field_name][subject_id].add( new_value )

                                else:
                                    
                                    # New conflicting value; previously populated. Null the field, log the observed values and block further updates to this field in this record.

                                    subject_rows[subject_id][i] = ''

                                    if field_name not in blocked_ids['subject']:
                                        
                                        blocked_ids['subject'][field_name] = dict()

                                    if subject_id not in blocked_ids['subject'][field_name]:
                                        
                                        blocked_ids['subject'][field_name][subject_id] = set()

                                    blocked_ids['subject'][field_name][subject_id].add( old_value )
                                    blocked_ids['subject'][field_name][subject_id].add( new_value )

                # Load atomic researchsubject metadata and cache for the researchsubject table.

                rs_values = dict()

                for field_name in self.cda_headers['researchsubject']:
                    
                    rs_values[field_name] = ''

                rs_values['id'] = rs_id

                if record['tcia_tumorLocation'] != '':
                    
                    rs_values['primary_diagnosis_site'] = record['tcia_tumorLocation']

                rs_values['member_of_research_project'] = record['collection_id']

                row_result = [ rs_values[field_name] for field_name in self.cda_headers['researchsubject'] ]

                if rs_id not in rs_rows:
                    
                    rs_rows[rs_id] = row_result

                else:
                    
                    for i in range( len( rs_rows[rs_id] ) ):
                        
                        if row_result[i] != rs_rows[rs_id][i]:
                            
                            if rs_rows[rs_id][i] == '' or row_result[i] != '':
                                
                                # Either
                                #     Sub a non-null value in for a previously-recorded null value for this same researchsubject.
                                #     ( rs_rows[rs_id][i] == '' )
                                # or
                                #     Sub a non-null value for another, different non-null value previously recorded for this same researchsubject.
                                #     ( rs_rows[rs_id][i] != '' and row_result[i] != '' )

                                field_name = self.cda_headers['researchsubject'][i]

                                old_value = rs_rows[rs_id][i]

                                new_value = row_result[i]

                                if old_value == '' and rs_id not in blocked_ids['researchsubject'][field_name]:
                                    
                                    # New value; previously blank. Update record.

                                    rs_rows[rs_id][i] = new_value

                                    # Log inconsistencies by swapped value.

                                    if field_name not in transform_counts['researchsubject']:
                                        
                                        transform_counts['researchsubject'][field_name] = dict()

                                    if old_value not in transform_counts['researchsubject'][field_name]:
                                        
                                        transform_counts['researchsubject'][field_name][old_value] = dict()

                                    if new_value not in transform_counts['researchsubject'][field_name][old_value]:
                                        
                                        transform_counts['researchsubject'][field_name][old_value][new_value] = 1

                                    else:
                                        
                                        transform_counts['researchsubject'][field_name][old_value][new_value] = transform_counts['researchsubject'][field_name][old_value][new_value] + 1

                                elif old_value == '':
                                    
                                    # Already blocked; record the observed value and do not update the researchsubject record.

                                    blocked_ids['researchsubject'][field_name][rs_id].add( new_value )

                                else:
                                    
                                    # New conflicting value; previously populated. Null the field, log the observed values and block further updates to this field in this record.

                                    rs_rows[rs_id][i] = ''

                                    if field_name not in blocked_ids['researchsubject']:
                                        
                                        blocked_ids['researchsubject'][field_name] = dict()

                                    if rs_id not in blocked_ids['researchsubject'][field_name]:
                                        
                                        blocked_ids['researchsubject'][field_name][rs_id] = set()

                                    blocked_ids['researchsubject'][field_name][rs_id].add( old_value )
                                    blocked_ids['researchsubject'][field_name][rs_id].add( new_value )

                line_count = line_count + 1

                if line_count % display_increment == 0:
                    
                    print( f"   ...scanned {line_count} lines...", file=sys.stderr )

            # end for record in reader (i.e. for line in input file)

        # end with ( open self.input_files['dicom_all'] as IN )

        print( f"...done. Scanned {line_count} lines in all.", file=sys.stderr )

        print( 'Writing TSVs...', end='', file=sys.stderr )

        with open( transform_log, 'w' ) as LOG:
            
            print( *[ 'table', 'field_name', 'old_value', 'new_value', 'swap_count' ], sep='\t', end='\n', file=LOG )

            for table in sorted( transform_counts ):
                
                for field_name in self.cda_headers[table]:
                    
                    if field_name in transform_counts[table]:
                        
                        for old_value in sorted( transform_counts[table][field_name] ):
                            
                            for new_value in sorted( transform_counts[table][field_name][old_value] ):
                                
                                swap_count = transform_counts[table][field_name][old_value][new_value]

                                print( *[ table, field_name, old_value, new_value, swap_count ], sep='\t', end='\n', file=LOG )

        # end with ... ( write transform log )

        with open( transform_block_log, 'w' ) as LOG:
            
            print( *[ 'entity_type', 'field_name', 'entity_id', 'clashing_values' ], sep='\t', end='\n', file=LOG )

            for entity_type in sorted( blocked_ids ):
                
                for field_name in self.cda_headers[entity_type]:
                    
                    if field_name in blocked_ids[entity_type]:
                        
                        for entity_id in sorted( blocked_ids[entity_type][field_name] ):
                            
                            print( *[ entity_type, field_name, entity_id, str( sorted( blocked_ids[entity_type][field_name][entity_id] ) ) ], sep='\t', end='\n', file=LOG )

        # end with ... ( write transform-blocked log )

        with gzip.open( self.output_files['subject'], 'wt' ) as SUBJECT_OUT:

            print( *self.cda_headers['subject'], sep='\t', end='\n', file=SUBJECT_OUT )

            for subject_id in sorted( subject_rows ):
                
                print( *subject_rows[subject_id], sep='\t', end='\n', file=SUBJECT_OUT )

        # end with ... ( write subject.tsv )

        with gzip.open( self.output_files['researchsubject'], 'wt' ) as RS_OUT:
            
            print( *self.cda_headers['researchsubject'], sep='\t', end='\n', file=RS_OUT )

            for rs_id in sorted( rs_rows ):
                
                print( *rs_rows[rs_id], sep='\t', end='\n', file=RS_OUT )

        # end with ... ( write researchsubject.tsv )

        with gzip.open( self.output_files['subject_identifier'], 'wt' ) as OUT:
            
            print( *[ 'subject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=OUT )

            for subject_id in sorted( subject_identifiers ):
                
                for field_name in sorted( subject_identifiers[subject_id] ):
                    
                    for value in sorted( subject_identifiers[subject_id][field_name] ):
                        
                        print( *[ subject_id, 'IDC', field_name, value ], sep='\t', end='\n', file=OUT )

        with gzip.open( self.output_files['rs_identifier'], 'wt' ) as OUT:
            
            print( *[ 'researchsubject_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=OUT )

            for rs_id in sorted( rs_identifiers ):
                
                for field_name in sorted( rs_identifiers[rs_id] ):
                    
                    for value in sorted( rs_identifiers[rs_id][field_name] ):
                        
                        print( *[ rs_id, 'IDC', field_name, value ], sep='\t', end='\n', file=OUT )

        with gzip.open( self.output_files['subject_associated_project'], 'wt' ) as SUBJECT_PROJECT:
            
            print( *[ 'subject_id', 'associated_project' ], sep='\t', end='\n', file=SUBJECT_PROJECT )

            for subject_id in sorted( subject_collection_ids ):
                
                for collection_id in sorted( subject_collection_ids[subject_id] ):
                    
                    print( *[ subject_id, collection_id ], sep='\t', end='\n', file=SUBJECT_PROJECT )

        with gzip.open( self.output_files['subject_researchsubject'], 'wt' ) as SUBJECT_RS:
            
            print( *[ 'subject_id', 'researchsubject_id' ], sep='\t', end='\n', file=SUBJECT_RS )

            for subject_id in sorted( subject_researchsubjects ):
                
                for rs_id in sorted( subject_researchsubjects[subject_id] ):
                    
                    print( *[ subject_id, rs_id ], sep='\t', end='\n', file=SUBJECT_RS )

        with gzip.open( self.aux_files['subject_idc_webapp_collection_id'], 'wt' ) as SUBJECT_IDC_WEBAPP_COLLS:
            
            print( *[ 'subject_id', 'idc_webapp_collection_id' ], sep='\t', end='\n', file=SUBJECT_IDC_WEBAPP_COLLS )

            for subject_id in sorted( subject_idc_webapp_collection_ids ):
                
                for idc_webapp_collection_id in sorted( subject_idc_webapp_collection_ids[subject_id] ):
                    
                    print( *[ subject_id, idc_webapp_collection_id ], sep='\t', end='\n', file=SUBJECT_IDC_WEBAPP_COLLS )

        print( 'done.', file=sys.stderr )

    def map_case_ids_to_collection_ids( self ):
        
        subject_project_tsv = self.output_files['subject_associated_project']

        output_file = self.aux_files['collection_id_to_entity_id']

        subject_id_to_idc_case_id = dict()

        subject_id_to_collection_id = dict()

        with gzip.open( subject_project_tsv, 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                subject_id_to_collection_id[subject_id] = associated_project

        idc_case_id_to_submitter_case_id = map_columns_one_to_one( self.aux_files['idc_case_id_to_submitter_case_id'], 'idc_case_id', 'submitter_case_id' )

        with gzip.open( self.output_files['subject_identifier'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                values = line.split('\t')

                if values[2] == 'idc_case_id':
                    
                    subject_id_to_idc_case_id[values[0]] = values[3]

        with open( output_file, 'w' ) as OUT:
            
            print( *[ 'collection_id', 'submitter_case_id', 'idc_case_id', 'entity_type' ], sep='\t', end='\n', file=OUT )

            for subject_id in subject_id_to_collection_id:
                
                collection_id = subject_id_to_collection_id[subject_id]

                idc_case_id = subject_id_to_idc_case_id[subject_id]

                submitter_case_id = idc_case_id_to_submitter_case_id[idc_case_id]

                entity_type = 'case'

                print( *[ collection_id, submitter_case_id, idc_case_id, entity_type ], sep='\t', end='\n', file=OUT )

        sort_file_with_header( output_file )

    def populate_rs_primary_diagnosis_condition( self ):
        
        collection_id_to_cancer_type = dict()

        with gzip.open( self.input_files['original_collections_metadata'] ) as IN:
            
            reader = jsonlines.Reader( IN )

            for record in reader:
                
                if 'idc_webapp_collection_id' in record and 'CancerType' in record:
                    
                    collection_id = record['idc_webapp_collection_id']

                    cancer_type = record['CancerType']

                    if collection_id in collection_id_to_cancer_type and collection_id_to_cancer_type[collection_id] != cancer_type:
                        
                        print( f"WARNING: Multiple cancer types ('{collection_id_to_cancer_type[collection_id]}', '{cancer_type}'[, possibly more]) observed for collection_id '{collection_id}' -- please investigate.", sep='', end='\n', file=sys.stderr )

                    collection_id_to_cancer_type[collection_id] = cancer_type

        rs_to_subject = dict()

        with gzip.open( self.output_files['subject_researchsubject'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, rs_id ] = line.split('\t')

                rs_to_subject[rs_id] = subject_id

        subject_to_collection = dict()

        with gzip.open( self.output_files['subject_associated_project'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                subject_to_collection[subject_id] = associated_project

        rs = dict()

        with gzip.open( self.output_files['researchsubject'], 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                rs_id = record['id']

                rs[rs_id] = record

        with gzip.open( self.output_files['researchsubject'], 'wt' ) as OUT:
            
            print( *[ field_name for field_name in self.cda_headers['researchsubject'] ], sep='\t', end='\n', file=OUT )

            for rs_id in sorted( rs ):
                
                primary_diagnosis_condition = ''

                subject_id = rs_to_subject[rs_id]

                collection_id = subject_to_collection[subject_id]

                if collection_id in collection_id_to_cancer_type:
                    
                    rs[rs_id]['primary_diagnosis_condition'] = collection_id_to_cancer_type[collection_id]

                print( *[ rs[rs_id][field_name] for field_name in self.cda_headers['researchsubject'] ], sep='\t', end='\n', file=OUT )

    def populate_tcga_clinical_subject_metadata( self ):
        
        subject = dict()

        with gzip.open( self.output_files['subject'], 'rt' ) as IN:
            
            colnames = next(IN).rstrip('\n').split('\t')

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                record = dict( zip( colnames, line.split('\t') ) )

                subject_id = record['id']

                subject[subject_id] = record

        subject_to_collection = dict()

        with gzip.open( self.output_files['subject_associated_project'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                subject_to_collection[subject_id] = associated_project

        submitter_id_to_subject_id = dict()

        with gzip.open( self.output_files['subject_identifier'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, id_system, field_name, id_value ] = line.split('\t')

                if field_name == 'submitter_case_id' and re.search( r'^tcga', subject_id ) is not None:
                    
                    submitter_id_to_subject_id[id_value] = subject_id

        with gzip.open( self.input_files['tcga_clinical_rel9'] ) as IN:
            
            reader = jsonlines.Reader( IN )

            for record in reader:
                
                if 'case_barcode' in record and record['case_barcode'] in submitter_id_to_subject_id:
                    
                    subject_id = submitter_id_to_subject_id[record['case_barcode']]

                    for field_name in [ 'race', 'days_to_birth', 'vital_status', 'days_to_death' ]:
                        
                        if field_name in record:
                            
                            subject[subject_id][field_name] = record[field_name]

        with gzip.open( self.output_files['subject'], 'wt' ) as OUT:
            
            print( *[ field_name for field_name in self.cda_headers['subject'] ], sep='\t', end='\n', file=OUT )

            for subject_id in sorted( subject ):
                
                print( *[ subject[subject_id][field_name] for field_name in self.cda_headers['subject'] ], sep='\t', end='\n', file=OUT )

    def extract_specimen_data_from_tcga_biospecimen_rel9( self ):
        
        subject_id_to_collection_id = dict()

        with gzip.open( self.output_files['subject_associated_project'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ subject_id, associated_project ] = line.split('\t')

                subject_id_to_collection_id[subject_id] = associated_project

        submitter_case_id_to_subject_id = dict()

        submitter_case_id_to_collection_id = dict()

        with gzip.open( self.output_files['subject_identifier'], 'rt' ) as IN:
            
            header = next(IN)

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                values = line.split('\t')

                if values[2] == 'submitter_case_id':
                    
                    submitter_case_id_to_subject_id[values[3]] = values[0]

                    submitter_case_id_to_collection_id[values[3]] = subject_id_to_collection_id[values[0]]

        rs_to_specimen = dict()

        with gzip.open( self.input_files['tcga_biospecimen_rel9'] ) as IN, gzip.open( self.output_files['specimen'], 'wt' ) as SPECIMEN, gzip.open( self.output_files['specimen_identifier'], 'wt' ) as SPECIMEN_ID, \
            open( self.aux_files['tcga_biospecimen_submitter_case_id_to_gdc_id'], 'w' ) as CASE_TO_GDC, open( self.aux_files['tcga_biospecimen_sample_barcode_to_gdc_id'], 'w' ) as SAMPLE_TO_GDC, \
            open( self.log_files['no_subject_specimens'], 'w' ) as SKIP_LOG:
            
            print( *self.cda_headers['specimen'], sep='\t', end='\n', file=SPECIMEN )

            print( *[ 'specimen_id', 'system', 'field_name', 'value' ], sep='\t', end='\n', file=SPECIMEN_ID )

            print( *[ 'submitter_case_id', 'gdc_id' ], sep='\t', end='\n', file=CASE_TO_GDC )

            print( *[ 'sample_barcode', 'gdc_id' ], sep='\t', end='\n', file=SAMPLE_TO_GDC )

            reader = jsonlines.Reader( IN )

            line_count = 0

            printed_case_gdc_id = set()

            for record in reader:
                
                line_count = line_count + 1

                # sample_barcode	case_barcode	days_to_collection	sample_type_name	case_gdc_id	sample_gdc_id

                if 'sample_barcode' not in record or record['sample_barcode'] == '':
                    
                    print( f"WARNING: record in {self.input_files['tcga_biospecimen_rel9']} with no `sample_barcode` (line {line_count}); skipping" )

                elif 'case_barcode' not in record or record['case_barcode'] == '':
                    
                    print( f"WARNING: record in {self.input_files['tcga_biospecimen_rel9']} with no `case_barcode` (line {line_count}); skipping" )

                elif record['case_barcode'] not in submitter_case_id_to_subject_id:
                    
                    print( f"Skipping specimen with `sample_barcode` '{record['sample_barcode']}' because there was no data loaded for its `case_barcode` '{record['case_barcode']}'.", sep='', end='\n', file=SKIP_LOG )

                else:
                    
                    current_row = dict()

                    for field_name in self.cda_headers['specimen']:
                        
                        # id	days_to_collection	primary_disease_type	anatomical_site	source_material_type	specimen_type	associated_project	derived_from_specimen	derived_from_subject

                        current_row[field_name] = ''

                    submitter_case_id = record['case_barcode']

                    specimen_id = record['sample_barcode']

                    current_row['id'] = specimen_id

                    if 'case_gdc_id' in record and record['case_gdc_id'] != '' and submitter_case_id not in printed_case_gdc_id:
                        
                        print( *[ submitter_case_id, record['case_gdc_id'] ], sep='\t', end='\n', file=CASE_TO_GDC )

                        printed_case_gdc_id.add( submitter_case_id )

                    if 'sample_gdc_id' in record and record['sample_gdc_id'] != '':
                        
                        print( *[ specimen_id, record['sample_gdc_id'] ], sep='\t', end='\n', file=SAMPLE_TO_GDC )

                    if 'days_to_collection' in record and record['days_to_collection'] != '':
                        
                        current_row['days_to_collection'] = int( record['days_to_collection'] )

                    if 'sample_type_name' in record and record['sample_type_name'] != '':
                        
                        current_row['source_material_type'] = record['sample_type_name']

                    subject_id = submitter_case_id_to_subject_id[submitter_case_id]

                    current_row['associated_project'] = submitter_case_id_to_collection_id[submitter_case_id]

                    current_row['derived_from_subject'] = subject_id

                    rs_id = subject_id + '.RS'

                    if rs_id not in rs_to_specimen:
                        
                        rs_to_specimen[rs_id] = set()

                    rs_to_specimen[rs_id].add( specimen_id )

                    print( *[ current_row[field_name] for field_name in self.cda_headers['specimen'] ], sep='\t', end='\n', file=SPECIMEN )

                    print( *[ specimen_id, 'IDC', 'sample_barcode', specimen_id ], sep='\t', end='\n', file=SPECIMEN_ID )

        with gzip.open( self.output_files['rs_specimen'], 'wt' ) as RS_SPECIMEN:
            
            print( *[ 'researchsubject_id', 'specimen_id' ], sep='\t', end='\n', file=RS_SPECIMEN )

            for rs_id in sorted( rs_to_specimen ):
                
                for specimen_id in sorted( rs_to_specimen[rs_id] ):
                    
                    print( *[ rs_id, specimen_id ], sep='\t', end='\n', file=RS_SPECIMEN )

        with gzip.open( self.aux_files['file_researchsubject'], 'rt' ) as IN, gzip.open( self.output_files['file_specimen'], 'wt' ) as FILE_SPECIMEN:
            
            print( *[ 'file_id', 'specimen_id' ], sep='\t', end='\n', file=FILE_SPECIMEN )

            header = next( IN )

            for line in [ next_line.rstrip('\n') for next_line in IN ]:
                
                [ file_id, rs_id ] = line.split('\t')

                if rs_id in rs_to_specimen:
                    
                    for specimen_id in sorted( rs_to_specimen[rs_id] ):
                        
                        print( *[ file_id, specimen_id ], sep='\t', end='\n', file=FILE_SPECIMEN )


