import gzip
import re
import sys

from os import path, rename

def add_to_map( association_map, id_one, id_two ):
    
    if id_one not in association_map:
        
        association_map[id_one] = set()

    association_map[id_one].add(id_two)

def associate_id_list_with_parent( parent, parent_id, list_field_name, list_element_id_field_name, association_map, reverse_column_order=False ):
    
    if list_field_name in parent:
        
        for item in parent[list_field_name]:
            
            if list_element_id_field_name not in item:
                
                sys.exit(f"FATAL: The given list element does not have the expected {list_element_id_field_name} field; aborting.")

            child_id = item[list_element_id_field_name]

            if not reverse_column_order:
                
                add_to_map( association_map, parent_id, child_id )

            else:
                
                add_to_map( association_map, child_id, parent_id )

def columns_to_count( data_source ):
    """
    Return a Python dict listing all columns for which we will provide
    count profiles, organized by data_source and table.
    """

    enumerable_columns = {
        
        'gdc': {
            
            "aliquot": [
                
                "analyte_type",
                "analyte_type_id",
                "no_matched_normal_low_pass_wgs",
                "no_matched_normal_targeted_sequencing",
                "no_matched_normal_wgs",
                "no_matched_normal_wxs",
                "selected_normal_low_pass_wgs",
                "selected_normal_targeted_sequencing",
                "selected_normal_wgs",
                "selected_normal_wxs",
                "state"
            ],
            "analysis": [
                
                "state",
                "workflow_type"
            ],
            "analyte": [
                
                "analyte_type",
                "analyte_type_id",
                "experimental_protocol_type",
                "normal_tumor_genotype_snp_match",
                "spectrophotometer_method",
                "state"
            ],
            "annotation": [
                
                "category",
                "classification",
                "state",
                "status"
            ],
            "archive": [
                
                "data_category",
                "data_format",
                "data_type",
                "state"
            ],
            "case": [
                
                "consent_type",
                "disease_type",
                "index_date",
                "lost_to_followup",
                "primary_site",
                "state"
            ],
            "center": [
                
                "center_type"
            ],
            "demographic": [
                
                "age_is_obfuscated",
                "cause_of_death",
                "country_of_residence_at_enrollment",
                "ethnicity",
                "gender",
                "race",
                "state",
                "vital_status",
                "year_of_birth",
                "year_of_death"
            ],
            "diagnosis": [
                
                "ajcc_clinical_m",
                "ajcc_clinical_n",
                "ajcc_clinical_stage",
                "ajcc_clinical_t",
                "ajcc_pathologic_m",
                "ajcc_pathologic_n",
                "ajcc_pathologic_stage",
                "ajcc_pathologic_t",
                "ajcc_staging_system_edition",
                "ann_arbor_b_symptoms",
                "ann_arbor_clinical_stage",
                "ann_arbor_extranodal_involvement",
                "ann_arbor_pathologic_stage",
                "best_overall_response",
                "burkitt_lymphoma_clinical_variant",
                "classification_of_tumor",
                "cog_neuroblastoma_risk_group",
                "cog_renal_stage",
                "cog_rhabdomyosarcoma_risk_group",
                "eln_risk_classification",
                "enneking_msts_grade",
                "enneking_msts_metastasis",
                "enneking_msts_tumor_site",
                "esophageal_columnar_dysplasia_degree",
                "esophageal_columnar_metaplasia_present",
                "figo_stage",
                "figo_staging_edition_year",
                "gastric_esophageal_junction_involvement",
                "goblet_cells_columnar_mucosa_present",
                "icd_10_code",
                "igcccg_stage",
                "inpc_grade",
                "inss_stage",
                "international_prognostic_index",
                "irs_group",
                "iss_stage",
                "last_known_disease_status",
                "laterality",
                "masaoka_stage",
                "metastasis_at_diagnosis",
                "method_of_diagnosis",
                "mitosis_karyorrhexis_index",
                "morphology",
                "ovarian_specimen_status",
                "ovarian_surface_involvement",
                "peritoneal_fluid_cytological_status",
                "pregnant_at_diagnosis",
                "primary_diagnosis",
                "primary_gleason_grade",
                "prior_malignancy",
                "prior_treatment",
                "progression_or_recurrence",
                "residual_disease",
                "satellite_nodule_present",
                "secondary_gleason_grade",
                "site_of_resection_or_biopsy",
                "state",
                "synchronous_malignancy",
                "tissue_or_organ_of_origin",
                "tumor_confined_to_organ_of_origin",
                "tumor_focality",
                "tumor_grade",
                "who_cns_grade",
                "wilms_tumor_histologic_subtype",
                "year_of_diagnosis"
            ],
            "diagnosis_has_site_of_involvement": [
                
                "site_of_involvement_id"
            ],
            "exposure": [
                
                "alcohol_days_per_week",
                "alcohol_drinks_per_day",
                "alcohol_history",
                "alcohol_intensity",
                "exposure_type",
                "parent_with_radiation_exposure",
                "secondhand_smoke_as_child",
                "state",
                "tobacco_smoking_onset_year",
                "tobacco_smoking_quit_year",
                "tobacco_smoking_status",
                "type_of_smoke_exposure",
                "type_of_tobacco_used"
            ],
            "family_history": [
                
                "relationship_gender",
                "relationship_primary_diagnosis",
                "relationship_type",
                "relative_with_cancer_history",
                "state"
            ],
            "file": [
                
                "access",
                "channel",
                "data_category",
                "data_format",
                "data_type",
                "error_type",
                "experimental_strategy",
                "msi_status",
                "platform",
                "state",
                "type",
                "wgs_coverage"
            ],
            "file_has_acl": [
                
                "acl_id"
            ],
            "follow_up": [
                
                "adverse_event",
                "adverse_event_grade",
                "aids_risk_factors",
                "barretts_esophagus_goblet_cells_present",
                "cdc_hiv_risk_factors",
                "comorbidity",
                "diabetes_treatment_type",
                "disease_response",
                "ecog_performance_status",
                "evidence_of_recurrence_type",
                "haart_treatment_indicator",
                "hormonal_contraceptive_use",
                "hysterectomy_type",
                "imaging_result",
                "imaging_type",
                "immunosuppressive_treatment_type",
                "karnofsky_performance_status",
                "menopause_status",
                "pancreatitis_onset_year",
                "pregnancy_outcome",
                "procedures_performed",
                "progression_or_recurrence",
                "progression_or_recurrence_anatomic_site",
                "progression_or_recurrence_type",
                "risk_factor",
                "risk_factor_treatment",
                "state"
            ],
            "molecular_test": [
                
                "aa_change",
                "antigen",
                "biospecimen_type",
                "chromosome",
                "clonality",
                "cytoband",
                "gene_symbol",
                "histone_family",
                "laboratory_test",
                "mismatch_repair_mutation",
                "molecular_analysis_method",
                "ploidy",
                "second_gene_symbol",
                "specialized_molecular_test",
                "state",
                "test_result",
                "test_units",
                "variant_type"
            ],
            "pathology_detail": [
                
                "additional_pathology_findings",
                "anaplasia_present",
                "anaplasia_present_type",
                "bone_marrow_malignant_cells",
                "consistent_pathology_review",
                "largest_extrapelvic_peritoneal_focus",
                "lymph_node_involved_site",
                "lymph_node_involvement",
                "lymphatic_invasion_present",
                "morphologic_architectural_pattern",
                "necrosis_present",
                "perineural_invasion_present",
                "peripancreatic_lymph_nodes_positive",
                "state",
                "vascular_invasion_present",
                "vascular_invasion_type"
            ],
            "portion": [
                
                "is_ffpe",
                "state"
            ],
            "program": [
                
                "dbgap_accession_number"
            ],
            "project": [
                
                "dbgap_accession_number",
                "releasable",
                "released",
                "state"
            ],
            "project_data_category_summary_data": [
                
                "data_category"
            ],
            "project_experimental_strategy_summary_data": [
                
                "experimental_strategy"
            ],
            "project_studies_disease_type": [
                
                "disease_type"
            ],
            "project_studies_primary_site": [
                
                "primary_site"
            ],
            "read_group": [
                
                "base_caller_name",
                "chipseq_antibody",
                "chipseq_target",
                "includes_spike_ins",
                "instrument_model",
                "is_paired_end",
                "library_preparation_kit_name",
                "library_preparation_kit_vendor",
                "library_preparation_kit_version",
                "library_selection",
                "library_strand",
                "library_strategy",
                "platform",
                "single_cell_library",
                "spike_ins_fasta",
                "state",
                "target_capture_kit",
                "target_capture_kit_name",
                "target_capture_kit_vendor",
                "to_trim_adapter_sequence"
            ],
            "read_group_qc": [
                
                "adapter_content",
                "basic_statistics",
                "encoding",
                "kmer_content",
                "overrepresented_sequences",
                "per_base_n_content",
                "per_base_sequence_content",
                "per_base_sequence_quality",
                "per_sequence_gc_content",
                "per_sequence_quality_score",
                "per_tile_sequence_quality",
                "sequence_duplication_levels",
                "sequence_length_distribution",
                "state",
                "workflow_type"
            ],
            "sample": [
                
                "biospecimen_anatomic_site",
                "biospecimen_laterality",
                "composition",
                "diagnosis_pathologically_confirmed",
                "freezing_method",
                "is_ffpe",
                "method_of_sample_procurement",
                "oct_embedded",
                "preservation_method",
                "sample_ordinal",
                "sample_type",
                "sample_type_id",
                "specimen_type",
                "state",
                "tissue_collection_type",
                "tissue_type",
                "tumor_code",
                "tumor_code_id",
                "tumor_descriptor"
            ],
            "slide": [
                
                "section_location",
                "state",
                "tissue_microarray_coordinates"
            ],
            "tissue_source_site": [
                
                "bcr_id",
                "code"
            ],
            "treatment": [
                
                "chemo_concurrent_to_radiation",
                "initial_disease_status",
                "number_of_cycles",
                "reason_treatment_ended",
                "regimen_or_line_of_therapy",
                "state",
                "therapeutic_agents",
                "treatment_anatomic_site",
                "treatment_frequency",
                "treatment_intent_type",
                "treatment_or_therapy",
                "treatment_outcome",
                "treatment_type"
            ]
        }

        # end enumerable_columns['gdc']
    }

    if data_source in enumerable_columns:
        
        return enumerable_columns[data_source]

    else:
        
        sys.exit( f"FATAL: data_source '{data_source}' not recognized; cannot provide list of enumerable columns." )

def get_safe_value( record, field_name ):
    
    if field_name in record:
        
        return record[field_name]

    else:
        
        sys.exit(f"FATAL: The given record does not have the expected {field_name} field; aborting.")

def get_unique_values_from_tsv_column( tsv_path, column_name ):
    
    if not path.exists( tsv_path ):
        
        sys.exit(f"FATAL: Can't find specified TSV \"{tsv_path}\"; aborting.\n")

    with open( tsv_path ) as IN:
        
        headers = next( IN ).rstrip( '\n' ).split( '\t' )

        if column_name not in headers:
            
            sys.exit( f"FATAL: TSV '{tsv_path}' has no column named '{column_name}'; aborting.\n" )

        values_seen = set()

        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
            
            row_dict = dict( zip( headers, line.split( '\t' ) ) )

            values_seen.add( row_dict[column_name] )

        return sorted( values_seen )

def load_qualified_id_association( input_file, qualifier_field_name, id_one_field_name, id_two_field_name ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
            
            values = line.split( '\t' )

            record = dict( zip( colnames, values ) )

            qualifier = record[qualifier_field_name]

            id_one = record[id_one_field_name]

            id_two = record[id_two_field_name]

            if qualifier not in result:
                
                result[qualifier] = dict()

            if id_one not in result[qualifier]:
                
                result[qualifier][id_one] = set()

            result[qualifier][id_one].add( id_two )

    return result

def load_tsv_as_dict( input_file, id_column_count=1 ):
    
    result = dict()

    with open( input_file ) as IN:
        
        colnames = next( IN ).rstrip( '\n' ).split( '\t' )

        if id_column_count == 1:
            
            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                values = line.split( '\t' )

                # First column is a unique ID column, according to id_column_count.
                # If this doesn't end up being true, only the last record will
                # be stored for any repeated ID.

                result[values[0]] = dict( zip( colnames, values ) )

        elif id_column_count < 1:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count cannot be less than 1 (your value: '{id_column_count}'). Aborting." )

        elif id_column_count > 2:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count values greater than 2 (your value: '{id_column_count}') are not currently supported. Aborting." )

        elif id_column_count != 2:
            
            sys.exit( f"load_tsv_as_dict(): FATAL: id_column_count value (your value: '{id_column_count}') must be a nonnegative integer. Aborting." )

        else:
            
            for line in [ next_line.rstrip( '\n' ) for next_line in IN ]:
                
                values = line.split( '\t' )

                key_one = values[0]

                key_two = values[1]

                if key_one not in result:
                    
                    result[key_one] = dict()

                # First two columns comprise a unique composite ID, according to id_column_count.
                # If this doesn't end up being true, only the last record will be stored for any repeated composite ID.

                result[key_one][key_two] = dict( zip( colnames, values ) )

    return result

def map_columns_one_to_one( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):
    
    return_map = dict()

    if gzipped:
        
        IN = gzip.open( input_file, 'rt' )

    else:
        
        IN = open( input_file )

    header = next(IN).rstrip('\n')

    column_names = header.split('\t')

    if from_field not in column_names or to_field not in column_names:
        
        sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        values = line.split('\t')

        current_from = ''

        current_to = ''

        passed_where = True

        if where_field is not None:
            
            passed_where = False

        for i in range( 0, len( column_names ) ):
            
            if column_names[i] == from_field:
                
                current_from = values[i]

            if column_names[i] == where_field:
                
                if where_value is not None and values[i] is not None and where_value == values[i]:
                    
                    passed_where = True

            if column_names[i] == to_field:
                
                current_to = values[i]

        if passed_where:
            
            return_map[current_from] = current_to

    IN.close()

    return return_map

def map_columns_one_to_many( input_file, from_field, to_field, gzipped=False ):
    
    return_map = dict()

    if gzipped:
        
        IN = gzip.open( input_file, 'rt' )

    else:
        
        IN = open( input_file )

    column_names = next(IN).rstrip('\n').split('\t')

    if from_field not in column_names or to_field not in column_names:
        
        sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

    for line in [ next_line.rstrip('\n') for next_line in IN ]:
        
        values = line.split('\t')

        current_from = ''

        current_to = ''

        for i in range( 0, len( column_names ) ):
            
            if column_names[i] == from_field:
                
                current_from = values[i]

            if column_names[i] == to_field:
                
                current_to = values[i]

        if current_from not in return_map:
            
            return_map[current_from] = set()

        return_map[current_from].add(current_to)

    IN.close()

    return return_map

def singularize( name ):
    
    if name in [ 'aliquots',
                 'analytes',
                 'annotations',
                 'cases',
                 'exposures',
                 'files',
                 'follow_ups',
                 'molecular_tests',
                 'pathology_details',
                 'portions',
                 'projects',
                 'read_groups',
                 'read_group_qcs',
                 'samples',
                 'slides',
                 'treatments' ]:
        
        return re.sub(r's$', r'', name)

    elif name == 'diagnoses':
        
        return 'diagnosis'

    elif name in [ 'data_categories',
                   'family_histories',
                   'experimental_strategies' ]:
        
        return re.sub(r'ies$', r'y', name)

    elif name == 'sites_of_involvement':
        
        return 'site_of_involvement'

    elif name == 'weiss_assessment_findings':
        
        return 'weiss_assessment_finding'

    else:
        
        return name

def sort_file_with_header( file_path, gzipped=False ):
    
    if not gzipped:
        
        with open( file_path ) as IN:
            
            header = next(IN).rstrip('\n')

            lines = [ line.rstrip('\n') for line in sorted( IN ) ]

        if len( lines ) > 0:
            
            with open( file_path + '.tmp', 'w' ) as OUT:
                
                print( header, sep='', end='\n', file=OUT )

                print( *lines, sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

    else:
        
        with gzip.open( file_path, 'rt' ) as IN:
            
            header = next(IN).rstrip('\n')

            lines = [ line.rstrip('\n') for line in sorted(IN) ]

        if len( lines ) > 0:
            
            with gzip.open( file_path + '.tmp', 'wt' ) as OUT:
                
                print( header, sep='', end='\n', file=OUT )

                print( *lines, sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

def write_association_pairs( association_map, tsv_filename, field_one_name, field_two_name ):
    
    sys.stderr.write(f"Making {tsv_filename}...")

    sys.stderr.flush()

    with open( tsv_filename, 'w' ) as OUT:
        
        print( *[field_one_name, field_two_name], sep='\t', file=OUT )
    
        for value_one in sorted(association_map):
            
            for value_two in sorted(association_map[value_one]):
                
                print( *[value_one, value_two], sep='\t', file=OUT )

    sys.stderr.write("done.\n")


