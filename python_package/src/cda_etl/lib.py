import gzip
import re
import requests
import sys

from datetime import datetime
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
            
            'aliquot': [
                
                'analyte_type',
                'analyte_type_id',
                'no_matched_normal_low_pass_wgs',
                'no_matched_normal_targeted_sequencing',
                'no_matched_normal_wgs',
                'no_matched_normal_wxs',
                'selected_normal_low_pass_wgs',
                'selected_normal_targeted_sequencing',
                'selected_normal_wgs',
                'selected_normal_wxs',
                'state',
                # Fields below this line were added at the request of DSS.
                'aliquot_quantity',
                'aliquot_volume'
            ],
            'analysis': [
                
                'state',
                'workflow_type'
            ],
            'analyte': [
                
                'analyte_type',
                'analyte_type_id',
                'experimental_protocol_type',
                'normal_tumor_genotype_snp_match',
                'spectrophotometer_method',
                'state',
                # Fields below this line were added at the request of DSS.
                'a260_a280_ratio'
            ],
            'annotation': [
                
                'category',
                'classification',
                'state',
                'status'
            ],
            'archive': [
                
                'data_category',
                'data_format',
                'data_type',
                'state'
            ],
            'case': [
                
                'consent_type',
                'disease_type',
                'index_date',
                'lost_to_followup',
                'primary_site',
                'state'
            ],
            'center': [
                
                'center_type'
            ],
            'demographic': [
                
                'age_is_obfuscated',
                'cause_of_death',
                'country_of_residence_at_enrollment',
                'ethnicity',
                'gender',
                'race',
                'state',
                'vital_status',
                'year_of_birth',
                'year_of_death'
            ],
            'diagnosis': [
                
                'ajcc_clinical_m',
                'ajcc_clinical_n',
                'ajcc_clinical_stage',
                'ajcc_clinical_t',
                'ajcc_pathologic_m',
                'ajcc_pathologic_n',
                'ajcc_pathologic_stage',
                'ajcc_pathologic_t',
                'ajcc_staging_system_edition',
                'ann_arbor_b_symptoms',
                'ann_arbor_clinical_stage',
                'ann_arbor_extranodal_involvement',
                'ann_arbor_pathologic_stage',
                'best_overall_response',
                'burkitt_lymphoma_clinical_variant',
                'classification_of_tumor',
                'cog_neuroblastoma_risk_group',
                'cog_renal_stage',
                'cog_rhabdomyosarcoma_risk_group',
                'eln_risk_classification',
                'enneking_msts_grade',
                'enneking_msts_metastasis',
                'enneking_msts_tumor_site',
                'esophageal_columnar_dysplasia_degree',
                'esophageal_columnar_metaplasia_present',
                'figo_stage',
                'figo_staging_edition_year',
                'gastric_esophageal_junction_involvement',
                'goblet_cells_columnar_mucosa_present',
                'icd_10_code',
                'igcccg_stage',
                'inpc_grade',
                'inss_stage',
                'international_prognostic_index',
                'irs_group',
                'iss_stage',
                'last_known_disease_status',
                'laterality',
                'masaoka_stage',
                'metastasis_at_diagnosis',
                'method_of_diagnosis',
                'mitosis_karyorrhexis_index',
                'morphology',
                'ovarian_specimen_status',
                'ovarian_surface_involvement',
                'peritoneal_fluid_cytological_status',
                'pregnant_at_diagnosis',
                'primary_diagnosis',
                'primary_gleason_grade',
                'prior_malignancy',
                'prior_treatment',
                'progression_or_recurrence',
                'residual_disease',
                'satellite_nodule_present',
                'secondary_gleason_grade',
                'site_of_resection_or_biopsy',
                'state',
                'synchronous_malignancy',
                'tissue_or_organ_of_origin',
                'tumor_confined_to_organ_of_origin',
                'tumor_focality',
                'tumor_grade',
                'who_cns_grade',
                'wilms_tumor_histologic_subtype',
                'year_of_diagnosis',
                # Fields below this line were added at the request of DSS.
                'cog_liver_stage',
                'enneking_msts_stage',
                'ensat_clinical_m',
                'ensat_pathologic_n',
                'ensat_pathologic_stage',
                'ensat_pathologic_t',
                'gleason_grade_group',
                'gleason_grade_tertiary',
                'gleason_score',
                'inrg_stage',
                'irs_stage',
                'ishak_fibrosis_score',
                'metastasis_at_diagnosis_site',
                'pediatric_kidney_staging',
                'perineural_invasion_present',
                'primary_disease',
                'tumor_grade_category',
                'tumor_regression_grade',
                'tumor_stage',
                'tumor_stage',
                'uicc_clinical_m',
                'uicc_clinical_n',
                'uicc_clinical_stage',
                'uicc_clinical_t',
                'uicc_pathologic_m',
                'uicc_pathologic_n',
                'uicc_pathologic_stage',
                'uicc_pathologic_t',
                'uicc_staging_system_edition',
                'vascular_invasion_present',
                'weiss_assessment_score',
                'who_nte_grade'
            ],
            'diagnosis_has_site_of_involvement': [
                
                'site_of_involvement_id'
            ],
            'exposure': [
                
                'alcohol_days_per_week',
                'alcohol_drinks_per_day',
                'alcohol_history',
                'alcohol_intensity',
                'exposure_type',
                'parent_with_radiation_exposure',
                'secondhand_smoke_as_child',
                'state',
                'tobacco_smoking_onset_year',
                'tobacco_smoking_quit_year',
                'tobacco_smoking_status',
                'type_of_smoke_exposure',
                'type_of_tobacco_used'
            ],
            'family_history': [
                
                'relationship_gender',
                'relationship_primary_diagnosis',
                'relationship_type',
                'relative_with_cancer_history',
                'state'
            ],
            'file': [
                
                'access',
                'channel',
                'data_category',
                'data_format',
                'data_type',
                'error_type',
                'experimental_strategy',
                'msi_status',
                'platform',
                'state',
                'type',
                'wgs_coverage',
                # Fields below this line were added at the request of DSS.
                'msi_score'
            ],
            'file_has_acl': [
                
                'acl_id'
            ],
            'follow_up': [
                
                'adverse_event',
                'adverse_event_grade',
                'aids_risk_factors',
                'barretts_esophagus_goblet_cells_present',
                'cdc_hiv_risk_factors',
                'comorbidity',
                'diabetes_treatment_type',
                'disease_response',
                'ecog_performance_status',
                'evidence_of_recurrence_type',
                'haart_treatment_indicator',
                'hormonal_contraceptive_use',
                'hysterectomy_type',
                'imaging_result',
                'imaging_type',
                'immunosuppressive_treatment_type',
                'karnofsky_performance_status',
                'menopause_status',
                'pancreatitis_onset_year',
                'pregnancy_outcome',
                'procedures_performed',
                'progression_or_recurrence',
                'progression_or_recurrence_anatomic_site',
                'progression_or_recurrence_type',
                'risk_factor',
                'risk_factor_treatment',
                'state'
            ],
            'molecular_test': [
                
                'aa_change',
                'antigen',
                'biospecimen_type',
                'chromosome',
                'clonality',
                'cytoband',
                'gene_symbol',
                'histone_family',
                'laboratory_test',
                'mismatch_repair_mutation',
                'molecular_analysis_method',
                'ploidy',
                'second_gene_symbol',
                'specialized_molecular_test',
                'state',
                'test_result',
                'test_units',
                'variant_type'
            ],
            'pathology_detail': [
                
                'additional_pathology_findings',
                'anaplasia_present',
                'anaplasia_present_type',
                'bone_marrow_malignant_cells',
                'consistent_pathology_review',
                'largest_extrapelvic_peritoneal_focus',
                'lymph_node_involved_site',
                'lymph_node_involvement',
                'lymphatic_invasion_present',
                'morphologic_architectural_pattern',
                'necrosis_present',
                'perineural_invasion_present',
                'peripancreatic_lymph_nodes_positive',
                'state',
                'vascular_invasion_present',
                'vascular_invasion_type'
            ],
            'portion': [
                
                'is_ffpe',
                'state'
            ],
            'program': [
                
                'dbgap_accession_number'
            ],
            'project': [
                
                'dbgap_accession_number',
                'releasable',
                'released',
                'state'
            ],
            'project_data_category_summary_data': [
                
                'data_category'
            ],
            'project_experimental_strategy_summary_data': [
                
                'experimental_strategy'
            ],
            'project_studies_disease_type': [
                
                'disease_type'
            ],
            'project_studies_primary_site': [
                
                'primary_site'
            ],
            'read_group': [
                
                'base_caller_name',
                'chipseq_antibody',
                'chipseq_target',
                'includes_spike_ins',
                'instrument_model',
                'is_paired_end',
                'library_preparation_kit_name',
                'library_preparation_kit_vendor',
                'library_preparation_kit_version',
                'library_selection',
                'library_strand',
                'library_strategy',
                'platform',
                'single_cell_library',
                'spike_ins_fasta',
                'state',
                'target_capture_kit',
                'target_capture_kit_name',
                'target_capture_kit_vendor',
                'to_trim_adapter_sequence'
            ],
            'read_group_qc': [
                
                'adapter_content',
                'basic_statistics',
                'encoding',
                'kmer_content',
                'overrepresented_sequences',
                'per_base_n_content',
                'per_base_sequence_content',
                'per_base_sequence_quality',
                'per_sequence_gc_content',
                'per_sequence_quality_score',
                'per_tile_sequence_quality',
                'sequence_duplication_levels',
                'sequence_length_distribution',
                'state',
                'workflow_type'
            ],
            'sample': [
                
                'biospecimen_anatomic_site',
                'biospecimen_laterality',
                'composition',
                'diagnosis_pathologically_confirmed',
                'freezing_method',
                'is_ffpe',
                'method_of_sample_procurement',
                'oct_embedded',
                'preservation_method',
                'sample_ordinal',
                'sample_type',
                'sample_type_id',
                'specimen_type',
                'state',
                'tissue_collection_type',
                'tissue_type',
                'tumor_code',
                'tumor_code_id',
                'tumor_descriptor',
                # Fields below this line were added at the request of DSS.
                'distributor_reference'
            ],
            'slide': [
                
                'section_location',
                'state',
                'tissue_microarray_coordinates'
            ],
            'tissue_source_site': [
                
                'bcr_id',
                'code'
            ],
            'treatment': [
                
                'chemo_concurrent_to_radiation',
                'initial_disease_status',
                'number_of_cycles',
                'reason_treatment_ended',
                'regimen_or_line_of_therapy',
                'state',
                'therapeutic_agents',
                'treatment_anatomic_site',
                'treatment_frequency',
                'treatment_intent_type',
                'treatment_or_therapy',
                'treatment_outcome',
                'treatment_type',
                # Fields below this line were added at the request of DSS.
                'drug_category'
            ]
        },

        # end enumerable_columns['gdc']

        'pdc': {
            
            'Aliquot/Aliquot': [
                
                'status',
                'aliquot_is_ref',
                'pool',
                'analyte_type',
                'concentration',
                'taxon',
                # Fields below this line were added at the request of DSS.
                'aliquot_quantity',
                'aliquot_volume'
            ],
            'AliquotRunMetadata/AliquotRunMetadata': [
                
                'label',
                'fraction',
                'replicate_number',
                'analyte'
            ],
            'Case/Case': [
                
                'case_is_ref',
                'consent_type',
                'index_date',
                'tissue_source_site_code',
                'lost_to_followup',
                'disease_type',
                'primary_site',
                'case_status'
            ],
            'Demographic/Demographic': [
                
                'ethnicity',
                'gender',
                'race',
                'country_of_residence_at_enrollment',
                'age_is_obfuscated',
                'year_of_birth',
                'premature_at_birth',
                'weeks_gestation_at_birth',
                'vital_status',
                'year_of_death',
                'cause_of_death',
                'cause_of_death_source'
            ],
            'Diagnosis/Diagnosis': [
                
                'classification_of_tumor',
                'last_known_disease_status',
                'disease_type',
                'morphology',
                'primary_diagnosis',
                'progression_or_recurrence',
                'site_of_resection_or_biopsy',
                'tissue_or_organ_of_origin',
                'tumor_grade',
                'tumor_stage',
                'prior_malignancy',
                'ajcc_clinical_m',
                'ajcc_clinical_n',
                'ajcc_clinical_stage',
                'ajcc_clinical_t',
                'ajcc_pathologic_m',
                'ajcc_pathologic_n',
                'ajcc_pathologic_stage',
                'ajcc_pathologic_t',
                'ajcc_staging_system_edition',
                'ann_arbor_b_symptoms',
                'ann_arbor_clinical_stage',
                'ann_arbor_extranodal_involvement',
                'ann_arbor_pathologic_stage',
                'best_overall_response',
                'burkitt_lymphoma_clinical_variant',
                'circumferential_resection_margin',
                'colon_polyps_history',
                'figo_stage',
                'hiv_positive',
                'hpv_positive_type',
                'hpv_status',
                'iss_stage',
                'laterality',
                'ldh_level_at_diagnosis',
                'ldh_normal_range_upper',
                'lymph_nodes_positive',
                'lymphatic_invasion_present',
                'method_of_diagnosis',
                'new_event_anatomic_site',
                'new_event_type',
                'overall_survival',
                'perineural_invasion_present',
                'prior_treatment',
                'progression_free_survival',
                'progression_free_survival_event',
                'residual_disease',
                'vascular_invasion_present',
                'year_of_diagnosis',
                'icd_10_code',
                'synchronous_malignancy',
                'anaplasia_present',
                'anaplasia_present_type',
                'child_pugh_classification',
                'cog_liver_stage',
                'cog_neuroblastoma_risk_group',
                'cog_renal_stage',
                'cog_rhabdomyosarcoma_risk_group',
                'enneking_msts_grade',
                'enneking_msts_metastasis',
                'enneking_msts_stage',
                'enneking_msts_tumor_site',
                'esophageal_columnar_dysplasia_degree',
                'esophageal_columnar_metaplasia_present',
                'first_symptom_prior_to_diagnosis',
                'gastric_esophageal_junction_involvement',
                'goblet_cells_columnar_mucosa_present',
                'gross_tumor_weight',
                'inpc_grade',
                'inpc_histologic_group',
                'inrg_stage',
                'inss_stage',
                'irs_group',
                'irs_stage',
                'ishak_fibrosis_score',
                'lymph_nodes_tested',
                'medulloblastoma_molecular_classification',
                'metastasis_at_diagnosis',
                'metastasis_at_diagnosis_site',
                'mitosis_karyorrhexis_index',
                'peripancreatic_lymph_nodes_positive',
                'peripancreatic_lymph_nodes_tested',
                'supratentorial_localization',
                'tumor_confined_to_organ_of_origin',
                'tumor_focality',
                'tumor_regression_grade',
                'vascular_invasion_type',
                'wilms_tumor_histologic_subtype',
                'breslow_thickness',
                'gleason_grade_group',
                'igcccg_stage',
                'international_prognostic_index',
                'largest_extrapelvic_peritoneal_focus',
                'masaoka_stage',
                'non_nodal_regional_disease',
                'non_nodal_tumor_deposits',
                'ovarian_specimen_status',
                'ovarian_surface_involvement',
                'percent_tumor_invasion',
                'peritoneal_fluid_cytological_status',
                'primary_gleason_grade',
                'secondary_gleason_grade',
                'weiss_assessment_score',
                'adrenal_hormone',
                'ann_arbor_b_symptoms_described',
                'diagnosis_is_primary_disease',
                'eln_risk_classification',
                'figo_staging_edition_year',
                'gleason_grade_tertiary',
                'gleason_patterns_percent',
                'margin_distance',
                'margins_involved_site',
                'pregnant_at_diagnosis',
                'satellite_nodule_present',
                'sites_of_involvement',
                'tumor_depth',
                'tumor_cell_content',
                'who_cns_grade',
                'who_nte_grade',
                'annotation'
            ],
            'Exposure/Exposure': [
                
                'exposure_type',
                'exposure_duration',
                'exposure_duration_years',
                'age_at_onset',
                'coal_dust_exposure',
                'asbestos_exposure',
                'respirable_crystalline_silica_exposure',
                'parent_with_radiation_exposure',
                'radon_exposure',
                'alcohol_days_per_week',
                'alcohol_drinks_per_day',
                'alcohol_history',
                'alcohol_intensity',
                'alcohol_type',
                'tobacco_smoking_status',
                'tobacco_smoking_onset_year',
                'tobacco_smoking_quit_year',
                'smokeless_tobacco_quit_age',
                'type_of_tobacco_used',
                'cigarettes_per_day',
                'tobacco_use_per_day',
                'pack_years_smoked',
                'environmental_tobacco_smoke_exposure',
                'secondhand_smoke_as_child',
                'marijuana_use_per_week',
                'type_of_smoke_exposure',
                'smoking_frequency',
                'time_between_waking_and_first_smoke',
                'years_smoked'
            ],
            'FamilyHistory/FamilyHistory': [
                
                'relationship_type',
                'relationship_gender',
                'relationship_primary_diagnosis',
                'relative_with_cancer_history',
                'relatives_with_cancer_history_count'
            ],
            'File/File.instrument': [
                
                'instrument'
            ],
            'File/File': [
                
                'file_type',
                'file_format',
                'data_category',
                'downloadable',
                'analyte',
                'fraction_number',
                'experiment_type',
                'access'
            ],
            'FollowUp/FollowUp': [
                
                'adverse_event',
                'barretts_esophagus_goblet_cells_present',
                'cause_of_response',
                'comorbidity',
                'comorbidity_method_of_diagnosis',
                'diabetes_treatment_type',
                'disease_response',
                'dlco_ref_predictive_percent',
                'ecog_performance_status',
                'fev1_ref_post_bronch_percent',
                'fev1_ref_pre_bronch_percent',
                'fev1_fvc_pre_bronch_percent',
                'fev1_fvc_post_bronch_percent',
                'hepatitis_sustained_virological_response',
                'hpv_positive_type',
                'karnofsky_performance_status',
                'menopause_status',
                'pancreatitis_onset_year',
                'progression_or_recurrence',
                'progression_or_recurrence_anatomic_site',
                'progression_or_recurrence_type',
                'reflux_treatment_type',
                'risk_factor',
                'risk_factor_treatment',
                'viral_hepatitis_serologies',
                'adverse_event_grade',
                'aids_risk_factors',
                'body_surface_area',
                'cd4_count',
                'cdc_hiv_risk_factors',
                'evidence_of_recurrence_type',
                'eye_color',
                'haart_treatment_indicator',
                'history_of_tumor',
                'history_of_tumor_type',
                'hiv_viral_load',
                'hormonal_contraceptive_type',
                'hormonal_contraceptive_use',
                'hormone_replacement_therapy_type',
                'hysterectomy_margins_involved',
                'hysterectomy_type',
                'imaging_result',
                'imaging_type',
                'immunosuppressive_treatment_type',
                'nadir_cd4_count',
                'pregnancy_outcome',
                'procedures_performed',
                'recist_targeted_regions_number',
                'recist_targeted_regions_sum',
                'scan_tracer_used',
                'undescended_testis_corrected',
                'undescended_testis_corrected_age',
                'undescended_testis_corrected_laterality',
                'undescended_testis_corrected_method',
                'undescended_testis_history',
                'undescended_testis_history_laterality'
            ],
            'Program/Program': [
                
                'program_id',
                'program_submitter_id',
                'name',
                'sponsor',
                'start_date',
                'end_date',
                'program_manager',
                'data_size_TB',
                'project_count',
                'study_count',
                'data_file_count'
            ],
            'Project/Project.experiment_type': [
                
                'experiment_type'
            ],
            'Project/Project': [
                
                'project_id',
                'project_submitter_id',
                'name'
            ],
            'Protocol/Protocol': [
                
                'experiment_type',
                'quantitation_strategy',
                'label_free_quantitation',
                'labeled_quantitation',
                'isobaric_labeling_reagent',
                'reporter_ion_ms_level',
                'starting_amount_uom',
                'digestion_reagent',
                'alkylation_reagent',
                'enrichment_strategy',
                'enrichment',
                'chromatography_dimensions_count',
                'one_d_chromatography_type',
                'two_d_chromatography_type',
                'fractions_analyzed_count',
                'column_type',
                'amount_on_column_uom',
                'column_length',
                'column_length_uom',
                'column_inner_diameter',
                'column_inner_diameter_uom',
                'particle_size',
                'particle_size_uom',
                'particle_type',
                'gradient_length',
                'gradient_length_uom',
                'instrument_make',
                'instrument_model',
                'dissociation_type',
                'ms1_resolution',
                'ms2_resolution',
                'dda_topn',
                'normalized_collision_energy',
                'acquistion_type',
                'dia_multiplexing',
                'dia_ims',
                'analytical_technique',
                'chromatography_instrument_make',
                'chromatography_instrument_model',
                'polarity',
                'reconstitution_solvent',
                'reconstitution_volume',
                'reconstitution_volume_uom',
                'internal_standards',
                'extraction_method',
                'ionization_mode',
                # Fields below this line were added at the request of DSS.
                'protocol_name',
                'document_name'
            ],
            'Sample/Sample': [
                
                'sample_type',
                'sample_type_id',
                'sample_ordinal',
                'sample_is_ref',
                'status',
                'pool',
                'tissue_collection_type',
                'method_of_sample_procurement',
                'preservation_method',
                'tumor_code',
                'tumor_code_id',
                'tumor_descriptor',
                'diagnosis_pathologically_confirmed',
                'tissue_type',
                'biospecimen_anatomic_site',
                'biospecimen_laterality',
                'distance_normal_to_tumor',
                'growth_rate',
                'composition',
                'freezing_method',
                'passage_count',
                'catalog_reference',
                'distributor_reference',
                'annotation',
                'taxon',
                # Fields below this line were added at the request of DSS.
                'initial_weight',
                'current_weight'
            ],
            'Study/Study.dbgap_id': [
                
                'dbgap_id'
            ],
            'Study/Study.disease_type': [
                
                'disease_type'
            ],
            'Study/Study.instrument': [
                
                'instrument'
            ],
            'Study/Study.primary_site': [
                
                'primary_site'
            ],
            'Study/Study': [
                
                'study_version',
                'is_latest_version',
                'embargo_date',
                'analytical_fraction',
                'experiment_type',
                'acquisition_type'
            ],
            'StudyRunMetadata/StudyRunMetadata': [
                
                'experiment_type',
                'fraction',
                'analyte',
                'replicate_number'
            ],
            'Treatment/Treatment': [
                
                'treatment_type',
                'initial_disease_status',
                'regimen_or_line_of_therapy',
                'therapeutic_agents',
                'treatment_anatomic_site',
                'treatment_effect',
                'treatment_intent_type',
                'treatment_or_therapy',
                'treatment_outcome',
                'treatment_arm',
                'treatment_dose_units',
                'treatment_effect_indicator',
                'treatment_frequency',
                'chemo_concurrent_to_radiation',
                'number_of_cycles',
                'reason_treatment_ended',
                'route_of_administration'
            ],
            'WorkflowMetadata/WorkflowMetadata': [
                
                'analytical_fraction',
                'experiment_type',
                'instrument',
                'phosphosite_localization',
                'hgnc_version',
                'gene_to_prot',
                'raw_data_processing',
                'raw_data_conversion',
                'sequence_database_search',
                'search_database_parameters',
                'ms1_data_analysis',
                'psm_report_generation',
                'cdap_reports',
                'mzidentml_refseq',
                'refseq_database_version',
                'mzidentml_uniprot',
                'uniport_database_version',
                'cptac_dcc_mzidentml',
                'cptac_galaxy_workflows',
                'cptac_galaxy_tools',
                'cptac_dcc_tools',
                'cptac_study_id'
            ]
        },

        # end enumerable_columns['pdc']

        'cds': {
            
            'diagnosis': [
                
                'disease_type',
                'vital_status',
                'primary_diagnosis',
                'primary_site',
                'tumor_grade',
                'tumor_stage_clinical_m',
                'tumor_stage_clinical_n',
                'tumor_stage_clinical_t',
                'morphology',
                'incidence_type',
                'progression_or_recurrence',
                'last_known_disease_status'
            ],
            'file': [
                
                'file_type',
                'file_description',
                'experimental_strategy_and_data_subtypes'
            ],
            'genomic_info': [
                
                'reference_genome_assembly',
                'library_strategy',
                'library_layout',
                'library_source',
                'library_selection',
                'platform',
                'instrument_model',
                'sequence_alignment_software'
            ],
            'image': [
                
                'de_identification_method_type',
                'embedding_medium',
                'image_modality',
                'imaging_assay_type',
                'imaging_equipment_manufacturer',
                'imaging_equipment_model',
                'imaging_protocol',
                'imaging_sofware',
                'immersion',
                'lens_numerical_aperture',
                'license',
                'nominal_magnification',
                'objective',
                'organ_or_tissue',
                'pyramid',
                'staining_method',
                'tissue_fixative',
                'tumor_tissue_type',
                'working_distance'
            ],
            'participant': [
                
                'race',
                'gender',
                'ethnicity'
            ],
            'program': [
                
                'program_name',
                'program_acronym',
                'program_short_description',
                'program_full_description',
                'program_external_url',
                'program_sort_order'
            ],
            'sample': [
                
                'sample_type',
                'sample_tumor_status',
                'sample_anatomic_site'
            ],
            'study': [
                
                'study_name',
                'study_acronym',
                'study_description',
                'short_description',
                'study_external_url',
                'phs_accession',
                'bioproject_accession',
                'index_date',
                'cds_requestor',
                'funding_agency',
                'funding_source_program_name',
                'grant_id',
                'clinical_trial_system',
                'clinical_trial_identifier',
                'clinical_trial_arm',
                'organism_species',
                'adult_or_childhood_study',
                'data_types',
                'file_types',
                'data_access_level',
                'cds_primary_bucket',
                'cds_secondary_bucket',
                'cds_tertiary_bucket',
                'study_data_types',
                'file_types_and_format',
                'size_of_data_being_uploaded_unit',
                'size_of_data_being_uploaded_original_unit',
                'study_access'
            ],
            'treatment': [
                
                'treatment_type',
                'treatment_outcome',
                'therapeutic_agents'
            ]
        },

        # end enumerable_columns['cds']

        'icdc': {
            
            'adverse_event/adverse_event': [
                
                'day_in_cycle',
                'existing_adverse_event',
                'ongoing_adverse_event',
                'adverse_event_term',
                'adverse_event_description',
                'adverse_event_grade',
                'adverse_event_grade_description',
                'adverse_event_agent_name',
                'adverse_event_agent_dose',
                'attribution_to_research',
                'attribution_to_ind',
                'attribution_to_disease',
                'attribution_to_commercial',
                'attribution_to_other',
                'other_attribution_description',
                'dose_limiting_toxicity',
                'unexpected_adverse_event'
            ],
            'agent/agent': [
                
                'medication'
            ],
            'agent_administration/agent_administration': [
                
                'medication',
                'route_of_administration',
                'medication_actual_units_of_measure',
                'medication_duration',
                'medication_duration_unit',
                'medication_duration_original',
                'medication_duration_original_unit',
                'medication_units_of_measure',
                'medication_actual_dose',
                'medication_actual_dose_unit',
                'medication_actual_dose_original',
                'medication_actual_dose_original_unit',
                'phase',
                'dose_level',
                'dose_level_unit',
                'dose_level_original',
                'dose_level_original_unit',
                'dose_units_of_measure',
                'medication_missed_dose',
                'missed_dose_amount',
                'missed_dose_amount_unit',
                'missed_dose_amount_original',
                'missed_dose_amount_original_unit',
                'missed_dose_units_of_measure',
                'medication_course_number',
                'comment'
            ],
            'biospecimen_source/biospecimen_source': [
                
                'biospecimen_repository_acronym',
                'biospecimen_repository_full_name'
            ],
            'cohort/cohort': [
                
                'cohort_description',
                'cohort_dose'
            ],
            'cycle/cycle': [
                
                'cycle_number'
            ],
            'demographic/demographic': [
                
                'breed',
                'additional_breed_detail',
                'patient_age_at_enrollment',
                'patient_age_at_enrollment_unit',
                'patient_age_at_enrollment_original',
                'patient_age_at_enrollment_original_unit',
                'sex',
                'weight',
                'weight_unit',
                'weight_original',
                'weight_original_unit',
                'neutered_indicator'
            ],
            'diagnosis/diagnosis': [
                
                'disease_term',
                'primary_disease_site',
                'stage_of_disease',
                'histology_cytopathology',
                'histological_grade',
                'best_response',
                'pathology_report',
                'treatment_data',
                'follow_up_data',
                'concurrent_disease',
                'concurrent_disease_type'
            ],
            'disease_extent/disease_extent': [
                
                'lesion_number',
                'lesion_site',
                'lesion_description',
                'previously_irradiated',
                'previously_treated',
                'measurable_lesion',
                'target_lesion',
                'measured_how',
                'longest_measurement',
                'longest_measurement_unit',
                'longest_measurement_original',
                'longest_measurement_original_unit',
                'evaluation_number',
                'evaluation_code'
            ],
            'enrollment/enrollment': [
                
                'registering_institution',
                'site_short_name',
                'veterinary_medical_center',
                'patient_subgroup'
            ],
            'file/file': [
                
                'file_type',
                'file_description',
                'file_format',
                'file_status'
            ],
            'follow_up/follow_up': [
                
                'patient_status',
                'explain_unknown_status',
                'contact_type',
                'treatment_since_last_contact',
                'physical_exam_performed',
                'physical_exam_changes'
            ],
            'image_collection/image_collection': [
                
                'image_type_included',
                'repository_name',
                'collection_access'
            ],
            'off_study/off_study': [
                
                'reason_off_study',
                'best_resp_vet_tx_tp_secondary_response',
                'best_resp_vet_tx_tp_best_response'
            ],
            'off_treatment/off_treatment': [
                
                'reason_off_treatment',
                'best_resp_vet_tx_tp_secondary_response',
                'best_resp_vet_tx_tp_best_response'
            ],
            'physical_exam/physical_exam': [
                
                'day_in_cycle',
                'body_system',
                'pe_finding',
                'pe_comment',
                'phase_pe',
                'assessment_timepoint'
            ],
            'prior_surgery/prior_surgery': [
                
                'procedure',
                'anatomical_site_of_surgery',
                'residual_disease',
                'therapeutic_indicator'
            ],
            'prior_therapy/prior_therapy': [
                
                'agent_name',
                'dose_schedule',
                'total_dose',
                'total_dose_unit',
                'total_dose_original',
                'total_dose_original_unit',
                'agent_units_of_measure',
                'best_response_to_prior_therapy',
                'nonresponse_therapy_type',
                'prior_therapy_type',
                'prior_steroid_exposure',
                'number_of_prior_regimens_steroid',
                'total_number_of_doses_steroid',
                'prior_nsaid_exposure',
                'number_of_prior_regimens_nsaid',
                'total_number_of_doses_nsaid',
                'tx_loc_geo_loc_ind_nsaid',
                'min_rsdl_dz_tx_ind_nsaids_treatment_pe',
                'therapy_type',
                'any_therapy',
                'number_of_prior_regimens_any_therapy',
                'total_number_of_doses_any_therapy',
                'treatment_performed_at_site',
                'treatment_performed_in_minimal_residual'
            ],
            'program/program': [
                
                'program_name',
                'program_acronym',
                'program_short_description',
                'program_full_description',
                'program_external_url',
                'program_sort_order'
            ],
            'registration/registration': [
                
                'registration_origin'
            ],
            'sample/sample': [
                
                'sample_site',
                'physical_sample_type',
                'general_sample_pathology',
                'tumor_sample_origin',
                'summarized_sample_type',
                'molecular_subtype',
                'specific_sample_pathology',
                'sample_chronology',
                'necropsy_sample',
                'tumor_grade',
                'length_of_tumor',
                'length_of_tumor_unit',
                'length_of_tumor_original',
                'length_of_tumor_original_unit',
                'width_of_tumor',
                'width_of_tumor_unit',
                'width_of_tumor_original',
                'width_of_tumor_original_unit',
                'volume_of_tumor',
                'volume_of_tumor_unit',
                'volume_of_tumor_original',
                'volume_of_tumor_original_unit',
                'percentage_tumor',
                'sample_preservation',
                'comment'
            ],
            'study/study': [
                
                'clinical_study_type',
                'study_disposition'
            ],
            'study_arm/study_arm': [
                
                'ctep_treatment_assignment_code'
            ],
            'study_site/study_site': [
                
                'site_short_name',
                'veterinary_medical_center',
                'registering_institution'
            ],
            'visit/visit': [
                
                'visit_number'
            ],
            'vital_signs/vital_signs': [
                
                'body_temperature',
                'body_temperature_unit',
                'body_temperature_original',
                'body_temperature_original_unit',
                'pulse',
                'pulse_unit',
                'pulse_original',
                'pulse_original_unit',
                'respiration_rate',
                'respiration_rate_unit',
                'respiration_rate_original',
                'respiration_rate_original_unit',
                'respiration_pattern',
                'systolic_bp',
                'systolic_bp_unit',
                'systolic_bp_original',
                'systolic_bp_original_unit',
                'pulse_ox',
                'pulse_ox_unit',
                'pulse_ox_original',
                'pulse_ox_original_unit',
                'patient_weight',
                'patient_weight_unit',
                'patient_weight_original',
                'patient_weight_original_unit',
                'body_surface_area',
                'body_surface_area_unit',
                'body_surface_area_original',
                'body_surface_area_original_unit',
                'modified_ecog',
                'ecg',
                'assessment_timepoint',
                'phase'
            ]
        },

        # end enumerable_columns['icdc']

        'idc': {
            
            'dicom_instance': [
                
                'SOPClassUID'
            ],
            'dicom_series': [
                
                'Modality',
                'PatientSpeciesDescription',
                'PatientSex',
                'EthnicGroup',
                'collection_id',
                'collection_tumorLocation'
            ],
            'original_collections_metadata': [
                
                'Program',
                'collection_id',
                'collection_name',
                'CancerTypes'
            ],
            'tcga_biospecimen_rel9': [
                
                'sample_type_name',
                'program_name',
                'project_short_name'
            ],
            'tcga_clinical_rel9': [
                
                'race',
                'gender',
                'vital_status',
                'year_of_diagnosis',
                'disease_code',
                'icd_o_3_histology',
                'neoplasm_histologic_grade',
                'clinical_stage',
                'pathologic_stage',
                'histological_type',
                'anatomic_neoplasm_subdivision',
                'ethnicity',
                'icd_10',
                'icd_o_3_site',
                'person_neoplasm_cancer_status',
                'tumor_tissue_site',
                'tumor_type'
            ]
        },

        # end enumerable_columns['idc']

        'cda': {
            
            'file': [
                
                'access',
                'format',
                'type',
                'category'
            ],
            'file_anatomic_site': [
                
                'anatomic_site'
            ],
            'file_tumor_vs_normal': [
                
                'tumor_vs_normal'
            ],
            'mutation': [
                
                'hugo_symbol',
                'entrez_gene_id',
                'hotspot',
                'ncbi_build',
                'chromosome',
                'variant_type',
                'mutation_status',
                'gene',
                'one_consequence',
                'hgnc_id',
                'primary_site'
            ],
            'observation': [
                
                'vital_status',
                'sex',
                'year_of_observation',
                'diagnosis',
                'morphology',
                'grade',
                'stage',
                'observed_anatomic_site',
                'resection_anatomic_site'
            ],
            'project': [
                
                'type',
                'name',
                'short_name'
            ],
            'subject': [
                
                'species',
                'year_of_birth',
                'year_of_death',
                'cause_of_death',
                'race',
                'ethnicity'
            ],
            'treatment': [
                
                'anatomic_site',
                'type',
                'therapeutic_agent'
            ]
        }

        # end enumerable_columns['cda']
    }

    if data_source in enumerable_columns:
        
        return enumerable_columns[data_source]

    else:
        
        sys.exit( f"FATAL: data_source '{data_source}' not recognized; cannot provide list of enumerable columns." )

def deduplicate_sorted_file_with_header( file_path, gzipped=False ):
    
    IN = open( path.join( file_path ) )

    OUT = open( path.join( file_path + '.tmp' ), 'w' )

    if gzipped:
        
        IN.close()

        OUT.close()

        IN = gzip.open( path.join( file_path ), 'rt' )

        OUT = gzip.open( path.join( file_path + '.tmp' ), 'wt' )

    header = next( IN ).rstrip( '\n' )

    print( header, file=OUT )

    last_line = ''

    line_count = 0

    for next_line in [ line.rstrip( '\n' ) for line in IN ]:
        
        line_count = line_count + 1

        if next_line != last_line:
            
            print( next_line, file=OUT )

        last_line = next_line

    IN.close()

    OUT.close()

    rename( file_path + '.tmp', file_path )

def deduplicate_and_sort_unsorted_file_with_header( file_path, gzipped=False, ignore_primary_id_field=False ):
    
    IN = open( path.join( file_path ) )

    OUT = open( path.join( file_path + '.tmp' ), 'w' )

    if gzipped:
        
        IN.close()

        OUT.close()

        IN = gzip.open( path.join( file_path ), 'rt' )

        OUT = gzip.open( path.join( file_path + '.tmp' ), 'wt' )

    header = next( IN ).rstrip( '\n' )

    print( header, file=OUT )

    if not ignore_primary_id_field:
        
        # Sort the set of lines (as unparsed strings) and make sure we don't repeat identical adjacent lines.

        last_line = ''

        line_count = 0

        for next_line in sorted( [ line.rstrip( '\n' ) for line in IN ] ):
            
            line_count = line_count + 1

            if next_line != last_line:
                
                print( next_line, file=OUT )

            last_line = next_line

    else:
        
        # Find the id field.

        column_names = header.split( '\t' )

        alias_index = None

        if 'id' in column_names:
            
            alias_index = column_names.index( 'id' )

        elif 'id_alias' in column_names:
            
            alias_index = column_names.index( 'id_alias' )

        else:
            
            sys.exit( f"\n   FATAL: deduplicate_and_sort_unsorted_file_with_header(): Parameter 'ignore_primary_id_field' was set to True, but the table encoded in '{file_path}' has neither 'id' nor 'id_alias' fields. Cannot continue.\n" )

        record_data = set()

        output_lines = set()

        for next_line in [ line.rstrip( '\n' ) for line in IN ]:
            
            record_string = '\t'.join( [ value for index, value in enumerate( next_line.split( '\t' ) ) if index != alias_index ] )

            if record_string not in record_data:
                
                output_lines.add( next_line )

            record_data.add( record_string )

        for next_line in sorted( output_lines ):
            
            print( next_line, file=OUT )

    IN.close()

    OUT.close()

    rename( file_path + '.tmp', file_path )

def get_cda_project_ancestors( project_in_project_map, project_id ):
    
    ancestor_projects = set()

    if project_id in project_in_project_map:
        
        for ancestor_project_id in project_in_project_map[project_id]:
            
            ancestor_projects.add( ancestor_project_id )

            if ancestor_project_id in project_in_project_map:
                
                # There should never be cycles in the project hierarchy, but if there are, this will never terminate.

                ancestor_projects = ancestor_projects | get_cda_project_ancestors( project_in_project_map, ancestor_project_id )

    return ancestor_projects

def get_column_metadata( table_name=None, column_name=None ):
    
    column_metadata = {
        
        'column_metadata': {
            
            'cda_table': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'cda_column': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'column_type': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'summary_display': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'fetch_rows_returns': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'process_before_display': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'virtual_table': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        },
        'release_metadata': {
            
            'cda_table': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'cda_column': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source_version': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source_extraction_date': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source_row_count': {
                
                'column_type': 'numeric',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source_unique_value_count': {
                
                'column_type': 'numeric',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source_null_count': {
                
                'column_type': 'numeric',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        },
        'project': {
            
            'id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'crdc_id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'type': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'name': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'short_name': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'file': {
            
            'id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'crdc_id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'name': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'description': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'drs_uri': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'access': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'size': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'format': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'type': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'category': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'subject': {
            
            'id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'crdc_id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'species': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'year_of_birth': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'year_of_death': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'cause_of_death': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'race': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'ethnicity': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'observation': {
            
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'subject_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'vital_status': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'sex': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'year_of_observation': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'diagnosis': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'morphology': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'grade': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'stage': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'observed_anatomic_site': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'resection_anatomic_site': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'treatment': {
            
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'subject_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'anatomic_site': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'type': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'therapeutic_agent': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'mutation': {
            
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'subject_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'hugo_symbol': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'entrez_gene_id': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'hotspot': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'ncbi_build': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'chromosome': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'variant_type': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'reference_allele': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'tumor_seq_allele1': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'tumor_seq_allele2': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'dbsnp_rs': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'mutation_status': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'transcript_id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'gene': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'one_consequence': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'hgnc_id': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'primary_site': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True
            },
            'case_barcode': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'case_id': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'sample_barcode_tumor': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'tumor_submitter_uuid': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'sample_barcode_normal': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'normal_submitter_uuid': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'aliquot_barcode_tumor': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'tumor_aliquot_uuid': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'aliquot_barcode_normal': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'matched_norm_aliquot_uuid': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': True
            },
            'data_at_cds': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_gdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_icdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_idc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_at_pdc': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source'
            },
            'data_source_count': {
                
                'column_type': 'numeric',
                'summary_display': True,
                'fetch_rows_returns': False,
                'process_before_display': 'data_source_count'
            }
        },
        'upstream_identifiers': {
            
            'cda_table': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'id_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'data_source': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False,
                'process_before_display': 'upstream_identifier_metadata'
            },
            'data_source_id_field_name': {
                
                'column_type': 'categorical',
                'summary_display': False,
                'fetch_rows_returns': False,
                'process_before_display': 'upstream_identifier_metadata'
            },
            'data_source_id_value': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False,
                'process_before_display': 'upstream_identifier_metadata'
            }
        },
        'file_describes_subject': {
            
            'file_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'subject_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        },
        'file_anatomic_site': {
            
            'file_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'anatomic_site': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True,
                'process_before_display': 'virtual_list',
                'virtual_table': 'file'
            }
        },
        'file_tumor_vs_normal': {
            
            'file_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'tumor_vs_normal': {
                
                'column_type': 'categorical',
                'summary_display': True,
                'fetch_rows_returns': True,
                'process_before_display': 'virtual_list',
                'virtual_table': 'file'
            }
        },
        'file_in_project': {
            
            'file_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'project_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        },
        'project_in_project': {
            
            'child_project_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'parent_project_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        },
        'subject_in_project': {
            
            'subject_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            },
            'project_alias': {
                
                'column_type': 'unbounded',
                'summary_display': False,
                'fetch_rows_returns': False
            }
        }
    }

    if table_name is None:
        
        return column_metadata

    elif table_name in column_metadata and column_name in column_metadata[table_name]:
        
        return column_metadata[table_name][column_name]

    else:
        
        return dict()

def get_current_date():
    
    return datetime.today().strftime( '%Y-%m-%d' )

def get_current_timestamp():
    
    return datetime.today().strftime( '%Y-%m-%d %I:%M:%S%p' )

def get_dbgap_study_metadata( dbgap_study_id ):
    
    # example general information page describing a dbGaP study:
    # 
    #    https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs000235

    # potential sanity checks:
    # 
    #    count the number of substudies of a given study: https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=phs000235
    # 
    #    name the parent study of a given study: https://www.ncbi.nlm.nih.gov/gap/advanced_search/?TERM=phs000527

    dbgap_base_web_url = 'https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id='

    dbgap_full_web_url = f"{dbgap_base_web_url}{dbgap_study_id}"

    # Make the http request.

    response = requests.get( dbgap_full_web_url )

    if not response.ok:
        
        response.raise_for_status()

    # Load the retrieved page as a string.

    response_string = str( response.content )

    # Replace newline codes so we can match patterns around line breaks, which appear unpredictably, at the whim of the dbGaP CGI coders.

    response_string = re.sub( r"^b'", r'', response_string )
    response_string = re.sub( r"'$", r'', response_string )
    response_string = re.sub( r'\\n', r' ', response_string )
    response_string = re.sub( r'\\t', r' ', response_string )

    # Parse study metdata from the retrieved page.

    study_name_match = re.search( r'<span id="study-name"[^>]*>(.*?)<\/span>', response_string )

    if study_name_match is not None:
        
        result = dict()

        result['study_id'] = dbgap_study_id

        result['study_name'] = study_name_match.group(1).strip()

        parent_study_id_match = re.search( r'A substudy of\s*<a href="#"\s*onclick="[^"]+\\\'study\.cgi\\\',\s*\\\'(phs[0-9]+).*?\\\'', response_string )

        if parent_study_id_match is not None:
            
            result['parent_study_id'] = parent_study_id_match.group(1).strip()

        substudy_block_match = re.search( r'<dl\s+class="report">.*?<dt>\s*Substudies\s*<\/dt>.*?<dd>\s*(.*?)\s*<\/dd>', response_string )

        if substudy_block_match is not None:
            
            substudy_ids = list()

            substudy_block = substudy_block_match.group(1)

            for substudy_id in re.findall( r'<td>(phs[0-9]+).*?<\/td>', substudy_block ):
                
                substudy_ids.append( substudy_id )

            result['substudy_ids'] = sorted( substudy_ids )

        # print( response_string )

        return result

    else:
        
        sys.exit( f"FATAL: Could not load study_name for accession '{phs_accession}' from dbGaP page {dbgap_full_web_url}; aborting." )

def get_idc_extraction_fields( ):
    
    target_field_lists_by_table = [
        
        {
            'auxiliary_metadata': [
                
                'idc_case_id',
                'submitter_case_id'
            ]
        },
        {
            'original_collections_metadata': [
                
                'Program',
                'collection_id',
                'collection_name',
                'CancerTypes'
            ]
        },
        {
            'tcga_biospecimen_rel9': [
                
                'sample_barcode',
                'case_barcode',
                'days_to_collection',
                'sample_type_name',
                'case_gdc_id',
                'sample_gdc_id',
                'program_name',
                'project_short_name'
            ]
        },
        {
            'tcga_clinical_rel9': [
                
                'case_barcode',
                'race',
                'gender',
                'days_to_birth',
                'vital_status',
                'year_of_diagnosis',
                'disease_code',
                'icd_o_3_histology',
                'neoplasm_histologic_grade',
                'clinical_stage',
                'pathologic_stage',
                'histological_type',
                'days_to_death',
                'case_gdc_id',
                'anatomic_neoplasm_subdivision',
                'ethnicity',
                'icd_10',
                'icd_o_3_site',
                'person_neoplasm_cancer_status',
                'tumor_tissue_site',
                'tumor_type'
            ]
        },
        {
            'dicom_all': [
                
                'SOPInstanceUID',
                'gcs_url',
                'SOPClassUID',
                'crdc_instance_uuid',
                'instance_size',
                'instance_hash',
                'Modality',
                'PatientSpeciesDescription',
                'PatientBirthDate',
                'PatientSex',
                'EthnicGroup',
                'crdc_series_uuid',
                'SeriesDescription',
                'collection_id',
                'PatientID',
                'idc_case_id',
                'collection_tumorLocation',
                'AnatomicRegionSequence',
                'SpecimenDescriptionSequence',
                'SegmentSequence',
                'RTROIObservationsSequence',
                'SharedFunctionalGroupsSequence'
            ]
        }
    ]

    return target_field_lists_by_table

def get_safe_value( record, field_name ):
    
    if field_name in record:
        
        return record[field_name]

    else:
        
        sys.exit( f"FATAL: The given record does not have the expected {field_name} field; aborting." )

def get_submitter_id_patterns_not_to_merge_across_projects( ):
    
    return [
        
        r'^[Rr][Ee][Ff]$',
        r'^P?[0-9]+$',
        r'[Pp]ooled [Ss]ample'
    ]

def get_tcga_study_name( study_abbreviation ):
    
    # Retrieved 2024-09-06 from https://gdc.cancer.gov/resources-tcga-users/tcga-code-tables/tcga-study-abbreviations

    tcga_study_abbreviation_to_study_name = {
        
        'LAML': 'Acute Myeloid Leukemia',
        'ACC': 'Adrenocortical carcinoma',
        'BLCA': 'Bladder Urothelial Carcinoma',
        'LGG': 'Brain Lower Grade Glioma',
        'BRCA': 'Breast invasive carcinoma',
        'CESC': 'Cervical squamous cell carcinoma and endocervical adenocarcinoma',
        'CHOL': 'Cholangiocarcinoma',
        'LCML': 'Chronic Myelogenous Leukemia',
        'COAD': 'Colon adenocarcinoma',
        'CNTL': 'Controls',
        'ESCA': 'Esophageal carcinoma',
        'FPPP': 'FFPE Pilot Phase II',
        'GBM': 'Glioblastoma multiforme',
        'HNSC': 'Head and Neck squamous cell carcinoma',
        'KICH': 'Kidney Chromophobe',
        'KIRC': 'Kidney renal clear cell carcinoma',
        'KIRP': 'Kidney renal papillary cell carcinoma',
        'LIHC': 'Liver hepatocellular carcinoma',
        'LUAD': 'Lung adenocarcinoma',
        'LUSC': 'Lung squamous cell carcinoma',
        'DLBC': 'Lymphoid Neoplasm Diffuse Large B-cell Lymphoma',
        'MESO': 'Mesothelioma',
        'MISC': 'Miscellaneous',
        'OV': 'Ovarian serous cystadenocarcinoma',
        'PAAD': 'Pancreatic adenocarcinoma',
        'PCPG': 'Pheochromocytoma and Paraganglioma',
        'PRAD': 'Prostate adenocarcinoma',
        'READ': 'Rectum adenocarcinoma',
        'SARC': 'Sarcoma',
        'SKCM': 'Skin Cutaneous Melanoma',
        'STAD': 'Stomach adenocarcinoma',
        'TGCT': 'Testicular Germ Cell Tumors',
        'THYM': 'Thymoma',
        'THCA': 'Thyroid carcinoma',
        'UCS': 'Uterine Carcinosarcoma',
        'UCEC': 'Uterine Corpus Endometrial Carcinoma',
        'UVM': 'Uveal Melanoma'
    }

    if study_abbreviation in tcga_study_abbreviation_to_study_name:
        
        return tcga_study_abbreviation_to_study_name[study_abbreviation]

    else:
        
        sys.exit( f"[get_tcga_study_name()] FATAL: Unknown TCGA study abbreviation '{study_abbreviation}' encountered; cannot continue, aborting. Please handle." )

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

def get_universal_value_deletion_patterns( ):
    
    # Enumerate (case-insensitive, space-collapsed) values (as regular expressions) that
    # should be deleted wherever they are found in search metadata.

    delete_everywhere = {
        
        r'-',
        r'--',
        r'999:unknown,tobecompletedlater',
        r'anonymized',
        r'clinical',
        r'genomics',
        r'histopathology',
        r'imageanalyses',
        r'invalidvalues',
        r'mixedhistology(pleasespecify)',
        r'n/a',
        r'notallowedtocollect',
        r'notapplicable',
        r'notdetermined',
        r'nototherwisespecified',
        r'notprovided',
        r'notreported',
        r'notreported/unknown',
        r'notreportedtocds',
        r'notspecifiedindata',
        r'null',
        r'other',
        r'patientrefused',
        r'removed',
        r'undefined',
        r'unk',
        r'unknown',
        r'unknown,tobecompletedlater',
        r'unknowntumorstatus',
        r'unspecified'
    }

    return delete_everywhere

def load_obo_file( input_file ):
    
    obo_data = dict()

    with open( input_file ) as IN:
        
        in_term = False

        current_term = dict()

        for next_line in IN:
            
            line = next_line.rstrip( '\n' )

            if line == '':
                
                if len( current_term ) > 0:
                    
                    obo_data[current_term['id']] = dict()

                    for property_name in current_term:
                        
                        if property_name != 'id':
                            
                            obo_data[current_term['id']][property_name] = current_term[property_name]

                current_term = dict()

                in_term = False

            elif line == r'[Term]':
                
                in_term = True

            elif in_term:
                
                match_result = re.search( r'^([^:]+):\s*(\S.*)\s*$', line )

                if match_result is not None:
                    
                    property_name = match_result.group(1)

                    property_value = match_result.group(2)

                    if property_name == 'id':
                        
                        current_term[property_name] = property_value

                    else:
                        
                        if property_name not in current_term:
                            
                            current_term[property_name] = set()

                        current_term[property_name].add( property_value )

    return obo_data

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

def map_columns_one_to_many( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):
    
    return_map = dict()

    if gzipped:
        
        IN = gzip.open( input_file, 'rt' )

    else:
        
        IN = open( input_file )

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    if from_field not in column_names or to_field not in column_names:
        
        sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

    if where_field is not None and where_field not in column_names:
        
        sys.exit( f"FATAL: Requested filter field (where_field='{where_field}') not found in specified input file '{input_file}'; aborting.\n" )

    for next_line in IN:
        
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )
        
        current_from = record[from_field]

        current_to = record[to_field]

        map_current = False

        if where_field is not None:
            
            current_where_value = record[where_field]

            if current_where_value == where_value:
                
                map_current = True

        else:
            
            map_current = True

        if map_current:
            
            if current_from not in return_map:
                
                return_map[current_from] = set()

            return_map[current_from].add( current_to )

    IN.close()

    return return_map

def map_columns_one_to_one( input_file, from_field, to_field, where_field=None, where_value=None, gzipped=False ):
    
    return_map = dict()

    if gzipped:
        
        IN = gzip.open( input_file, 'rt' )

    else:
        
        IN = open( input_file )

    column_names = next( IN ).rstrip( '\n' ).split( '\t' )

    if from_field not in column_names or to_field not in column_names:
        
        sys.exit( f"FATAL: One or both requested map fields ('{from_field}', '{to_field}') not found in specified input file '{input_file}'; aborting.\n" )

    if where_field is not None and where_field not in column_names:
        
        sys.exit( f"FATAL: Requested filter field (where_field='{where_field}') not found in specified input file '{input_file}'; aborting.\n" )

    for next_line in IN:
        
        record = dict( zip( column_names, next_line.rstrip( '\n' ).split( '\t' ) ) )

        current_from = record[from_field]

        current_to = record[to_field]

        map_current = False

        if where_field is not None:
            
            current_where_value = record[where_field]

            if current_where_value == where_value:
                
                map_current = True

        else:
            
            map_current = True

        if map_current:
            
            return_map[current_from] = current_to

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

def sort_and_uniquify_file_with_header( file_path, gzipped=False ):
    
    if not gzipped:
        
        with open( file_path ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            lines = set()

            for line in IN:
                
                lines.add( line.rstrip( '\n' ) )

        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print( header, sep='', end='\n', file=OUT )

            if len( lines ) > 0:
                
                print( *sorted( lines ), sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

    else:
        
        with gzip.open( file_path, 'rt' ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            lines = set()

            for line in IN:
                
                lines.add( line.rstrip( '\n' ) )

        with gzip.open( file_path + '.tmp', 'wt' ) as OUT:
            
            print( header, sep='', end='\n', file=OUT )

            if len( lines ) > 0:
                
                print( *sorted( lines ), sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

def sort_file_with_header( file_path, gzipped=False ):
    
    if not gzipped:
        
        with open( file_path ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            lines = [ line.rstrip( '\n' ) for line in sorted( IN ) ]

        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print( header, sep='', end='\n', file=OUT )

            if len( lines ) > 0:
                
                print( *lines, sep='\n', end='\n', file=OUT )

            rename( file_path + '.tmp', file_path )

    else:
        
        with gzip.open( file_path, 'rt' ) as IN:
            
            header = next( IN ).rstrip( '\n' )

            lines = [ line.rstrip( '\n' ) for line in sorted( IN ) ]

        with gzip.open( file_path + '.tmp', 'wt' ) as OUT:
            
            print( header, sep='', end='\n', file=OUT )

            if len( lines ) > 0:
                
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


