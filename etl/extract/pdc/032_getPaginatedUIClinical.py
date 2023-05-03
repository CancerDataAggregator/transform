#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

# SUBROUTINES

def sort_file_with_header( file_path ):
    
    with open(file_path) as IN:
        
        header = next(IN).rstrip('\n')

        lines = [ line.rstrip('\n') for line in sorted(IN) ]

    if len(lines) > 0:
        
        with open( file_path + '.tmp', 'w' ) as OUT:
            
            print(header, sep='', end='\n', file=OUT)

            print(*lines, sep='\n', end='\n', file=OUT)

        rename(file_path + '.tmp', file_path)

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

uiclinical_out_dir = f"{output_root}/UIClinical"

uiclinical_tsv = f"{uiclinical_out_dir}/UIClinical.tsv"
uiclinical_exposures_tsv = f"{uiclinical_out_dir}/UIClinical.exposures.tsv"
uiclinical_family_histories_tsv = f"{uiclinical_out_dir}/UIClinical.family_histories.tsv"
uiclinical_follow_ups_tsv = f"{uiclinical_out_dir}/UIClinical.follow_ups.tsv"
uiclinical_treatments_tsv = f"{uiclinical_out_dir}/UIClinical.treatments.tsv"
uiclinical_samples_tsv = f"{uiclinical_out_dir}/UIClinical.samples.tsv"
uiclinical_externalReferences_tsv = f"{uiclinical_out_dir}/UIClinical.externalReferences.tsv"

# We pull these from multiple sources and don't want to clobber a single
# master list over multiple pulls, so we store these in the UIClinical directory
# to indicate that's where this version came from. This is half-normalization; will
# merge later during aggregation across the designated object groups:

# Case and Clinical and UIClinical
exposure_tsv = f"{uiclinical_out_dir}/Exposure.tsv"

# Case and Clinical and UIClinical
family_history_tsv = f"{uiclinical_out_dir}/FamilyHistory.tsv"

# Case and Clinical and UIClinical
follow_up_tsv = f"{uiclinical_out_dir}/FollowUp.tsv"

# Case and Clinical and UIClinical
treatment_tsv = f"{uiclinical_out_dir}/Treatment.tsv"

# Case and Clinical and UIClinical
sample_tsv = f"{uiclinical_out_dir}/Sample.tsv"
sample_aliquots_tsv = f"{uiclinical_out_dir}/Sample.aliquots.tsv"
sample_diagnoses_tsv = f"{uiclinical_out_dir}/Sample.diagnoses.tsv"
sample_case_tsv = f"{uiclinical_out_dir}/Sample.case.tsv"
sample_project_tsv = f"{uiclinical_out_dir}/Sample.project.tsv"

# Case and Biospecimen and Clinical and UIClinical
entity_reference_tsv = f"{uiclinical_out_dir}/EntityReference.tsv"

getPaginatedUIClinical_json_output_file = f"{json_out_dir}/getPaginatedUIClinical.json"

scalar_uiclinical_fields = (
    'case_id',
    'case_submitter_id',
    'external_case_id',
    'diagnosis_id',
    'diagnosis_uuid',
    'status',
    'disease_type',
    'primary_site',
    'consent_type',
    'days_to_consent',
    'ethnicity',
    'gender',
    'race',
    'vital_status',
    'days_to_birth',
    'days_to_death',
    'year_of_birth',
    'year_of_death',
    'age_at_index',
    'premature_at_birth',
    'weeks_gestation_at_birth',
    'age_is_obfuscated',
    'cause_of_death_source',
    'occupation_duration_years',
    'country_of_residence_at_enrollment',
    'age_at_diagnosis',
    'classification_of_tumor',
    'days_to_last_follow_up',
    'days_to_last_known_disease_status',
    'days_to_recurrence',
    'last_known_disease_status',
    'morphology',
    'primary_diagnosis',
    'progression_or_recurrence',
    'site_of_resection_or_biopsy',
    'tissue_or_organ_of_origin',
    'tumor_grade',
    'tumor_stage',
    'tumor_cell_content',
    'tumor_largest_dimension_diameter',
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
    'cause_of_death',
    'circumferential_resection_margin',
    'colon_polyps_history',
    'days_to_best_overall_response',
    'days_to_diagnosis',
    'days_to_hiv_diagnosis',
    'days_to_new_event',
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
    'who_cns_grade',
    'who_nte_grade',
    'auxiliary_data',
    'project_name',
    'program_name'
)

scalar_exposure_fields = (
    'exposure_id',
    'exposure_submitter_id',
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
    'years_smoked',
    'case_id',
    'case_submitter_id',
    'project_id',
    'project_submitter_id'
)

scalar_family_history_fields = (
    'family_history_id',
    'family_history_submitter_id',
    'relationship_type',
    'relationship_gender',
    'relationship_age_at_diagnosis',
    'relationship_primary_diagnosis',
    'relative_with_cancer_history',
    'relatives_with_cancer_history_count',
    'case_id',
    'case_submitter_id',
    'project_id',
    'project_submitter_id'
)

scalar_follow_up_fields = (
    'follow_up_id',
    'follow_up_submitter_id',
    'adverse_event',
    'barretts_esophagus_goblet_cells_present',
    'bmi',
    'cause_of_response',
    'comorbidity',
    'comorbidity_method_of_diagnosis',
    'days_to_adverse_event',
    'days_to_comorbidity',
    'days_to_follow_up',
    'days_to_progression',
    'days_to_progression_free',
    'days_to_recurrence',
    'diabetes_treatment_type',
    'disease_response',
    'dlco_ref_predictive_percent',
    'ecog_performance_status',
    'fev1_ref_post_bronch_percent',
    'fev1_ref_pre_bronch_percent',
    'fev1_fvc_pre_bronch_percent',
    'fev1_fvc_post_bronch_percent',
    'height',
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
    'weight',
    'adverse_event_grade',
    'aids_risk_factors',
    'body_surface_area',
    'cd4_count',
    'cdc_hiv_risk_factors',
    'days_to_imaging',
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
    'undescended_testis_history_laterality',
    'case_id',
    'case_submitter_id',
    'project_id',
    'project_submitter_id'
)

scalar_treatment_fields = (
    'treatment_id',
    'treatment_submitter_id',
    'treatment_type',
    'initial_disease_status',
    'days_to_treatment_end',
    'days_to_treatment_start',
    'regimen_or_line_of_therapy',
    'therapeutic_agents',
    'treatment_anatomic_site',
    'treatment_effect',
    'treatment_intent_type',
    'treatment_or_therapy',
    'treatment_outcome',
    'treatment_arm',
    'treatment_dose',
    'treatment_dose_units',
    'treatment_effect_indicator',
    'treatment_frequency',
    'chemo_concurrent_to_radiation',
    'number_of_cycles',
    'reason_treatment_ended',
    'route_of_administration',
    'case_id',
    'case_submitter_id',
    'project_id',
    'project_submitter_id'
)

scalar_sample_fields = (
    'sample_id',
    'sample_submitter_id',
    'gdc_sample_id',
    'sample_type',
    'sample_type_id',
    'sample_ordinal',
    'sample_is_ref',
    'status',
    'pool',
    'days_to_collection',
    'tissue_collection_type',
    'days_to_sample_procurement',
    'method_of_sample_procurement',
    'preservation_method',
    'time_between_clamping_and_freezing',
    'time_between_excision_and_freezing',
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
    'initial_weight',
    'current_weight',
    'freezing_method',
    'shortest_dimension',
    'intermediate_dimension',
    'longest_dimension',
    'passage_count',
    'catalog_reference',
    'distributor_reference',
    'pathology_report_uuid',
    'annotation',
    'case_submitter_id',
    'gdc_project_id'
)

scalar_entity_reference_fields = (
    'external_reference_id',
    'entity_id',
    'entity_type',
    'reference_id',
    'reference_type',
    'reference_entity_type',
    'reference_entity_alias',
    'reference_entity_location',
    'reference_resource_name',
    'reference_resource_shortname',
    'submitter_id_name',
    'annotation'
)

# EXECUTION

for output_dir in ( json_out_dir, uiclinical_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Open handles for output files to save TSVs describing the returned objects and (as needed) association TSVs
# enumerating containment relationships between objects and sub-objects as well as association TSVs enumerating
# relationships between objects and keyword-style dictionaries.
# 
# We can't always safely use the Python `with` keyword in cases like this because the Python interpreter
# enforces an arbitrary hard-coded limit on the total number of simultaneous indents, and each `with` keyword
# creates another indent, even if you use the
# 
#     with open(A) as A, open(B) as B, ...
# 
# (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
# have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
# indentation, especially when the syntax above should've avoided creating multiple nested indents in the first place.

### OLD COMMENT: Open files to save raw API output JSON plus TSVs describing:
### 
###     * the returned UIClinical, EntityReference, Sample, Exposure, FamilyHistory, FollowUp and Treatment objects
###     * association TSVs enumerating containment relationships between UIClinical records and sub-objects (e.g. UIClinical->Sample)
###     * association TSVs enumerating inter-object relationships for sub-objects (e.g. [UIClinical->]Sample->Project)
### 
### We can't use the Python `with` keyword here because the Python interpreter enforces an arbitrary hard-coded limit on the
### total number of simultaneous indents, and each `with` keyword creates a new one, even if you use the
### 
###     with open(A) as A, open(B) as B, ...
### 
### (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
### have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
### indentation, especially when the syntax above should've avoided adding indentation in the first place.

output_tsv_keywords = (
    'UICLINICAL',
    'UICLINICAL_EXPOSURES',
    'UICLINICAL_FAMILY_HISTORIES',
    'UICLINICAL_FOLLOW_UPS',
    'UICLINICAL_TREATMENTS',
    'UICLINICAL_SAMPLES',
    'UICLINICAL_EXTERNAL_REFERENCES',
    'EXPOSURE',
    'FAMILY_HISTORY',
    'FOLLOW_UP',
    'TREATMENT',
    'SAMPLE',
    'SAMPLE_ALIQUOTS',
    'SAMPLE_DIAGNOSES',
    'SAMPLE_CASE',
    'SAMPLE_PROJECT',
    'ENTITY_REFERENCE'
)

output_tsv_filenames = (
    uiclinical_tsv,
    uiclinical_exposures_tsv,
    uiclinical_family_histories_tsv,
    uiclinical_follow_ups_tsv,
    uiclinical_treatments_tsv,
    uiclinical_samples_tsv,
    uiclinical_externalReferences_tsv,
    exposure_tsv,
    family_history_tsv,
    follow_up_tsv,
    treatment_tsv,
    sample_tsv,
    sample_aliquots_tsv,
    sample_diagnoses_tsv,
    sample_case_tsv,
    sample_project_tsv,
    entity_reference_tsv
)

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

with open(getPaginatedUIClinical_json_output_file, 'w') as JSON:
    
    # Table headers.

    print( *scalar_uiclinical_fields, sep='\t', end='\n', file=output_tsvs['UICLINICAL'] )
    print( *scalar_exposure_fields, sep='\t', end='\n', file=output_tsvs['EXPOSURE'] )
    print( *scalar_family_history_fields, sep='\t', end='\n', file=output_tsvs['FAMILY_HISTORY'] )
    print( *scalar_follow_up_fields, sep='\t', end='\n', file=output_tsvs['FOLLOW_UP'] )
    print( *scalar_treatment_fields, sep='\t', end='\n', file=output_tsvs['TREATMENT'] )
    print( *scalar_sample_fields, sep='\t', end='\n', file=output_tsvs['SAMPLE'] )
    print( *scalar_entity_reference_fields, sep='\t', end='\n', file=output_tsvs['ENTITY_REFERENCE'] )

    # UIClinical records don't have their own IDs. The combination of case_id and diagnisis_id suffices for disambiguation at time of writing.
    print( *('case_id', 'diagnosis_id', 'exposure_id'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_EXPOSURES'] )
    print( *('case_id', 'diagnosis_id', 'family_history_id'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_FAMILY_HISTORIES'] )
    print( *('case_id', 'diagnosis_id', 'follow_up_id'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_FOLLOW_UPS'] )
    print( *('case_id', 'diagnosis_id', 'treatment_id'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_TREATMENTS'] )
    print( *('case_id', 'diagnosis_id', 'sample_id'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_SAMPLES'] )

    # entity_reference['external_reference_id'] is always null when retrieved via getPaginatedUIClinical() (and entity_reference['reference_entity_location'] has control characters in it). Hurray.
    print( *('case_id', 'diagnosis_id', 'reference_entity_location'), sep='\t', end='\n', file=output_tsvs['UICLINICAL_EXTERNAL_REFERENCES'] )

    print( *['sample_id', 'aliquot_id'], sep='\t', end='\n', file=output_tsvs['SAMPLE_ALIQUOTS'] )
    print( *['sample_id', 'diagnosis_id'], sep='\t', end='\n', file=output_tsvs['SAMPLE_DIAGNOSES'] )
    print( *['sample_id', 'case_id'], sep='\t', end='\n', file=output_tsvs['SAMPLE_CASE'] )
    print( *['sample_id', 'project_id'], sep='\t', end='\n', file=output_tsvs['SAMPLE_PROJECT'] )

    api_query_json = {
        'query': '''    {
            getPaginatedUIClinical( getAll: true ) {
                uiClinical {
                    ''' + '\n                    '.join(scalar_uiclinical_fields) + '''
                    exposures {
                        ''' + '\n                        '.join(scalar_exposure_fields) + '''
                    }
                    family_histories {
                        ''' + '\n                        '.join(scalar_family_history_fields) + '''
                    }
                    follow_ups {
                        ''' + '\n                        '.join(scalar_follow_up_fields) + '''
                    }
                    treatments {
                        ''' + '\n                        '.join(scalar_treatment_fields) + '''
                    }
                    samples {
                        ''' + '\n                        '.join(scalar_sample_fields) + '''
                        aliquots {
                            aliquot_id
                        }
                        diagnoses {
                            diagnosis_id
                        }
                        case {
                            case_id
                        }
                        project {
                            project_id
                        }
                    }
                    externalReferences {
                        ''' + '\n                        '.join(scalar_entity_reference_fields) + '''
                    }
                }
            }
        }'''
    }

    # Send the getPaginatedUIClinical() query to the API server.

    response = requests.post(api_url, json=api_query_json)

    # If the HTTP response code is not OK (200), dump the query, print the http
    # error result and exit.

    if not response.ok:
        
        print( api_query_json['query'], file=sys.stderr )

        response.raise_for_status()

    # Retrieve the server's JSON response as a Python object.

    result = json.loads(response.content)

    # Save a version of the returned data as JSON.

    print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

    # These repeat. Print each only once to EntityReference.tsv.

    seen_reference_entity_locations = set()

    # Parse the returned data and save to TSV.

    for uiclinical in result['data']['getPaginatedUIClinical']['uiClinical']:
        
        uiclinical_row = list()

        for field_name in scalar_uiclinical_fields:
            
            if uiclinical[field_name] is not None:
                
                # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                uiclinical_row.append(json.dumps(uiclinical[field_name]).strip('"'))

            else:
                
                uiclinical_row.append('')

        print( *uiclinical_row, sep='\t', end='\n', file=output_tsvs['UICLINICAL'] )

        if uiclinical['exposures'] is not None and len(uiclinical['exposures']) > 0:
            
            for exposure in uiclinical['exposures']:
                
                exposure_row = list()

                for field_name in scalar_exposure_fields:
                    
                    if exposure[field_name] is not None:
                        
                        exposure_row.append(exposure[field_name])

                    else:
                        
                        exposure_row.append('')

                print( *exposure_row, sep='\t', end='\n', file=output_tsvs['EXPOSURE'] )

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], exposure['exposure_id']), sep='\t', end='\n', file=output_tsvs['UICLINICAL_EXPOSURES'] )

        if uiclinical['family_histories'] is not None and len(uiclinical['family_histories']) > 0:
            
            for family_history in uiclinical['family_histories']:
                
                family_history_row = list()

                for field_name in scalar_family_history_fields:
                    
                    if family_history[field_name] is not None:
                        
                        family_history_row.append(family_history[field_name])

                    else:
                        
                        family_history_row.append('')

                print( *family_history_row, sep='\t', end='\n', file=output_tsvs['FAMILY_HISTORY'] )

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], family_history['family_history_id']), sep='\t', end='\n', file=output_tsvs['UICLINICAL_FAMILY_HISTORIES'] )

        if uiclinical['follow_ups'] is not None and len(uiclinical['follow_ups']) > 0:
            
            for follow_up in uiclinical['follow_ups']:
                
                follow_up_row = list()

                for field_name in scalar_follow_up_fields:
                    
                    if follow_up[field_name] is not None:
                        
                        follow_up_row.append(follow_up[field_name])

                    else:
                        
                        follow_up_row.append('')

                print( *follow_up_row, sep='\t', end='\n', file=output_tsvs['FOLLOW_UP'] )

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], follow_up['follow_up_id']), sep='\t', end='\n', file=output_tsvs['UICLINICAL_FOLLOW_UPS'] )

        if uiclinical['treatments'] is not None and len(uiclinical['treatments']) > 0:
            
            for treatment in uiclinical['treatments']:
                
                treatment_row = list()

                for field_name in scalar_treatment_fields:
                    
                    if treatment[field_name] is not None:
                        
                        treatment_row.append(treatment[field_name])

                    else:
                        
                        treatment_row.append('')

                print( *treatment_row, sep='\t', end='\n', file=output_tsvs['TREATMENT'] )

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], treatment['treatment_id']), sep='\t', end='\n', file=output_tsvs['UICLINICAL_TREATMENTS'] )

        if uiclinical['samples'] is not None and len(uiclinical['samples']) > 0:
            
            for sample in uiclinical['samples']:
                
                sample_row = list()

                for field_name in scalar_sample_fields:
                    
                    if sample[field_name] is not None:
                        
                        sample_row.append(sample[field_name])

                    else:
                        
                        sample_row.append('')

                print( *sample_row, sep='\t', end='\n', file=output_tsvs['SAMPLE'] )

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], sample['sample_id']), sep='\t', end='\n', file=output_tsvs['UICLINICAL_SAMPLES'] )

                if sample['aliquots'] is not None and len(sample['aliquots']) > 0:
                    
                    for aliquot in sample['aliquots']:
                        
                        print( *(sample['sample_id'], aliquot['aliquot_id']), sep='\t', end='\n', file=output_tsvs['SAMPLE_ALIQUOTS'] )

                if sample['diagnoses'] is not None and len(sample['diagnoses']) > 0:
                    
                    for diagnosis in sample['diagnoses']:
                        
                        print( *(sample['sample_id'], diagnosis['diagnosis_id']), sep='\t', end='\n', file=output_tsvs['SAMPLE_DIAGNOSES'] )

                if sample['case'] is not None:
                    
                    print( *(sample['sample_id'], sample['case']['case_id']), sep='\t', end='\n', file=output_tsvs['SAMPLE_CASE'] )

                if sample['project'] is not None:
                    
                    print( *(sample['sample_id'], sample['project']['project_id']), sep='\t', end='\n', file=output_tsvs['SAMPLE_PROJECT'] )

        if uiclinical['externalReferences'] is not None and len(uiclinical['externalReferences']) > 0:
            
            for entity_reference in uiclinical['externalReferences']:
                
                # entity_reference['external_reference_id'] is always null when retrieved via getPaginatedUIClinical(); entity_reference['reference_entity_location'] has control characters in it. Hurray.

                print( *(uiclinical['case_id'], uiclinical['diagnosis_id'], json.dumps(entity_reference['reference_entity_location']).strip('"')), sep='\t', end='\n', file=output_tsvs['UICLINICAL_EXTERNAL_REFERENCES'] )

                # Don't write duplicates to EntityReference.tsv.

                if entity_reference['reference_entity_location'] not in seen_reference_entity_locations:
                    
                    seen_reference_entity_locations.add(entity_reference['reference_entity_location'])

                    entity_reference_row = list()

                    for field_name in scalar_entity_reference_fields:
                        
                        if entity_reference[field_name] is not None:
                            
                            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                            entity_reference_row.append(json.dumps(entity_reference[field_name]).strip('"'))

                        else:
                            
                            entity_reference_row.append('')

                    print( *entity_reference_row, sep='\t', end='\n', file=output_tsvs['ENTITY_REFERENCE'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


