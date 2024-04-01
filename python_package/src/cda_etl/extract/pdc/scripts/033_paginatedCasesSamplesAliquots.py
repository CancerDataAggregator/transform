#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

cases_samples_aliquots_out_dir = f"{output_root}/CasesSamplesAliquots"

# All of the following are extracted elsewhere; this call is for replication validation
# (and to see if we get anything not available via other queries), so all extracted
# data is localized to its own directory.

case_tsv = f"{cases_samples_aliquots_out_dir}/Case.tsv"
sample_tsv = f"{cases_samples_aliquots_out_dir}/Sample.tsv"
diagnosis_tsv = f"{cases_samples_aliquots_out_dir}/Diagnosis.tsv"
aliquot_tsv = f"{cases_samples_aliquots_out_dir}/Aliquot.tsv"
aliquot_run_metadata_tsv = f"{cases_samples_aliquots_out_dir}/AliquotRunMetadata.tsv"
protocol_tsv = f"{cases_samples_aliquots_out_dir}/Protocol.tsv"

case_samples_tsv = f"{cases_samples_aliquots_out_dir}/Case.samples.tsv"
sample_diagnoses_tsv = f"{cases_samples_aliquots_out_dir}/Sample.diagnoses.tsv"
sample_aliquots_tsv = f"{cases_samples_aliquots_out_dir}/Sample.aliquots.tsv"
aliquot_aliquot_run_metadata_tsv = f"{cases_samples_aliquots_out_dir}/Aliquot.aliquot_run_metadata.tsv"
aliquot_run_metadata_protocol_tsv = f"{cases_samples_aliquots_out_dir}/AliquotRunMetadata.protocol.tsv"

scalar_case_fields = (
    'case_id',
    'case_submitter_id',
    'case_is_ref',
    'consent_type',
    'days_to_consent',
    'index_date',
    'tissue_source_site_code',
    'lost_to_followup',
    'days_to_lost_to_followup',
    'disease_type',
    'primary_site',
    'count',
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

scalar_diagnosis_fields = (
    'diagnosis_id',
    'diagnosis_submitter_id',
    'diagnosis_uuid',
    'age_at_diagnosis',
    'classification_of_tumor',
    'days_to_last_follow_up',
    'days_to_last_known_disease_status',
    'days_to_recurrence',
    'last_known_disease_status',
    'disease_type',
    'morphology',
    'primary_diagnosis',
    'progression_or_recurrence',
    'site_of_resection_or_biopsy',
    'tissue_or_organ_of_origin',
    'tumor_grade',
    'tumor_stage',
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
    'tumor_cell_content',
    'who_cns_grade',
    'who_nte_grade',
    'auxiliary_data',
    'annotation',
    'cases_count',
    'case_id',
    'case_submitter_id',
    'external_case_id',
    'project_id',
    'project_submitter_id'
)

scalar_aliquot_fields = (
    'aliquot_id',
    'aliquot_submitter_id',
    'status',
    'aliquot_is_ref',
    'pool',
    'aliquot_quantity',
    'aliquot_volume',
    'amount',
    'analyte_type',
    'concentration',
    'sample_id',
    'sample_submitter_id',
    'case_id',
    'case_submitter_id'
)

scalar_aliquot_run_metadata_fields = (
    'aliquot_run_metadata_id',
    'aliquot_submitter_id',
    'label',
    'experiment_number',
    'fraction',
    'replicate_number',
    'date',
    'alias',
    'analyte',
    'aliquot_id'
)

scalar_protocol_fields = (
    'protocol_id',
    'protocol_submitter_id',
    'protocol_name',
    'protocol_date',
    'document_name',
    'experiment_type',
    'quantitation_strategy',
    'label_free_quantitation',
    'labeled_quantitation',
    'isobaric_labeling_reagent',
    'reporter_ion_ms_level',
    'starting_amount',
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
    'amount_on_column',
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
    'auxiliary_data',
    'cud_label',
    'study_id',
    'study_submitter_id',
    'pdc_study_id',
    'project_submitter_id',
    'program_id',
    'program_submitter_id'
)

offset = 0

offset_increment = 500

returned_nothing = False

# EXECUTION

for output_dir in ( json_out_dir, cases_samples_aliquots_out_dir ):
    
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

### OLD COMMENT: (none at this level)

output_tsv_keywords = [
    'CASE',
    'SAMPLE',
    'DIAGNOSIS',
    'ALIQUOT',
    'ARM',
    'PROTOCOL',
    'CASE_SAMPLES',
    'SAMPLE_DIAGNOSES',
    'SAMPLE_ALIQUOTS',
    'ALIQUOT_ARM',
    'ARM_PROTOCOL'
]

output_tsv_filenames = [
    case_tsv,
    sample_tsv,
    diagnosis_tsv,
    aliquot_tsv,
    aliquot_run_metadata_tsv,
    protocol_tsv,
    case_samples_tsv,
    sample_diagnoses_tsv,
    sample_aliquots_tsv,
    aliquot_aliquot_run_metadata_tsv,
    aliquot_run_metadata_protocol_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_case_fields, sep='\t', end='\n', file=output_tsvs['CASE'] )
print( *scalar_sample_fields, sep='\t', end='\n', file=output_tsvs['SAMPLE'] )
print( *scalar_diagnosis_fields, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )
print( *scalar_aliquot_fields, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )
print( *scalar_aliquot_run_metadata_fields, sep='\t', end='\n', file=output_tsvs['ARM'] )
print( *scalar_protocol_fields, sep='\t', end='\n', file=output_tsvs['PROTOCOL'] )

print( *('case_id', 'sample_id'), sep='\t', end='\n', file=output_tsvs['CASE_SAMPLES'] )
print( *('sample_id', 'diagnosis_id'), sep='\t', end='\n', file=output_tsvs['SAMPLE_DIAGNOSES'] )
print( *('sample_id', 'aliquot_id'), sep='\t', end='\n', file=output_tsvs['SAMPLE_ALIQUOTS'] )
print( *('aliquot_id', 'aliquot_run_metadata_id'), sep='\t', end='\n', file=output_tsvs['ALIQUOT_ARM'] )
print( *('aliquot_run_metadata_id', 'protocol_id'), sep='\t', end='\n', file=output_tsvs['ARM_PROTOCOL'] )

while not returned_nothing:
    
    api_query_json = {
        'query': '''            {
            paginatedCasesSamplesAliquots( ''' + f"offset: {offset}, limit: {offset_increment}" + ''', acceptDUA: true ) {
                casesSamplesAliquots {
                    ''' + '\n                        '.join(scalar_case_fields) + '''
                    samples {
                        ''' + '\n                            '.join(scalar_sample_fields) + '''
                        diagnoses {
                            ''' + '\n                                '.join(scalar_diagnosis_fields) + '''
                        }
                        aliquots {
                            ''' + '\n                                '.join(scalar_aliquot_fields) + '''
                            aliquot_run_metadata {
                                ''' + '\n                                    '.join(scalar_aliquot_run_metadata_fields) + '''
                                protocol {
                                    ''' + '\n                                        '.join(scalar_protocol_fields) + '''
                                }
                            }
                        }
                    }
                }
            }
        }'''
    }

    # Send the fileMetadata() query to the API server.

    response = requests.post(api_url, json=api_query_json)

    # If the HTTP response code is not OK (200), dump the query, print the http
    # error result and exit.

    if not response.ok:
        
        print( api_query_json['query'], file=sys.stderr )

        response.raise_for_status()

    # Retrieve the server's JSON response as a Python object.

    result = json.loads(response.content)

    if result['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots'] is None or len(result['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']) == 0:
        
        returned_nothing = True

    else:
        
        # Save a version of the returned data as JSON.

        upper_limit = offset + (len(result['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']) - 1)

        paginatedCasesSamplesAliquots_json_output_file = f"{json_out_dir}/paginatedCasesSamplesAliquots.{offset:06}-{upper_limit:06}.json"

        with open(paginatedCasesSamplesAliquots_json_output_file, 'w') as JSON:
            
            print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

        # Parse the returned data and save to TSV.

        for case in result['data']['paginatedCasesSamplesAliquots']['casesSamplesAliquots']:
            
            case_row = list()

            for field_name in scalar_case_fields:
                
                if case[field_name] is not None:
                    
                    case_row.append(case[field_name])

                else:
                    
                    case_row.append('')

            print( *case_row, sep='\t', end='\n', file=output_tsvs['CASE'] )

            if case['samples'] is not None and len(case['samples']) > 0:
                
                for sample in case['samples']:
                    
                    sample_row = list()

                    for field_name in scalar_sample_fields:
                        
                        if sample[field_name] is not None:
                            
                            sample_row.append(sample[field_name])

                        else:
                            
                            sample_row.append('')

                    print( *sample_row, sep='\t', end='\n', file=output_tsvs['SAMPLE'] )

                    print( *( case['case_id'], sample['sample_id'] ), sep='\t', end='\n', file=output_tsvs['CASE_SAMPLES'] )

                    if sample['diagnoses'] is not None and len(sample['diagnoses']) > 0:
                        
                        for diagnosis in sample['diagnoses']:
                            
                            diagnosis_row = list()

                            for field_name in scalar_diagnosis_fields:
                                
                                if diagnosis[field_name] is not None:
                                    
                                    # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                                    diagnosis_row.append(json.dumps(diagnosis[field_name]).strip('"'))

                                else:
                                    
                                    diagnosis_row.append('')

                            print( *diagnosis_row, sep='\t', end='\n', file=output_tsvs['DIAGNOSIS'] )

                            print( *( sample['sample_id'], diagnosis['diagnosis_id'] ), sep='\t', end='\n', file=output_tsvs['SAMPLE_DIAGNOSES'] )

                    if sample['aliquots'] is not None and len(sample['aliquots']) > 0:
                        
                        for aliquot in sample['aliquots']:
                            
                            aliquot_row = list()

                            for field_name in scalar_aliquot_fields:
                                
                                if aliquot[field_name] is not None:
                                    
                                    aliquot_row.append(aliquot[field_name])

                                else:
                                    
                                    aliquot_row.append('')

                            print( *aliquot_row, sep='\t', end='\n', file=output_tsvs['ALIQUOT'] )

                            print( *( sample['sample_id'], aliquot['aliquot_id'] ), sep='\t', end='\n', file=output_tsvs['SAMPLE_ALIQUOTS'] )

                            if aliquot['aliquot_run_metadata'] is not None and len(aliquot['aliquot_run_metadata']) > 0:
                                
                                for aliquot_run_metadata in aliquot['aliquot_run_metadata']:
                                    
                                    aliquot_run_metadata_row = list()

                                    for field_name in scalar_aliquot_run_metadata_fields:
                                        
                                        if aliquot_run_metadata[field_name] is not None:
                                            
                                            aliquot_run_metadata_row.append(aliquot_run_metadata[field_name])

                                        else:
                                            
                                            aliquot_run_metadata_row.append('')

                                    print( *aliquot_run_metadata_row, sep='\t', end='\n', file=output_tsvs['ARM'] )

                                    print( *( aliquot['aliquot_id'], aliquot_run_metadata['aliquot_run_metadata_id'] ), sep='\t', end='\n', file=output_tsvs['ALIQUOT_ARM'] )

                                    if aliquot_run_metadata['protocol'] is not None:
                                        
                                        protocol = aliquot_run_metadata['protocol']

                                        protocol_row = list()

                                        for field_name in scalar_protocol_fields:
                                            
                                            if protocol[field_name] is not None:
                                                
                                                # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

                                                protocol_row.append(json.dumps(protocol[field_name]).strip('"'))

                                            else:
                                                
                                                protocol_row.append('')

                                        print( *protocol_row, sep='\t', end='\n', file=output_tsvs['PROTOCOL'] )

                                        print( *( aliquot_run_metadata['aliquot_run_metadata_id'], protocol['protocol_id'] ), sep='\t', end='\n', file=output_tsvs['ARM_PROTOCOL'] )

        # Increment the paging offset in advance of the next query iteration.

        offset = offset + offset_increment

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


