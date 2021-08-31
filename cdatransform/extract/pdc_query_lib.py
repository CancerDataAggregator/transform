from string import Template


def query_all_cases():
    query = """{
  allCases(acceptDUA: true) {
    case_id
  }
}"""
    return query


def query_single_case(case_id):
    template = Template(
        """{
  case(case_id: "$case_id" acceptDUA: true) {
    case_id
    case_submitter_id
    disease_type
    primary_site
    project_submitter_id
    demographics {
      ethnicity
      gender
      race
      days_to_birth
    }
    samples {
      sample_id
      sample_submitter_id
      biospecimen_anatomic_site
      sample_type
      aliquots {
          aliquot_id
          aliquot_submitter_id
      }
    }
    diagnoses {
      age_at_diagnosis
      diagnosis_id
      tumor_grade
      tumor_stage
      morphology
      primary_diagnosis
    }
  }
}
"""
    )

    query = template.substitute(case_id=case_id)
    return query


def query_files_bulk(offset, limit):
    template = Template(
        """{
  fileMetadata(offset: $offset limit: $limit acceptDUA: true) {
    file_id
    file_name
    file_location
    md5sum
    file_size
    file_submitter_id
    data_category
    file_type
    file_format
    experiment_type
    aliquots {
      sample_id
      aliquot_id
      case_id
    }
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query


def query_files_paginated(offset, limit):
    template = Template(
        """{
  getPaginatedFiles(offset: $offset limit: $limit acceptDUA: true) {
    total
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query


def make_all_programs_query():
    """
    Creates a graphQL string for querying the PDC API's allPrograms endpoint.
    :return: GraphQL query string
    """
    return """{
        allPrograms (acceptDUA: true) {
            program_id
            program_submitter_id
            name
            start_date
            end_date
            program_manager
            projects {
                project_id
                project_submitter_id
                name
                studies {
                    pdc_study_id
                    study_id
                    study_submitter_id
                    submitter_id_name
                    analytical_fraction
                    experiment_type
                    acquisition_type
                    embargo_date
                    study_name
                    disease_types
                    primary_sites
                }
            }
        }
    }"""


def make_study_query(pdc_study_id):
    """
    Creates a graphQL string for querying the PDC API's study endpoint.
    :return: GraphQL query string
    """
    template = Template(
        """{
        study (pdc_study_id: "$pdc_study_id" acceptDUA: true) {
            study_id
            pdc_study_id
            study_name
            study_submitter_id
            analytical_fraction
            experiment_type
            disease_type
            primary_site
            embargo_date
        }
    }""")
    return template.substitute(pdc_study_id=pdc_study_id)


def case_demographics(pdc_study_id, offset, limit):
    template = Template(
        """{
        paginatedCaseDemographicsPerStudy (pdc_study_id: "$pdc_study_id" offset: $offset limit:
        $limit acceptDUA: true) {
        total
        caseDemographicsPerStudy {
            case_id
            case_submitter_id
            disease_type
            primary_site
            demographics {
                demographic_id
                ethnicity
                gender
                demographic_submitter_id
                race
                cause_of_death
                days_to_birth
                days_to_death
                vital_status
                year_of_birth
                year_of_death
                }
            }
            pagination {
                count
                sort
                from
                page
                total
                pages
                size
                }
            }
        }""")
    return template.substitute(pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit))


def case_diagnoses(pdc_study_id, offset, limit):
    template = Template(
        """{
        paginatedCaseDiagnosesPerStudy (pdc_study_id: "$pdc_study_id" offset: $offset limit:
        $limit acceptDUA: true) {
        total
        caseDiagnosesPerStudy {
            case_id
            case_submitter_id
            disease_type
            primary_site
            diagnoses {
                diagnosis_id
                tissue_or_organ_of_origin
                age_at_diagnosis
                primary_diagnosis
                tumor_grade
                tumor_stage
                diagnosis_submitter_id
                classification_of_tumor
                days_to_last_follow_up
                days_to_last_known_disease_status
                days_to_recurrence
                last_known_disease_status
                morphology
                progression_or_recurrence
                site_of_resection_or_biopsy
                prior_malignancy
                ajcc_clinical_m
                ajcc_clinical_n
                ajcc_clinical_stage
                ajcc_clinical_t
                ajcc_pathologic_m
                ajcc_pathologic_n
                ajcc_pathologic_stage
                ajcc_pathologic_t
                ann_arbor_b_symptoms
                ann_arbor_clinical_stage
                ann_arbor_extranodal_involvement
                ann_arbor_pathologic_stage
                best_overall_response
                burkitt_lymphoma_clinical_variant
                circumferential_resection_margin
                colon_polyps_history
                days_to_best_overall_response
                days_to_diagnosis
                days_to_hiv_diagnosis
                days_to_new_event
                figo_stage
                hiv_positive
                hpv_positive_type
                hpv_status iss_stage
                laterality
                ldh_level_at_diagnosis
                ldh_normal_range_upper
                lymph_nodes_positive
                lymphatic_invasion_present
                method_of_diagnosis
                new_event_anatomic_site
                new_event_type
                overall_survival
                perineural_invasion_present
                prior_treatment
                progression_free_survival
                progression_free_survival_event
                residual_disease
                vascular_invasion_present
                year_of_diagnosis
                icd_10_code
                synchronous_malignancy
                tumor_largest_dimension_diameter
                }
            }
            pagination {
                count
                sort
                from
                page
                total
                pages
                size
                }
            }
        }""")
    return template.substitute(pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit))


def case_samples(pdc_study_id, offset, limit):
    template = Template(
        """ {
    paginatedCasesSamplesAliquots(pdc_study_id: "$pdc_study_id" offset: $offset limit:
    $limit acceptDUA: true)
    {
    total
    casesSamplesAliquots {
        case_id
        case_submitter_id
        days_to_lost_to_followup
        disease_type
        index_date
        lost_to_followup
        primary_site
        samples {
            sample_id
            sample_submitter_id
            sample_type
            sample_type_id
            gdc_sample_id
            gdc_project_id
            biospecimen_anatomic_site
            composition
            current_weight
            days_to_collection
            days_to_sample_procurement
            diagnosis_pathologically_confirmed
            freezing_method
            initial_weight
            intermediate_dimension
            is_ffpe
            longest_dimension
            method_of_sample_procurement
            oct_embedded
            pathology_report_uuid
            preservation_method
            sample_type_id
            shortest_dimension
            time_between_clamping_and_freezing
            time_between_excision_and_freezing
            tissue_type
            tumor_code
            tumor_code_id
            tumor_descriptor
            aliquots {
                aliquot_id
                aliquot_submitter_id
                analyte_type
                aliquot_run_metadata {
                    aliquot_run_metadata_id
                    }
                }
            }
        }
    pagination {
        count
        sort
        from
        page
        total
        pages
        size
        }
    }
    }""")
    return template.substitute(pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit))
