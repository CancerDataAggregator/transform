from string import Template


def query_all_cases():
    query = """{
  allCases(acceptDUA: true) {
    case_id
  }
}"""
    return query


def query_single_case_a(case_id):
    template = Template(
        """{
    case(case_id: "$case_id" acceptDUA: true) {
        case_id
        case_submitter_id
        project_submitter_id
        days_to_lost_to_followup
        disease_type
        index_date
        lost_to_followup
        primary_site
        consent_type
        days_to_consent
        demographics { 
            demographic_id
            ethnicity gender
            demographic_submitter_id
            race cause_of_death
            days_to_birth
            days_to_death
            vital_status
            year_of_birth
            year_of_death
            age_at_index
            premature_at_birth
            weeks_gestation_at_birth
            age_is_obfuscated
            cause_of_death_source
            occupation_duration_years
            country_of_residence_at_enrollment
            } 
        samples { 
            sample_id
            sample_submitter_id
            sample_type
            sample_type_id
            gdc_sample_id
            gdc_project_id
            biospecimen_anatomic_site
            composition current_weight
            days_to_collection
            days_to_sample_procurement
            diagnosis_pathologically_confirmed
            freezing_method initial_weight
            intermediate_dimension
            longest_dimension
            method_of_sample_procurement
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
            biospecimen_laterality
            catalog_reference
            distance_normal_to_tumor
            distributor_reference
            growth_rate passage_count
            sample_ordinal
            tissue_collection_type
            diagnoses { 
                diagnosis_id
                diagnosis_submitter_id
                annotation
            } 
            aliquots { 
                aliquot_id
                aliquot_submitter_id
                analyte_type
                aliquot_run_metadata {
                    aliquot_run_metadata_id
                } 
            } 
        } diagnoses { 
            diagnosis_id
            tissue_or_organ_of_origin
            age_at_diagnosis
            primary_diagnosis
            tumor_grade tumor_stage
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
            hpv_status
            iss_stage
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
            anaplasia_present
            anaplasia_present_type
            child_pugh_classification
            cog_liver_stage
            cog_neuroblastoma_risk_group
            cog_renal_stage
            cog_rhabdomyosarcoma_risk_group
            enneking_msts_grade
            enneking_msts_metastasis
            enneking_msts_stage
            enneking_msts_tumor_site
            esophageal_columnar_dysplasia_degree
            esophageal_columnar_metaplasia_present
            first_symptom_prior_to_diagnosis
            gastric_esophageal_junction_involvement
            goblet_cells_columnar_mucosa_present
            gross_tumor_weight
            inpc_grade inpc_histologic_group
            inrg_stage inss_stage
            irs_group
            irs_stage
            ishak_fibrosis_score
            lymph_nodes_tested
            medulloblastoma_molecular_classification
            metastasis_at_diagnosis
            metastasis_at_diagnosis_site
            mitosis_karyorrhexis_index
            peripancreatic_lymph_nodes_positive
            peripancreatic_lymph_nodes_tested
            supratentorial_localization
            tumor_confined_to_organ_of_origin
            tumor_focality
            tumor_regression_grade
            vascular_invasion_type
            wilms_tumor_histologic_subtype
            breslow_thickness
            gleason_grade_group
            igcccg_stage
            international_prognostic_index
            largest_extrapelvic_peritoneal_focus
            masaoka_stage
            non_nodal_regional_disease
            non_nodal_tumor_deposits
            ovarian_specimen_status
            ovarian_surface_involvement
            percent_tumor_invasion
            peritoneal_fluid_cytological_status
            primary_gleason_grade
            secondary_gleason_grade
            weiss_assessment_score
            adrenal_hormone
            ann_arbor_b_symptoms_described
            diagnosis_is_primary_disease
            eln_risk_classification
            figo_staging_edition_year
            gleason_grade_tertiary
            gleason_patterns_percent
            margin_distance
            margins_involved_site
            pregnant_at_diagnosis
            satellite_nodule_present
            sites_of_involvement
            tumor_depth
            who_cns_grade
            who_nte_grade
            samples { 
                sample_id
                sample_submitter_id
                annotation
            } 
        } 
    }
    }"""
    )

    blah = """case_id
    case_submitter_id
    disease_type
    primary_site
    project_submitter_id
    demographics {
      ethnicity
      gender
      race
      days_to_birth
      days_to_death
      cause_of_death
      vital_status
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
      method_of_diagnosis
    }
  }
}
"""
    # )
    query = template.substitute(case_id=case_id)
    return query


def query_single_case_b(case_id):
    template = Template(
        """{
    case(case_id: "$case_id" acceptDUA: true) {
        case_id
        exposures { 
            exposure_id 
            exposure_submitter_id 
            alcohol_days_per_week 
            alcohol_drinks_per_day 
            alcohol_history 
            alcohol_intensity 
            asbestos_exposure 
            cigarettes_per_day 
            coal_dust_exposure 
            environmental_tobacco_smoke_exposure 
            pack_years_smoked 
            radon_exposure 
            respirable_crystalline_silica_exposure 
            smoking_frequency 
            time_between_waking_and_first_smoke 
            tobacco_smoking_onset_year 
            tobacco_smoking_quit_year 
            tobacco_smoking_status 
            type_of_smoke_exposure 
            type_of_tobacco_used 
            years_smoked 
            age_at_onset 
            alcohol_type 
            exposure_duration 
            exposure_duration_years 
            exposure_type 
            marijuana_use_per_week 
            parent_with_radiation_exposure 
            secondhand_smoke_as_child 
            smokeless_tobacco_quit_age 
            tobacco_use_per_day
            } 
        follow_ups {
            follow_up_id 
            follow_up_submitter_id
            adverse_event 
            barretts_esophagus_goblet_cells_present 
            bmi 
            cause_of_response comorbidity 
            comorbidity_method_of_diagnosis 
            days_to_adverse_event 
            days_to_comorbidity 
            days_to_follow_up days_to_progression 
            days_to_progression_free 
            days_to_recurrence 
            diabetes_treatment_type 
            disease_response 
            dlco_ref_predictive_percent 
            ecog_performance_status 
            fev1_ref_post_bronch_percent 
            fev1_ref_pre_bronch_percent 
            fev1_fvc_pre_bronch_percent 
            fev1_fvc_post_bronch_percent 
            height hepatitis_sustained_virological_response 
            hpv_positive_type 
            karnofsky_performance_status 
            menopause_status 
            pancreatitis_onset_year 
            progression_or_recurrence 
            progression_or_recurrence_anatomic_site 
            progression_or_recurrence_type 
            reflux_treatment_type 
            risk_factor 
            risk_factor_treatment 
            viral_hepatitis_serologies 
            weight 
            adverse_event_grade 
            aids_risk_factors 
            body_surface_area 
            cd4_count 
            cdc_hiv_risk_factors 
            days_to_imaging 
            evidence_of_recurrence_type 
            eye_color 
            haart_treatment_indicator 
            history_of_tumor 
            history_of_tumor_type 
            hiv_viral_load 
            hormonal_contraceptive_type 
            hormonal_contraceptive_use 
            hormone_replacement_therapy_type 
            hysterectomy_margins_involved 
            hysterectomy_type 
            imaging_result 
            imaging_type 
            immunosuppressive_treatment_type 
            nadir_cd4_count 
            pregnancy_outcome 
            procedures_performed 
            recist_targeted_regions_number 
            recist_targeted_regions_sum 
            scan_tracer_used 
            undescended_testis_corrected 
            undescended_testis_corrected_age 
            undescended_testis_corrected_laterality 
            undescended_testis_corrected_method 
            undescended_testis_history 
            undescended_testis_history_laterality
            }
        }
    }"""
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
      case_submitter_id
    }
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query


def query_metadata_file(file_id):
    template = Template(
        """{
  fileMetadata(file_id: "$file_id" acceptDUA: true) {
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
      case_submitter_id
    }
  }
}"""
    )
    query = template.substitute(file_id=file_id)
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


def query_uifiles_paginated_total(offset, limit):
    template = Template(
        """{
  getPaginatedUIFile(offset: $offset limit: $limit) {
    total
  }
}"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query


def query_ui_file(file_id):
    template = Template(
        """{
    getPaginatedUIFile(file_id: "$file_id" acceptDUA: true) {
        uiFiles {
            file_id
            study_id
            pdc_study_id
            submitter_id_name
            embargo_date
            file_name
            study_run_metadata_submitter_id
            project_name
            data_category
            data_source
            file_type
            downloadable
            access
            md5sum
            file_size
        }
    }
    }"""
    )
    query = template.substitute(file_id=file_id)
    return query


def query_UIfiles_bulk(offset, limit):
    template = Template(
        """{
    getPaginatedUIFile(offset: $offset limit: $limit) {
        uiFiles {
            file_id
            study_id
            pdc_study_id
            submitter_id_name
            embargo_date
            file_name
            study_run_metadata_submitter_id
            project_name
            data_category
            data_source
            file_type
            downloadable
            access
            md5sum
            file_size
        }
    }
    }"""
    )
    query = template.substitute(offset=offset, limit=limit)
    return query


def query_study_files(pdc_study_id):
    template = Template(
        """{ filesPerStudy (pdc_study_id: "$pdc_study_id" acceptDUA: true) {
            study_id
            pdc_study_id
            study_submitter_id
            study_name
            file_id
            file_name
            file_submitter_id
            file_type md5sum
            file_location
            file_size
            data_category
            file_format
            signedUrl {
                url}
            } 
        }
"""
    )
    return template.substitute(pdc_study_id=pdc_study_id)


def make_all_programs_query():
    """
    Creates a graphQL string for querying the PDC API's allPrograms endpoint.
    :return: GraphQL query string
    """

    return """{allPrograms (acceptDUA: true) {program_id program_submitter_id name projects {project_id project_submitter_id name studies {pdc_study_id study_id study_submitter_id submitter_id_name analytical_fraction study_name disease_types primary_sites embargo_date experiment_type acquisition_type} }}}"""
    blah = """
    {
        allPrograms (acceptDUA: true) {
            program_id
            program_submitter_id
            name
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
    }"""
    )
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
        }"""
    )
    return template.substitute(
        pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit)
    )


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
        }"""
    )
    return template.substitute(
        pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit)
    )


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
            longest_dimension
            method_of_sample_procurement
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
    }"""
    )
    return template.substitute(
        pdc_study_id=pdc_study_id, offset=str(offset), limit=str(limit)
    )


def specimen_taxon(pdc_study_id):
    template = Template(
        """{
        biospecimenPerStudy (pdc_study_id: "$pdc_study_id" acceptDUA: true) {
        case_id
        taxon
        }
        }"""
    )
    return template.substitute(pdc_study_id=pdc_study_id)
