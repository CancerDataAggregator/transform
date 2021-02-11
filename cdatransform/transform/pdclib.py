"""
Transforms specific to PDC data structures
"""
from copy import deepcopy

from cdatransform.transform.commonlib import constrain_research_subject


# pdc.patient ------------------------------------------

def patient(tip, orig, **kwargs):
    """Promote select case fields to Patient."""
    demog = orig.get("demographics")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    patient = {
        "id": orig.get("case_submitter_id"),
        "ethnicity": demog.get("ethnicity"),
        "sex": demog.get("gender"),
        "race": demog.get("race"),
    }
    tip.update(patient)
    return tip


# pdc.research_subject ------------------------------------------

def research_subject(tip, orig, **kwargs):
    res_subj = [
        {
            "id": orig.get("case_id"),
            "identifier": [{"value": orig.get("case_id"), "system": "PDC"}],
            "primary_disease_type": orig.get("disease_type"),
            "primary_disease_site": orig.get("primary_site"),
            "Project": {"label": orig.get("project_submitter_id")},
        }
    ]
    tip["ResearchSubject"] = res_subj

    return tip


# pdc.diagnosis --------------------------------------------------


def diagnosis(tip, orig, **kwargs):
    """Convert fields needed for Diagnosis"""
    constrain_research_subject(tip)

    harmonized_diagnosis = []
    diagnosis_fields = ["diagnosis_id", "age_at_diagnosis", "primary_diagnosis", "tumor_grade", "tumor_stage", "morphology"]
    for d in orig.get("diagnoses",[]):
        this_d = {
            f: d.get(f)
            for f in diagnosis_fields 
        }
        this_d["id"] = this_d.pop("diagnosis_id")

        this_d["Treatment"] = [
            {
                "outcome": treatment.get("treatment_outcome"),
                "type": treatment.get("treatment_type")
            }
            for treatment in d.get("treatments", [])
        ]
        
        harmonized_diagnosis += [this_d]

    # ResearchSubject is a list with one element at this stage
    research_subject[0]["Diagnosis"] = harmonized_diagnosis

    return tip


# pdc.entity_to_specimen -----------------------------------------

def entity_to_specimen(transform_in_progress, original, **kwargs):
    """Convert samples, portions and aliquots to specimens"""
    specimens = [
        specimen_from_entity(*s) for s in get_entities(original)
    ]

    transform_in_progress["ResearchSubject"][0]["Specimen"] = specimens
    return transform_in_progress


def get_entities(original):
    for sample in original.get("samples", []):
        yield (sample, "sample", "Initial specimen", sample, original)
        for aliquot in sample.get("aliquots", []):
            yield (aliquot, "aliquot", sample.get("sample_id"), sample, original)


def specimen_from_entity(entity, _type, parent_id, sample, case):
    id_key = f"{_type}_id"
    demog = case.get("demographics")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    return {
        "id": entity.get(id_key),
        "identifier": [{"value": entity.get(id_key), "system": "PDC"}],
        "derived_from_subject": case.get("submitter_id"),
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "source_material_type": entity.get("sample_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "days_to_birth": demog.get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id,
        "CDA_context": "PDC",
    }


# pdc.files -------------------------------------------------------

def add_files(transform_in_progress, original, **kwargs):
    files = [
        f for f in original.get("files", [])
    ]

    transform_in_progress["ResearchSubject"][0]["Files"] = files
    return transform_in_progress