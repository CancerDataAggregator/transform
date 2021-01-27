"""
Transforms specific to PDC data structures
"""
from copy import deepcopy

# pdc.research_subject ------------------------------------------
def patient(tip, orig, **kwargs):
    """Convert fields needed for ResearchSubject"""
    demog = orig.get("demographics")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    patient = {
        "id": orig.get("case_id"),
        "ethnicity": demog.get("ethnicity"),
        "sex": demog.get("gender"),
        "race": demog.get("race"),
        "created_datetime": deomg.get("created_datetime"),
        "Research_Subject": dict({})
    }
    tip.update(patient)
    return tip
# pdc.research_subject ------------------------------------------

def research_subject(tip, orig, **kwargs):

    res_subj = {
        "id": orig.get("case_id"),
        "patient_id": orig.get("case_submitter_id"),
        "identifier": [{"value": orig.get("case_id"), "system": "PDC"}],
        "primary_disease_type": orig.get("disease_type"),
        "primary_disease_site": orig.get("primary_site"),
        "externalReferences": orig.get("externalReferences"),
        "Project": {
            "label": orig.get("project", {}).get("project_submitter_id")
        }
    }
    tip["Research_Subject"] = res_subj

    return tip

# pdc.diagnosis --------------------------------------------------

def diagnosis(tip, orig, **kwargs):
    """Convert fields needed for Diagnosis"""
    tip["Research_Subject"]["Diagnosis"] = deepcopy(orig.get("diagnoses", []))
    for d in tip["Research_Subject"]["Diagnosis"]:
        if "diagnosis_id" in d:
            d["id"] = d.pop("diagnosis_id")
        d["Treatment"] = []

    return tip


# pdc.entity_to_specimen -----------------------------------------

def entity_to_specimen(transform_in_progress, original, **kwargs):
    """Convert samples, portions and aliquots to specimens"""
    transform_in_progress["Research_Subject"]["Specimen"] = [
        specimen_from_entity(*s)
        for s in get_entities(original)
    ]
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
        "derived_from_subject": entity.get("submitter_id"),
        "id": entity.get(id_key),
        "identifier": [{"value": entity.get(id_key), "system": "PDC"}],
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "source_material_type": entity.get("sample_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "days_to_birth": demog.get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id,
        "CDA_context": "PDC"
    }


# pdc.files -------------------------------------------------------

def add_files(transform_in_progress, original, **kwargs):
    transform_in_progress["Research_Subject"]["File"] = [
        f for f in original.get("files", [])
    ]
    return transform_in_progress    

# pdc.aggregate_to_patient
