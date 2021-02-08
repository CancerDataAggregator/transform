"""
Transforms specific to GDC data structures
"""
from copy import deepcopy

# gdc.patient ------------------------------------------
def patient(tip, orig, **kwargs):
    """Convert fields needed for ResearchSubject"""
    demog = orig.get("demographic")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    patient = {
        "id": orig.get("submitter_id"),
        "ethnicity": demog.get("ethnicity"),
        "sex": demog.get("gender"),
        "race": demog.get("race"),
    }
    tip.update(patient)
    return tip
# gdc.research_subject ------------------------------------------

def research_subject(tip, orig, **kwargs):
    
    res_subj = [{
        "id": orig.get("case_id"),
        "identifier": [{"value": orig.get("case_id"), "system": "GDC"}],
        "primary_disease_type": orig.get("disease_type"),
        "primary_disease_site": orig.get("primary_site"),
        "Project": {
            "label": orig.get("project", {}).get("project_id")
        }
    }]
    tip["Research_Subject"] = res_subj

    return tip

# gdc.diagnosis --------------------------------------------------

def diagnosis(tip, orig, **kwargs):
    """Convert fields needed for Diagnosis"""
    diag_field_map = dict({"diagnosis_id":"id","age_at_diagnosis":"age_at_diagnosis",
                           "primary_diagnosis":"primary_diagnosis","tumor_grade":"tumor_grade",
                           "tumor_stage":"tumor_stage", "morphology":"morphology"})
    treat_field_map = dict({"treatment_outcome":"outcome","treatment_type":"type"})
    
    #tip["Research_Subject"][0]["Diagnosis"] = deepcopy(orig.get("diagnoses", []))
    diag_rec_copy = deepcopy(orig.get("diagnoses", []))
    tip["Research_Subject"][0]["Diagnosis"] = []
    for d in diag_rec_copy:
        diag_entry = dict({})
        for field in diag_field_map:
            diag_entry[diag_field_map[field]] = d.get(field)
        diag_entry["Treatment"] = []
        treat_rec_copy = d.get("Treatments",[])
        for treat in treat_rec_copy:
            treat_entry = dict({})
            for field in treat_field_map:
                treat_entry[triet_field_map[field]] = treat.get(field)
            diag_entry["Treatment"].append(treat_entry)
        tip["Research_Subject"][0]["Diagnosis"].append(diag_entry)
    return tip


# gdc.entity_to_specimen -----------------------------------------

def entity_to_specimen(transform_in_progress, original, **kwargs):
    """Convert samples, portions and aliquots to specimens"""
    transform_in_progress["Research_Subject"][0]["Specimen"] = [
        specimen_from_entity(*s)
        for s in get_entities(original)
    ]
    return transform_in_progress


def get_entities(original):
    for sample in original.get("samples", []):
        yield (sample, "sample", "Initial specimen", sample, original)
        for portion in sample.get("portions", []):
            yield (portion, "portion", sample.get("sample_id"), sample, original)
            for aliquot in portion.get("aliquots", []):
                yield (aliquot, "aliquot", portion.get("portion_id"), sample, original)


def specimen_from_entity(entity, _type, parent_id, sample, case):
    id_key = f"{_type}_id"
    return {
        "derived_from_subject": case.get("submitter_id"),
        "id": entity.get(id_key),
        "identifier": [{"value": entity.get(id_key), "system": "GDC"}],
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "source_material_type": entity.get("sample_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "days_to_birth": case.get("demographic", {}).get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id,
        "CDA_context": "GDC"
    }


# gdc.files -------------------------------------------------------

def add_files(transform_in_progress, original, **kwargs):
    transform_in_progress["Research_Subject"][0]["File"] = [
        f for f in original.get("files", [])
    ]
    return transform_in_progress    
