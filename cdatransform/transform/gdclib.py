"""
Transforms specific to GDC data structures
"""
from copy import deepcopy


# gdc.research_subject ------------------------------------------
from cdatransform.transform.validate import LogValidation


def research_subject(tip, orig, log: LogValidation, **kwargs: object) -> object:
    """Convert fields needed for ResearchSubject"""
    demog = orig.get("demographic")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}

    res_subj = {
        "id": orig.get("case_id"),
        "identifier": orig.get("submitter_id"),
        "ethnicity": demog.get("ethnicity"),
        "sex": demog.get("gender"),
        "race": demog.get("race"),
        "primary_disease_type": orig.get("disease_type"),
        "primary_disease_site": orig.get("primary_site"),
        "Project": {
            "label": orig.get("project", {}).get("project_id")
        }
    }
    for field in ["ethnicity", "sex", "race", "primary_disease_type", "primary_disease_site"]:
        log.distinct(res_subj, field)
    log.agree(res_subj, res_subj["id"], ["ethnicity", "sex", "race"])
    tip.update(res_subj)

    return tip


# gdc.diagnosis --------------------------------------------------

def diagnosis(tip, orig, log: LogValidation, **kwargs):
    """Convert fields needed for Diagnosis"""
    tip["Diagnosis"] = deepcopy(orig.get("diagnoses", []))
    for d in tip["Diagnosis"]:
        d["id"] = d.pop("diagnosis_id")
        d["Treatment"] = []
        for field in ["primary_diagnosis", "tumor_grade", "tumor_stage", "morphology"]:
            log.distinct(d, field)

    return tip


# gdc.entity_to_specimen -----------------------------------------

def entity_to_specimen(transform_in_progress, original, log: LogValidation, **kwargs):
    """Convert samples, portions and aliquots to specimens"""
    transform_in_progress["Specimen"] = [
        specimen_from_entity(*s)
        for s in get_entities(original)
    ]
    for specimen in transform_in_progress["Specimen"]:
        for field in ["primary_disease_type", "source_material_type", "anatomical_site"]:
            log.distinct(specimen, field)
        # days to birth is negative days from birth until diagnosis. 7300 days is 200 years.
        log.validate(specimen, "days_to_birth", lambda x: not x or -73000 < x < 0)

    return transform_in_progress


def get_entities(original):
    for sample in original.get("samples", []):
        yield (sample, "sample", "Initial specimen", sample, original)
        for portion in sample.get("portions", []):
            yield (portion, "portion", sample.get("sample_id"), sample, original)
            for slide in portion.get("slides", []):
                yield (slide, "slide", portion.get("portion_id"), sample, original)
            for analyte in portion.get("analytes", []):
                yield (analyte, "analyte", portion.get("portion_id"), sample, original)
                for aliquot in analyte.get("aliquots", []):
                    yield (aliquot, "aliquot", analyte.get("analyte_id"), sample, original)


def specimen_from_entity(entity, _type, parent_id, sample, case):
    id_key = f"{_type}_id"
    return {
        "derived_from_subject": case.get("submitter_id"),
        "identifier": [{"value": entity.get(id_key), "system": "GDC"}],
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "source_material_type": sample.get("sample_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "days_to_birth": case.get("demographic", {}).get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id,
        "CDA_context": "GDC"
    }


# gdc.files -------------------------------------------------------

def add_files(transform_in_progress, original, log: LogValidation, **kwargs):
    transform_in_progress["File"] = [
        f for f in original.get("files", [])
    ]
    return transform_in_progress    
