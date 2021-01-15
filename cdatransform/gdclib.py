"""
Transforms specific to GDC data structures
"""

# gdc.research_subject ---------------------------------------------

def research_subject(tip, orig, **kwargs):
    """Convert fields needed for ResearchSubject"""
    demog = orig.get("demographic")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}

    tip["id"] = orig.get("case_id")
    tip["identifier"] = orig.get("submitter_id")
    tip["ethnicity"] = demog.get("ethnicity")
    tip["sex"] = demog.get("gender")
    tip["race"] = demog.get("race")
    tip["primary_disease_type"] = orig.get("disease_type")
    tip["primary_disease_site"] = orig.get("primary_site")
    tip["Project"] = {
        "label": orig.get("project", {}).get("project_id")
    }

    return tip

# gdc.entity_to_specimen -----------------------------------------

def entity_to_specimen(transform_in_progress, original, **kwargs):
    """Convert samples, portions and aliquots to specimens"""
    transform_in_progress["specimen"] = [
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
        "derived_from_subject": entity.get("submitter_id"),
        "identifier": entity.get(id_key),
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "days_to_birth": case.get("demographic", {}).get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id
    }
