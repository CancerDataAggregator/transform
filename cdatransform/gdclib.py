"""
Transforms specific to GDC data structures
"""

def demographics(transform_in_progress, original):
    """Transform the demographic data."""
    demographic = original.get("demographic")
    if isinstance(demographic, list):
        demographic = demographic[0]
    elif demographic is None:
        demographic = {}

    transform_in_progress["id"] = original.get("case_id", "unknown")
    transform_in_progress["sex"] = demographic.get("gender", "unknown")
    transform_in_progress["days_to_birth"] = demographic.get("days_to_birth", "unknown")

    return transform_in_progress


def entity_to_specimen(transform_in_progress, original):
    """Convert samples, portions and aliquots to specimens"""
    transform_in_progress["specimen"] = [
        specimen_from_entity(*s)
        for s in get_entities(original)
    ]
    return transform_in_progress


def get_entities(original):
    for sample in original.get("samples", []):
        yield (sample, "sample", None)
        for portion in sample.get("portions", []):
            yield (portion, "portion", sample.get("sample_id"))
            for aliquot in portion.get("aliquots", []):
                yield (aliquot, "aliquot", portion.get("portion_id"))


def specimen_from_entity(entity, _type, parent_id):
    return {
        "sample": specimen_from_sample,
        "portion": specimen_from_portion,
        "aliquot": specimen_from_aliqot
    }.get(_type)(entity, parent_id)


def specimen_from_sample(entity, parent_id=None):
    return {
        "id": entity["sample_id"],
        "specimen_type": "sample",
        "derived_from": parent_id
    }


def specimen_from_portion(entity, parent_id):
    return {
        "id": entity["portion_id"],
        "specimen_type": "portion",
        "derived_from": parent_id
    }

def specimen_from_aliqot(entity, parent_id):
    return {
        "id": entity["aliquot_id"],
        "specimen_type": "aliquot",
        "derived_from": parent_id
    }
