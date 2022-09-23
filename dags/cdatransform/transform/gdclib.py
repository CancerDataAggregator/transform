"""
Transforms specific to GDC data structures
"""

from .commonlib import constrain_research_subject, lower
from .validate import LogValidation


# gdc.patient ------------------------------------------
def patient(tip, orig, log: LogValidation, **kwargs: object) -> dict:
    """Promote select case fields to Patient."""
    demog = orig.get("demographic")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    patient = {
        "id": orig.get("submitter_id"),
        "ethnicity": lower(demog.get("ethnicity")),
        "sex": lower(demog.get("gender")),
        "race": lower(demog.get("race")),
        "days_to_birth": demog.get("days_to_birth"),
    }

    for field in ["ethnicity", "sex", "race"]:
        log.distinct(patient, field)
    log.agree(patient, patient["id"], ["ethnicity", "sex", "race"])

    tip.update(patient)
    return tip


# gdc.research_subject ------------------------------------------
def research_subject(tip, orig, log: LogValidation, **kwargs: object) -> object:
    """Create ResearchSubject from case."""
    _this_research_subject = {
        "id": orig.get("case_id"),
        "identifier": [{"value": orig.get("case_id"), "system": "GDC"}],
        "primary_disease_type": orig.get("disease_type"),
        "primary_disease_site": orig.get("primary_site"),
        # "Project": {"label": orig.get("project", {}).get("project_id")},
        "associated_project": orig.get("project", {}).get("project_id"),
    }

    for field in ["primary_disease_type", "primary_disease_site"]:
        log.distinct(_this_research_subject, field)
    log.agree(
        _this_research_subject,
        _this_research_subject["id"],
        ["primary_disease_type", "primary_disease_site"],
    )

    tip["ResearchSubject"] = [_this_research_subject]
    return tip


# gdc.diagnosis --------------------------------------------------
def diagnosis(tip, orig, log: LogValidation, **kwargs):
    """Convert fields needed for Diagnosis. Needs ResearchSubject first."""

    constrain_research_subject(tip)

    harmonized_diagnosis = []
    diagnosis_fields = [
        "diagnosis_id",
        "age_at_diagnosis",
        "primary_diagnosis",
        "tumor_grade",
        "tumor_stage",
        "morphology",
    ]
    for d in orig.get("diagnoses", []):
        this_d = {f: d.get(f) for f in diagnosis_fields}
        this_d["id"] = this_d.pop("diagnosis_id")

        this_d["Treatment"] = [
            {
                "outcome": treatment.get("treatment_outcome"),
                "type": treatment.get("treatment_type"),
            }
            for treatment in d.get("treatments", [])
        ]

        harmonized_diagnosis += [this_d]

    # ResearchSubject is a list with one element at this stage
    tip["ResearchSubject"][0]["Diagnosis"] = harmonized_diagnosis

    return tip


# gdc.entity_to_specimen -----------------------------------------
def entity_to_specimen(tip, original, log: LogValidation, **kwargs):
    """Convert samples, portions and aliquots to specimens"""

    constrain_research_subject(tip)

    specimens = [specimen_from_entity(*s) for s in get_entities(original)]

    for specimen in specimens:
        for field in [
            "primary_disease_type",
            "source_material_type",
            "anatomical_site",
        ]:
            log.distinct(specimen, field)
        # days to birth is negative days from birth until diagnosis. 73000 days is 200 years.
        log.validate(specimen, "days_to_birth", lambda x: not x or -73000 < x < 0)

    tip["ResearchSubject"][0]["Specimen"] = specimens
    return tip


def get_entities(original):
    for sample in original.get("samples", []):
        yield (sample, "sample", "Initial sample", sample, original)
        for portion in sample.get("portions", []):
            yield (portion, "portion", sample.get("sample_id"), sample, original)
            for slide in portion.get("slides", []):
                yield (slide, "slide", portion.get("portion_id"), sample, original)
            for analyte in portion.get("analytes", []):
                yield (analyte, "analyte", portion.get("portion_id"), sample, original)
                for aliquot in analyte.get("aliquots", []):
                    yield (
                        aliquot,
                        "aliquot",
                        analyte.get("analyte_id"),
                        sample,
                        original,
                    )


def specimen_from_entity(entity, _type, parent_id, sample, case):
    id_key = f"{_type}_id"
    demog = case.get("demographics")
    if isinstance(demog, list):
        demog = demog[0]
    elif demog is None:
        demog = {}
    return {
        "derived_from_subject": case.get("submitter_id"),
        "id": entity.get(id_key),
        "identifier": [{"value": entity.get(id_key), "system": "GDC"}],
        "specimen_type": _type,
        "primary_disease_type": case.get("disease_type"),
        "source_material_type": sample.get("sample_type"),
        "anatomical_site": sample.get("biospecimen_anatomic_site"),
        "age_at_collection": demog.get("days_to_birth"),
        "associated_project": case.get("project", {}).get("project_id"),
        "derived_from_specimen": parent_id,
        "File": harmonized_files(entity.get("files", []) or [], case),
    }


def harmonized_files(files, case):
    file_fields = [
        "file_id",
        "file_name",
        "data_type",
        "type",
        "file_size",
        "data_category",
        "md5sum",
    ]
    h_files = []
    for fil in files:
        this_file = {f: fil.get(f) for f in file_fields}
        this_file["identifier"] = [{"value": this_file.get("file_id"), "system": "GDC"}]
        this_file["drs_uri"] = "".join(["drs://dg.4DFC:", this_file.get("file_id")])
        this_file["id"] = this_file.pop("file_id")
        this_file["associated_project"] = [case.get("project", {}).get("project_id")]
        this_file["byte_size"] = this_file.pop("file_size")
        this_file["checksum"] = this_file.pop("md5sum")
        this_file["label"] = this_file.pop("file_name")
        h_files.append(this_file)

    return h_files
