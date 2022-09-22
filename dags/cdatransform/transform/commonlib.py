# Small utilities


def constrain_research_subject(tip):
    # Constraints on transform in progress re: ResearchSubject
    if "ResearchSubject" not in tip:
        raise RuntimeError("gdc.diagnosis has to come after gdc.research_subject")
    research_subject = tip["ResearchSubject"]
    if not isinstance(research_subject, list):
        raise RuntimeError("ResearchSubject has to be list")
    if len(research_subject) != 1:
        raise RuntimeError("There should be exactly one ResearchSubject.")


def lower(val):
    return val.lower() if val is not None else None
