# List of all functions to be applied to values of fields.
def lower_case(val):
    if isinstance(val, str):
        val = val.lower()
    return val


def format_drs(val):
    if isinstance(val, str):
        val = "".join(["drs://dg.4DFC:", val])
    return val


def str_to_list(val):
    if isinstance(val, str):
        val = [val]
    return val


def idc_species_mapping(val):
    return "CASE " + val[0] + " WHEN 'Human' THEN 'Homo sapiens' WHEN 'Canine' THEN 'Canis familiaris' WHEN 'Mouse' THEN 'Mus musculus' ELSE ''END"


def idc_substr(val):
    return "SUBSTR(" + val[0] + ", " + str(val[1]) + ")"


def idc_drs_uri(val):
    return """CONCAT("drs://dg.4DFC:", """ + val[0] + """)"""


def idc_researchsubject_id(val):
    return """CONCAT(""" + val[0] + """, "__", """ + val[1] + """)"""
