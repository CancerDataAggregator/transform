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


def idc_species_mapping():
    return "CASE x WHEN 'Human' THEN 'Homo sapiens' WHEN 'Canine' THEN 'Canis familiaris' WHEN 'Mouse' THEN 'Mus musculus' ELSE ''END"


def idc_substr(val):
    return "SUBSTR(x, " + str(val[0]) + ")"
