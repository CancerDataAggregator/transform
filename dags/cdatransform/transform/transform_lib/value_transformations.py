# List of all functions to be applied to values of fields.
from typing import Optional


def lower_case(val: Optional[str]):
    """
    This function will lower case a str
    Args:
        val (_type_): _description_

    Returns:
        _type_: _description_
    """
    if isinstance(val, str):
        val = val.lower()
    return val


def format_drs(val: Optional[str]):
    """
    This will concat a a drs url to the value str
    Args:
        val (Optional[str]): _description_

    Returns:
        _type_: _description_
    """
    if isinstance(val, str):
        val = "".join(["drs://dg.4DFC:", val])
    return val


def str_to_list(val):
    if isinstance(val, str):
        val = [val]
    return val


def coalesce(val):
    """
    This Get a list of values and picks the first value that is not None
    Args:
        val (_type_): _description_

    Returns:
        _type_: _description_
    """
    if isinstance(val, list):
        for el in val:
            if el is not None:
                return el
    return None


def idc_species_mapping(val):
    return (
        "CASE "
        + val[0]
        + " WHEN 'Human' THEN 'homo sapiens' WHEN 'Canine' THEN 'canis familiaris' WHEN 'Mouse' THEN 'mus musculus' ELSE ''END"
    )


def idc_substr(val):
    return "SUBSTR(" + val[0] + ", " + str(val[1]) + ")"


def idc_drs_uri(val):
    return """CONCAT("drs://dg.4DFC:", """ + val[0] + """)"""


def idc_researchsubject_id(val):
    return """CONCAT(""" + val[0] + """, "__", """ + val[1] + """)"""


def idc_label(val):
    # splits gcs_url, gets last element in array for label
    return """ARRAY_REVERSE(SPLIT(""" + val[0] + """, '/'))[OFFSET(0)]"""
