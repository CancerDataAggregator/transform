# List of all functions to be applied to values of fields.
def lower_case(val):
    if isinstance(val,str):
        val = val.lower()
    return val
def format_drs(val):
    if isinstance(val,str):
        val = "".join(["drs://dg.4DFC:",val])
    return val
    