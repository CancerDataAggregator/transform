# Transform functions go here

def none_to_zero(value):
    return 0 if value == 'None' else value


# For each transform function, add an entry here

transform_library = {
    "none_to_zero": none_to_zero
}
