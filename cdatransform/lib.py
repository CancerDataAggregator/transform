"""Program wide utility functions and classes."""


def get_case_ids(case_list_file):
    if case_list_file is None:
        return None
    else:
        with open(case_list_file, "r") as fp:
            return [l.strip() for l in fp.readlines()]
