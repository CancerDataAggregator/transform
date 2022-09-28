"""Program wide utility functions and classes."""


def get_ids(id: str = None, id_list_file: list[str] = None) -> list[str] | None:
    if id is not None:
        return [id]

    if id_list_file is None:
        return None
    else:
        with open(id_list_file, "r") as fp:
            return [line.strip() for line in fp.readlines()]
