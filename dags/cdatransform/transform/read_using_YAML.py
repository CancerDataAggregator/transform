from typing import Optional, Union, Iterator
from collections import defaultdict
from typing_extensions import DefaultDict


# reading functions - simple read and read_entity
def simp_read(
    orig: dict,
    ptr_o: Union[str, list],
    cp_o: list,
):
    """This function takes an original row from output file (1 case or file - dict), a string denoting
    a generic pointer path to try to read in the row(ptr_o), and a current_path list[str and int] denoting
    where exactly in the record you are. Using this info, tries to read down the pointer path, using the current
    path as a guide of where exactly to go

    Args:
        orig (dict[str,Union[str,int,list,dict]]): One record of a case/file
        ptr_o (str): a generic pointer path we hope to read e.g: "cases.samples.portions.portion_id"
        cp_o (list[str]): a current path that you are "located" in e.g: ["cases","samples",2,"portions",1]
            Goal is to mimic getting orig["samples"][2]["portions"][1] generically for any generic/current path.
            I tried looking at DeepDiff which has a similar function, but it may not be applicable here.

    Returns:
        Union[str,int,list[str],list[dict],dict]: Returns exactly what is in orig[whatever you are reading]
    """
    cp = cp_o.copy()
    if ptr_o is None:
        return ptr_o
    if ptr_o == "cases" or ptr_o == "files":
        return orig
    ptr: list[str] = ptr_o.split(".")
    if len(ptr) == 1:
        return ptr[0]
    rmoved: str = cp.pop(0)
    ptr.remove(rmoved)
    rec: Union[list, dict] = orig.copy()
    while len(ptr) > 0 and rec is not None:
        # While the pointer path still has places to go, and the record still has things to explore...
        if cp != [] and ptr[0] == cp[0]:
            # if current path isn't empty, and the next step in pointer path matches the current path
            # e.g. cp = ["portions",1] and ptr = ["portions", "portion_id"]
            rec = rec.get(ptr[0])
            # get next step in path
            ptr.pop(0)
            # remove ptr[0], cp[0]
            try:
                cp.pop(0)
            except Exception:
                pass
        elif cp != [] and isinstance(cp[0], int):
            # rec is a list, and we want to next access the cp[0]th element
            # then remove it from cp
            rec = rec[cp[0]]
            cp.pop(0)
        elif ptr[0] == "demographics" or ptr[0] == "demographic":
            # GDC stores demographics as dict, whereas PDC stores it as list(EVEN THOUGH
            # THERE IS ONLY EVER ONE RECORD). Reason 456 that I hate PDC. This ensures that
            # you can read from demographics if it is a list, without denoting if it is in PDC or not
            rec = rec.get(ptr[0])
            if isinstance(rec, list):
                rec = rec[0]
            ptr.pop(0)
        elif cp != [] and ptr[0] != cp[0]:
            # Branching away from the current path. That is ok in some instances
            rec = rec.get(ptr[0])
            ptr.pop(0)
        else:
            # Usually used to explore what is further down from current path. Like seeing
            # if there are analytes listed in the portion.
            # e.g. cp = [samples,0,portions,1] ptr = [samples,portions,analytes]
            rec = rec.get(ptr[0], None)
            ptr.pop(0)
    return rec


def read_entry(orig: dict, MandT: dict, entity: str, **kwargs) -> dict:
    """_summary_

    Args:
        orig (dict[str,Union[str,int,list,dict]]): _description_
        MandT (dict[str,dict[str,dict[str,Union[str,dict[str,str],list]]]]): Mapping ant Transformation dictionary
            from mapping yaml file
        entity (str): entity from CDA model you are trying to currently build (ResearchSubject, Specimen, etc.)

    Returns:
        dict[str,Union[str,int,list]]: Returns one entity of a specific type. For instance from a case
        record, it can return one Subject, or one ResearchSubject. From one sample it returns one Specimen
    """
    endpoint: str = kwargs.get("endpoint", "cases")
    cur_path: list = kwargs.get("cur_path", [endpoint])
    samp_rec: dict = {}
    # if no identifier, no entry, return
    for field, val in MandT[entity]["Mapping"].items():
        if field != "identifier":
            if isinstance(val, str):
                field_path = val
                if field_path in [
                    "files.cases.project.project_id",
                    "files.cases.project.dbgap_accession_number",
                ]:  # This is specifically a GDC workaround, and may need further exploration.
                    # This assumes all cases associated with the file are from the same project.
                    temp_cur_path = ["files", "cases", 0]
                    samp_rec[field] = simp_read(orig, field_path, temp_cur_path)
                else:
                    samp_rec[field] = simp_read(orig, field_path, cur_path)

            elif isinstance(val, dict):
                # Used with Specimen mapping.
                # id: {"samples":"cases.samples.sample_id", "portions":"cases.samples.portions.portion_id"}
                # not sure if this is needed samp_rec[field] = []
                spec_type: str = spec_type_from_path(cur_path)
                path: str = val[spec_type]
                samp_rec[field] = simp_read(orig, path, cur_path)
            elif isinstance(val, list):
                # Append all values from paths in list
                samp_rec[field] = [simp_read(orig, path, cur_path) for path in val]
        else:  # build identifier struct - [{system: 'GDC' or 'PDC', value:'whatever_id}]
            samp_rec["identifier"] = {}
            # make identifier.value
            paths = val["value"]
            if isinstance(paths, dict):
                spec_type = spec_type_from_path(cur_path)
                samp_rec["identifier"]["value"] = simp_read(
                    orig, paths[spec_type], cur_path
                )
            else:
                samp_rec["identifier"]["value"] = simp_read(orig, paths, cur_path)
            # make identifier.system
            field_path = val["system"]
            samp_rec["identifier"]["system"] = simp_read(orig, field_path, cur_path)
            samp_rec["identifier"] = [samp_rec["identifier"]]
    return samp_rec


# Functions to determine tree structure of nested things in YAML.
# e.g. From id: {"samples": 'cases.samples.sample_id',
#               "portions": 'cases.samples.portions.portion_id',
#               "slides": 'cases.samples.portions.slides.slide_id',
#               "analytes":'cases.samples.portions.analytes.analyte_id',
#               "aliquots":'cases.samples.portions.analytes.aliquots.aliquot_id'}
# To {"samples":
#       {"portions":
#           {"slides": None, "analytes":
#                               {"aliquots": None}
#           }
#       }
#   }
def det_tree_to_collapse(MandT: dict, entity: str, **kwargs) -> dict:
    """Functions to determine tree structure of nested things in YAML.
        e.g. From id: {"samples": 'cases.samples.sample_id',
                    "portions": 'cases.samples.portions.portion_id',
                    "slides": 'cases.samples.portions.slides.slide_id',
                    "analytes":'cases.samples.portions.analytes.analyte_id',
                    "aliquots":'cases.samples.portions.analytes.aliquots.aliquot_id'}
        To {"samples":
            {"portions":
                {"slides": None, "analytes":
                                    {"aliquots": None}
                }
            }
        }

    Args:
        MandT (dict[str, dict[str, dict[str, Union[str, dict[str, str], list]]]]): Mapping and transformation dict
        entity (str): Entity you are trying to see how it needs to collapse

    Returns:
        dict[str, Union[dict, None]]: See above
    """
    if kwargs.get("linker") is not None:
        temp_dict: dict = MandT[entity]["Linkers"][kwargs.get("linker")].copy()
    else:
        temp_dict: dict = MandT[entity]["Mapping"]["id"].copy()
        # will change from dict[str,str] to dict[str,list]
    paths_lst: list = []
    for k in temp_dict:
        # Split, remove first and last
        temp_dict[k] = temp_dict[k].split(".")
        temp_dict[k] = temp_dict[k][1:-1]
        # makes temp_dict[k] a list of strings, excluding the aliquot_id, sample_id, etc.
        temp = None
        # builds one path in tree from end branch
        for path in reversed(temp_dict[k]):
            temp = {path: temp}
        paths_lst.append(temp)
    tree = paths_lst.pop()
    for paths in paths_lst:
        tree = dict(mergedicts(tree, paths))
    return tree


def mergedicts(dict1: dict, dict2: dict) -> Iterator:
    for k in set(dict1.keys()).union(dict2.keys()):
        if k in dict1 and k in dict2:
            if isinstance(dict1[k], dict) and isinstance(dict2[k], dict):
                yield (k, dict(mergedicts(dict1[k], dict2[k])))
            else:
                # If one of the values is not a dict, you can't continue merging it.
                # Value from second dict overrides one in first and we move on.
                if isinstance(dict2[k], dict):
                    yield (k, dict2[k])
                elif isinstance(dict1[k], dict):
                    yield (k, dict1[k])
                else:
                    print("wtf?!")
                # Alternatively, replace this with exception raiser to alert you of value conflicts
        elif k in dict1:
            yield (k, dict1[k])
        else:
            yield (k, dict2[k])


def spec_type_from_path(cur_path: list) -> Optional[str]:
    if isinstance(cur_path[-1], str):
        return cur_path[-1]
    if isinstance(cur_path[-2], str):
        return cur_path[-2]


def add_linkers(orig: dict, MandT: dict, entity: str, **kwargs):
    """This function looks at a record of an entity (Subject, ResearchSubject, etc.) and adds any linker
    fields (like Files to Subject entities). This function is now only used to add Subjects, ResearchSubjects,
    and Specimens to the Files table

    Args:
        orig (dict[str, Union[str, int, list, dict]]): one case/file record
        MandT (dict[str, dict[str, dict[str, Union[str, dict[str, str], list]]]]): Mapping and transformation dict
        entity (str): name of entity you are adding linkers to. (File)

    Returns:
        defaultdict[str, list[str]]: _description_
    """
    endpoint: str = kwargs.get("endpoint", "cases")
    cur_path: list[Union[str, int]] = kwargs.get("cur_path", [endpoint])

    link_recs: defaultdict[str, list[str]] = defaultdict(list)
    for field, val in MandT[entity]["Linkers"].items():
        if isinstance(val, str):
            temp_val: list[str] = val.split(".")
            linker: str = temp_val.pop()
            temp_val: str = ".".join(temp_val)
            recs = simp_read(orig, temp_val, cur_path)
            if isinstance(recs, list):
                for rec in recs:
                    link_recs[field].append(rec.get(linker))
                try:
                    link_recs[field] = list(set(link_recs[field]))
                except:
                    pass
            elif isinstance(recs, dict):
                link_recs[field] = list(recs.get(linker))
            # print(link_recs[field])
        elif isinstance(val, dict) and endpoint == "cases":
            spec_type: str = spec_type_from_path(cur_path)
            path: str = val[spec_type]
            temp_val: list[str] = path.split(".")
            linker = temp_val.pop()
            temp_val: str = ".".join(temp_val)
            recs = simp_read(orig, temp_val, cur_path)
            if isinstance(recs, list):
                for rec in recs:
                    link_recs[field].append(rec.get(linker))
                link_recs[field] = list(set(link_recs[field]))
            else:
                link_recs[field] = list(recs.get(linker))
        elif isinstance(val, dict) and endpoint == "files":
            tree = det_tree_to_collapse(MandT, entity, linker=field)
            link_recs[field] = add_nested_linkers(orig, val, tree=tree, endpt=endpoint)
        elif isinstance(val, list):
            # print(val)
            for index in range(len(val)):
                temp_val = val[index].split(".")
                linker = temp_val.pop()
                temp_val = ".".join(temp_val)
                recs = simp_read(orig, temp_val, cur_path)
                if isinstance(recs, list):
                    for rec in recs:
                        link_recs[field].append(rec.get(linker))

                elif isinstance(recs, dict):
                    link_recs[field].append(recs.get(linker))
            link_recs[field] = list(set(link_recs[field]))

    return link_recs


def add_nested_linkers(orig, linker_dict, **kwargs):
    """This function was specifically made to create Specimen linkers to the File table when the extracted files
    endpoint info had a structure like files.cases.samples... . That is no longer the case, but the function
    may be useful in the future

    Args:
        orig (_type_): _description_
        linker_dict (_type_): _description_

    Returns:
        _type_: _description_
    """
    linkers = kwargs.get("linkers", [])
    tree = kwargs.get("tree")
    cur_path = kwargs.get("cur_path", None)
    rel_path = kwargs.get("rel_path", None)
    endpt = kwargs.get("endpt")
    if cur_path is None:
        cur_path = []
        cur_path = [endpt]
    if rel_path is None:
        rel_path = endpt
    for branch, leaves in tree.items():
        branch_rel_path = ".".join([rel_path, branch])
        branch_cur_path = cur_path + [branch]
        test_read = simp_read(orig, branch_rel_path, branch_cur_path)
        if test_read is None:
            test_read = []
        for branch_rec_ind in range(len(test_read)):
            if branch in linker_dict:
                linkers.append(
                    simp_read(
                        orig,
                        linker_dict[branch],
                        branch_cur_path + [branch_rec_ind],
                    )
                )
            if leaves is not None:

                linkers = add_nested_linkers(
                    orig,
                    linker_dict,
                    cur_path=branch_cur_path + [branch_rec_ind],
                    rel_path=".".join([branch_rel_path]),
                    tree=tree[branch],
                    linkers=linkers,
                )
    return list(set(linkers))
