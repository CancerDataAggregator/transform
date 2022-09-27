import sys
from typing import Union
# reading functions - simple read and read_entity
def simp_read(orig, ptr, cp_o):
    cp = cp_o.copy()
    if ptr is None:
        return ptr
    if ptr == "cases" or ptr == "files":
        return orig
    ptr = ptr.split(".")
    if len(ptr) == 1:
        return ptr[0]
    rmoved = cp.pop(0)
    ptr.remove(rmoved)
    rec = orig.copy()
    while len(ptr) > 0 and rec is not None:
        if cp != [] and ptr[0] == cp[0]:
            rec = rec.get(ptr[0])
            ptr.pop(0)
            try:
                cp.pop(0)
            except Exception:
                pass
        elif cp != [] and isinstance(cp[0], int):
            rec = rec[cp[0]]
            cp.pop(0)
        elif ptr[0] == "demographics" or ptr[0] == "demographic":
            rec = rec.get(ptr[0])
            if isinstance(rec, list):
                rec = rec[0]
            ptr.pop(0)
        elif cp != [] and ptr[0] != cp[0]:
            rec = rec.get(ptr[0])
            ptr.pop(0)
        else:
            rec = rec.get(ptr[0], None)
            ptr.pop(0)
    return rec


def read_entry(orig, MandT, entity, **kwargs):
    # spec_type = kwargs.get('spec_type', None)
    endpoint = kwargs.get("endpoint", "cases")
    cur_path = kwargs.get("cur_path", [endpoint])
    samp_rec = {}
    # if no identifier, no entry, return
    for field, val in MandT[entity]["Mapping"].items():
        if field != "identifier":
            if isinstance(val, str):
                field_path = val
                if field_path in [
                    "files.cases.project.project_id",
                    "files.cases.project.dbgap_accession_number",
                ]:
                    temp_cur_path = ["files", "cases", 0]
                    samp_rec[field] = simp_read(orig, field_path, temp_cur_path)
                else:
                    samp_rec[field] = simp_read(orig, field_path, cur_path)

            elif isinstance(val, dict):
                samp_rec[field] = []
                spec_type = spec_type_from_path(cur_path)
                path = val[spec_type]
                samp_rec[field] = simp_read(orig, path, cur_path)
            elif isinstance(val, list):
                samp_rec[field] = []
                for path in val:
                    samp_rec[field].append(simp_read(orig, path, cur_path))
        else:
            samp_rec["identifier"] = dict({})
            paths = val["value"]
            if isinstance(paths, dict):
                spec_type = spec_type_from_path(cur_path)
                samp_rec["identifier"]["value"] = simp_read(
                    orig, paths[spec_type], cur_path)
            else:
                samp_rec["identifier"]["value"] = simp_read(orig, paths, cur_path)
            field_path = val["system"]
            samp_rec["identifier"]["system"] = simp_read(orig, field_path, cur_path)
            samp_rec["identifier"] = [samp_rec["identifier"]]
    return samp_rec


# Functions to determine tree structure of nested things in YAML - ex. Samples
def det_tree_to_collapse(MandT:dict, entity:str, **kwargs)->dict[str, Union[dict,None]]:
    if kwargs.get("linker") is not None:
        temp_dict:dict = MandT[entity]["Linkers"][kwargs.get("linker")].copy()
    else:
        temp_dict:dict = MandT[entity]["Mapping"]["id"].copy()

    paths_lst:list[dict[str, Union[dict, None]]] = []
    for k in temp_dict:
        temp_dict[k] = temp_dict[k].split(".")
        temp_dict[k] = temp_dict[k][1:]
        temp_dict[k].pop()
        # makes temp_dict[k] a list of strings, excluding the aliquot_id, sample_id, etc.
        temp = None
        for path in reversed(temp_dict[k]):
            temp = {path: temp}
        paths_lst.append(temp)
    tree = paths_lst.pop()
    for paths in paths_lst:
        tree = dict(mergedicts(tree, paths))
    return tree


def mergedicts(dict1, dict2):
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


def spec_type_from_path(cur_path)->Union[str,None]:
    if isinstance(cur_path[-1], str):
        return cur_path[-1]
    if isinstance(cur_path[-2], str):
        return cur_path[-2]


def add_linkers(orig, MandT, entity, **kwargs):
    endpoint = kwargs.get("endpoint", "cases")
    cur_path = kwargs.get("cur_path", [endpoint])

    link_recs = dict({})
    for field, val in MandT[entity]["Linkers"].items():
        link_recs[field] = []
        if isinstance(val, str):
            temp_val = val.split(".")
            linker = temp_val.pop()
            temp_val = ".".join(temp_val)
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
            spec_type = spec_type_from_path(cur_path)
            path = val[spec_type]
            temp_val = path.split(".")
            linker = temp_val.pop()
            temp_val = ".".join(temp_val)
            recs = simp_read(orig, temp_val, cur_path)
            if isinstance(recs, list):
                for rec in recs:
                    link_recs[field].append(rec.get(linker))
                link_recs[field] = list(set(link_recs[field]))
            else:
                link_recs[field] = list(recs.get(linker))
        elif isinstance(val, dict) and endpoint == "files":
            tree = det_tree_to_collapse(MandT, entity, linker=field)
            link_recs[field] = add_nested_linkers(
                orig, val, tree=tree, endpt=endpoint
            )
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
