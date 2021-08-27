# reading functions - simple read and read_entity
def simp_read(orig, ptr, cp_o, DC):
    cp = cp_o.copy()
    if ptr == 'cases':
        return orig
    ptr = ptr.split('.')
    if len(ptr) == 1:
        return ptr[0]
    cp.remove('cases')
    try:
        ptr.remove('cases')
    except Exception:
        ptr.insert(0, 'samples')
        if DC == 'PDC':
            ptr.remove('files')
            ptr.insert(1, 'File')
    rec = orig.copy()
    while (len(ptr) > 0 and rec is not None):
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
        elif ptr[0] == 'demographics' or ptr[0] == 'demographic':
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
    DC = kwargs.get('DC', 'GDC')
    # spec_type = kwargs.get('spec_type', None)
    cur_path = kwargs.get('cur_path', ['cases'])
    samp_rec = dict({})
    # if no identifier, no entry, return
    for field, val in MandT[entity]['Mapping'].items():
        if (field != 'identifier'):
            if isinstance(val, str):
                field_path = val
                samp_rec[field] = simp_read(orig, field_path, cur_path, DC)
            elif isinstance(val, dict):
                samp_rec[field] = []
                spec_type = spec_type_from_path(cur_path)
                path = val[spec_type]
                samp_rec[field] = simp_read(orig, path, cur_path, DC)
        else:
            samp_rec['identifier'] = dict({})
            paths = val['value']
            if isinstance(paths, dict):
                spec_type = spec_type_from_path(cur_path)
                samp_rec['identifier']['value'] = simp_read(orig, paths[spec_type], cur_path, DC)
            else:
                samp_rec['identifier']['value'] = simp_read(orig, paths, cur_path, DC)
            field_path = val['system']
            samp_rec['identifier']['system'] = simp_read(orig, field_path, cur_path, DC)
            samp_rec['identifier'] = [samp_rec['identifier']]
    return samp_rec


# Functions to determine tree structure of nested things in YAML - ex. Samples
def det_tree_to_collapse(MandT, entity):
    temp_dict = MandT[entity]['Mapping']['id'].copy()
    paths_lst = []
    for k, v in temp_dict.items():
        temp_dict[k] = temp_dict[k].split('.')
        temp_dict[k].remove('cases')
        temp_dict[k].pop()
        temp = None
        for path in reversed(temp_dict[k]):
            temp = dict({path: temp})
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
                    print('wtf?!')
                # Alternatively, replace this with exception raiser to alert you of value conflicts
        elif k in dict1:
            yield (k, dict1[k])
        else:
            yield (k, dict2[k])


def spec_type_from_path(cur_path):
    if isinstance(cur_path[-1], str):
        return cur_path[-1]
    if isinstance(cur_path[-2], str):
        return cur_path[-2]


# def read_file_entry(orig, MandT, entity, DC, **kwargs):
#    # spec_type = kwargs.get('spec_type', None)
#    cur_path = kwargs.get('cur_path', ['cases'])
#    samp_rec = dict({})
#    # if no identifier, no entry, return
#    for field, val in MandT[entity]['Mapping'].items():
#        if (field != 'identifier'):
#            if isinstance(val, str):
#                field_path = adjust_file_path(val, DC)
#                samp_rec[field] = simp_read(orig, field_path, cur_path, DC)
#            elif isinstance(val, dict):
#                samp_rec[field] = []
#                spec_type = spec_type_from_path(cur_path)
#                path = adjust_file_path(val[spec_type])
#                samp_rec[field] = simp_read(orig, path, cur_path, DC)
#        else:
#            samp_rec['identifier'] = dict({})
#            paths = val['value']
#            if isinstance(paths, dict):
#                spec_type = spec_type_from_path(cur_path)
#                samp_rec['identifier']['value'] = simp_read(orig, adjust_file_path(paths[spec_type], DC), cur_path, DC)
#            else:
#                samp_rec['identifier']['value'] = simp_read(orig, adjust_file_path(paths, DC), cur_path, DC)
#            field_path = adjust_file_path(val['system'], DC)
#            samp_rec['identifier']['system'] = simp_read(orig, field_path, cur_path, DC)
#            samp_rec['identifier'] = [samp_rec['identifier']]
#    return samp_rec


def adjust_file_mapping_path(cur_rel_path, map_path):
    new_path = map_path.split('.')
    cur_rel_path = cur_rel_path.split('.')
    if new_path[0] == 'files':
        new_path.remove('files')
        new_path = cur_rel_path + new_path
    new_path = '.'.join(new_path)
    return new_path


def files_rec_name(DC):
    if DC == 'GDC':
        file_loc = 'files'
    if DC == 'PDC':
        file_loc = 'files'
    return file_loc


def read_file_entry_v2(orig, MandT, entity, DC, **kwargs):
    # spec_type = kwargs.get('spec_type', None)
    cur_path = kwargs.get('cur_path', ['cases', files_rec_name(DC), 0])
    rel_path = kwargs.get('rel_path', 'cases')
    file_rec = dict({})
    # if no identifier, no entry, return
    for field, val in MandT[entity]['Mapping'].items():
        if (field != 'identifier'):
            if isinstance(val, str):
                field_rel_path = adjust_file_mapping_path(rel_path, val)
                file_rec[field] = simp_read(orig, field_rel_path, cur_path, DC)
        else:
            file_rec['identifier'] = dict({})
            paths = val['value']
            if isinstance(paths, dict):
                spec_type = spec_type_from_path(cur_path)
                file_rec['identifier']['value'] = simp_read(orig, adjust_file_mapping_path(paths[spec_type]), cur_path, DC)
            else:
                file_rec['identifier']['value'] = simp_read(orig, adjust_file_mapping_path(rel_path, paths), cur_path, DC)
            file_rec['identifier']['system'] = simp_read(orig, adjust_file_mapping_path(rel_path, val['system']),
                                                        cur_path, DC)
            file_rec['identifier'] = [file_rec['identifier']]
    return file_rec
