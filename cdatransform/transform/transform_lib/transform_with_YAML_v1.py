import cdatransform.transform.transform_lib.value_transformations as vt
import cdatransform.transform.read_using_YAML as ruy
def transform(orig,MandT,DC,**kwargs):
    #list or dict as return? - if Patient - dict, else, list
    #where do I read from? - Need cur_path and general path
    print('read all entries')
    cur_path = kwargs.get("cur_path",['cases'])
    path_to_read = kwargs.get("path_to_read",'cases')
    tip = ruy.read_entry(orig,MandT,'Patient',DC = DC)
    tip = entity_value_transforms(tip,'Patient',MandT)
    RS = ruy.read_entry(orig,MandT,'ResearchSubject',DC=DC)
    RS = entity_value_transforms(RS,'ResearchSubject',MandT)
    RS['Diagnosis'] = []
    diag_path = MandT['Diagnosis']['Mapping']['id']
    diag_path = diag_path.split('.')
    diag_path.pop()
    diag_path = '.'.join(diag_path)
    cur_path = diag_path.split('.')
    ent_rec = ruy.simp_read(orig,diag_path,cur_path,DC)
    if isinstance(ent_rec,list):
        for diag_rec in range(len(ent_rec)):
            temp_diag = ruy.read_entry(orig,MandT,'Diagnosis',cur_path = cur_path + [diag_rec])
            
            treat_path = cur_path+[diag_rec,'Treatment']
            treat_rec = ruy.simp_read(orig,diag_path,cur_path,DC)
            if isinstance(treat_rec,list) and treat_rec !=[]:
                temp_diag['Treatment'] = []
                for treat in range(len(treat_rec)):
                    temp_diag['Treatment'].append(ruy.read_entry(orig,MandT,'Treatment',
                                                            cur_path = treat_path + [treat]))
            elif isinstance(treat_rec, dict):
                temp_diag['Treatment']=[ruy.read_entry(orig,MandT,'Treatment',
                                                            cur_path = treat_path)]
            else:
                temp_diag['Treatment'] = []
            RS['Diagnosis'].append(temp_diag)
    elif isinstance(ent_rec,dict):
        temp_diag = ruy.read_entry(orig,MandT,'Diagnosis',cur_path = cur_path)
        treat_path = cur_path+['Treatment']
        treat_rec = ruy.simp_read(orig,diag_path,cur_path,DC)
        if isinstance(treat_rec,list) and treat_rec !=[]:
            temp_diag['Treatment'] = []
            for treat in range(len(treat_rec)):
                temp_diag['Treatment'].append(ruy.read_entry(orig,MandT,'Treatment',
                                                        cur_path = treat_path + [treat]))
        elif isinstance(treat_rec, dict):
            temp_diag['Treatment']=[ruy.read_entry(orig,MandT,'Treatment',
                                                        cur_path = treat_path)]
        else:
            temp_diag['Treatment'] = []
        RS['Diagnosis'].append(temp_diag)
    else:
        RS['Diagnosis'] = []
    RS['Specimen'] = add_Specimen_rec(orig,MandT,DC)
    tip['ResearchSubject'] = [RS]
    return tip
def add_Specimen_rec(orig,MandT,DC,**kwargs):
    cur_path = kwargs.get('cur_path',['cases','samples'])
    spec_type = kwargs.get('spec_type','samples')
    rel_path = kwargs.get('rel_path','cases.samples')
    spec = []
    tree = kwargs.get('tree', ruy.det_tree_to_collapse(MandT,'Specimen'))
    if DC == 'GDC':
        file = 'files'
    if DC == 'PDC':
        file = 'File'
    if ruy.simp_read(orig,rel_path,cur_path,DC) is not None:
        for spec_rec_ind in range(len(ruy.simp_read(orig,rel_path,cur_path,DC))):
            spec_path = cur_path.copy()
            spec_path.append(spec_rec_ind)
            spec_rec = ruy.read_entry(orig,MandT,'Specimen', cur_path = spec_path, spec_type = spec_type)
            if spec_type == 'samples':
                spec_rec['File'] = []
                if isinstance(ruy.simp_read(orig,rel_path+'.'+file,spec_path+[file],DC),list):
                    for file_ind in range(len(ruy.simp_read(orig,rel_path+'.'+file,spec_path+[file],DC))):
                        file_path = spec_path.copy()
                        file_path.append(file)
                        file_path.append(file_ind)
                        file_rec = ruy.read_file_entry(orig,MandT,'File',DC,cur_path = file_path)
                        file_rec = entity_value_transforms(file_rec,'File',MandT)
                        spec_rec['File'].append(file_rec)
            spec_rec = [spec_rec]
            branches_dict = tree.get(spec_type)
            if branches_dict is not None:
                for k in branches_dict:
                    nest_path = spec_path.copy()+[k]
                    nest_spec_type = k
                    nest_rel_path = '.'.join([rel_path,k])
                    nest_rec = add_Specimen_rec(orig,MandT,DC,tree = branches_dict, 
                                            cur_path = nest_path, 
                                            spec_type = nest_spec_type,
                                           rel_path = nest_rel_path)
                    spec_rec+=nest_rec
            spec+= spec_rec
    return(spec)
#Functions to functionalize Transformation dictionary and apply the transforms to relevant fields
def entity_value_transforms(tip,entity,MandT):
    if 'Transformations' in MandT[entity] and MandT[entity]['Transformations'] is not None:
        trans_dict = MandT[entity]['Transformations']
        tip = apply_transformations(tip,trans_dict)
    return tip
def apply_transformations(tip,trans_dict):
    for field_name, trans in trans_dict.items():
        excluded_field =(trans=='exclude')
        #tempdict = dict({field_name:tip.pop(field_name)})
        print('before trans')
        print(tip[field_name])
        print(excluded_field)
        if not excluded_field:
            tip[field_name] = apply_list_of_lists(tip[field_name],trans)
            #tip.update(tempdict)
        print('after trans')
        print(tip[field_name])
    return(tip)
def apply_list_of_lists(data,list_trans):
    temp = data
    print(list_trans)
    if list_trans is not None:
        for lists in list_trans:
            if lists[1] == []:
                temp = lists[0](data)
                print('temp_aft_trans')
                print(temp)
            else:
                temp = lists[0](data,lists[1])
    return(temp)

def functionalize_trans_dict(trans_dict):
    temp = trans_dict.copy()
    for field_name, trans_vals in trans_dict.items():
        if trans_vals !='exclude':
            for trans in range(len(trans_vals)):
                #temp[field_name][trans][0] = globals()[trans_dict[field_name][trans][0]]
                print('before functionalize')
                print(trans_dict[field_name][trans])
                temp[field_name][trans][0] = getattr(vt, trans_dict[field_name][trans][0])

    return(temp)