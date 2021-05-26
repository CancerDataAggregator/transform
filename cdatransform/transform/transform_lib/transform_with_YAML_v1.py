import cdatransform.transform.transform_lib.value_transformations as vt
import cdatransform.transform.read_using_YAML as ruy
from cdatransform.transform.validate import LogValidation


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
            spec_rec['File'] = []
            if spec_type == 'samples':
                
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
#Functions to apply the transforms to relevant fields and functionalize Transformation dictionary
def entity_value_transforms(tip,entity,MandT):
    if 'Transformations' in MandT[entity] and MandT[entity]['Transformations'] is not None:
        trans_dict = MandT[entity]['Transformations']
        tip = apply_transformations(tip,trans_dict)
    return tip
def apply_transformations(tip,trans_dict):
    for field_name, trans in trans_dict.items():
        excluded_field =(trans=='exclude')
        if not excluded_field:
            tip[field_name] = apply_list_of_lists(tip[field_name],trans)
    return(tip)
def apply_list_of_lists(data,list_trans):
    temp = data
    if list_trans is not None:
        for lists in list_trans:
            if lists[1] == []:
                temp = lists[0](data)
            else:
                temp = lists[0](data,lists[1])
    return(temp)

def functionalize_trans_dict(trans_dict):
    temp = trans_dict.copy()
    for field_name, trans_vals in trans_dict.items():
        if trans_vals !='exclude':
            for trans in range(len(trans_vals)):
                temp[field_name][trans][0] = getattr(vt, trans_dict[field_name][trans][0])

    return(temp)

class Transform:
    def __init__(self, validate) -> None:
        #self._transforms = transform(orig,MandT,DC)
        self._validate = validate

    def transforms(self, source: dict) -> dict:
        destination = {}
        for vt in self._transforms:
            destination = vt[0](destination, source, self._validate, **vt[1])
        return destination
    def __call__(self,orig,MandT,DC,**kwargs):
        #list or dict as return? - if Patient - dict, else, list
        #where do I read from? - Need cur_path and general path
        print('read all entries')
        cur_path = kwargs.get("cur_path",['cases'])
        path_to_read = kwargs.get("path_to_read",'cases')
        tip = ruy.read_entry(orig,MandT,'Patient',DC = DC)
        tip = entity_value_transforms(tip,'Patient',MandT)
        
        for field in ["ethnicity", "sex", "race"]:
            self._validate.distinct(tip, field)
        self._validate.agree(tip, tip["id"], ["ethnicity", "sex", "race"])
        RS = ruy.read_entry(orig,MandT,'ResearchSubject',DC=DC)
        RS = entity_value_transforms(RS,'ResearchSubject',MandT)
        for field in ["primary_disease_type", "primary_disease_site"]:
            self._validate.distinct(RS, field)
        self._validate.agree(
            RS,
            RS["id"],
            ["primary_disease_type", "primary_disease_site"],
        )
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

                treat_path = cur_path+[diag_rec,'treatments']
                treat_rec = ruy.simp_read(orig,diag_path,cur_path,DC)
                if isinstance(treat_rec,list) and treat_rec !=[]:
                    temp_diag['Treatment'] = []
                    for treat in range(len(treat_rec)):
                        treat_rec = ruy.read_entry(orig,MandT,'Treatment',
                                                                cur_path = treat_path + [treat])
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
        for specimen in RS['Specimen']:
            for field in [
                "primary_disease_type",
                "source_material_type",
                "anatomical_site",
            ]:
                self._validate.distinct(specimen, field)
            # days to birth is negative days from birth until diagnosis. 73000 days is 200 years.
            self._validate.validate(specimen, "days_to_birth", lambda x: not x or -73000 < x < 0)
        tip['ResearchSubject'] = [RS]
        return tip