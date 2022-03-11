import cdatransform.transform.transform_lib.value_transformations as vt
import cdatransform.transform.read_using_YAML as ruy
from cdatransform.transform.validate import LogValidation


def add_Specimen_rec(orig, MandT, DC, **kwargs):
    cur_path = kwargs.get("cur_path", ["cases", "samples"])
    spec_type = kwargs.get("spec_type", "samples")
    rel_path = kwargs.get("rel_path", "cases.samples")
    endpoint = kwargs.get("endpoint", "cases")
    spec = []
    tree = kwargs.get("tree", ruy.det_tree_to_collapse(MandT, "Specimen"))
    if ruy.simp_read(orig, rel_path, cur_path, DC) is not None:
        for spec_rec_ind in range(len(ruy.simp_read(orig, rel_path, cur_path, DC))):
            spec_path = cur_path.copy()
            spec_path.append(spec_rec_ind)
            spec_rec = ruy.read_entry(
                orig, MandT, "Specimen", cur_path=spec_path, spec_type=spec_type
            )
            #spec_rec["File"] = add_File_rec(
            #    orig, MandT, DC, cur_path=spec_path, rel_path=rel_path
            #)
            if endpoint=="cases":
                linkers = ruy.add_linkers(orig, MandT, 'Specimen', DC, linker=True, 
                                cur_path=spec_path ,endpoint=endpoint)
                spec_rec.update(linkers)
            spec_rec = [spec_rec]
            if "cases" in tree:
                tree = tree["cases"]
            branches_dict = tree.get(spec_type)
            if branches_dict is not None:
                for k in branches_dict:
                    nest_path = spec_path.copy() + [k]
                    nest_spec_type = k
                    nest_rel_path = ".".join([rel_path, k])
                    nest_rec = add_Specimen_rec(
                        orig,
                        MandT,
                        DC,
                        tree=branches_dict,
                        cur_path=nest_path,
                        spec_type=nest_spec_type,
                        rel_path=nest_rel_path,
                        endpoint=endpoint,
                    )
                    spec_rec += nest_rec
            spec += spec_rec
    return spec


def add_File_rec(orig, MandT, DC, **kwargs):
    File_recs = []
    file_rec_name = ruy.files_rec_name(DC)
    cur_path = kwargs.get("cur_path", ["cases"])
    rel_path = kwargs.get("rel_path", "cases")
    file_rel_path = rel_path + "." + file_rec_name
    if isinstance(
        ruy.simp_read(orig, file_rel_path, cur_path + [file_rec_name], DC), list
    ):
        for file_ind in range(
            len(ruy.simp_read(orig, file_rel_path, cur_path + [file_rec_name], DC))
        ):
            file_path = cur_path.copy()
            file_path.append(file_rec_name)
            file_path.append(file_ind)
            file_rec = ruy.read_file_entry_v2(
                orig, MandT, "File", DC, cur_path=file_path, rel_path=file_rel_path
            )
            file_rec = entity_value_transforms(file_rec, "File", MandT)
            File_recs.append(file_rec)
    return File_recs


# Functions to apply the transforms to relevant fields and functionalize Transformation dictionary
def entity_value_transforms(tip, entity, MandT):
    if (
        "Transformations" in MandT[entity]
        and MandT[entity]["Transformations"] is not None
    ):
        trans_dict = MandT[entity]["Transformations"]
        tip = apply_transformations(tip, trans_dict)
    return tip


def apply_transformations(tip, trans_dict):
    for field_name, trans in trans_dict.items():
        excluded_field = trans == "exclude"
        if not excluded_field:
            tip[field_name] = apply_list_of_lists(tip[field_name], trans)
    return tip


def apply_list_of_lists(data, list_trans):
    temp = data
    if list_trans is not None:
        for lists in list_trans:
            if lists[1] == []:
                temp = lists[0](data)
            else:
                temp = lists[0](data, lists[1])
    return temp


def functionalize_trans_dict(trans_dict):
    temp = trans_dict.copy()
    for field_name, trans_vals in trans_dict.items():
        if trans_vals != "exclude":
            for trans in range(len(trans_vals)):
                temp[field_name][trans][0] = getattr(
                    vt, trans_dict[field_name][trans][0]
                )

    return temp


class Transform:
    def __init__(self, validate) -> None:
        # self._transforms = transform(orig, MandT, DC)
        self._validate = validate

    def transforms(self, source: dict) -> dict:
        destination = {}
        for val_transform in self._transforms:
            destination = val_transform[0](
                destination, source, self._validate, **val_transform[1]
            )
        return destination

    def __call__(self, orig, MandT, DC, **kwargs):
        # list or dict as return? - if Patient - dict, else, list
        # where do I read from? - Need cur_path and general path
        #cur_path = kwargs.get("cur_path", ["cases"])
        endpoint = kwargs.get("endpoint","cases")
        # path_to_read = kwargs.get("path_to_read", 'cases')
        if endpoint == "cases":
            return self.cases_transform(orig, MandT, DC, endpoint)
        elif endpoint == "files":
            return self.files_transform(orig, MandT, DC, endpoint)
    def cases_transform(self, orig, MandT, DC, endpoint):
        cur_path = ["cases"]
        tip = ruy.read_entry(orig, MandT, "Patient", DC=DC)
        tip = entity_value_transforms(tip, "Patient", MandT)
        linkers = ruy.add_linkers(orig, MandT, 'Patient', DC, linker=True, 
                              cur_path=cur_path, rel_path='cases',endpoint=endpoint)
        tip.update(linkers)
        #tip["File"] = add_File_rec(orig, MandT, DC)
        for field in ["ethnicity", "sex", "race"]:
            self._validate.distinct(tip, field)
        self._validate.agree(tip, tip["id"], ["ethnicity", "sex", "race"])
        RS = ruy.read_entry(orig, MandT, "ResearchSubject", DC=DC)
        RS = entity_value_transforms(RS, "ResearchSubject", MandT)
        linkers = ruy.add_linkers(orig, MandT, 'ResearchSubject', DC, linker=True, 
                              cur_path=cur_path, rel_path='cases',endpoint=endpoint)
        RS.update(linkers)
        for field in ["primary_diagnosis_condition", "primary_diagnosis_site"]:
            self._validate.distinct(RS, field)
        self._validate.agree(
            RS,
            RS["id"],
            ["primary_diagnosis_condition", "primary_diagnosis_site"],
        )
        #RS["File"] = add_File_rec(orig, MandT, DC)
        RS["Diagnosis"] = []
        diag_path = MandT["Diagnosis"]["Mapping"]["id"]
        diag_path = diag_path.split(".")
        diag_path.pop()
        diag_path = ".".join(diag_path)
        cur_path = diag_path.split(".")
        ent_rec = ruy.simp_read(orig, diag_path, cur_path, DC)
        if isinstance(ent_rec, list):
            for diag_rec in range(len(ent_rec)):
                temp_diag = ruy.read_entry(
                    orig, MandT, "Diagnosis", cur_path=cur_path + [diag_rec]
                )
                """ may need transformation step when Diagnosis records eventually have
                transformations"""

                treat_path = cur_path + [diag_rec, "treatments"]
                treat_gen_path = MandT["Treatment"]["Mapping"]["id"]
                treat_gen_path = treat_gen_path.split(".")
                treat_gen_path.pop()
                treat_gen_path = ".".join(treat_gen_path)
                treat_recs = ruy.simp_read(orig, treat_gen_path, treat_path, DC)
                if isinstance(treat_recs, list) and treat_recs != []:
                    temp_diag["Treatment"] = []
                    for treat in range(len(treat_recs)):
                        temp_diag["Treatment"].append(
                            ruy.read_entry(
                                orig, MandT, "Treatment", cur_path=treat_path + [treat]
                            )
                        )
                elif isinstance(treat_recs, dict):
                    temp_diag["Treatment"] = [
                        ruy.read_entry(orig, MandT, "Treatment", cur_path=treat_path)
                    ]
                else:
                    temp_diag["Treatment"] = []
                RS["Diagnosis"].append(temp_diag)
        elif isinstance(ent_rec, dict):
            temp_diag = ruy.read_entry(orig, MandT, "Diagnosis", cur_path=cur_path)
            treat_path = cur_path + ["Treatment"]
            treat_rec = ruy.simp_read(orig, diag_path, cur_path, DC)
            if isinstance(treat_rec, list) and treat_rec != []:
                temp_diag["Treatment"] = []
                for treat in range(len(treat_rec)):
                    temp_diag["Treatment"].append(
                        ruy.read_entry(
                            orig, MandT, "Treatment", cur_path=treat_path + [treat]
                        )
                    )
            elif isinstance(treat_rec, dict):
                temp_diag["Treatment"] = [
                    ruy.read_entry(orig, MandT, "Treatment", cur_path=treat_path)
                ]
            else:
                temp_diag["Treatment"] = []
            RS["Diagnosis"].append(temp_diag)
        else:
            RS["Diagnosis"] = []
        RS["Specimen"] = add_Specimen_rec(orig, MandT, DC)
        for specimen in RS["Specimen"]:
            for field in [
                "primary_disease_type",
                "source_material_type",
                "anatomical_site",
            ]:
                self._validate.distinct(specimen, field)
            # days to birth is negative days from birth until diagnosis. 73000 days is 200 years.
            self._validate.validate(
                specimen, "days_to_birth", lambda x: not x or -73000 < x < 0
            )
        # if 'Study' in MandT:
        #    study_path = MandT['Study']['Mapping']['id']
        #    study_path = study_path.split('.')
        #   study_path.pop()
        #   study_path = '.'.join(study_path)
        #   cur_path = study_path.split('.')
        #   RS['Study'] = [ruy.read_entry(orig, MandT, 'Study', cur_path=cur_path)]
        tip["ResearchSubject"] = [RS]
        return tip
    def files_transform(self, orig, MandT, DC, endpoint):
        tip = ruy.read_entry(orig, MandT, 'File', DC=DC, endpoint=endpoint)
        tip = entity_value_transforms(tip, "File", MandT)
        #linkers = ruy.add_linkers(orig, MandT, 'File', DC, linker=True, 
        #                            cur_path=[endpoint], rel_path=endpoint, endpoint=endpoint)
        #tip.update(linkers)
        tip['Subject'] = []
        tip['ResearchSubject'] = []
        tip['Specimen'] = []
        subj_path = MandT["Patient"]["Mapping"]["id"]
        subj_path = subj_path.split(".")
        subj_path.pop()
        subj_path = ".".join(subj_path)
        cur_path = subj_path.split(".")
        subject_rec = ruy.simp_read(orig, subj_path, cur_path, DC)
        for index in range(len(subject_rec)):
            temp_subject = ruy.read_entry(
                orig, MandT, "Patient", cur_path=cur_path + [index]
            )
            temp_subject = entity_value_transforms(temp_subject, "Patient", MandT)
            tip['Subject'].append(temp_subject)

        rs_path = MandT["ResearchSubject"]["Mapping"]["id"]
        rs_path = rs_path.split(".")
        rs_path.pop()
        rs_path = ".".join(rs_path)
        cur_path = rs_path.split(".")
        rs_rec = ruy.simp_read(orig, rs_path, cur_path, DC)
        for index in range(len(rs_rec)):
            RS_current_path = cur_path + [index]
            RS = ruy.read_entry(
                orig, MandT, "ResearchSubject", cur_path=RS_current_path
            )
            RS = entity_value_transforms(RS, "ResearchSubject", MandT)
            spec_rel_path = MandT['Specimen']['Mapping']['id']['samples'].split('.')
            spec_rel_path.pop()
            spec_rel_path = '.'.join(spec_rel_path)
            tip['Specimen'] += add_Specimen_rec(orig, MandT, DC, cur_path=RS_current_path+["samples"],
            rel_path=spec_rel_path, endpoint='files')
            diag_path = MandT["Diagnosis"]["Mapping"]["id"]
            diag_path = diag_path.split(".")
            diag_path.pop()
            #diag_path = ".".join(diag_path)
            diagcur_path = RS_current_path + [diag_path[-1]]
            diag_path = ".".join(diag_path)
            RS['Diagnosis'] = []
            ent_rec = ruy.simp_read(orig, diag_path, diagcur_path, DC)
            if isinstance(ent_rec, list):
                for diag_rec in range(len(ent_rec)):
                    temp_diag = ruy.read_entry(
                        orig, MandT, "Diagnosis", cur_path=diagcur_path + [diag_rec]
                    )
                    treat_path = MandT["Treatment"]["Mapping"]["id"]
                    treat_path = treat_path.split(".")
                    treat_path.pop()
                    #diag_path = ".".join(diag_path)
                    treatcur_path = diagcur_path + [diag_rec]+[treat_path[-1]]
                    treat_path = ".".join(treat_path)
                    temp_diag['Treatment'] = []
                    treat_recs = ruy.simp_read(orig, treat_path, treatcur_path, DC)
                    if isinstance(treat_recs, list) and treat_recs != []:
                        temp_diag["Treatment"] = []
                        for treat in range(len(treat_recs)):
                            temp_diag["Treatment"].append(
                                ruy.read_entry(
                                    orig, MandT, "Treatment", cur_path=treatcur_path + [treat]
                                )
                            )
                    elif isinstance(treat_recs, dict):
                        temp_diag["Treatment"] = [
                            ruy.read_entry(orig, MandT, "Treatment", cur_path=treatcur_path)
                        ]
                    #else:
                    #    temp_diag["Treatment"] = []
                    RS["Diagnosis"].append(temp_diag)
            elif isinstance(ent_rec, dict):
                temp_diag = ruy.read_entry(orig, MandT, "Diagnosis", cur_path=diagcur_path)
                treat_path = MandT["Treatment"]["Mapping"]["id"]
                treat_path = treat_path.split(".")
                treat_path.pop()
                #diag_path = ".".join(diag_path)
                treatcur_path = diagcur_path +[treat_path[-1]]
                treat_path = ".".join(treat_path)
                temp_diag['Treatment'] = []
                treat_rec = ruy.simp_read(orig, diag_path, treatcur_path, DC)
                if isinstance(treat_rec, list) and treat_rec != []:
                    temp_diag["Treatment"] = []
                    for treat in range(len(treat_rec)):
                        temp_diag["Treatment"].append(
                            ruy.read_entry(
                                orig, MandT, "Treatment", cur_path=treat_path + [treat]
                            )
                        )
                elif isinstance(treat_rec, dict):
                    temp_diag["Treatment"] = [
                        ruy.read_entry(orig, MandT, "Treatment", cur_path=treatcur_path)
                    ]
                else:
                    temp_diag["Treatment"] = []
                RS["Diagnosis"].append(temp_diag)
            #else:
            #    RS["Diagnosis"] = []

            tip['ResearchSubject'].append(RS)
            

        return tip
