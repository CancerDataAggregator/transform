

from typing_extensions import Literal
from ..read_using_YAML import (
    simp_read,
    det_tree_to_collapse,
    read_entry,
    add_linkers,
)
import dags.cdatransform.transform.transform_lib.value_transformations 
from typing import Union, Callable


def add_Specimen_rec(
    orig, MandT, **kwargs
) -> list:
    cur_path: list[Union[str, int]] = kwargs.get("cur_path", ["cases", "samples"])
    spec_type: str = kwargs.get("spec_type", "samples")
    rel_path: str = kwargs.get("rel_path", "cases.samples")
    endpoint: str = kwargs.get("endpoint", "cases")
    spec: list = []
    tree: dict[str, Union[dict[str, Union[dict, None]], None]] = kwargs.get(
        "tree", det_tree_to_collapse(MandT, "Specimen")
    )
    if simp_read(orig, rel_path, cur_path) is not None:
        for spec_rec_ind in range(len(simp_read(orig, rel_path, cur_path))):
            spec_path = cur_path.copy()
            spec_path.append(spec_rec_ind)
            spec_rec = read_entry(
                orig, MandT, "Specimen", cur_path=spec_path, spec_type=spec_type
            )
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
                        tree=branches_dict,
                        cur_path=nest_path,
                        spec_type=nest_spec_type,
                        rel_path=nest_rel_path,
                        endpoint=endpoint,
                    )
                    spec_rec += nest_rec
            spec += spec_rec
    return spec


def add_File_rec(orig, MandT, **kwargs):
    File_recs = []
    file_rec_name = "files"
    cur_path: list[Union[str, int]] = kwargs.get("cur_path", ["cases"])
    rel_path = kwargs.get("rel_path", "cases")
    file_rel_path = rel_path + "." + file_rec_name
    if isinstance(simp_read(orig, file_rel_path, cur_path + [file_rec_name]), list):
        for file_ind in range(
            len(simp_read(orig, file_rel_path, cur_path + [file_rec_name]))
        ):
            file_path = cur_path.copy()
            file_path.append(file_rec_name)
            file_path.append(file_ind)
            file_rec = read_entry(
                orig, MandT, "File", cur_path=file_path, rel_path=file_rel_path
            )
            file_rec = entity_value_transforms(file_rec, "File", MandT)
            File_recs.append(file_rec)
    return File_recs


# Functions to apply the transforms to relevant fields and functionalize Transformation dictionary
def entity_value_transforms(
    tip: dict,
    entity: str,
    MandT: dict,
) -> dict:
    if (
        "Transformations" in MandT[entity]
        and MandT[entity]["Transformations"] is not None
    ):
        trans_dict: dict[str, list[list[Union[Callable, list]]]] = MandT[entity][
            "Transformations"
        ]
        tip = apply_transformations(tip, trans_dict)
    return tip


def apply_transformations(
    tip: dict,
    trans_dict: dict,
) -> dict:
    for field_name, trans in trans_dict.items():
        # excluded_field = trans == "exclude"
        # if not excluded_field:
        tip[field_name] = apply_list_of_lists(tip[field_name], trans)
    return tip


def apply_list_of_lists(
    data: Union[str, int, list],
    list_trans: Union[None, list],
) -> Union[str, int, list]:
    temp = data
    if list_trans is not None:
        for lists in list_trans:
            if lists[1] == []:
                temp = lists[0](data)
            else:
                temp = lists[0](data, lists[1])
    return temp


def functionalize_trans_dict(
    trans_dict: dict
) -> dict:
    """Takes a transformation dictionary from mapping_and_transformation loaded yaml, and replaces the string
    denoting a function call, with an actual callable. Searches local for function with name of
    the string
    Transformations:
        species:
            - ['lower_case', [] ]
        race:
            - ['lower_case', [] ]
        ethnicity:
            - ['lower_case', [] ]
    Replaces 'lower_case' str with the callable lower_case() function, as defined in the
    transform_lib/value_transformations.py file

    Args:
        trans_dict (dict[str, list[list[Union[str, list]]]]): The transformation dict as seen above
    Returns:
        dict[str, list[list[Union[Callable,str, list]]]]:
    """
    temp: dict[str, list[list[Union[Callable, str, list]]]] = trans_dict.copy()
    for field_name, trans_vals in trans_dict.items():
        if trans_vals != "exclude":
            for index in range(len(trans_vals)):
                # This dict get the first value from dict it will return a list of list or a function
                temp[field_name][index][0] = getattr(
                    value_transformations, trans_dict[field_name][index][0]
                )

    return temp


class Transform:
    def __init__(self, MandT, endpoint) -> None:
        self.MandT = MandT
        self.endpoint = endpoint

    def __call__(
        self, orig: dict
    ) -> dict:
        if self.endpoint == "cases":
            return self.cases_transform(orig)
        else:  # self.endpoint == "files": May add more endpoint transformations... like mutations
            return self.files_transform(orig)

    def cases_transform(
        self, orig: dict
    ) -> dict:
        cur_path: list[Literal["cases"]] = ["cases"]
        tip: dict = read_entry(
            orig, self.MandT, "Patient"
        )
        tip = entity_value_transforms(tip, "Patient", self.MandT)
        # linkers = add_linkers(
        #    orig,
        #    MandT,
        #    "Patient",
        #    linker=True,
        #    cur_path=cur_path,
        #    rel_path="cases",
        #    endpoint=endpoint,
        # )
        # tip.update(linkers)
        # tip["File"] = add_File_rec(orig, MandT, DC)
        # t0 = time.time()
        # for field in ["ethnicity", "sex", "race"]:
        #    self._validate.distinct(tip, field)
        # sys.stderr.write("Subject distinct time: " + str(t0 - time.time()))
        # t0 = time.time()
        # self._validate.agree(tip, tip["id"], ["ethnicity", "sex", "race"])
        # sys.stderr.write("Subject validation time: " + str(t0 - time.time()))
        RS = read_entry(orig, self.MandT, "ResearchSubject")
        RS = entity_value_transforms(RS, "ResearchSubject", self.MandT)
        # linkers = add_linkers(
        #    orig,
        #    MandT,
        #    "ResearchSubject",
        #    linker=True,
        #    cur_path=cur_path,
        #    rel_path="cases",
        #    endpoint=endpoint,
        # )
        # RS.update(linkers)
        # t0 = time.time()
        # for field in ["primary_diagnosis_condition", "primary_diagnosis_site"]:
        #    self._validate.distinct(RS, field)  #
        # sys.stderr.write("ResearchSubject distinct time: " + str(t0 - time.time()))
        # t0 = time.time()
        # self._validate.agree(
        #    RS,
        #    RS["id"],
        #    ["primary_diagnosis_condition", "primary_diagnosis_site"],
        # )
        # sys.stderr.write("ResearchSubject validation time: " + str(t0 - time.time()))
        # RS["File"] = add_File_rec(orig, MandT, DC)
        RS["Diagnosis"] = []
        diag_path = self.MandT["Diagnosis"]["Mapping"]["id"]
        diag_path = diag_path.split(".")
        diag_path.pop()
        diag_path = ".".join(diag_path)
        cur_path = diag_path.split(".")
        ent_rec = simp_read(orig, diag_path, cur_path)
        if isinstance(ent_rec, list):
            for diag_rec in range(len(ent_rec)):
                temp_diag = read_entry(
                    orig, self.MandT, "Diagnosis", cur_path=cur_path + [diag_rec]
                )
                """ may need transformation step when Diagnosis records eventually have
                transformations"""

                treat_path = cur_path + [diag_rec, "treatments"]
                treat_gen_path = self.MandT["Treatment"]["Mapping"]["id"]
                treat_gen_path = treat_gen_path.split(".")
                treat_gen_path.pop()
                treat_gen_path = ".".join(treat_gen_path)
                treat_recs = simp_read(orig, treat_gen_path, treat_path)
                if isinstance(treat_recs, list) and treat_recs != []:
                    temp_diag["Treatment"] = []
                    for treat in range(len(treat_recs)):
                        temp_diag["Treatment"].append(
                            read_entry(
                                orig,
                                self.MandT,
                                "Treatment",
                                cur_path=treat_path + [treat],
                            )
                        )
                elif isinstance(treat_recs, dict):
                    temp_diag["Treatment"] = [
                        read_entry(orig, self.MandT, "Treatment", cur_path=treat_path)
                    ]
                else:
                    temp_diag["Treatment"] = []
                RS["Diagnosis"].append(temp_diag)
        elif isinstance(ent_rec, dict):
            temp_diag = read_entry(orig, self.MandT, "Diagnosis", cur_path=cur_path)
            treat_path = cur_path + ["Treatment"]
            treat_rec = simp_read(orig, diag_path, cur_path)
            if isinstance(treat_rec, list) and treat_rec != []:
                temp_diag["Treatment"] = []
                for treat in range(len(treat_rec)):
                    temp_diag["Treatment"].append(
                        read_entry(
                            orig, self.MandT, "Treatment", cur_path=treat_path + [treat]
                        )
                    )
            elif isinstance(treat_rec, dict):
                temp_diag["Treatment"] = [
                    read_entry(orig, self.MandT, "Treatment", cur_path=treat_path)
                ]
            else:
                temp_diag["Treatment"] = []
            RS["Diagnosis"].append(temp_diag)
        else:
            RS["Diagnosis"] = []
        RS["Specimen"] = add_Specimen_rec(orig, self.MandT)
        for specimen in RS["Specimen"]:
            specimen = entity_value_transforms(specimen, "Specimen", self.MandT)

        # if 'Study' in MandT:
        #    study_path = MandT['Study']['Mapping']['id']
        #    study_path = study_path.split('.')
        #   study_path.pop()
        #   study_path = '.'.join(study_path)
        #   cur_path = study_path.split('.')
        #   RS['Study'] = [read_entry(orig, MandT, 'Study', cur_path=cur_path)]
        tip["ResearchSubject"] = [RS]
        return tip

    def files_transform(
        self, orig: dict
    ) -> dict:
        tip = read_entry(orig, self.MandT, "File", endpoint=self.endpoint)
        tip = entity_value_transforms(tip, "File", self.MandT)
        linkers = add_linkers(
            orig,
            self.MandT,
            "File",
            linker=True,
            cur_path=[self.endpoint],
            rel_path=self.endpoint,
            endpoint=self.endpoint,
        )
        tip.update(linkers)
        # tip["Subject"] = []
        # tip["ResearchSubject"] = []
        # tip["Specimen"] = []
        # subj_path = MandT["Patient"]["Mapping"]["id"]
        # subj_path = subj_path.split(".")
        # subj_path.pop()
        # subj_path = ".".join(subj_path)
        # cur_path = subj_path.split(".")
        # subject_rec = simp_read(orig, subj_path, cur_path)
        # for index in range(len(subject_rec)):
        #    temp_subject = read_entry(
        #        orig, MandT, "Patient", cur_path=cur_path + [index]
        #    )
        #    temp_subject = entity_value_transforms(temp_subject, "Patient", MandT)
        #    tip["Subject"].append(temp_subject)

        # rs_path = MandT["ResearchSubject"]["Mapping"]["id"]
        # rs_path = rs_path.split(".")
        # rs_path.pop()
        # rs_path = ".".join(rs_path)
        # cur_path = rs_path.split(".")
        # rs_rec = simp_read(orig, rs_path, cur_path)
        # for index in range(len(rs_rec)):
        #    RS_current_path = cur_path + [index]
        #    RS = read_entry(
        #        orig, MandT, "ResearchSubject", cur_path=RS_current_path
        #    )
        #    RS = entity_value_transforms(RS, "ResearchSubject", MandT)
        #    spec_rel_path = MandT["Specimen"]["Mapping"]["id"]["samples"].split(".")
        #    spec_rel_path.pop()
        #    spec_rel_path = ".".join(spec_rel_path)
        #    tip["Specimen"] += add_Specimen_rec(
        #        orig,
        #        MandT,
        #        cur_path=RS_current_path + ["samples"],
        #        rel_path=spec_rel_path,
        #        endpoint="files",
        #    )
        #    for specimen in tip["Specimen"]:
        #        specimen = entity_value_transforms(specimen, "Specimen", MandT)
        #    diag_path = MandT["Diagnosis"]["Mapping"]["id"]
        #    diag_path = diag_path.split(".")
        #    diag_path.pop()
        # diag_path = ".".join(diag_path)
        #    diagcur_path = RS_current_path + [diag_path[-1]]
        #    diag_path = ".".join(diag_path)
        #    RS["Diagnosis"] = []
        #    ent_rec = simp_read(orig, diag_path, diagcur_path)
        #    if isinstance(ent_rec, list):
        #        for diag_rec in range(len(ent_rec)):
        #            temp_diag = read_entry(
        #                orig, MandT, "Diagnosis", cur_path=diagcur_path + [diag_rec]
        #            )
        #            treat_path = MandT["Treatment"]["Mapping"]["id"]
        #            treat_path = treat_path.split(".")
        #            treat_path.pop()
        # diag_path = ".".join(diag_path)
        #            treatcur_path = diagcur_path + [diag_rec] + [treat_path[-1]]
        #            treat_path = ".".join(treat_path)
        #            temp_diag["Treatment"] = []
        #            treat_recs = simp_read(orig, treat_path, treatcur_path)
        #            if isinstance(treat_recs, list) and treat_recs != []:
        #                temp_diag["Treatment"] = []
        #                for treat in range(len(treat_recs)):
        #                    temp_diag["Treatment"].append(
        #                        read_entry(
        #                            orig,
        #                            MandT,
        #                            "Treatment",
        #                            cur_path=treatcur_path + [treat],
        #                        )
        #                    )
        #            elif isinstance(treat_recs, dict):
        #                temp_diag["Treatment"] = [
        #                    read_entry(
        #                        orig, MandT, "Treatment", cur_path=treatcur_path
        #                    )
        #                ]
        # else:
        #    temp_diag["Treatment"] = []
        #            RS["Diagnosis"].append(temp_diag)
        #    elif isinstance(ent_rec, dict):
        #        temp_diag = read_entry(
        #            orig, MandT, "Diagnosis", cur_path=diagcur_path
        #        )
        #        treat_path = MandT["Treatment"]["Mapping"]["id"]
        #        treat_path = treat_path.split(".")
        #        treat_path.pop()
        # diag_path = ".".join(diag_path)
        #        treatcur_path = diagcur_path + [treat_path[-1]]
        #        treat_path = ".".join(treat_path)
        #        temp_diag["Treatment"] = []
        #        treat_rec = simp_read(orig, diag_path, treatcur_path)
        #        if isinstance(treat_rec, list) and treat_rec != []:
        #            temp_diag["Treatment"] = []
        #            for treat in range(len(treat_rec)):
        #                temp_diag["Treatment"].append(
        #                    read_entry(
        #                        orig, MandT, "Treatment", cur_path=treat_path + [treat]
        #                    )
        #                )
        #        elif isinstance(treat_rec, dict):
        #            temp_diag["Treatment"] = [
        #                read_entry(orig, MandT, "Treatment", cur_path=treatcur_path)
        #            ]
        #        else:
        #            temp_diag["Treatment"] = []
        #        RS["Diagnosis"].append(temp_diag)
        # else:
        #    RS["Diagnosis"] = []

        #    tip["ResearchSubject"].append(RS)

        return tip
