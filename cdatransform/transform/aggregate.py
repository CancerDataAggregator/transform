import argparse
from typing import DefaultDict
import jsonlines
import yaml
import gzip
import logging
import cdatransform.transform.merge.merge_functions as mf
from cdatransform.transform.validate import LogValidation

def get_coalesce_field_names(merge_field_dict):
    coal_fields = []
    for key,val in merge_field_dict.items():
        if val.get("merge_type") == 'coalesce':
            coal_fields.append(key)
    return coal_fields
def prep_log_merge_error(entities,merge_field_dict):
    sources = list(entities.keys())
    coal_fields = get_coalesce_field_names(merge_field_dict)
    ret_dat = dict()
    for source in sources:
        temp = dict()
        for field in coal_fields:
            temp[field]=entities.get(source).get(field)
        ret_dat[source] = temp
    for source,val in entities.items():
        if val.get('id') is not None:
            patient_id = val.get('id')
            break
    return coal_fields,ret_dat,patient_id
def log_merge_error(entities,all_sources,fields):
    coal_fields,coal_dat,patient_id = prep_log_merge_error(entities,fields)
    log = LogValidation()
    all_sources.append(patient_id)
    prefix = '_'.join(all_sources)
    log.agree_sources(coal_dat, '_'.join(all_sources), coal_fields)
    log.generate_report(logging.getLogger('test'))
def main():
    parser = argparse.ArgumentParser(
        description="Aggregate cases data from DC to Patient level."
    )
    parser.add_argument(
        "merge_file", help="file name for how to merge fields. Should end with .yml"
    )
    parser.add_argument(
        "input_file",
        help="Input file name. Should be output file of transform function. Should end with .gz",
    )
    parser.add_argument("output_file", help="Out file name. Should end with .gz")
    args = parser.parse_args()
    # yaml.load(open(transform_file, "r"), Loader=Loader)
    with open(args.merge_file) as file:
        how_to_merge = yaml.full_load(file)
    with gzip.open(args.input_file, "r") as infp:
        readDC = jsonlines.Reader(infp)
        patient_case_mapping = DefaultDict(list)
        for case in readDC:
            patient_id = case.get("id")
            patient_case_mapping[patient_id] += [case]

    with gzip.open(args.output_file, "w") as outfp:
        writeDC = jsonlines.Writer(outfp)
        for patient_id, patients in patient_case_mapping.items():
            if len(patients) == 1:
                writeDC.write(patients[0])
            else:
                entities = {k: patient for k, patient in enumerate(patients)}
                lines_cases = list(range(len(patients)))

                merged_entry = mf.merge_fields_level(
                    entities, how_to_merge["Patient_merge"], lines_cases
                )
                case_ids = [patient.get('ResearchSubject')[0].get('id') for patient in patients]
                log_merge_error(entities,case_ids, how_to_merge["Patient_merge"])
                writeDC.write(merged_entry)


if __name__ == "__main__":
    main()
