import argparse
import json
import sys
import jsonlines
import yaml
import datetime
import gzip

import cdatransform.merge.merge_functions as mf
def main():
    parser = argparse.ArgumentParser(description="Aggregate cases data from DC to Patient level.")
    parser.add_argument("merge_file", help="file name for how to merge fields. Should end with .yml")
    parser.add_argument("input_file", help="Input file name. Should be output file of transform function. Should end with .gz")
    parser.add_argument("output_file", help="Out file name. Should end with .gz")
    args = parser.parse_args()
    with open(args["merge_file"]) as file:
        how_to_merge = yaml.full_load(file)
    with gzip.open(args.input_file, "r") as infp:
        readDC = jsonlines.Reader(infp)
        patient_case_mapping = dict({})
        line_count = 0
        lines = []
        for case in readDC:
            patient_id = case.get("id")
            if patient_id in patient_case_mapping:
                patient_case_mapping[patient_id]['cases'].append(case.get('Research_Subject')[0].
                                                                 get('id'))
                patient_case_mapping[patient_id]['lines'].append(line_count)
            else:
                patient_case_mapping[patient_id] = dict({'cases':
                                                         [case.get('Research_Subject')[0].get('id')],
                                                         'lines':[line_count]
                                                        })
            lines.append(case)
            line_count +=1
            
    with gzip.open(args.output_file, "w") as outfp:
        writeDC = jsonlines.Writer(outfp)
        for patient in patient_case_mapping:
            lines_cases = patient_case_mapping[patient]['lines']
            if len(lines_cases)==1:
                writeDC.write(lines[lines_cases[0]])
            else:
                entities = dict({})
                for line in lines_cases:
                    entities[line]=(lines[line])
                merged_entry = mf.merge_fields_level(entities,how_to_merge['Patient_merge'],
                                                     lines_cases)
                writeDC.write(merged_entry)
if __name__ == "__main__":
    main()
    