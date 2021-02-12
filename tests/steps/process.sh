# Generate all files for data transform steps on examples

cda-transform gdc_TARGET_case1.jsonl.gz gdc_TARGET_case1_harmonized_output.jsonl.gz ../../gdc-transform.yml
python json2yaml.py gdc_TARGET_case1_harmonized_output.jsonl.gz gdc_TARGET_case1_harmonized_output.yml

cda-transform pdc_QC1_case1.jsonl.gz pdc_QC1_case1_harmonized_output.jsonl.gz ../../pdc-transform.yml
python json2yaml.py gdc_TARGET_case1_harmonized_output.jsonl.gz gdc_TARGET_case1_harmonized_output.yml


# Generate the input data for the aggregation step
cat gdc_TARGET_case1.jsonl.gz gdc_TARGET_case2.jsonl.gz > gdc_TARGET.jsonl.gz
cda-transform gdc_TARGET.jsonl.gz gdc_TARGET_H.jsonl.gz ../../gdc-transform.yml
cda-aggregate ../../merge.yml gdc_TARGET_H.jsonl.gz gdc_TARGET_HA.jsonl.gz
