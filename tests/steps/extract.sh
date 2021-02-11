# Generate GDC examples

extract-gdc gdc_TARGET_case1.jsonl.gz ../integration/gdc.samples-per-file.jsonl.gz --case 7eeced68-1717-4116-bcee-328ac70a9682
extract-gdc gdc_TARGET_case2.jsonl.gz ../integration/gdc.samples-per-file.jsonl.gz --case 9e229e56-f7e1-58f9-984b-a9453be5dc9a


# Generate PDC examples
extract-pdc pdc_QC1_case1.jsonl.gz ../integration/pdc.files-per-sample-dict.json.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
extract-pdc pdc_QC1_case2.jsonl.gz ../integration/pdc.files-per-sample-dict.json.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83
