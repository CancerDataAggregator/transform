from dags.cdatransform.extract.gdc import GDC

GDC().save_cases("gdc.all_cases.jsonl.gz")
GDC().save_files("gdc.all_files.jsonl.gz")
