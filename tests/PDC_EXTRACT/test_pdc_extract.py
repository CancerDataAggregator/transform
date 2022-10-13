from dags.cdatransform.extract.pdc import PDC

pdc = PDC()

pdc.save_cases("pdc.all_cases.jsonl.gz")
# pdc.save_files("")
