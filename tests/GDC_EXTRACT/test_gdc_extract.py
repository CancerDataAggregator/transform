from dags.cdatransform.extract.gdc import GDC

GDC(make_spec_file="test.jsonl.gz").save_cases("tests_out.jsonl.gz")
