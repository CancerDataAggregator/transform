from airflow.decorators import task


@task(task_id="pdc_extract")
def pdc_extract(**kwargs):
    print("Extracting PDC")

    return "pdc_file.jsonl.gz"


@task(task_id="pdc_transform")
def pdc_transform(extraction_result: str, **kwargs):
    print("Transforming PDC")

    return "pdc_transformed.jsonl.gz"


@task(task_id="pdc_load")
def pdc_load(transform_result: str, **kwargs):
    print("Loading PDC")

    return "pdc_loaded.jsonl.gz"
