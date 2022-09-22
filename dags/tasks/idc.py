from airflow.decorators import task


@task(task_id="idc_extract")
def idc_extract(**kwargs):
    print("Extracting IDC")

    return "idc_file.jsonl.gz"


@task(task_id="idc_transform")
def idc_transform(transform_result: str, **kwargs):
    print("Transforming IDC")

    return "idc_transformed.jsonl.gz"


@task(task_id="idc_load")
def idc_load(transform_result: str, **kwargs):
    print("Loading IDC")

    return "idc_loaded.jsonl.gz"
