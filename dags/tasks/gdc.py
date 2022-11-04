from airflow.decorators import task
from cdatransform.extract.gdc import GDC
from cdatransform.transform.transform_main import transform_case_or_file
from cdatransform.lib import make_harmonized_file_name


@task(task_id="gdc_extract_cases")
def gdc_cases(uuid: str, **kwargs):
    print("Extracting GDC")
    gdc = GDC()
    file_name = f"gdc.all_cases_{uuid}.jsonl.gz"
    gdc.save_cases(file_name)

    return file_name


@task(task_id="gdc_extract_files")
def gdc_files(uuid: str, **kwargs):
    gdc = GDC()
    file_name = f"gdc.all_files_{uuid}.jsonl.gz"
    gdc.save_files(file_name)

    return file_name


@task(task_id="gdc_transform_cases")
def gdc_transform_cases(extract_result: str, **kwargs):
    output_file_name = make_harmonized_file_name(extract_result)
    transform_case_or_file(
        input_file=extract_result,
        output_file=output_file_name,
        endpoint="cases",
        yaml_mapping_transform_file="GDC_subject_endpoint_mapping.yml",
    )

    return output_file_name


@task(task_id="gdc_transform")
def gdc_transform_files(extract_result: str, **kwargs):
    output_file_name = make_harmonized_file_name(extract_result)
    transform_case_or_file(
        input_file=extract_result,
        output_file=output_file_name,
        endpoint="files",
        yaml_mapping_transform_file="GDC_file_endpoint_mapping.yml",
    )

    return output_file_name


@task(task_id="gdc_aggregate_cases")
def gdc_aggregate_cases(uuid: str, transform_result: str, **kwargs):
    return f"gdc.all_Subjects.{uuid}.jsonl.gz"


@task(task_id="gdc_aggregate_files")
def gdc_aggregate_files(uuid: str, transform_result: str, **kwargs):
    return f"gdc.all_Files.{uuid}.jsonl.gz"
