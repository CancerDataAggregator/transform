from airflow.decorators import task

@task(task_id='gdc_extract')
def gdc_extract(**kwargs):
  print('Extracting GDC')

  return 'gdc_file.jsonl.gz'

@task(task_id='gdc_transform')
def gdc_transform(transform_result: str, **kwargs):
  print('Transforming GDC')

  return 'gdc_transformed.jsonl.gz'

@task(task_id='gdc_load')
def gdc_load(transform_result: str, **kwargs):
  print('Loading GDC')

  return 'gdc_loaded.jsonl.gz'
