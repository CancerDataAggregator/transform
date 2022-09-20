from typing import Dict
from airflow.decorators import task

@task(task_id='load_extract')
def load_task(dc_result: Dict):
  print('Loading')

  return 'loaded.jsonl.gz'
