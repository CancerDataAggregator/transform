from typing import Dict
from airflow.decorators import task


@task(task_id="aggregation_task")
def aggregation_task(dc_result: Dict):
    print("Aggregating")

    return "aggregated.jsonl.gz"
