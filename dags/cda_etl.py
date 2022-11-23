import logging
from uuid import uuid4

import pendulum

from airflow.decorators import dag
from tasks.load import load_task
from tasks.task_groups import dc_task_group
from tasks.aggregation import aggregation_task


log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "wait_for_downstream": False,
    "concurrency": 3,
}


@dag(
    dag_id="cda_etl",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 2, 1, tz="UTC"),
    default_args=default_args,
    catchup=False,
    tags=["CDA"],
)
def cda_etl():
    dc_group = dc_task_group

    aggregator = aggregation_task

    loader = load_task

    uuid = str(uuid4().hex)
    loader(aggregator(dc_group(uuid)))


cda_etl = cda_etl()
