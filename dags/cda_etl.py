import logging
import os

import pendulum

from airflow.decorators import dag
from tasks.load import load_task
from tasks.task_groups import dc_task_group
from tasks.aggregation import aggregation_task


log = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
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

    loader(aggregator(dc_group()))


cda_etl = cda_etl()
