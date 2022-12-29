import logging
from uuid import uuid4

import pendulum
from airflow.decorators import dag
from tasks.schema_transform import schema_transform_task
from tasks.task_groups import (
    dc_task_group,
    load_task_group,
    merge_task_group,
    schema_task_group,
)

from cdatransform.services.context_service import ContextService

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
    ContextService().validate()

    dc_group = dc_task_group

    merge_group = merge_task_group

    schema_group = schema_task_group
    load_group = load_task_group

    uuid = str(uuid4().hex)
    schema_transform_task(load_group(schema_group(uuid, merge_group(dc_group(uuid)))))


cda_etl = cda_etl()
