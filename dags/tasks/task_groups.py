from airflow.decorators import task_group
from tasks.gdc import gdc_extract, gdc_load, gdc_transform
from tasks.idc import idc_extract, idc_load, idc_transform
from tasks.pdc import pdc_extract, pdc_load, pdc_transform

@task_group(group_id='GDC')
def gdc_task_group(**kwargs):
  return { 'gdc': gdc_load(gdc_transform(gdc_extract())) }

@task_group(group_id='IDC')
def idc_task_group(**kwargs):
  return { 'idc': idc_load(idc_transform(idc_extract())) }

@task_group(group_id='PDC')
def pdc_task_group(**kwargs):
  return { 'pdc': pdc_load(pdc_transform(pdc_extract())) }

@task_group(group_id='dc_group')
def dc_task_group():
  return {**gdc_task_group(), **idc_task_group(), **pdc_task_group()}
