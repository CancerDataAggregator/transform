#!/usr/bin/env python3

from etl.transform.idc.idc_transformer import IDC_transformer

from os import path, makedirs, system

idc = IDC_transformer( source_version = 'v13' )

idc.extract_file_subject_and_rs_data_from_dicom_all()


