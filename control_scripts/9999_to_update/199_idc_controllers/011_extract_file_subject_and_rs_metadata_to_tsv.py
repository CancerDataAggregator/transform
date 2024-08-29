#!/usr/bin/env python3 -u

import sys

from cda_etl.transform.idc.idc_transformer import IDC_transformer

from os import path, makedirs, system

version_string = sys.argv[1]

idc = IDC_transformer( source_version = version_string )

idc.extract_file_subject_and_rs_data_from_dicom_all()


