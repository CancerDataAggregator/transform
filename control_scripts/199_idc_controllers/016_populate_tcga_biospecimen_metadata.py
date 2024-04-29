#!/usr/bin/env python3 -u

import sys

from cda_etl.transform.idc.idc_transformer import IDC_transformer

version_string = sys.argv[1]

idc = IDC_transformer( source_version = version_string )

idc.extract_specimen_data_from_tcga_biospecimen_rel9()


