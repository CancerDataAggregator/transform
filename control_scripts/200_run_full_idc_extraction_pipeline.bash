#!/usr/bin/env bash

chmod 755 ./199_idc_controllers/*py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/000_idc_query_auxiliary_metadata_to_table_then_bucket_then_download.py
echo ==================================================================================================
echo
./199_idc_controllers/000_idc_query_auxiliary_metadata_to_table_then_bucket_then_download.py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/001_idc_query_original_collections_metadata_to_table_then_bucket_then_download.py
echo ==================================================================================================
echo
./199_idc_controllers/001_idc_query_original_collections_metadata_to_table_then_bucket_then_download.py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/002_idc_query_tcga_biospecimen_rel9_to_table_then_bucket_then_download.py
echo ==================================================================================================
echo
./199_idc_controllers/002_idc_query_tcga_biospecimen_rel9_to_table_then_bucket_then_download.py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/003_idc_query_tcga_clinical_rel9_to_table_then_bucket_then_download.py
echo ==================================================================================================
echo
./199_idc_controllers/003_idc_query_tcga_clinical_rel9_to_table_then_bucket_then_download.py

echo ==================================================================================================
echo BEGIN ./199_idc_controllers/004_idc_query_dicom_all_to_table_then_bucket_then_download.py
echo ==================================================================================================
echo
./199_idc_controllers/004_idc_query_dicom_all_to_table_then_bucket_then_download.py


