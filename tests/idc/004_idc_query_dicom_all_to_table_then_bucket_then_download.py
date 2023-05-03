#!/usr/bin/env python3

from etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './tests/idc/create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = 'v13',
    source_table = 'dicom_all',
    dest_table = 'broad-cda-dev.github_testing.idc_patient_testing',
    dest_bucket = 'gdc-bq-sample-bucket',
    dest_bucket_filename = 'dicom_all-*.jsonl.gz',
    output_filename = 'dicom_all.jsonl.gz'
)

fields_to_pull = [
    
    'SOPInstanceUID',
    'gcs_url',
    'SOPClassUID',
    'crdc_instance_uuid',
    'instance_size',
    'instance_hash',
    'Modality',
    'tcia_tumorLocation',
    'PatientSpeciesDescription',
    'PatientSex',
    'crdc_series_uuid',
    'collection_id',
    'idc_webapp_collection_id',
    'PatientID',
    'idc_case_id'
]

idc.query_idc_to_table( fields_to_pull )

idc.extract_table_to_bucket()

idc.download_bucket_data()

