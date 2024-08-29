#!/usr/bin/env python3 -u

import sys

from cda_etl.extract.idc.idc_extractor import IDC_extractor

from os import path, makedirs, system

version_string = sys.argv[1]

# Note: BigQuery returns rows in random order. Even if the underlying data hasn't
# changed, successive pulls will not generally produce precisely the same files.
# 
# Files from multiple pulls of the same data should be identical to one another
# after their rows have been sorted, though, and byte counts should match (once
# expanded -- zipped byte counts will still differ because each file's exact
# compression ratio depends on the order in which data is scanned during
# zip-style compression, and row order differs).

bq_project_name = YOUR_PROJECT_NAME

intermediate_bucket = YOUR_TEMP_BUCKET

target_table_path = YOUR_TARGET_TABLE

if not path.isfile( 'GCS-service-account-key.etl-github-testing.json' ):
    
    system( './999_create_service_account_key.etl-github-testing.bash' )

idc = IDC_extractor(
    
    gsa_key = 'GCS-service-account-key.etl-github-testing.json',
    source_version = version_string,
    source_table = 'dicom_all',
    dest_table = f'{bq_project_name}.{target_table_path}',
    dest_bucket = intermediate_bucket,
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
    'PatientBirthDate',
    'PatientSex',
    'EthnicGroup',
    'crdc_series_uuid',
    'SeriesDescription',
    'collection_id',
    'PatientID',
    'idc_case_id',
    'collection_tumorLocation',
    'AnatomicRegionSequence',
    'SpecimenDescriptionSequence',
    'SegmentSequence',
    'RTROIObservationsSequence',
    'SharedFunctionalGroupsSequence'
]

idc.query_idc_to_table( fields_to_pull )

idc.extract_table_to_bucket()

idc.download_bucket_data()


