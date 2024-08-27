#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/auxiliary_scripts/*pl

sop_class_uid_url="https://dicom.nema.org/medical/dicom/current/source/docbook/part04/part04.xml"

dicom_metadata_output_dir="./auxiliary_metadata/__DICOM_code_maps/raw"

mkdir -p $dicom_metadata_output_dir

sop_class_uid_file=`echo -n $sop_class_uid_url | sed "s/^.*\///"`

sop_class_uid_file="${dicom_metadata_output_dir}/$sop_class_uid_file"

echo curl -o $sop_class_uid_file $sop_class_uid_url
curl -o $sop_class_uid_file $sop_class_uid_url

echo ./package_root/auxiliary_scripts/200_get_SOPClassUID_tsv.pl
./package_root/auxiliary_scripts/200_get_SOPClassUID_tsv.pl

echo ./package_root/auxiliary_scripts/201_get_retired_SOPClassUID_tsv.pl
./package_root/auxiliary_scripts/201_get_retired_SOPClassUID_tsv.pl

dicom_controlled_terms_url="https://dicom.nema.org/medical/dicom/current/source/docbook/part16/part16.xml"

dicom_controlled_terms_file=`echo -n $dicom_controlled_terms_url | sed "s/^.*\///"`

dicom_controlled_terms_file="${dicom_metadata_output_dir}/$dicom_controlled_terms_file"

echo curl -o $dicom_controlled_terms_file $dicom_controlled_terms_url
curl -o $dicom_controlled_terms_file $dicom_controlled_terms_url

echo ./package_root/auxiliary_scripts/202_get_DICOM_controlled_terms_tsv.pl
./package_root/auxiliary_scripts/202_get_DICOM_controlled_terms_tsv.pl


