#!/usr/bin/env bash

chmod 755 ./package_root/auxiliary_scripts/*py ./package_root/transform/idc/scripts/phase_001_collate/*py

echo ./package_root/transform/idc/scripts/phase_001_collate/001_auxiliary_metadata.py
./package_root/transform/idc/scripts/phase_001_collate/001_auxiliary_metadata.py

echo ./package_root/transform/idc/scripts/phase_001_collate/002_original_collections_metadata.py
./package_root/transform/idc/scripts/phase_001_collate/002_original_collections_metadata.py

echo ./package_root/transform/idc/scripts/phase_001_collate/003_tcga_biospecimen_rel9.py
./package_root/transform/idc/scripts/phase_001_collate/003_tcga_biospecimen_rel9.py

echo ./package_root/transform/idc/scripts/phase_001_collate/004_tcga_clinical_rel9.py
./package_root/transform/idc/scripts/phase_001_collate/004_tcga_clinical_rel9.py

echo ./package_root/transform/idc/scripts/phase_001_collate/005_dicom_all.py
./package_root/transform/idc/scripts/phase_001_collate/005_dicom_all.py

echo ./package_root/transform/idc/scripts/phase_001_collate/010_collate_case_data.py
./package_root/transform/idc/scripts/phase_001_collate/010_collate_case_data.py

echo ./package_root/auxiliary_scripts/400_resolve_sample_crossrefs_for_IDC_DICOM_series_and_import_anatomic_and_tumor_vs_normal_annotations_from_other_DCs.py
./package_root/auxiliary_scripts/400_resolve_sample_crossrefs_for_IDC_DICOM_series_and_import_anatomic_and_tumor_vs_normal_annotations_from_other_DCs.py

################################################################################
# HELPFUL MESSAGE

icd_o_3_refdoc_url="https://iris.who.int/bitstream/handle/10665/96612/9789241548496_eng.pdf"

echo
echo "Please get the WHO ICD-O-3 reference publication (last seen at ${icd_o_3_refdoc_url}, p. 33ff) and export the Topography codes as a 2-column map (column headers: 'icd_o_3_code', 'icd_o_3_preferred_name') to ${ontology_metadata_output_root}/ICD-O-3/icd_topography.tsv before proceeding."
echo
echo

