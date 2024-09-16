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


