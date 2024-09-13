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

echo ./package_root/transform/idc/scripts/phase_001_collate/005_dicom_all.extract_series_records_and_compute_instance_summary_stats.py
./package_root/transform/idc/scripts/phase_001_collate/005_dicom_all.extract_series_records_and_compute_instance_summary_stats.py

echo ./package_root/transform/idc/scripts/phase_001_collate/006_dicom_all.extract_instance_records.py
./package_root/transform/idc/scripts/phase_001_collate/006_dicom_all.extract_instance_records.py

echo ./package_root/transform/idc/scripts/phase_001_collate/007_dicom_all.summarize_anatomy_annotations_by_series.py
./package_root/transform/idc/scripts/phase_001_collate/007_dicom_all.summarize_anatomy_annotations_by_series.py

echo ./package_root/transform/idc/scripts/phase_001_collate/008_dicom_all.summarize_tumor_vs_normal_annotations_by_series.py
./package_root/transform/idc/scripts/phase_001_collate/008_dicom_all.summarize_tumor_vs_normal_annotations_by_series.py

echo ./package_root/transform/idc/scripts/phase_001_collate/010_collate_case_data.py
./package_root/transform/idc/scripts/phase_001_collate/010_collate_case_data.py

echo ./package_root/transform/idc/scripts/phase_001_collate/011_tabulate_SOPClassUID_and_size_values_by_series.py
./package_root/transform/idc/scripts/phase_001_collate/011_tabulate_SOPClassUID_and_size_values_by_series.py

echo ./package_root/auxiliary_scripts/400_resolve_sample_crossrefs_for_IDC_DICOM_series_and_import_anatomic_and_tumor_vs_normal_annotations_from_other_DCs.py
./package_root/auxiliary_scripts/400_resolve_sample_crossrefs_for_IDC_DICOM_series_and_import_anatomic_and_tumor_vs_normal_annotations_from_other_DCs.py


