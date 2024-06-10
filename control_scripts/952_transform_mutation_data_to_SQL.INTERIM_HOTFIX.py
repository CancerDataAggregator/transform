#!/usr/bin/env python3

import sys

from cda_etl.transform.mutation.mutation_transformer_INTERIM_HOTFIX import mutation_transformer_INTERIM_HOTFIX

# A version string (e.g. 'hg38_gdc_current') is required by the processor scripts.
# 
# If the length of the argument list to this script is zero, fail.

if len( sys.argv ) <= 1:
    
    sys.exit( 'FATAL: This script needs an ISB source-version string (like "hg38_gdc_current") as its argument.' )

source_version = sys.argv[1]

columns_to_keep = [
    
    'project_short_name',
    'hugo_symbol',
    'entrez_gene_id',
    'hotspot',
    'ncbi_build',
    'chromosome',
    'variant_type',
    'variant_class',
    'reference_allele',
    'match_norm_seq_allele1',
    'match_norm_seq_allele2',
    'tumor_seq_allele1',
    'tumor_seq_allele2',
    'dbsnp_rs',
    'mutation_status',
    'transcript_id',
    'gene',
    'one_consequence',
    'hgnc_id',
    'primary_site',
    'case_barcode',
    'case_id',
    'sample_barcode_tumor',
    'tumor_submitter_uuid',
    'sample_barcode_normal',
    'normal_submitter_uuid',
    'aliquot_barcode_tumor',
    'tumor_aliquot_uuid',
    'aliquot_barcode_normal',
    'matched_norm_aliquot_uuid'
]

source_datasets = [
    
    'BEATAML1_0',
    'CDDP_EAGLE',
    'CGCI',
    'CMI',
    'CPTAC',
    'EXC_RESPONDERS',
    'HCMI',
    'MMRF',
    'TARGET',
    'TCGA'
]

mtx = mutation_transformer_INTERIM_HOTFIX(
    
    columns_to_keep = columns_to_keep,
    source_datasets = source_datasets,
    source_version = source_version
)

mtx.transform_files_to_SQL()


