#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.transform.mutation.mutation_transformer import mutation_transformer

version_file = path.join( 'extracted_data', 'mutation', 'source_data_version.txt' )

if not path.isfile( version_file ):
    
    # A version string (e.g. 'hg38_gdc_current') is required by the processor scripts.
    # 
    # If the file we're expecting to contain such a thing is not present, fail.

    sys.exit( f"FATAL: This script needs an ISB source-version string (like 'hg38_gdc_current') and was hoping to load one from {version_file}, but that file doesn't seem to exist. Can't continue." )

with open( version_file ) as IN:
    
    source_version = next( IN ).rstrip( '\n' )

columns_to_keep = [
    
    'hugo_symbol',
    'entrez_gene_id',
    'hotspot',
    'ncbi_build',
    'chromosome',
    'variant_type',
    'reference_allele',
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

mtx = mutation_transformer(
    
    columns_to_keep = columns_to_keep,
    source_datasets = source_datasets,
    source_version = source_version
)

mtx.make_mutation_TSV()


