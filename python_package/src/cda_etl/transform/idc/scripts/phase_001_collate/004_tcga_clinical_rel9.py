#!/usr/bin/env python3 -u

import gzip
import jsonlines

from os import path, makedirs

# PARAMETERS

extraction_root = path.join( 'extracted_data', 'idc' )

jsonl_input_dir = path.join( extraction_root, '__raw_BigQuery_JSONL' )

table_name = 'tcga_clinical_rel9'

input_file = path.join( jsonl_input_dir, f"{table_name}.jsonl.gz" )

toplevel_extraction_fields = [
    
    'case_barcode',
    'race',
    'gender',
    'days_to_birth',
    'vital_status',
    'year_of_diagnosis',
    'disease_code',
    'icd_o_3_histology',
    'neoplasm_histologic_grade',
    'clinical_stage',
    'pathologic_stage',
    'histological_type',
    'days_to_death',
    'case_gdc_id',
    'anatomic_neoplasm_subdivision',
    'ethnicity',
    'icd_10',
    'icd_o_3_site',
    'person_neoplasm_cancer_status',
    'tumor_tissue_site',
    'tumor_type'
]

output_dir = extraction_root

toplevel_output_file = path.join( output_dir, f"{table_name}.tsv" )

# EXECUTION

if not path.isdir( output_dir ):
    
    makedirs( output_dir )

with gzip.open( input_file ) as IN, open( toplevel_output_file, 'w' ) as OUT:
    
    print( *toplevel_extraction_fields, sep='\t', file=OUT )

    reader = jsonlines.Reader( IN )

    for record in reader:
        
        output_row = list()

        for field_name in toplevel_extraction_fields:
            
            if field_name in record and record[field_name] is not None:
                
                output_row.append( record[field_name] )

            else:
                
                output_row.append( '' )

        print( *output_row, sep='\t', file=OUT )


