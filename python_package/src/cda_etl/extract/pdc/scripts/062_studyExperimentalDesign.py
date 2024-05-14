#!/usr/bin/env python -u

import requests
import json
import sys

from os import makedirs, path, rename

from cda_etl.lib import sort_file_with_header

# PARAMETERS

api_url = 'https://pdc.cancer.gov/graphql'

output_root = 'extracted_data/pdc'

json_out_dir = f"{output_root}/__API_result_json"

study_experimental_design_out_dir = f"{output_root}/StudyExperimentalDesign"

study_experimental_design_tsv = f"{study_experimental_design_out_dir}/StudyExperimentalDesign.tsv"
study_experimental_design_label_aliquots_tsv = f"{study_experimental_design_out_dir}/StudyExperimentalDesign.LabelAliquots.tsv"

studyExperimentalDesign_json_output_file = f"{json_out_dir}/studyExperimentalDesign.json"

scalar_study_experimental_design_fields = (
    'study_run_metadata_id',
    'study_run_metadata_submitter_id',
    'aliquot_is_ref',
    'experiment_number',
    'experiment_type',
    'plex_dataset_name',
    'acquisition_type',
    'polarity',
    'number_of_fractions',
    'analyte',
    'label_free_asi',
    'itraq_113_asi',
    'itraq_114_asi',
    'itraq_115_asi',
    'itraq_116_asi',
    'itraq_117_asi',
    'itraq_118_asi',
    'itraq_119_asi',
    'itraq_121_asi',
    'tmt_126_asi',
    'tmt_127n_asi',
    'tmt_127c_asi',
    'tmt_128n_asi',
    'tmt_128c_asi',
    'tmt_129n_asi',
    'tmt_129c_asi',
    'tmt_129cc_asi',
    'tmt_130n_asi',
    'tmt_131_asi',
    'tmt_131c_asi',
    'tmt_132n_asi',
    'tmt_132c_asi',
    'tmt_133n_asi',
    'tmt_133c_asi',
    'tmt_134n_asi',
    'tmt_134c_asi',
    'tmt_135n_asi',
    'study_id',
    'study_submitter_id',
    'pdc_study_id'
)

scalar_label_aliquot_fields = (
    'aliquot_run_metadata_id',
    'aliquot_submitter_id',
    'aliquot_id',
    'label'
)

label_aliquot_study_experimental_design_fields = (
    'label_free',
    'itraq_113',
    'itraq_114',
    'itraq_115',
    'itraq_116',
    'itraq_117',
    'itraq_118',
    'itraq_119',
    'itraq_121',
    'tmt_126',
    'tmt_127n',
    'tmt_127c',
    'tmt_128n',
    'tmt_128c',
    'tmt_129n',
    'tmt_129c',
    'tmt_130c',
    'tmt_130n',
    'tmt_131',
    'tmt_131c',
    'tmt_132n',
    'tmt_132c',
    'tmt_133n',
    'tmt_133c',
    'tmt_134n',
    'tmt_134c',
    'tmt_135n'
)

query_text = '''    {
        studyExperimentalDesign( acceptDUA: true ) {
            ''' + '\n            '.join(scalar_study_experimental_design_fields)

for label_aliquot_name in label_aliquot_study_experimental_design_fields:
    
    query_text = query_text + f'''
            {label_aliquot_name} {{
                ''' + '\n                '.join(scalar_label_aliquot_fields) + '''
            }'''

query_text = query_text + '''
        }
    }'''

api_query_json = {
    'query': query_text
}

# EXECUTION

for output_dir in ( json_out_dir, study_experimental_design_out_dir ):
    
    if not path.exists(output_dir):
        
        makedirs(output_dir)

# Send the studyExperimentalDesign() query to the API server.

response = requests.post(api_url, json=api_query_json)

# If the HTTP response code is not OK (200), dump the query, print the http
# error result and exit.

if not response.ok:
    
    print( api_query_json['query'], file=sys.stderr )

    response.raise_for_status()

# Retrieve the server's JSON response as a Python object.

result = json.loads(response.content)

# Save a version of the returned data as JSON.

with open(studyExperimentalDesign_json_output_file, 'w') as JSON:
    
    print( json.dumps(result, indent=4, sort_keys=False), file=JSON )

# Open handles for output files to save TSVs describing the returned objects and (as needed) association TSVs
# enumerating containment relationships between objects and sub-objects as well as association TSVs enumerating
# relationships between objects and keyword-style dictionaries.
# 
# We can't always safely use the Python `with` keyword in cases like this because the Python interpreter
# enforces an arbitrary hard-coded limit on the total number of simultaneous indents, and each `with` keyword
# creates another indent, even if you use the
# 
#     with open(A) as A, open(B) as B, ...
# 
# (macro) syntax. For the record, I think this is a stupid hack on the part of the Python designers. We shouldn't
# have to write different but semantically identical code just because we hit some arbitrarily-set constant limit on
# indentation, especially when the syntax above should've avoided creating multiple nested indents in the first place.

### OLD COMMENT: Also save a version of the returned StudyExperimentalDesign objects in TSV form,
### along with an association table containing the embedded LabelAliquot metadata.


output_tsv_keywords = [
    'STUDY_EXP_DESIGN',
    'STUDY_EXP_DESIGN_LABEL_ALIQUOTS'
]

output_tsv_filenames = [
    study_experimental_design_tsv,
    study_experimental_design_label_aliquots_tsv
]

output_tsvs = dict( zip( output_tsv_keywords, [ open(file_name, 'w') for file_name in output_tsv_filenames ] ) )

# Table headers.

print( *scalar_study_experimental_design_fields, sep='\t', end='\n', file=output_tsvs['STUDY_EXP_DESIGN'] )
print( *(['StudyExperimentalDesign.study_run_metadata_id', 'LabelAliquot_field_name'] + list(scalar_label_aliquot_fields)), sep='\t', end='\n', file=output_tsvs['STUDY_EXP_DESIGN_LABEL_ALIQUOTS'] )

# Parse the returned data and save to TSV.

for study_experimental_design in result['data']['studyExperimentalDesign']:
    
    study_experimental_design_row = list()

    for field_name in scalar_study_experimental_design_fields:
        
        if study_experimental_design[field_name] is not None:
            
            # There are newlines, carriage returns, quotes and nonprintables in some PDC text fields, hence the json.dumps() wrap here.

            study_experimental_design_row.append(json.dumps(study_experimental_design[field_name]).strip('"'))

        else:
            
            study_experimental_design_row.append('')

    print( *study_experimental_design_row, sep='\t', end='\n', file=output_tsvs['STUDY_EXP_DESIGN'] )

    for label_aliquot_name in label_aliquot_study_experimental_design_fields:
        
        if study_experimental_design[label_aliquot_name] is not None and len(study_experimental_design[label_aliquot_name]) > 0:
            
            for label_aliquot in study_experimental_design[label_aliquot_name]:
                
                label_aliquot_row = [study_experimental_design['study_run_metadata_id'], label_aliquot_name]

                for field_name in scalar_label_aliquot_fields:
                    
                    if label_aliquot[field_name] is not None:
                        
                        label_aliquot_row.append(label_aliquot[field_name])

                    else:
                        
                        label_aliquot_row.append('')

                print( *label_aliquot_row, sep='\t', end='\n', file=output_tsvs['STUDY_EXP_DESIGN_LABEL_ALIQUOTS'] )

# Sort the rows in the TSV output files.

for keyword in output_tsv_keywords:
    
    output_tsvs[keyword].close()

for file in output_tsv_filenames:
    
    sort_file_with_header( file )


