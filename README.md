The goal of this repository is to provide standalone code that allows anybody to
perform an end-to-end ETL, starting from the DCs and ending up with a `.jsonl`
file they can upload to BQ. 

The transform and merge code also produces logs of issues with the data that can
then be conveyed to the DCs.

# Transform flow

![](overallflow.png)


# Example Patient data with annotations explaining data harmonization, logging and merging

_Note: Here we pull specific cases for purposes of example. In practice you will_
_be processing data in bulk. We unzip the files to show them, but in general you_
_will not be doing this either._

The full script is available [here](tests/steps/example-run.sh)

## Extraction
We pull 2 cases from PDC and 2 from GDC using the following commands.

```
extract-gdc gdc_TARGET_case1.jsonl.gz ../../cdatransform/extract/gdc_case_fields.txt --case 7eeced68-1717-4116-bcee-328ac70a9682
gunzip -c gdc_TARGET_case1.jsonl.gz > gdc_TARGET_case1.json
```
-> [GDC 1](tests/steps/gdc_TARGET_case1.json)


```
extract-gdc gdc_TARGET_case2.jsonl.gz ../../cdatransform/extract/gdc_case_fields.txt --case 9e229e56-f7e1-58f9-984b-a9453be5dc9a
gunzip -c gdc_TARGET_case2.jsonl.gz > gdc_TARGET_case2.json
```
-> [GDC 2](tests/steps/gdc_TARGET_case2.json)


```
extract-pdc pdc_QC1_case1.jsonl.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
gunzip -c pdc_QC1_case1.jsonl.gz > pdc_QC1_case1.json
```
-> [PDC 1](tests/steps/pdc_QC1_case1.json)


```
extract-pdc pdc_QC1_case2.jsonl.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83
gunzip -c pdc_QC1_case2.jsonl.gz > pdc_QC1_case2.json
```
-> [PDC 2](tests/steps/pdc_QC1_case2.json)

## Transformation

Transform GDC 2 to harmonized format
```
cda-transform gdc_TARGET_case1.jsonl.gz gdc.case1.H.json.gz ../../GDC_subject_endpoint_mapping.yml --endpoint cases
```
-> [Harmonized GDC Patient (1 case) with annotations](tests/steps/gdc_TARGET_case2_harmonized.yaml)

(The example is a hand crafted, hand annotated example for teaching. The code
generates the identical content but without the hand formatting and annotations.)

## Aggregation

Combine multiple ResearchSubjects from the same Patient, in one DC into one
Patient entry.

```
cat gdc_TARGET_case1.jsonl.gz gdc_TARGET_case2.jsonl.gz > gdc.cases.jsonl.gz
cda-transform gdc.cases.jsonl.gz gdc.cases.H.jsonl.gz ../../GDC_subject_endpoint_mapping.yml --endpoint cases
cda-aggregate ../../subject_endpoint_merge.yml gdc.cases.H.jsonl.gz gdc.cases.A.jsonl.gz
```
-> [Aggregated, harmonized GDC Patient with annotations](tests/steps/gdc_TARGET_aggregated.yaml)

## Merging

Combine multiple ResearchSubjects from the same Patient, across DCs into one
Patient entry.

```
cda-merge [TO DO]
```
-> [Merged, aggregated, harmonized GDC + PDC Patient](tests/steps/gdc_pdc_TCGA-E2-A10A_merged.yaml)

# Install

Clone repository and install
```
git clone git@github.com:CancerDataAggregator/transform.git
pip install -e .
```
or
```
git clone https://github.com/CancerDataAggregator/transform.git
pip install -e .
```


# Extract raw JSONL from DCs
The `extract-*` program is used to pull data from the DCs. For example
`extract-gdc` is used to extract data from GDC. Use `extract-gdc -h` to obtain
usage information.


Pull all cases
```
extract-gdc gdc.all_cases.jsonl.gz cdatransform/extract/gdc_case_fields.txt --endpoint cases
extract-pdc pdc.all_cases.jsonl.gz --endpoint cases
```

OR, pull specific list of cases
```
extract-gdc gdc.jsonl.gz cdatransform/extract/gdc_case_fields.txt --cases gdc-case-list.txt
extract-pdc pdc.all_cases.jsonl.gz --cases pdc-case-list.txt
```

where `gdc-case-list.txt` looks like 

```
375436b3-66ac-4d5e-b495-18a96d812a69
74543fa4-ce73-46e4-9c59-224e8242b4a2
f8970455-bfb2-4b1d-ab71-3c5d619898ad
```
and `pdc-case-list.txt` is in a similar format

Examples are in `cdatransform/extract/*-case-list.txt`

Extracting files endpoints is exactly the same, just replace "cases" with "files".

# Transform and harmonize data

The `cda-transform` program is used to transform the raw data from the DCs into
the harmonized format. Do `cda-transform -h` to obtain usage.

```
cda-transform gdc.all_cases.jsonl.gz gdc.all_cases.H.jsonl.gz GDC_subject_endpoint_mapping.yml --endpoint cases
cda-transform gdc.all_files.jsonl.gz gdc.all_files.H.jsonl.gz GDC_file_endpoint_mapping.yml --endpoint files
```

This code will ingest the raw data from the individual DCs (`.jsonl.gz` produced
by the extract-gdc scripts), apply transforms to the cases and write them back
out as another `.jsonl.gz` file ready for ingest into BQ or merging.

By default the log file is named `transform.log`

- [GDC_subject_endpoint_mapping.yml](GDC_subject_endpoint_mapping.yml): Yaml file which defines how GDC cases map 
   to CDA schema. Also denotes what value transformation functions are implemented for each field.
- [GDC_file_endpoint_mapping.yml](GDC_file_endpoint_mapping.yml): Yaml file which defines how GDC files map 
   to CDA schema. Also denotes what value transformation functions are implemented for each field.
- [value_transformations.py](cdatransform/transform/transform_lib/value_transformations.py): Library of 
   transformations for GDC, PDC, and IDC 

Transforming PDC is exactly the same, just replace "gdc/GDC" with "pdc/PDC"
# Aggregate data within individual DCs

The `cda-aggregate` program is used to aggregate multiple Subjects or Files records together. At this step in the 
process, Subjects information is written such that each row represents one ResearchSubject. Multiple ResearchSubjects
can be associated with the same Subject - hence the need for aggregation.

```
cda-aggregate subject_endpoint_merge.yml gdc.all_cases.H.jsonl.gz gdc.all_Subjects.jsonl.gz --endpoint subject
cda-aggregate file_endpoint_merge.yml gdc.all_files.H.jsonl.gz gdc.all__Files.jsonl.gz --endpoint files
```

- [subject_endpoint_merge.yml](subject_endpoint_merge.yml): Yaml file which defines how fields from two or 
   more records of the same entity are to be merged into on. For Subjects table.
- [file_endpoint_merge.yml](file_endpoint_merge.yml): Yaml file which defines how fields from two or 
   more records of the same entity are to be merged into on. For Files table.

Aggregating PDC is exactly the same, just replace "gdc" with "pdc"

# Fixing PDC issues

PDC currently has issues with getting associated project information. Long story short there are some types 
of files that are supplemental, and do not get returned in the files_per_study queries, but do get returned
in the metadata query, and have NO associated aliquots(which is not how it is supposed to be at PDC!) This 
next script removes those files since BQ cannot handle Nullable repeated strings. It grabs project info from
the Subjects table and adds it to files.

```
python cdatransform/transform/pdc_add_projects_to_files.py pdc.all_Subjects.jsonl.gz pdc.all__Files.jsonl.gz pdc.all_Files_fixed.jsonl.gz
```

# Merging files from all DCs
This code works similarly to the aggregation code, except it can accept more than one input file. Can be easily
updated to add more DCs in the future. The code is only needed for Subjects data. No Files are shared between DCs
(As of yet, that we know of)

```
cda-merge all_Subjects_3_1.jsonl.gz --gdc_subjects gdc.all_Subjects.jsonl.gz --pdc_subjects pdc.all_Subjects.jsonl.gz\
--idc_subjects idc_v10_Subjects.jsonl.gz --subject_how_to_merge_file subject_endpoint_merge.yml --merge_subjects True

cat gdc.all__Files.jsonl.gz pdc.all_Files_fixed.jsonl.gz idc_v10_Files.jsonl.gz > all_Files_3_1.jsonl.gz
```

# Create BigQuery Schemas for Subjects and Files tables

# Loading to BigQuery
A python script has been written to upload the files from local computer to BigQuery
```
brew install --cask google-cloud-sdk
```

Log into Google cloud

```
gcloud auth login
```

(This will let you set a default project, set this to `gdc-bq-sample`)


In this project there is a dataset called `dev`. You can use your own.

```
python cdatransform/load/load_gdc_pdc_to_bigquery.py all_Subjects_3_1.jsonl.gz subjects_schema.json --dest_table_id \
gdc-bq-sample.dev.all_Subjects_v3_1_test1 --gsa_key ../../GCS-service-account-key.json
```


# End-to-end example

```
cd data
extract-gdc gdc.jsonl.gz 
cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz ../gdc-transform.yml
bq load --autodetect --source_format NEWLINE_DELIMITED_JSON kg_expt.gdc gdc.transf.jsonl.gz
```

# [Example of creating small data sets for testing](tests/small/Readme.md)

# Some tips for working with ETL
1. Use the `--case` or `--cases` options to run tests on small subsets of the data.
1. You can extract one case from a larger file for inspection by doing `zcat <
   gdc.jsonl.gz| sed -n '1p' > test.json`. Change `1p` to what ever line you
   need. 

# Upload script

A complete end-to-end upload script is available [here](data/upload.sh).


# Testing out local install

```
git clone git@github.com:CancerDataAggregator/transform.git
cd transform
pip install -e .
```

Working on your own branch

```
git branch <mine>
git checkout <mine>
```

If you used the editable install (`pip install -e .`) Python will use your
changes live.
