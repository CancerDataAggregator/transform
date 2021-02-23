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
extract-gdc gdc_TARGET_case1.jsonl.gz ../../data/gdc.samples-per-file.jsonl.gz ../../data/gdc.fileuuid.jsonl.gz --case 7eeced68-1717-4116-bcee-328ac70a9682
gunzip -c gdc_TARGET_case1.jsonl.gz > gdc_TARGET_case1.json
```
-> [GDC 1](tests/steps/gdc_TARGET_case1.json)


```
extract-gdc gdc_TARGET_case2.jsonl.gz ../../data/gdc.samples-per-file.jsonl.gz ../../data/gdc.fileuuid.jsonl.gz --case 9e229e56-f7e1-58f9-984b-a9453be5dc9a
gunzip -c gdc_TARGET_case2.jsonl.gz > gdc_TARGET_case2.json
```
-> [GDC 2](tests/steps/gdc_TARGET_case2.json)


```
extract-pdc pdc_QC1_case1.jsonl.gz ../../pdc.files-per-sample-dict.json.gz --case 0809987b-1fba-11e9-b7f8-0a80fada099c
gunzip -c pdc_QC1_case1.jsonl.gz > pdc_QC1_case1.json
```
-> [PDC 1](tests/steps/pdc_QC1_case1.json)


```
extract-pdc pdc_QC1_case2.jsonl.gz ../../pdc.files-per-sample-dict.json.gz --case df4f2aaf-8f98-11ea-b1fd-0aad30af8a83
gunzip -c pdc_QC1_case2.jsonl.gz > pdc_QC1_case2.json
```
-> [PDC 2](tests/steps/pdc_QC1_case2.json)

## Transformation

Transform GDC 2 to harmonized format
```
cda-transform gdc_TARGET_case2.jsonl.gz gdc_TARGET_case2_harmonized_output.jsonl.gz ../../gdc-transform.yml
```
-> [Harmonized GDC Patient (1 case) with annotations](tests/steps/gdc_TARGET_case2_harmonized.yaml)

(The example is a hand crafted, hand annotated example for teaching. The code
generates the identical content but without the hand formatting and annotations.)

## Aggregation

Combine multiple ResearchSubjects from the same Patient, in one DC into one
Patient entry.

```
cda-aggregate  [TO DO]
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
pip install -e
```


# Extract raw JSONL from DCs
The `extract-*` program is used to pull data from the DCs. For example
`extract-gdc` is used to extract data from GDC. Use `extract-gdc -h` to obtain
usage information.


Pull all cases
```
extract-gdc gdc.jsonl.gz
extract-pdc pdc.jsonl.gz
```

OR, pull specific list of cases
```
extract-gdc gdc.jsonl.gz --cases gdc-case-list.txt
```

where `gdc-case-list.txt` looks like 

```
375436b3-66ac-4d5e-b495-18a96d812a69
74543fa4-ce73-46e4-9c59-224e8242b4a2
f8970455-bfb2-4b1d-ab71-3c5d619898ad
```

Examples are in `cdatransform/extract/*-case-list.txt`


# Transform and harmonize data

The `cda-transform` program is used to transform the raw data from the DCs into
the harmonized format. Do `cda-transform -h` to obtain usage.

```
cda-transform gdc.jsonl.gz gdc.transf.jsonl.gz ../gdc-transform.yml
```

This code will ingest the raw data from the individual DCs (`.jsonl.gz` produced
by the extract-gdc scripts), apply transforms to the cases and write them back
out as another `.jsonl.gz` file ready for ingest into BQ or merging.

By default the log file is named `transform.log`

- [gdc-transform.yml](gdc-transform.yml): Sample transforms list file for GDC

- [gdclib.py](cdatransform/gdclib.py): Library of transforms for GDC 
- [pdclib.py](cdatransform/pdclib.py): Library of transforms for PDC 


# Load data into BigQuery

Install BQ command line tools as needed
```
brew install --cask google-cloud-sdk
```

Log into Google cloud

```
gcloud auth login
```

(This will let you set a default project, set this to `gdc-bq-sample`)


In this project there is a dataset called `kg-expt`. You can use your own.

```
bq load --autodetect --source_format NEWLINE_DELIMITED_JSON kg_expt.gdc gdc.transf.jsonl.gz
```

# Downloading cached files
Several of the ETL steps require cached files either to avoid bottlenecking a DC
API or to load auxiliary information. These files are: 
- gdc.samples-per-file.jsonl.gz (Match GDC samples and files)
- pdc.files-per-sample-dict.json.gz (Match PDC files and samples)
- gdc.fileuuid.jsonl.gz (Match download links for GDC files)

These should be pulled down before running any ETL tests or processes into the
`data` directory using the [`get-cache-files.sh`](data/get-cache-files.sh) script

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
