The goal of this repository is to provide standalone code that allows anybody to
perform an end-to-end ETL, starting from the DCs and ending up with a `.jsonl`
file they can upload to BQ.

The transform and merge code also produces logs of issues with the data that can
then be conveyed to the DCs.

# Transform flow

![](overallflow.png)


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
