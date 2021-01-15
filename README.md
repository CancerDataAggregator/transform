The goal of this repository is to provide standalone code that allows anybody to
perform an end-to-end ETL, starting from the DCs and ending up with a `.jsonl`
file they can upload to BQ.

# Transform flow

![](overallflow.png)


# Install

Clone repository and install
```
git clone git@github.com:CancerDataAggregator/transform.git
pip install -e .
```


# Extract raw JSONL from DCs

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

Examples are in `cdatransform/dcapi/*-case-list.txt`


# Transform and harmonize data

Python code for implementing transforms on data extracted from DCs

```
$ cda-transform -h
usage: cda-transform [-h] [--limit LIMIT] input output transforms

positional arguments:
  input          Input data file.
  output         Output data file.
  transforms     Transforms list file.

optional arguments:
  -h, --help     show this help message and exit
  --limit LIMIT  If present, will transform only the first N entries.
```

This code will ingest the `.jsonl` produced by the ISB-CGC E script, apply
transforms to it and write it out back as another `.jsonl` file ready for ingest
into BQ.

- [gdc-transform.yml](gdc-transform.yml): Sample transforms list file for GDC

- [transformlib.py](cdatransform/transformlib.py): The library of generalized
transform functions.
- [gdclib.py](cdatransform/gdclib.py): Library of transforms for GDC 
- [pdclib.py](cdatransform/pdclib.py): Library of transforms for PDC 


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
