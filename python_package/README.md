This repo contains all the code currently used for CDA ETL flows from CRDC DCs into the central CDA metadatabase.

This is beta code: all of it is under active development, but we have reached a point where it can stably generate CDA data releases.

Planned code-level updates include:

* investigate all explicit DC-level cross-references; validate, compare to auto-detected identities, report coverage, integrate as/where needed and safe to do so:
    * PDC Reference entity
    * PDC Sample.gdc\_sample\_id
    * PDC Sample.gdc\_project\_id
    * IDC tcga\_clinical\_rel9.case\_gdc\_id
    * IDC tcga\_biospecimen\_rel9.sample\_gdc\_id
    * IDC tcga\_biospecimen\_rel9.sample\_barcode
    * IDC tcga\_biospecimen\_rel9.case\_gdc\_id
* merge CDA Specimen records across DCs using crossrefs
* update loader object
    * make pre- and post-INSERT SQL scripts to handle index management around main ingest
* cache more identifiers
    * IDC dicom\_all.crdc\_instance\_uuid
    * all from list of cross-ref fields above
    * check for others
* sync extracted\_data/, other processing subdirs for full inter-DC interop
* build architecture for ingest audit trails
    * encapsulate into a single phase within each DC flow
    * includes "as stored" and "as indexed" data exposed to user
* build CDA release metadata table
    * version metadata
    * source-field provenance metadata
    * precomputed count stats for release data
* check GDC v37 data model update/diff and patch if needed
    * specifically check diagnoses.sites\_of\_involvement
* add GDC index files as regular file records
* Collect GDC transformation-phase code into a Python object
* build mutation processing into ETL
* collect all PDC ETL flow code into Python objects
* create ingest flow for CDS
* synchronize flow deployment with CDA's migration to an RDBMS instance from BigQuery
* port entire system to cloud operations using AirFlow


