2026-03-23: 3.0.5:
* adjust aggregators to handle individual IDC cases mapping to multiple distinct GC participants
* harden GC extractor to handle new array values coming in encoded as strings (race, ethnicity)
* add support for GC consent\_group, investigator entities
* harden PDC extractor to handle new ambiguities in project associations introduced by new CPTAC3 reprocessing data of subjects originating elsewhere
* add 'concept' to column\_metadata to flag harmonized columns
* harden OBO processor
* add postgres tsvector data and keyword associations to support new global search
* profile all keyword frequencies per release
* fill in all missing file, subject aliases, partnered with nulls, into file\_describes\_subject to strengthen API assumptions designed to improve query efficiency
* process controlled term sets defined by automatically-loaded external ontologies and by custom CDA maps, including processing of related terms like synonyms, slim-map category assignments and containing/superset (hierarchical) concepts
* add referential indirection for all controlled terms for better master data management and leaner data tables
* adjust PDC extractor delays to minimize run-breakage risk
* expand slim-map format and add count profiling for all categories during each release
* add anatomic\_site slim


