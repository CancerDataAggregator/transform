#!/usr/bin/env python3 -u

import sys

from os import path

from cda_etl.load.cda_loader import CDA_loader

# ARGUMENT

if len( sys.argv ) != 2:
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

tsv_dir = sys.argv[1]

if not path.isdir( tsv_dir ):
    sys.exit( f"\n   Usage: {sys.argv[0]} <input CDA-formatted TSV directory>\n" )

# PARAMETERS

# Fields to load and process into English lexeme vectors for prose-keyword search independent of number, grammatical endings, tense, etc.
# 
# Fields containing CDA-harmonized values will be automatically detected (via populated column_metadata->column->concept).
# In addition to the harmonized values themselves, the loader will also process synonyms, slims and containing terms (but
# not 'related' terms). (2026-02-12)

text_fields = {
    'file': {
        'file.description',
        'file.format',
        'file.type',
        'file.category',
        'file_anatomic_site.anatomic_site'
    },
    'subject': {
        'subject.species',
        'subject.cause_of_death',
        'subject.race',
        'subject.ethnicity',
        'observation.vital_status',
        'observation.sex',
        'observation.diagnosis',
        'observation.morphology',
        'observation.observed_anatomic_site',
        'observation.resection_anatomic_site',
        'treatment.anatomic_site',
        'treatment.type',
        'treatment.therapeutic_agent',
        'mutation.primary_site'
    }
}

# Fields to load whose values will be searched to find quasi-exact* [sub]string matches (IDs, numeric values, project names, cancer stage codes, CV term names, etc.).
# These include many of the same fields as in the text-processing set above: this enables searches (via tsvector) for e.g. 'lungs' to match 'lung cancer' and
# also (via this structure here) for 'cholangio*' to match 'cholangiocarcinoma'.
# 
# *Case-insensitive matches, maybe with some tokenization to allow for differences in precise encoding of embedded whitespace, etc. -- but no grammar parsing,
# singular/plural normalization, etc. as with lexemes (and no consequent information loss from prefix preprocessing, etc.).
# 
# Fields containing CDA-harmonized values will be automatically detected (via populated column_metadata->column->concept).
# In addition to the harmonized values themselves, the loader will also process synonyms, slims and containing terms (but
# not 'related' terms). (2026-02-12)

exact_match_fields = {
    'file': {
        'file.id',
        'file.crdc_id',
        'file.name',
        'file.format',
        'file.type',
        'file.category',
        'file_anatomic_site.anatomic_site'
    },
    'subject': {
        'subject.id',
        'subject.crdc_id',
        'subject.species',
        'subject.year_of_birth',
        'subject.year_of_death',
        'subject.cause_of_death',
        'subject.race',
        'subject.ethnicity',
        'observation.vital_status',
        'observation.sex',
        'observation.year_of_observation',
        'observation.age_at_observation',
        'observation.diagnosis',
        'observation.morphology',
        'observation.grade',
        'observation.stage',
        'observation.observed_anatomic_site',
        'observation.resection_anatomic_site',
        'mutation.hugo_symbol',
        'mutation.entrez_gene_id',
        'mutation.ncbi_build',
        'mutation.chromosome',
        'mutation.variant_type',
        'mutation.dbsnp_rs',
        'mutation.mutation_status',
        'mutation.transcript_id',
        'mutation.gene',
        'mutation.one_consequence',
        'mutation.hgnc_id',
        'mutation.primary_site',
        'mutation.case_barcode',
        'mutation.case_id',
        'mutation.sample_barcode_tumor',
        'mutation.tumor_submitter_uuid',
        'mutation.sample_barcode_normal',
        'mutation.normal_submitter_uuid',
        'mutation.aliquot_barcode_tumor',
        'mutation.tumor_aliquot_uuid',
        'mutation.aliquot_barcode_normal',
        'mutation.matched_norm_aliquot_uuid',
        'treatment.anatomic_site',
        'treatment.type',
        'treatment.therapeutic_agent'
    }
}

# EXECUTION

loader = CDA_loader()

loader.collect_keyword_data( tsv_dir, text_fields, exact_match_fields )


