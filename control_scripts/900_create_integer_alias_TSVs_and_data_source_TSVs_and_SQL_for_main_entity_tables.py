#!/usr/bin/env python3 -u

from cda_etl.load.cda_loader import CDA_loader

loader = CDA_loader()

loader.create_integer_alias_TSVs_for_main_entity_tables()

loader.create_data_source_TSVs_and_SQL_for_main_entity_tables()


