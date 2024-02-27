#!/usr/bin/env python3 -u

from cda_etl.load.cda_loader import CDA_loader

loader = CDA_loader()

loader.create_integer_aliases_for_entity_tables()


