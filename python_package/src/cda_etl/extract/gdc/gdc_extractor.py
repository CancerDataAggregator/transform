import gzip
import json
import jsonlines
import re
import requests
import shutil
import sys
import time

from collections import defaultdict
from os import listdir, makedirs, path, rename

from cda_etl.lib import add_to_map, associate_id_list_with_parent, get_current_date, get_safe_value, singularize, sort_file_with_header, write_association_pairs

class GDC_extractor:
    
    def __init__( self, endpoint, source='gdc', refresh=True ):
        
        self.endpoint = endpoint

        # If this is set to True, we'll retrieve and compute all needed data as we go.
        # Otherwise, if cache files exist, we'll use those (instead of, say, making calls
        # to the API).

        self.refresh = refresh

        self.endpoint_singular = singularize(endpoint)

        # Output directories.

        self.METADATA_DIR = f"extracted_data/{source}/{endpoint}/metadata"

        self.JSON_DIR = f"extracted_data/{source}/{endpoint}/api_json"

        self.TSV_DIR = f"extracted_data/{source}/{endpoint}/tsv"

        self.STATUS_DIR = f"extracted_data/{source}/{endpoint}/API_release_metadata"

        self.MERGED_OUTPUT_DIR = f"extracted_data/{source}/all_TSV_output"

        if not path.exists(self.METADATA_DIR):
            
            makedirs(self.METADATA_DIR)

        if not path.exists(self.JSON_DIR):
            
            makedirs(self.JSON_DIR)

        if not path.exists(self.TSV_DIR):
            
            makedirs(self.TSV_DIR)

        if not path.exists(self.STATUS_DIR):
            
            makedirs(self.STATUS_DIR)

        if not path.exists(self.MERGED_OUTPUT_DIR):
            
            makedirs(self.MERGED_OUTPUT_DIR)

        # Base URL at the REST API for the current API release metadata (commit hash,
        # data release version, API server status, API version and release tag). This is the same for all endpoints.

        self.status_url = f"https://api.gdc.cancer.gov/status"

        # Cache file for the current API release metadata (commit hash,
        # data release version, API server status, API version and release tag).

        self.status_file = f"{self.STATUS_DIR}/status.json"

        # Base URL at the REST API for the current endpoint, to which we'll append parameters as a query string.

        self.endpoint_url = f"https://api.gdc.cancer.gov/{endpoint}"

        # _mapping URL at the REST API for the current endpoint, which gives lists of available fields and field groups.

        self.mapping_url = f"https://api.gdc.cancer.gov/{endpoint}/_mapping"

        # Cache files. If these exist, we'll use their contents instead of regenerating
        # them (unless self.refresh is set to True, in which case everything will be reloaded
        # and updated data will be cached in these files).

        self.field_list_file = f"{self.METADATA_DIR}/fields_to_extract.txt"

        self.expand_group_file = f"{self.METADATA_DIR}/groups_to_expand.txt"

        self.api_result_file = f"{self.JSON_DIR}/{source}.{endpoint}.jsonl.gz"

        self.complex_structure_list_file = f"{self.METADATA_DIR}/non-atomic_substructures.txt"

        self.base_table_tsv = f"{self.TSV_DIR}/{self.endpoint_singular}.tsv"

        # A place to record the date of data extraction.

        self.extraction_date_file = f"{self.MERGED_OUTPUT_DIR}/extraction_date.txt"

        # Load endpoint-specific configuration metadata.

        endpoint_config = __import__( f"cda_etl.extract.gdc.config.{endpoint}", fromlist=[None] )

        # Number of partitions into which we need to split lists of fields we're
        # requesting by name from the API. Too many fields in one query string
        # makes the server vomit.

        self.number_of_field_list_chunks = endpoint_config.number_of_field_list_chunks

        # Number of results we want the API to return per page of output.

        self.result_page_size = endpoint_config.result_page_size

        # Sub-objects we'll need to ask for with the API's 'expand' parameter
        # because asking for them field-by-field makes URLs that are too big
        # for the server to handle.
        # 
        # These will be treated as string literals and saved in a special list.
        # 
        # They will also be converted (below) to regular expressions and used
        # to filter out individual fields from the target request list.

        self.groups_to_expand = endpoint_config.groups_to_expand

        # Fields we don't want.
        # 
        # Note: these will be interpreted as regular expressions matching
        # entire field names end-to-end.

        self.fields_to_filter = endpoint_config.fields_to_filter

        # Prefixes for fields we know we don't want yet (except for IDs;
        # see fields_to_use).
        # 
        # Note: these will be interpreted as regular expressions.

        self.prefixes_to_filter = endpoint_config.prefixes_to_filter

        # Fields we want to keep. These will be filtered because of matches
        # to prefixes_to_filter unless we explicitly preserve them, so that's
        # what this data structure does.
        # 
        # Note: these will be interpreted as string literals.

        self.fields_to_use = endpoint_config.fields_to_use

        # Prefixes for substructures we know we don't want to scan for field names.
        # 
        # Note: these will be interpreted as regular expressions.

        self.substructures_to_filter = endpoint_config.substructures_to_filter

        # Field lists we will auto-detect for non-atomic substructures.

        self.substructure_field_lists = dict()

        # Substructures containing statistical summaries, not sub-entity records.

        self.statistical_summary_substructures = endpoint_config.statistical_summary_substructures

        # Once in a while we get an array of strings attached to the top-level entity
        # for which we want to create a sub-entity table (e.g. files.acl). These need to
        # be managed separately, since they're not picked up during the nested-object scan in
        # get_substructure_field_lists() (because they have no substructures). We'll
        # name the sub-entity table for each (key, value) pair in this dict using the key,
        # and the value will indicate the association map in which we'll store links
        # back to the top-level entity.
        
        self.array_entities = endpoint_config.array_entities

        # Once in a while we need to save a thing with one name as a thing with
        # a different name. The elements of the file.downstream_analyses list,
        # for example, should be saved as 'analysis' objects, not 'downstream_analysis' objects.

        self.save_entity_list_as = endpoint_config.save_entity_list_as

        # Dicts in which to store association relationship data.

        self.association_maps = endpoint_config.association_maps

        # Do we need to load association data by recursively scanning substructures
        # of records at this endpoint? (If not, we'll scrape all needed association
        # data from the top level without recursion.)

        self.scan_substructures_for_association_relationships = endpoint_config.scan_substructures_for_association_relationships

    def set_refresh( self, refresh ):
        
        if isinstance( refresh, bool ):
            
            self.refresh = refresh

        else:
            
            sys.exit("FATAL: value of 'refresh' must be boolean; aborting.")

    def get_field_lists_for_API_calls( self ):
        
        if self.refresh or not path.exists( self.expand_group_file ) or not path.exists( self.field_list_file ):
            
            # Compute self.fields_to_use and self.groups_to_expand using the config
            # data for this endpoint along with live results from its _mapping url.

            json_result = requests.get( self.mapping_url )

            if not json_result.ok:
                
                sys.exit( f"FATAL: call to API /{self.endpoint}/_mapping endpoint failed. Response content: " + str( json_result.content ) )

            result = json_result.json()

            for field_name in result["fields"]:
                
                skip_current = False

                for filtered_field in self.fields_to_filter:
                    
                    if field_name == filtered_field:
                        
                        skip_current = True

                for prefix in self.prefixes_to_filter:
                    
                    if re.search( r'^' + prefix + r'\.', field_name ) is not None:
                        
                        skip_current = True

                for expand_group in self.groups_to_expand:
                    
                    if field_name == expand_group:
                        
                        skip_current = True

                    else:
                        
                        # I know this looks like a bug. It isn't. I promise.

                        expand_group_re = re.sub( r'\.', r'\.', expand_group )

                        if re.search( r'^' + expand_group_re + r'\.', field_name ) is not None:
                            
                            skip_current = True

                if not skip_current:
                    
                    self.fields_to_use.add( field_name )

            with open( self.expand_group_file, 'w' ) as OUT:
                
                if len( self.groups_to_expand ) > 0:
                    
                    print( *sorted( self.groups_to_expand ), sep='\n', file=OUT )

            with open( self.field_list_file, 'w' ) as OUT:
                
                print( *sorted( self.fields_to_use ), sep='\n', file=OUT )

        else:
            
            # Load cached copies of these lists.

            self.fields_to_use = list()

            with open( self.field_list_file ) as IN:
                
                self.fields_to_use = [ line.rstrip() for line in IN ]

            self.groups_to_expand = list()

            with open( self.expand_group_file ) as IN:
                
                self.groups_to_expand = [ line.rstrip() for line in IN ]

    def __get_endpoint_JSON( self, parameters ):
        
        """
        Make a single GET call to the REST API at `url` with a query string built from `parameters`.
        """

        result = requests.Response()

        try:
            result = requests.get( self.endpoint_url, params=parameters )
        except Exception as e:
            try:
                print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} generated an error: {e}", file=sys.stderr )
                print( 'Retrying after 15s...', file=sys.stderr )
                time.sleep( 15 )
                result = requests.get( self.endpoint_url, params=parameters )
            except Exception as e_2:
                try:
                    print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} generated an error: {e_2}", file=sys.stderr )
                    print( 'Retrying after 30s...', file=sys.stderr )
                    time.sleep( 30 )
                    result = requests.get( self.endpoint_url, params=parameters )
                except Exception as e_3:
                    sys.exit( f"Unrecoverable error. Final error message: {e_3}" )

        if result.ok:
            return result

        else:
            print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} failed. Response content: " + str( result.content ), file=sys.stderr )
            print( 'Retrying after 15s...', file=sys.stderr )
            try:
                time.sleep( 15 )
                result = requests.get( self.endpoint_url, params=parameters )
            except Exception as e_2:
                try:
                    print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} generated an error: {e_2}", file=sys.stderr )
                    print( 'Retrying after 30s...', file=sys.stderr )
                    time.sleep( 30 )
                    result = requests.get( self.endpoint_url, params=parameters )
                except Exception as e_3:
                    sys.exit( f"Unrecoverable error. Final error message: {e_3}" )

            if result.ok:
                return result

            else:
                print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} failed. Response content: " + str( result.content ), file=sys.stderr )
                print( 'Retrying after 30s...', file=sys.stderr )
                try:
                    time.sleep( 30 )
                    result = requests.get( self.endpoint_url, params=parameters )
                except Exception as e_2:
                        print( f"WARNING: call to API /{self.endpoint_url} endpoint with parameters {parameters} generated an error: {e_2}", file=sys.stderr )
                        print( 'Retrying after 60s...', file=sys.stderr )
                        time.sleep( 60 )
                        result = requests.get( self.endpoint_url, params=parameters )
                    except Exception as e_3:
                        sys.exit( f"Unrecoverable error. Final error message: {e_3}" )

                if result.ok:
                    return result
                else:
                    # Give up.
                    sys.exit( f"FATAL: call to API /{self.endpoint_url} endpoint with parameters {parameters} failed. Response content: " + str( result.content ) )

    def __partition_field_list_into_chunks( self ):
        
        """
        Split self.fields_to_use into self.number_of_field_list_chunks chunks,
        keeping fields with the same top-level prefix together in the same chunk
        to avoid merging headaches later on.

        Returns:
            list: returns list of lists (chunks) [ [field1, field2], [field3, field4]...]
        """

        fields = self.fields_to_use

        number_of_chunks = self.number_of_field_list_chunks

        field_groups_by_prefix = defaultdict(list)

        field_list_chunks = [ [] for i in range(number_of_chunks) ]

        # Keep fields together by top-level prefix so merging is easier later on.

        for field in fields:
            
            field_groups_by_prefix[field.split(".")[0]].append(field)

        chunk_index = 0

        # Split `fields` into chunks.

        for field_group in field_groups_by_prefix.values():
            
            field_list_chunks[chunk_index].extend(field_group)

            if len(field_list_chunks[chunk_index]) > len(fields) / number_of_chunks:
                
                # This chunk's big enough. Start a new one.

                chunk_index += 1

        for list_chunk in field_list_chunks:
            
            # Make sure there's a key to merge on, downstream.

            if "id" not in list_chunk:
                
                list_chunk.append("id")

        # Send back the list of list-chunks (sub-lists).

        return [ list_chunk for list_chunk in field_list_chunks if len(list_chunk) > 1 ]

    def __paginate_endpoint_calls( self ):
        
        """
        Get data from the API at `url` one page at a time; yield resulting records one at a time.
        
        Returns:
            List of dictionaries, each of which contains one record from the given endpoint.

        Yields:
            One dictionary representing one record from the given endpoint.
        """

        fields = self.fields_to_use

        expands = self.groups_to_expand

        url = self.endpoint_url

        page_size = self.result_page_size

        number_of_chunks = self.number_of_field_list_chunks

        field_chunks = self.__partition_field_list_into_chunks()

        # Track the current page number. This'll be populated from API result data after it's used once;
        # these are seed values so the loop invariant for the first iteration of the while loop is true and
        # so we can print a "Pulling page 1" message for the first page at the right time.

        page_number = 1

        total_pages = 10000

        # Keep track of how many records we've completed so we can tell the API where to begin each successive page.

        record_offset = 0

        while page_number < total_pages:

            record_chunks = defaultdict( list )

            print( f"Pulling page {page_number} / {total_pages}...", file=sys.stderr )

            for field_chunk in field_chunks:
                
                field_parameters = ",".join(field_chunk)

                parameters = {
                    
                    "format": "json",
                    "fields": field_parameters,
                    "size": page_size,
                    "from": record_offset,
                }

                expand_parameters= ",".join(expands)

                if len(expands) > 0:
                    
                    expand_parameters= ",".join(expands)

                    parameters["expand"] = expand_parameters

                result = self.__get_endpoint_JSON( parameters )

                resultJSON = result.json()

                for result_chunk in resultJSON["data"]["hits"]:
                    
                    record_chunks[result_chunk["id"]].append(result_chunk)

                page_number = resultJSON["data"]["pagination"]["page"]

                total_pages = resultJSON["data"]["pagination"]["pages"]

            # Merge chunks of the same record.

            result_list = [ { key: value for record in record_chunk for key, value in record.items() } for record_chunk in record_chunks.values() ]

            # Yield merged records one at a time.

            for result in result_list:
                
                yield result

            record_offset += page_size

    def get_endpoint_records( self ):
        
        if self.refresh or not path.exists( self.api_result_file ):
            
            start_time = time.time()

            record_count = 0

            with gzip.open( self.api_result_file, "wb" ) as fp:
                
                writer = jsonlines.Writer(fp)

                for record in self.__paginate_endpoint_calls():
                    
                    writer.write(record)

                    record_count += 1

                    if record_count % self.result_page_size == 0:
                        
                        elapsed_time = time.time() - start_time

                        sys.stderr.write(f"done. Wrote {record_count} '{self.endpoint_singular}' records in {elapsed_time:.1f}s.\n")

            elapsed_time = time.time() - start_time

            sys.stderr.write(f"\nEndpoint pull complete. Wrote {record_count} '{self.endpoint_singular}' records in {elapsed_time:.1f}s.\n")

    def make_base_table( self ):
        
        if self.refresh or not path.exists( self.base_table_tsv ):
            
            input_file = self.api_result_file

            output_tsv = self.base_table_tsv

            fields_to_include = { field for field in self.fields_to_use if re.search( r'\.', field ) is None }

            # We'll explicitly make the id field the first output column, then append the rest.

            id_field = self.endpoint_singular + '_id'

            fields_to_include.remove(id_field)

            sys.stderr.write(f"Making {output_tsv}...")

            sys.stderr.flush()

            # For all metadata retrieved from the current endpoint, the goal here is to
            # automatically distinguish between atomic fields (properties) directly bound
            # to the endpoint entity and more complex structures, which we will handle separately.
            # 
            # More complex structures include both (a) associations with other entity types
            # and (b) metadata structures that are more complicated than single fields,
            # e.g. the `demographic` data structures attached to cases.
            # 
            # We need two read passes here to avoid having to manually enumerate which
            # fields are which (necessitating the creation and loading of field-list files
            # that then have to be explicitly maintained in sync with what the GDC API
            # provides).
            # 
            # The 'acl' field, for example, is described by the files/_mapping endpoint as
            # being of type "keyword", with no hint that it's actually an array. 'acl'
            # isn't listed anywhere at that _mapping endpoint as a non-atomic structure:
            # it does not appear under 'expand', nor under 'nested', nor under 'multi'.
            # The only way to reliably find out which fields like this one (which don't
            # contain '.' characters hinting at structural depth) aren't flat values
            # is to actually check the API result JSON to observe the field's
            # structure -- in our case, we check to see what sort of thing Python converts
            # the JSON field's structure into (e.g. list or dict). Since not all fields
            # are present in all records, all records must be actually checked to make
            # sure no such structural information is missed.

            complex_structures = set()

            with gzip.open(input_file) as IN:
                
                reader = jsonlines.Reader(IN)

                for record in reader:
                    
                    for key in record:
                        
                        if isinstance(record[key], list) or isinstance(record[key], dict):
                            
                            complex_structures.add(key)

                            if key in fields_to_include:
                                
                                fields_to_include.remove(key)

            # Save the autodetected 'more complex structures' list for validation.

            with open(self.complex_structure_list_file, 'w') as OUT:
                
                if len(complex_structures) > 0:
                    
                    print(*(sorted(complex_structures)), sep='\n', file=OUT)

            # Save names of list substructures that have been aliased to the top-level entity type
            # (e.g. "save contents of `index_files` as `file` records).

            lists_to_promote = set()

            for substructure_name in self.save_entity_list_as:
                
                if self.save_entity_list_as[substructure_name] == self.endpoint_singular:
                    
                    lists_to_promote.add(substructure_name)

            # Now load all data in atomic fields (and any substructures flagged to load as the top-level entity) and save this data to output_tsv.

            with gzip.open(input_file) as IN:
                
                reader = jsonlines.Reader(IN)
                
                with open(output_tsv, 'w') as OUT:
                    
                    print_field_list = list()

                    print_field_list.append(id_field)

                    print_field_list.extend(sorted(fields_to_include))

                    print( *print_field_list, sep='\t', file=OUT )

                    for record in reader:
                        
                        out_fields = list()

                        if id_field not in record:
                            
                            sys.exit(f"FATAL: encountered '{self.endpoint_singular}' record with no {id_field} field. Aborting.")

                        out_fields.append(record[id_field])

                        # Load values for target atomic fields and print to the base table file.

                        for key in sorted(fields_to_include):
                            
                            if key in record and record[key] is not None:
                                
                                out_fields.append(record[key])

                            else:
                                
                                out_fields.append('')

                        print(*out_fields, sep='\t', file=OUT)

                        # Load records from any substructures flagged as lists of top-level entity records (e.g. "load index_files as file records").

                        for subrecord_list_name in sorted( lists_to_promote ):
                            
                            if subrecord_list_name in record and record[subrecord_list_name] is not None:
                                
                                for subrecord in record[subrecord_list_name]:
                                    
                                    # Doesn't matter if we overwrite same-named `out_fields` list as above: we're not using the antecedent list any more.

                                    out_fields = list()

                                    if id_field not in subrecord:
                                        
                                        sys.exit(f"FATAL: encountered '{self.endpoint_singular}.{subrecord_list_name}' record with no {id_field} field. Aborting.")

                                    out_fields.append( subrecord[id_field] )

                                    # Load values for target atomic fields and print to the base table file.

                                    for key in sorted( fields_to_include ):
                                        
                                        if key in subrecord and subrecord[key] is not None:
                                            
                                            out_fields.append( subrecord[key] )

                                        else:
                                            
                                            out_fields.append( '' )

                                    print( *out_fields, sep='\t', file=OUT )

            # Sort the TSV output.

            sort_file_with_header(output_tsv)

            sys.stderr.write("done.\n")

    def __update_field_lists( self, field_name, field_lists ):
        """
            Take a (possibly mutiply nested) field name and recursively parse it to
            find atomic substructures, then bind those to the right entity types.
        """

        if len( re.findall( r'\.', field_name ) ) == 1:
            
            ( entity, field ) = re.split( r'\.', field_name )

            if entity not in field_lists:
                
                field_lists[entity] = set()

            field_lists[entity].add( field )

        else:
            
            field_name = re.sub( r'^[^\.]*\.', '', field_name )

            self.__update_field_lists( field_name, field_lists )

    def get_substructure_field_lists( self ):
        
        json_result = requests.get(self.mapping_url)

        if not json_result.ok:
            
            sys.exit(f"FATAL: call to API /{self.endpoint}/_mapping endpoint failed. Response content: " + str(json_result.content))

        result = json_result.json()

        for field_name in result["fields"]:
            
            if re.search( r'\.', field_name ) is not None:
                
                # Whatever this is, it has sub-objects. Did we say we wanted it filtered out?

                skip_current = False

                for prefix in self.substructures_to_filter:
                    
                    if re.search( r'^' + prefix + r'\.', field_name ) is not None:
                        
                        skip_current = True

                if not skip_current:
                    
                    # We did not want this filtered.

                    self.__update_field_lists( field_name, self.substructure_field_lists )

        for entity_name in self.substructure_field_lists:
            
            out_file = f"{self.METADATA_DIR}/substructure_field_list.{entity_name}.txt"

            if self.refresh or not path.exists( out_file ):
                
                sys.stderr.write(f"Making {out_file}...")

                sys.stderr.flush()

                with open(out_file, 'w') as OUT:
                    
                    print(*sorted(self.substructure_field_lists[entity_name]), sep='\n', file=OUT)

                sys.stderr.write("done.\n")

    def __traverse_substructure( self, entity_type, record, output_files, seen_ids, field_lists ):
        
        """
        Recursively explore substructures of endpoint records looking for sub-entities
        whose data we want to save in their own entity tables.

        Whenever we find one, pass it to self.__scan_and_save() which will write it to
        the appropriate TSV.
        """

        # We're only looking for atomic (non-nestable) properties of _dicts_ (general objects).
        # If we've been passed a _list_, open it up and recurse on traversable elements to
        # explore it, but the list itself is not something we care about.

        if isinstance( record, list ):
            
            for item in record:
                
                # Assumptions are great, but let's not make any if we don't
                # have to. Make sure what we pass to the recursion is a list or dict.

                if isinstance(item, list) or isinstance(item, dict):
                    
                    if entity_type in self.save_entity_list_as:
                        
                        # Aliases to top-level entity records (that is: records of the entity type
                        # this endpoint is named for) are handled elsewhere, in make_base_table().
                        # Check to make sure we're not trying to redo those.

                        if self.save_entity_list_as[entity_type] != self.endpoint_singular:
                            
                            # Process these as the designated entity type.

                            self.__traverse_substructure( self.save_entity_list_as[entity_type], item, output_files, seen_ids, field_lists )

                    else:
                        
                        self.__traverse_substructure( entity_type, item, output_files, seen_ids, field_lists )

        elif entity_type in output_files:
            
            # This is a dict that we want to scan & save. Pass it to the function that does that.

            self.__scan_and_save( entity_type, record, output_files, seen_ids, field_lists )

        else:
            
            # Now it's safe to assume the given record is a dict and that we are only
            # potentially interested in its nested substructures, not the record itself.

            for field_name in sorted(record):
                
                if isinstance( record[field_name], list ) or isinstance( record[field_name], dict ):
                    
                    if field_name in output_files:
                        
                        # This is a thing we want to save.

                        self.__scan_and_save( field_name, record[field_name], output_files, seen_ids, field_lists )

                    elif entity_type == self.endpoint_singular and field_name in self.array_entities:
                        
                        # This is a list of IDs hanging off of an endpoint record, and we want to save them
                        # as sub-entities. Save in seen_ids.

                        for array_entity_id in record[field_name]:
                            
                            seen_ids[field_name].add(array_entity_id)

                    else:
                        
                        # We don't want this thing itself, but might want some of its
                        # nested children. Check them out.
                    
                        self.__traverse_substructure( field_name, record[field_name], output_files, seen_ids, field_lists )

    def __scan_and_save( self, entity_type, entity_data, output_files, seen_ids, field_lists ):
        
        """
        Save a record for a sub-entity of interest to its own TSV table.
        """

        id_field = singularize(entity_type) + '_id'

        # If entity_data is a list, then it's a list of objects, each of which
        # needs to be processed individually. Forward the current entity_type
        # on down when we recurse.

        if isinstance( entity_data, list ):
            
            for item in entity_data:
                
                # Assumptions are great, but let's not make any if we don't
                # have to. Make sure what we pass to the recursion is a list or dict.

                if isinstance(item, list) or isinstance(item, dict):
                    
                    self.__scan_and_save( entity_type, item, output_files, seen_ids, field_lists )

        else:

            # Now it's safe to assume entity_data is a dict.

            if id_field in entity_data:
                
                # Save the ID of this record and check if we've seen it before.

                current_id = entity_data[id_field]

                if current_id not in seen_ids[entity_type]:
                    
                    # We haven't seen this ID yet for this entity type. Start recording and
                    # cache the ID in the "seen" dict so we don't load it again. See note
                    # at top of script.

                    current_record = dict()

                    seen_ids[entity_type].add(current_id)

                    for key in entity_data:
                        
                        # Save distinct value sets for array fields in their own tables.
                        # 
                        # example:
                        # entity_type == 'diagnoses'
                        # key == 'sites_of_involvement' or 'weiss_assessment_findings'
                        # 
                        # Note: skip top-level (endpoint-entity-level) arrays -- those are handled in __traverse_substructure().

                        if singularize(entity_type) != self.endpoint_singular and isinstance( entity_data[key], list ) and f"{singularize(entity_type)}.{key}" in self.array_entities:

                            # This is a list of values, and we want to save them in their own table
                            # as sub-entity IDs. Save in seen_ids.

                            for array_entity_id in entity_data[key]:
                                
                                seen_ids[key].add( array_entity_id )

                        elif isinstance( entity_data[key], list ) or isinstance( entity_data[key], dict ):
                            
                            # Send this sub-object back for further recursion. We're only looking for atomic fields here.

                            self.__traverse_substructure( key, entity_data[key], output_files, seen_ids, field_lists )

                        else:
                            
                            current_record[key] = entity_data[key]

                    # We've got all the data we're going to get for this record. Build an output
                    # row for the appropriate TSV, making sure to include empty strings for fields
                    # present in the schema but unseen in the current record.

                    output_row = list()

                    for field_name in field_lists[entity_type]:
                        
                        if field_name in current_record and current_record[field_name] is not None:
                            
                            output_row.append(current_record[field_name])

                        else:
                            
                            output_row.append('')

                    # Write the output row to the appropriate TSV.

                    print(*output_row, sep='\t', file=output_files[entity_type])

            else:
                
                sys.exit(f"FATAL: Couldn't find expected id field '{id_field}' in {entity_type} substructure; aborting.")

    def make_substructure_tables( self ):
        
        # Note: This subroutine will automatically collapse multiple objects with the
        # same ID, and it will not check multiple objects with the same ID for
        # possible discrepancies across repeated instances. If your goal is to
        # fully debug the GDC API data, this will need to be done differently.

        input_file = self.api_result_file

        output_files = dict()

        seen_ids = dict()

        # Configure field lists for sub-entity records for TSV output.

        output_field_lists = dict()

        for entity_type in self.substructure_field_lists:
            
            # Filter out substructures that are statistical summaries and not sub-entities per se.

            if entity_type not in self.statistical_summary_substructures:
                
                output_filename = f"{self.TSV_DIR}/{singularize(entity_type)}.tsv"

                # Refresh-aware condition: don't rebuild existing tables if asked not to.

                if self.refresh or not path.exists( output_filename ):
                    
                    # Let's make sure "{entity_type}_id" fields are always the first column in our TSVs.

                    id_field = singularize(entity_type) + '_id'

                    output_field_lists[entity_type] = [ id_field ]

                    output_field_lists[entity_type].extend( [ field_name for field_name in sorted(self.substructure_field_lists[entity_type]) if field_name != id_field ] )

        # Create & open a new TSV for each loaded entity type and cache a file pointer to each TSV
        # in a dict, keyed on entity name.
        # 
        # Programming note: we will not use the 'with' keyword here to manage file pointers because
        # (a) we're opening a variable number of files and (b) we're doing it simultaneously, and
        # Python's exit-stack context handling suddenly becomes a lot more cumbersome than just
        # using open() and close() in this case.

        output_files = dict( zip( sorted(output_field_lists), [ open( f"{self.TSV_DIR}/{singularize(entity_type)}.tsv", 'w' ) for entity_type in sorted(output_field_lists) ] ) )

        for entity_type in output_files:
            
            OUT = output_files[entity_type]

            print( *(output_field_lists[entity_type]), sep='\t', file=OUT )

        # As we progress through the API result JSON, track IDs already seen (by entity type)
        # so we can avoid duplicate rows. See note at top of this subroutine.

        for entity_type in output_files:
            
            seen_ids[entity_type] = set()

        # Once in a while we get an array of strings attached to the top-level entity
        # for which we want to create its own entity table (e.g. files.acl). Add these in
        # explicitly, since they're not picked up during the nested-object scan in
        # get_substructure_field_lists() (because they have no substructures).

        for entity_type in self.array_entities:
            
            entity_name = entity_type

            if re.search( r'\.', entity_type ) is not None:
                
                entity_name = re.sub( r'^.*\.', r'', entity_type )

            array_entity_output_file = f"{self.TSV_DIR}/{singularize(entity_name)}.tsv"

            if self.refresh or not path.exists( array_entity_output_file ):
                
                seen_ids[entity_name] = set()

        # Refresh-aware condition: only parse the API JSON if we're aiming to build (or rebuild) any TSV files.

        if len(seen_ids) > 0:
            
            # Scan the API result JSON and extract substructure data directly into
            # the appropriate output TSVs.

            with gzip.open(input_file) as IN:
                
                id_field = self.endpoint_singular + '_id'

                reader = jsonlines.Reader(IN)

                for record in reader:
                    
                    if id_field not in record:
                        
                        sys.exit(f"FATAL (and strange): This '{self.endpoint_singular}' record doesn't have any '{id_field}' ID field. Aborting after dump. {record}")

                    self.__traverse_substructure( self.endpoint_singular, record, output_files, seen_ids, output_field_lists )

        # Close TSV output files.

        for entity_type in output_files:
            
            output_files[entity_type].close()

        # Sort TSV output files.

        for output_file in [ f"{self.TSV_DIR}/{singularize(entity_type)}.tsv" for entity_type in sorted(output_field_lists) ]:
            
            sort_file_with_header(output_file)

            sys.stderr.write(f"Making {output_file}...done.\n")

        # Save self.array_entities ID lists, which we had to handle separately in order to
        # normalize this metadata.

        for entity_type in self.array_entities:
            
            entity_name = entity_type

            if re.search( r'\.', entity_type ) is not None:
                
                entity_name = re.sub( r'^.*\.', r'', entity_type )

            output_file = f"{self.TSV_DIR}/{singularize(entity_name)}.tsv"

            if self.refresh or not path.exists( output_file ):
                
                sys.stderr.write( f"Making {output_file}..." )

                sys.stderr.flush()

                with open(output_file, 'w') as OUT:
                    
                    id_field = f"{singularize(entity_name)}_id"

                    print(id_field, file=OUT)

                    for entity_id in sorted(seen_ids[entity_name]):
                        
                        print(entity_id, file=OUT)

                sys.stderr.write( 'done.\n' )

    def __explore_substructure_for_association_data( self, root_id, parent_id, record_type, record, target_record_types, seen_ids, association_maps ):
        
        # If we've been passed a list, open it up and recurse on traversable elements to
        # explore it, but the list itself is not something we care about.

        if isinstance( record, list ):
            
            for item in record:
                
                # Assumptions are great, but let's not make any if we don't
                # have to. Make sure what we pass to the recursion is a list or dict.

                if isinstance(item, list) or isinstance(item, dict):
                    
                    self.__explore_substructure_for_association_data( root_id, parent_id, record_type, item, target_record_types, seen_ids, association_maps )

        elif record_type in target_record_types:
            
            # This is a dict that we want to scan. Pass it to the function that does that.

            self.__scan_for_containment( root_id, parent_id, record_type, record, target_record_types, seen_ids, association_maps )

        else:
            
            # Now it's safe to assume the given record is a dict and that we are only
            # potentially interested in its nested substructures, not the record itself.

            for field_name in sorted(record):
                
                if isinstance( record[field_name], list ) or isinstance( record[field_name], dict ):
                    
                    if field_name in target_record_types:
                        
                        # This is a thing we want to examine.

                        self.__scan_for_containment( root_id, parent_id, field_name, record[field_name], target_record_types, seen_ids, association_maps )

                    elif record_type == self.endpoint_singular and field_name in self.array_entities and self.array_entities[field_name] in association_maps:
                        
                        # This is a list of IDs hanging off of an endpoint record: we've saved the values as
                        # sub-entity IDs, and now we link each ID to the endpoint record with an association map.

                        for array_entity_id in record[field_name]:
                            
                            add_to_map( association_maps[self.array_entities[field_name]], record[f"{self.endpoint_singular}_id"], array_entity_id )

                    else:
                        
                        # We don't want this thing itself, but might want some of its
                        # nested children. Check them out.
                    
                        self.__explore_substructure_for_association_data( root_id, parent_id, field_name, record[field_name], target_record_types, seen_ids, association_maps )

    def __scan_for_containment( self, root_id, parent_id, record_type, record, target_record_types, seen_ids, association_maps ):
        
        id_field = f"{singularize(record_type)}_id"

        if record_type in self.save_entity_list_as:
            
            id_field = f"{save_entity_list_as[record_type]}_id"

        # If record is a list, then it's a list of objects, each of which
        # needs to be processed individually. Forward the current record_type
        # on down when we recurse.

        if isinstance( record, list ):
            
            for item in record:
                
                # Assumptions are great, but let's not make any if we don't
                # have to. Make sure what we pass to the recursion is a list or dict.

                if isinstance(item, list) or isinstance(item, dict):
                    
                    self.__scan_for_containment( root_id, parent_id, record_type, item, target_record_types, seen_ids, association_maps )

        else:

            # Now it's safe to assume record is a dict.

            if id_field in record:
                
                # Save the ID of this record, record associative links, and check if we've seen it before.
                # If we haven't, recurse on its substructures as needed.

                current_id = record[id_field]

                # Cache associative links.

                # TO_DO: Parametrizing the following if statement into the per-endpoint config files is on my wish list.
                # It's also totally unnecessary and a giant pain in the butt, so I'm skipping it until
                # I get a free week.

                if record_type == 'diagnoses':
                    
                    if 'diagnosis_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['diagnosis_has_annotation'] )

                    if 'diagnosis_has_site_of_involvement' in association_maps:
                        
                        if 'sites_of_involvement' in record and record['sites_of_involvement'] is not None:
                            
                            for site_of_involvement in record['sites_of_involvement']:
                                
                                add_to_map( association_maps['diagnosis_has_site_of_involvement'], current_id, site_of_involvement )

                    if 'diagnosis_has_weiss_assessment_finding' in association_maps:
                        
                        if 'weiss_assessment_findings' in record and record['weiss_assessment_findings'] is not None:
                            
                            for weiss_assessment_finding in record['weiss_assessment_findings']:
                                
                                add_to_map( association_maps['diagnosis_has_weiss_assessment_finding'], current_id, weiss_assessment_finding )

                elif record_type == 'samples':
                    
                    if 'sample_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['sample_has_annotation'] )

                elif record_type == 'portions':
                    
                    if 'portion_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['portion_has_annotation'] )

                    if 'portion_from_sample' in association_maps:

                        add_to_map( association_maps['portion_from_sample'], current_id, parent_id )

                    if 'portion_from_case' in association_maps:

                        add_to_map( association_maps['portion_from_case'], current_id, root_id )

                    if 'center' in record and 'portion_from_center' in association_maps:
                        
                        add_to_map( association_maps['portion_from_center'], current_id, get_safe_value( record['center'], 'center_id' ) )

                elif record_type == 'analytes':
                    
                    if 'analyte_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['analyte_has_annotation'] )

                    if 'analyte_from_portion' in association_maps:

                        add_to_map( association_maps['analyte_from_portion'], current_id, parent_id )

                    if 'analyte_from_case' in association_maps:

                        add_to_map( association_maps['analyte_from_case'], current_id, root_id )

                elif record_type == 'aliquots':
                    
                    if 'aliquot_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['aliquot_has_annotation'] )

                    if 'aliquot_of_analyte' in association_maps:

                        add_to_map( association_maps['aliquot_of_analyte'], current_id, parent_id )

                    if 'aliquot_from_case' in association_maps:

                        add_to_map( association_maps['aliquot_from_case'], current_id, root_id )

                    if 'center' in record and 'aliquot_from_center' in association_maps:
                        
                        add_to_map( association_maps['aliquot_from_center'], current_id, get_safe_value( record['center'], 'center_id' ) )

                elif record_type == 'slides':
                    
                    if 'slide_has_annotation' in association_maps:

                        associate_id_list_with_parent( record, current_id, 'annotations', 'annotation_id', association_maps['slide_has_annotation'] )

                    if 'slide_from_portion' in association_maps:

                        add_to_map( association_maps['slide_from_portion'], current_id, parent_id )

                    if 'slide_from_case' in association_maps:

                        add_to_map( association_maps['slide_from_case'], current_id, root_id )

                elif record_type == 'pathology_details':
                    
                    if 'pathology_detail_of_diagnosis' in association_maps:

                        add_to_map( association_maps['pathology_detail_of_diagnosis'], current_id, parent_id )

                elif record_type == 'treatments':
                    
                    if 'treatment_of_diagnosis' in association_maps:

                        add_to_map( association_maps['treatment_of_diagnosis'], current_id, parent_id )

                elif record_type == 'molecular_tests':
                    
                    if 'molecular_test_from_follow_up' in association_maps:

                        add_to_map( association_maps['molecular_test_from_follow_up'], current_id, parent_id )

                elif record_type == 'analysis':
                    
                    if 'analysis_produced_file' in association_maps:

                        add_to_map( association_maps['analysis_produced_file'], current_id, parent_id )

                elif record_type == 'archive':
                    
                    if 'file_in_archive' in association_maps:

                        add_to_map( association_maps['file_in_archive'], parent_id, current_id )

                elif record_type == 'read_groups':
                    
                    if 'read_group_in_analysis' in association_maps:

                        add_to_map( association_maps['read_group_in_analysis'], current_id, parent_id )

                elif record_type == 'read_group_qcs':
                    
                    if 'read_group_qc_in_read_group' in association_maps:

                        add_to_map( association_maps['read_group_qc_in_read_group'], current_id, parent_id )

                # If we haven't seen this record before, recurse on its substructures as needed.

                if record_type not in seen_ids:
                    
                    seen_ids[record_type] = set()

                if current_id not in seen_ids[record_type]:
                    
                    # We haven't seen this ID yet for this entity type. Start recording and
                    # cache the ID in the "seen" dict so we don't load it again. See note
                    # at top of script.

                    seen_ids[record_type].add(current_id)

                    for key in record:
                        
                        if isinstance( record[key], list ) or isinstance( record[key], dict ):
                            
                            # Send this sub-object back for further recursion. We're only looking for containment relationships here.
                            # 
                            # Update parent_id to the ID of the immediately containing object.

                            self.__explore_substructure_for_association_data( root_id, current_id, key, record[key], target_record_types, seen_ids, association_maps )

            else:
                
                sys.exit(f"FATAL: Couldn't find expected id field '{id_field}' in {record_type} substructure; aborting.")

    def make_association_tables( self ):
        
        # Note: This subroutine will automatically collapse multiple objects with the
        # same ID, and it will not check multiple objects with the same ID for
        # possible discrepancies across repeated instances. If your goal is to
        # fully debug the GDC API data, this will need to be done differently.

        input_file = self.api_result_file

        seen_ids = dict()

        # Dicts to store association relationships.

        association_maps = dict()

        # Refresh-aware filter.

        for association_name in self.association_maps:
            
            association_table_file = f"{self.TSV_DIR}/{association_name}.tsv"

            if self.refresh or not path.exists( association_table_file ):
                
                association_maps[association_name] = dict()

        # Cache a target list of sub-object entity types of interest.

        target_record_types = { entity_type for entity_type in self.substructure_field_lists if entity_type not in self.statistical_summary_substructures }

        # Load field lists for special summary structures.

        summary_field_list = dict()

        for summary_type in self.statistical_summary_substructures:
            
            summary_field_list[summary_type] = [ field_name for field_name in sorted(self.substructure_field_lists[summary_type]) if field_name != singularize(summary_type) ]

        # Refresh-aware condition: only load the API JSON if we're aiming to build some TSVs.

        if len(association_maps) > 0:
            
            # Scan the API result JSON and extract association data.

            with gzip.open(input_file) as IN:
                
                reader = jsonlines.Reader(IN)
                
                for record in reader:
                    
                    id_field = f"{self.endpoint_singular}_id"

                    self_id = get_safe_value( record, id_field )

                    # Load data for nested containment associations (when appropriate).

                    if self.scan_substructures_for_association_relationships:
                        
                        self.__explore_substructure_for_association_data( self_id, self_id, self.endpoint_singular, record, target_record_types, seen_ids, association_maps )

                    # Grab top-level foreign-key associations (links between this record and other entities, by ID).

                    # I'm so tired.

                    if self.endpoint == 'annotations':
                        
                        # annotation_in_project

                        if 'project' in record and 'annotation_in_project' in association_maps:
                            
                            add_to_map( association_maps['annotation_in_project'], self_id, get_safe_value( record['project'], 'project_id' ) )

                    elif self.endpoint == 'cases':
                        
                        # case_has_annotation
                        
                        if 'annotations' in record and 'case_has_annotation' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'annotations', 'annotation_id', association_maps['case_has_annotation'] )
            
                        # demographic_of_case
                        
                        if 'demographic' in record and 'demographic_of_case' in association_maps:
                            
                            add_to_map( association_maps['demographic_of_case'], get_safe_value( record['demographic'], 'demographic_id' ), self_id )
            
                        # diagnosis_of_case
                        
                        if 'diagnoses' in record and 'diagnosis_of_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'diagnoses', 'diagnosis_id', association_maps['diagnosis_of_case'], reverse_column_order=True )
            
                        # exposure_of_case
            
                        if 'exposures' in record and 'exposure_of_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'exposures', 'exposure_id', association_maps['exposure_of_case'], reverse_column_order=True )
            
                        # family_history_of_case
            
                        if 'family_histories' in record and 'family_history_of_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'family_histories', 'family_history_id', association_maps['family_history_of_case'], reverse_column_order=True )
            
                        # follow_up_of_case
            
                        if 'follow_ups' in record and 'follow_up_of_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'follow_ups', 'follow_up_id', association_maps['follow_up_of_case'], reverse_column_order=True )
            
                        # case_in_project
            
                        if 'project' in record and 'case_in_project' in association_maps:
                            
                            add_to_map( association_maps['case_in_project'], self_id, get_safe_value( record['project'], 'project_id' ) )
            
                        # sample_from_case
            
                        if 'samples' in record and 'sample_from_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'samples', 'sample_id', association_maps['sample_from_case'], reverse_column_order=True )
            
                        # tissue_source_site_of_case
            
                        if 'tissue_source_site' in record and 'tissue_source_site_of_case' in association_maps:
                            
                            add_to_map( association_maps['tissue_source_site_of_case'], get_safe_value( record['tissue_source_site'], 'tissue_source_site_id' ), self_id )

                    elif self.endpoint == 'files':
                        
                        # analysis_consumed_input_file

                        if 'analysis' in record and 'analysis_consumed_input_file' in association_maps:
                            
                            associate_id_list_with_parent( record['analysis'], get_safe_value( record['analysis'], 'analysis_id' ), 'input_files', 'file_id', association_maps['analysis_consumed_input_file'] )

                        # file_from_center

                        if 'center' in record and 'file_from_center' in association_maps:
                            
                            add_to_map( association_maps['file_from_center'], self_id, get_safe_value( record['center'], 'center_id' ) )

                        # analysis_downstream_from_file, downstream_analysis_produced_output_file

                        if 'downstream_analyses' in record:
                            
                            if 'analysis_downstream_from_file' in association_maps:
                                
                                associate_id_list_with_parent( record, self_id, 'downstream_analyses', 'analysis_id', association_maps['analysis_downstream_from_file'], reverse_column_order=True )

                            if 'downstream_analysis_produced_output_file' in association_maps:
                                
                                for downstream_analysis in record['downstream_analyses']:
                                    
                                    associate_id_list_with_parent( downstream_analysis, get_safe_value( downstream_analysis, 'analysis_id' ), 'output_files', 'file_id', association_maps['downstream_analysis_produced_output_file'] )

                        # file_has_annotation

                        if 'annotations' in record and 'file_has_annotation' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'annotations', 'annotation_id', association_maps['file_has_annotation'] )

                        # file_has_index_file

                        if 'index_files' in record and 'file_has_index_file' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'index_files', 'file_id', association_maps['file_has_index_file'] )

                        # file_has_metadata_file

                        if 'metadata_files' in record and 'file_has_metadata_file' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'metadata_files', 'file_id', association_maps['file_has_metadata_file'] )

                        # file_in_case

                        if 'cases' in record and 'file_in_case' in association_maps:
                            
                            associate_id_list_with_parent( record, self_id, 'cases', 'case_id', association_maps['file_in_case'] )

                            # Index files don't appear as normal results from the `files` endpoint and so must be manually connected to their corresponding cases (transitively via the case associations with the files for which they are indexes).

                            if 'index_files' in record:
                                
                                for index_file in record['index_files']:
                                    
                                    associate_id_list_with_parent( record, index_file['file_id'], 'cases', 'case_id', association_maps['file_in_case'] )

                        # file_associated_with_entity

                        if 'associated_entities' in record and 'file_associated_with_entity' in association_maps:
                            
                            for associated_entity in record['associated_entities']:
                                
                                if 'case_id' not in associated_entity or 'entity_id' not in associated_entity or 'entity_submitter_id' not in associated_entity or 'entity_type' not in associated_entity:
                                    
                                    sys.exit(f"FATAL: Something screwy is missing with one of the associated_entities attached to file {self_id}; aborting.")

                                if self_id not in association_maps['file_associated_with_entity']:
                                    
                                    association_maps['file_associated_with_entity'][self_id] = dict()

                                case_id = associated_entity['case_id']
                                entity_id = associated_entity['entity_id']
                                entity_type = associated_entity['entity_type']
                                entity_submitter_id = associated_entity['entity_submitter_id']

                                if entity_id not in association_maps['file_associated_with_entity'][self_id]:
                                    
                                    association_maps['file_associated_with_entity'][self_id][entity_id] = dict()

                                association_maps['file_associated_with_entity'][self_id][entity_id]['case_id'] = case_id
                                association_maps['file_associated_with_entity'][self_id][entity_id]['entity_type'] = entity_type
                                association_maps['file_associated_with_entity'][self_id][entity_id]['entity_submitter_id'] = entity_submitter_id

                    elif self.endpoint == 'projects':
                        
                        # project_in_program

                        if 'program' in record and 'project_in_program' in association_maps:
                            
                            add_to_map( association_maps['project_in_program'], self_id, get_safe_value( record['program'], 'program_id' ) )

                        # project_studies_primary_site
                        
                        if 'primary_site' in record and 'project_studies_primary_site' in association_maps:
                            
                            # This is a flat list.

                            for primary_site in record['primary_site']:
                                
                                add_to_map( association_maps['project_studies_primary_site'], self_id, primary_site )

                        # project_studies_disease_type
                        
                        if 'disease_type' in record and 'project_studies_disease_type' in association_maps:
                            
                            # This is a flat list.

                            for disease_type in record['disease_type']:
                                
                                add_to_map( association_maps['project_studies_disease_type'], self_id, disease_type )

                        # project_summary_data

                        if 'summary' in record and 'project_summary_data' in association_maps:
                            
                            summary = record['summary']

                            association_maps['project_summary_data'][self_id] = dict()

                            for field in summary_field_list['summary']:
                                
                                if field in summary:
                                    
                                    association_maps['project_summary_data'][self_id][field] = summary[field]

                                else:
                                    
                                    association_maps['project_summary_data'][self_id][field] = ''

                            # project_data_category_summary_data

                            if 'data_categories' in summary and 'project_data_category_summary_data' in association_maps:
                                
                                association_maps['project_data_category_summary_data'][self_id] = dict()

                                # This is a list of dicts.

                                data_category_summaries = summary['data_categories']

                                for data_category_summary in data_category_summaries:
                                    
                                    data_category = get_safe_value( data_category_summary, 'data_category' )

                                    association_maps['project_data_category_summary_data'][self_id][data_category] = dict()

                                    for field in summary_field_list['data_categories']:
                                        
                                        if field in data_category_summary:
                                            
                                            association_maps['project_data_category_summary_data'][self_id][data_category][field] = data_category_summary[field]

                                        else:
                                            
                                            association_maps['project_data_category_summary_data'][self_id][data_category][field] = ''

                            # project_experimental_strategy_summary_data

                            if 'experimental_strategies' in summary and 'project_experimental_strategy_summary_data' in association_maps:
                                
                                association_maps['project_experimental_strategy_summary_data'][self_id] = dict()

                                # This is a list of dicts.

                                experimental_strategy_summaries = summary['experimental_strategies']

                                for experimental_strategy_summary in experimental_strategy_summaries:
                                    
                                    experimental_strategy = get_safe_value( experimental_strategy_summary, 'experimental_strategy' )

                                    association_maps['project_experimental_strategy_summary_data'][self_id][experimental_strategy] = dict()

                                    for field in summary_field_list['experimental_strategies']:
                                        
                                        if field in experimental_strategy_summary:
                                            
                                            association_maps['project_experimental_strategy_summary_data'][self_id][experimental_strategy][field] = experimental_strategy_summary[field]

                                        else:
                                            
                                            association_maps['project_experimental_strategy_summary_data'][self_id][experimental_strategy][field] = ''

        # Save all association data to TSV.

        if self.endpoint == 'annotations':
            
            if 'annotation_in_project' in association_maps:
                
                write_association_pairs( association_maps['annotation_in_project'], f"{self.TSV_DIR}/annotation_in_project.tsv", 'annotation_id', 'project_id' )

        elif self.endpoint == 'cases':
            
            if 'case_has_annotation' in association_maps:
                write_association_pairs( association_maps['case_has_annotation'], f"{self.TSV_DIR}/case_has_annotation.tsv", 'case_id', 'annotation_id' )


            if 'diagnosis_has_annotation' in association_maps:
                write_association_pairs( association_maps['diagnosis_has_annotation'], f"{self.TSV_DIR}/diagnosis_has_annotation.tsv", 'diagnosis_id', 'annotation_id' )


            if 'diagnosis_has_site_of_involvement' in association_maps:
                write_association_pairs( association_maps['diagnosis_has_site_of_involvement'], f"{self.TSV_DIR}/diagnosis_has_site_of_involvement.tsv", 'diagnosis_id', 'site_of_involvement_id' )


            if 'diagnosis_has_weiss_assessment_finding' in association_maps:
                write_association_pairs( association_maps['diagnosis_has_weiss_assessment_finding'], f"{self.TSV_DIR}/diagnosis_has_weiss_assessment_finding.tsv", 'diagnosis_id', 'weiss_assessment_finding_id' )


            if 'sample_has_annotation' in association_maps:
                write_association_pairs( association_maps['sample_has_annotation'], f"{self.TSV_DIR}/sample_has_annotation.tsv", 'sample_id', 'annotation_id' )


            if 'portion_has_annotation' in association_maps:
                write_association_pairs( association_maps['portion_has_annotation'], f"{self.TSV_DIR}/portion_has_annotation.tsv", 'portion_id', 'annotation_id' )


            if 'analyte_has_annotation' in association_maps:
                write_association_pairs( association_maps['analyte_has_annotation'], f"{self.TSV_DIR}/analyte_has_annotation.tsv", 'analyte_id', 'annotation_id' )


            if 'aliquot_has_annotation' in association_maps:
                write_association_pairs( association_maps['aliquot_has_annotation'], f"{self.TSV_DIR}/aliquot_has_annotation.tsv", 'aliquot_id', 'annotation_id' )


            if 'slide_has_annotation' in association_maps:
                write_association_pairs( association_maps['slide_has_annotation'], f"{self.TSV_DIR}/slide_has_annotation.tsv", 'slide_id', 'annotation_id' )


            if 'case_in_project' in association_maps:
                write_association_pairs( association_maps['case_in_project'], f"{self.TSV_DIR}/case_in_project.tsv", 'case_id', 'project_id' )


            if 'aliquot_of_analyte' in association_maps:
                write_association_pairs( association_maps['aliquot_of_analyte'], f"{self.TSV_DIR}/aliquot_of_analyte.tsv", 'aliquot_id', 'analyte_id' )


            if 'aliquot_from_center' in association_maps:
                write_association_pairs( association_maps['aliquot_from_center'], f"{self.TSV_DIR}/aliquot_from_center.tsv", 'aliquot_id', 'center_id' )


            if 'analyte_from_portion' in association_maps:
                write_association_pairs( association_maps['analyte_from_portion'], f"{self.TSV_DIR}/analyte_from_portion.tsv", 'analyte_id', 'portion_id' )


            if 'slide_from_portion' in association_maps:
                write_association_pairs( association_maps['slide_from_portion'], f"{self.TSV_DIR}/slide_from_portion.tsv", 'slide_id', 'portion_id' )


            if 'portion_from_sample' in association_maps:
                write_association_pairs( association_maps['portion_from_sample'], f"{self.TSV_DIR}/portion_from_sample.tsv", 'portion_id', 'sample_id' )


            if 'portion_from_center' in association_maps:
                write_association_pairs( association_maps['portion_from_center'], f"{self.TSV_DIR}/portion_from_center.tsv", 'portion_id', 'center_id' )


            if 'sample_from_case' in association_maps:
                write_association_pairs( association_maps['sample_from_case'], f"{self.TSV_DIR}/sample_from_case.tsv", 'sample_id', 'case_id' )


            if 'demographic_of_case' in association_maps:
                write_association_pairs( association_maps['demographic_of_case'], f"{self.TSV_DIR}/demographic_of_case.tsv", 'demographic_id', 'case_id' )


            if 'diagnosis_of_case' in association_maps:
                write_association_pairs( association_maps['diagnosis_of_case'], f"{self.TSV_DIR}/diagnosis_of_case.tsv", 'diagnosis_id', 'case_id' )


            if 'exposure_of_case' in association_maps:
                write_association_pairs( association_maps['exposure_of_case'], f"{self.TSV_DIR}/exposure_of_case.tsv", 'exposure_id', 'case_id' )


            if 'family_history_of_case' in association_maps:
                write_association_pairs( association_maps['family_history_of_case'], f"{self.TSV_DIR}/family_history_of_case.tsv", 'family_history_id', 'case_id' )


            if 'follow_up_of_case' in association_maps:
                write_association_pairs( association_maps['follow_up_of_case'], f"{self.TSV_DIR}/follow_up_of_case.tsv", 'follow_up_id', 'case_id' )


            if 'molecular_test_from_follow_up' in association_maps:
                write_association_pairs( association_maps['molecular_test_from_follow_up'], f"{self.TSV_DIR}/molecular_test_from_follow_up.tsv", 'molecular_test_id', 'follow_up_id' )


            if 'pathology_detail_of_diagnosis' in association_maps:
                write_association_pairs( association_maps['pathology_detail_of_diagnosis'], f"{self.TSV_DIR}/pathology_detail_of_diagnosis.tsv", 'pathology_detail_id', 'diagnosis_id' )


            if 'treatment_of_diagnosis' in association_maps:
                write_association_pairs( association_maps['treatment_of_diagnosis'], f"{self.TSV_DIR}/treatment_of_diagnosis.tsv", 'treatment_id', 'diagnosis_id' )


            if 'tissue_source_site_of_case' in association_maps:
                write_association_pairs( association_maps['tissue_source_site_of_case'], f"{self.TSV_DIR}/tissue_source_site_of_case.tsv", 'tissue_source_site_id', 'case_id' )


            if 'aliquot_from_case' in association_maps:
                write_association_pairs( association_maps['aliquot_from_case'], f"{self.TSV_DIR}/aliquot_from_case.tsv", 'aliquot_id', 'case_id' )


            if 'analyte_from_case' in association_maps:
                write_association_pairs( association_maps['analyte_from_case'], f"{self.TSV_DIR}/analyte_from_case.tsv", 'analyte_id', 'case_id' )


            if 'portion_from_case' in association_maps:
                write_association_pairs( association_maps['portion_from_case'], f"{self.TSV_DIR}/portion_from_case.tsv", 'portion_id', 'case_id' )


            if 'slide_from_case' in association_maps:
                write_association_pairs( association_maps['slide_from_case'], f"{self.TSV_DIR}/slide_from_case.tsv", 'slide_id', 'case_id' )

        elif self.endpoint == 'files':
            
            if 'analysis_consumed_input_file' in association_maps:
                write_association_pairs( association_maps['analysis_consumed_input_file'], f"{self.TSV_DIR}/analysis_consumed_input_file.tsv", 'analysis_id', 'input_file_id' )


            if 'analysis_downstream_from_file' in association_maps:
                write_association_pairs( association_maps['analysis_downstream_from_file'], f"{self.TSV_DIR}/analysis_downstream_from_file.tsv", 'analysis_id', 'file_id' )


            if 'analysis_produced_file' in association_maps:
                write_association_pairs( association_maps['analysis_produced_file'], f"{self.TSV_DIR}/analysis_produced_file.tsv", 'analysis_id', 'file_id' )


            if 'downstream_analysis_produced_output_file' in association_maps:
                write_association_pairs( association_maps['downstream_analysis_produced_output_file'], f"{self.TSV_DIR}/downstream_analysis_produced_output_file.tsv", 'analysis_id', 'output_file_id' )


            if 'file_from_center' in association_maps:
                write_association_pairs( association_maps['file_from_center'], f"{self.TSV_DIR}/file_from_center.tsv", 'file_id', 'center_id' )


            if 'file_has_acl' in association_maps:
                write_association_pairs( association_maps['file_has_acl'], f"{self.TSV_DIR}/file_has_acl.tsv", 'file_id', 'acl_id' )


            if 'file_has_annotation' in association_maps:
                write_association_pairs( association_maps['file_has_annotation'], f"{self.TSV_DIR}/file_has_annotation.tsv", 'file_id', 'annotation_id' )


            if 'file_has_index_file' in association_maps:
                write_association_pairs( association_maps['file_has_index_file'], f"{self.TSV_DIR}/file_has_index_file.tsv", 'file_id', 'index_file_id' )


            if 'file_has_metadata_file' in association_maps:
                write_association_pairs( association_maps['file_has_metadata_file'], f"{self.TSV_DIR}/file_has_metadata_file.tsv", 'file_id', 'metadata_file_id' )


            if 'file_in_archive' in association_maps:
                write_association_pairs( association_maps['file_in_archive'], f"{self.TSV_DIR}/file_in_archive.tsv", 'file_id', 'archive_id' )


            if 'file_in_case' in association_maps:
                write_association_pairs( association_maps['file_in_case'], f"{self.TSV_DIR}/file_in_case.tsv", 'file_id', 'case_id' )


            if 'read_group_in_analysis' in association_maps:
                write_association_pairs( association_maps['read_group_in_analysis'], f"{self.TSV_DIR}/read_group_in_analysis.tsv", 'read_group_id', 'analysis_id' )


            if 'read_group_qc_in_read_group' in association_maps:
                write_association_pairs( association_maps['read_group_qc_in_read_group'], f"{self.TSV_DIR}/read_group_qc_in_read_group.tsv", 'read_group_qc_id', 'read_group_id' )

            if 'file_associated_with_entity' in association_maps:
                
                file_associated_with_entity_tsv = f"{self.TSV_DIR}/file_associated_with_entity.tsv"

                sys.stderr.write(f"Making {file_associated_with_entity_tsv}...")

                sys.stderr.flush()

                with open( file_associated_with_entity_tsv, 'w' ) as OUT:
                    
                    print(*['file_id', 'entity_id', 'entity_type', 'entity_submitter_id', 'entity_case_id'], sep='\t', file=OUT)

                    for file_id in sorted(association_maps['file_associated_with_entity']):
                        
                        for entity_id in sorted(association_maps['file_associated_with_entity'][file_id]):
                            
                            print(*[file_id, entity_id, association_maps['file_associated_with_entity'][file_id][entity_id]['entity_type'],
                                association_maps['file_associated_with_entity'][file_id][entity_id]['entity_submitter_id'],
                                association_maps['file_associated_with_entity'][file_id][entity_id]['case_id']],
                                sep='\t', file=OUT)

                sys.stderr.write("done.\n")

        elif self.endpoint == 'projects':
            
            if 'project_in_program' in association_maps:
                write_association_pairs( association_maps['project_in_program'], f"{self.TSV_DIR}/project_in_program.tsv", 'project_id', 'program_id' )


            if 'project_studies_primary_site' in association_maps:
                write_association_pairs( association_maps['project_studies_primary_site'], f"{self.TSV_DIR}/project_studies_primary_site.tsv", 'project_id', 'primary_site' )


            if 'project_studies_disease_type' in association_maps:
                write_association_pairs( association_maps['project_studies_disease_type'], f"{self.TSV_DIR}/project_studies_disease_type.tsv", 'project_id', 'disease_type' )


            if 'project_summary_data' in association_maps:
                
                project_summary_data_tsv = f"{self.TSV_DIR}/project_summary_data.tsv"

                sys.stderr.write(f"Making {project_summary_data_tsv}...")

                sys.stderr.flush()

                with open(project_summary_data_tsv, 'w') as OUT:
                    
                    print(*(['project_id'] + sorted(summary_field_list['summary'])), sep='\t', file=OUT)

                    for project_id in sorted(association_maps['project_summary_data']):
                        
                        print(*([project_id] + [ association_maps['project_summary_data'][project_id][field] for field in sorted(summary_field_list['summary']) ]), sep='\t', file=OUT)

                sys.stderr.write("done.\n")

            if 'project_data_category_summary_data' in association_maps:
                
                project_data_category_summary_data_tsv = f"{self.TSV_DIR}/project_data_category_summary_data.tsv"

                sys.stderr.write(f"Making {project_data_category_summary_data_tsv}...")

                sys.stderr.flush()

                with open(project_data_category_summary_data_tsv, 'w') as OUT:
                    
                    print(*(['project_id'] + ['data_category'] + sorted(summary_field_list['data_categories'])), sep='\t', file=OUT)

                    for project_id in sorted(association_maps['project_data_category_summary_data']):
                        
                        for data_category in sorted(association_maps['project_data_category_summary_data'][project_id]):
                            
                            print(*([project_id] + [data_category] + [ association_maps['project_data_category_summary_data'][project_id][data_category][field] for field in sorted(summary_field_list['data_categories']) ]), sep='\t', file=OUT)

                sys.stderr.write("done.\n")

            if 'project_experimental_strategy_summary_data' in association_maps:
                
                project_experimental_strategy_summary_data_tsv = f"{self.TSV_DIR}/project_experimental_strategy_summary_data.tsv"

                sys.stderr.write(f"Making {project_experimental_strategy_summary_data_tsv}...")

                sys.stderr.flush()

                with open(project_experimental_strategy_summary_data_tsv, 'w') as OUT:
                    
                    print(*(['project_id'] + ['experimental_strategy'] + sorted(summary_field_list['experimental_strategies'])), sep='\t', file=OUT)

                    for project_id in sorted(association_maps['project_experimental_strategy_summary_data']):
                        
                        for experimental_strategy in sorted(association_maps['project_experimental_strategy_summary_data'][project_id]):
                            
                            print(*([project_id] + [experimental_strategy] + [ association_maps['project_experimental_strategy_summary_data'][project_id][experimental_strategy][field] for field in sorted(summary_field_list['experimental_strategies']) ]), sep='\t', file=OUT)

                sys.stderr.write("done.\n")

    def save_API_status_endpoint( self ):
        
        """
        Make a single GET call to the REST API at {self.status_url} and cache
        the result in a local file as a part of the current retrieved dataset.

        """

        if self.refresh or not path.exists( self.status_file ):
            
            result = requests.get( self.status_url )

            if result.ok:
                
                # Print as JSON

                with open( self.status_file, "w" ) as OUT:
                    
                    print( json.dumps( result.json(), indent=4 ), file=OUT )

            else:
                
                sys.stderr.write(str(result.content))

                sys.exit( f"FATAL: API status call failed. Aborting.")

    def update_merged_output_directory( self ):
        
        if path.exists( self.status_file ):
            
            dest_status_file = path.join( self.MERGED_OUTPUT_DIR, f"API_version_metadata.{self.endpoint}_endpoint_extraction.json" )

            shutil.copy2( self.status_file, dest_status_file )

        for source_file_basename in listdir(self.TSV_DIR):
            
            source_file_full_path = path.join( self.TSV_DIR, source_file_basename )

            dest_file_full_path = path.join( self.MERGED_OUTPUT_DIR, source_file_basename )

            if path.isfile( source_file_full_path ) and re.search( r'\.tsv$', source_file_basename ) is not None:
                
                if not path.exists( dest_file_full_path ):
                    
                    shutil.copy2( source_file_full_path, dest_file_full_path )

                elif self.refresh:
                    
                    temp_file_full_path = path.join( self.MERGED_OUTPUT_DIR, 'temp_' + source_file_basename )

                    with open( temp_file_full_path, 'w' ) as OUT:
                        
                        lines = list()

                        header = ''

                        with open( source_file_full_path ) as IN:
                            
                            header = next(IN).rstrip("\n")

                            lines = [ line.rstrip("\n") for line in IN ]

                        with open( dest_file_full_path ) as IN:
                            
                            header = next(IN).rstrip("\n")

                            lines.extend([ line.rstrip("\n") for line in IN ])

                        row_data = sorted(set(lines))

                        lines = [ header ]

                        lines.extend(row_data)

                        print( *lines, sep="\n", end="\n", file=OUT )

                    rename( temp_file_full_path, dest_file_full_path )

    def extract( self ):
        
        self.save_API_status_endpoint()

        self.get_field_lists_for_API_calls()

        self.get_endpoint_records()

        self.make_base_table()

        self.get_substructure_field_lists()

        self.make_substructure_tables()

        self.make_association_tables()

        self.update_merged_output_directory()

        with open( self.extraction_date_file, 'w' ) as OUT:
            
            print( get_current_date(), file=OUT )


