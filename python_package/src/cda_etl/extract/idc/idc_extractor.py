import re

from google.cloud import bigquery, storage
from google.oauth2 import service_account

from math import ceil

from os import makedirs, path

from cda_etl.lib import get_current_date

class IDC_extractor:
    
    def __init__( self, gsa_key=None, gsa_info=None, source_version=None, source_table=None, dest_table=None,
                    dest_bucket=None, dest_bucket_filename=None, output_filename=None ):
        
        self.gsa_key = gsa_key
        self.gsa_info = gsa_info
        self.bq_idc_version_path_string = f"idc_{source_version}"

        self.output_root = path.join( 'extracted_data', 'idc' )

        self.output_dir = path.join( self.output_root, '__raw_BigQuery_JSONL' )

        self.version_file = path.join( self.output_root, 'data_version.txt' )
        self.extraction_file = path.join( self.output_root, 'extraction_date.txt' )

        self.source_project = 'bigquery-public-data'
        self.source_dataset = f"{self.source_project}.{self.bq_idc_version_path_string}"
        self.source_table = f"{self.source_dataset}.{source_table}"

        self.dest_table = dest_table
        self.dest_bucket = dest_bucket
        self.dest_bucket_filename = dest_bucket_filename
        self.output_filename = output_filename

        self.service_account_credentials = self.__load_service_account_credentials()

        self.max_blobs_compose = 32  # Maximum number of GCS blobs to compose (merge) at one time

        if not path.isdir( self.output_dir ):
            
            makedirs( self.output_dir )

        with open( self.version_file, 'w' ) as OUT:
            
            print( source_version, file=OUT )

        with open( self.extraction_file, 'w' ) as OUT:
            
            print( get_current_date(), file=OUT )

    def __load_service_account_credentials( self ):
        
        if self.gsa_info is not None:
            
            credentials = service_account.Credentials.from_service_account_info( self.gsa_info )

        else:
            
            credentials = service_account.Credentials.from_service_account_file( self.gsa_key, scopes=["https://www.googleapis.com/auth/cloud-platform"] )

        return credentials

    def query_idc_to_table( self, fields_to_pull=list() ):
        
        """Query `fields_to_pull` from `self.source_table` and save the results in `self.dest_table`."""

        client = bigquery.Client(
            credentials=self.service_account_credentials,
            project=self.service_account_credentials.project_id
        )

        job_config = bigquery.QueryJobConfig(
            allow_large_results=True,
            destination=self.dest_table,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )

        field_sequence = ', '.join( fields_to_pull )

        # This is an ugly hack to remove STRUCT fields from the ORDER BY clause when querying dicom_all.

        order_by_fields = list()

        for field in fields_to_pull:
            
            if re.search( r'Sequence$', field ) is None:
                
                order_by_fields.append( field )

        order_by_field_sequence = ', '.join( order_by_fields )

        query_sql = f"SELECT DISTINCT {field_sequence} FROM `{self.source_table}` ORDER BY {order_by_field_sequence}"

        print('Querying\n\n    ' + query_sql + '\n\ninto\n\n    ' + self.dest_table, end=' ...\n\n')

        # Start the query, passing in the extra configuration.

        query_job = client.query( query_sql, location="US", job_config=job_config )

        print('...done.\n\n    Job result: ' + str(query_job.result()), end='\n\n') # Waits for the job to complete.

    def extract_table_to_bucket( self ):
        
        """Save `self.dest_table` to a GCS bucket."""

        dataset_id = self.dest_table.split(".")[1]
        table_name = self.dest_table.split(".")[2]

        # Clear the destination bucket of old files.

        print('Clearing old bucket contents...', end='')

        storage_client = storage.Client.from_service_account_json(self.gsa_key)

        bucket = storage_client.bucket(self.dest_bucket)

        # Remove the final composed file, if present.

        previous_final_file = bucket.get_blob( f"{self.bq_idc_version_path_string}/{self.output_filename}" )

        if previous_final_file is not None:
            
            previous_final_file.delete()

        filename_components = self.dest_bucket_filename.split("*")

        if len( filename_components ) > 1:
            
            to_delete = list()

            for blob in bucket.list_blobs( prefix=f"{self.bq_idc_version_path_string}/{filename_components[0]}" ):
                
                to_delete.append(blob)

            for blob in to_delete:
                
                blob.delete()

        print('done.', end='\n\n')

        # Extract the table data to bucket files.

        print('Running extract job...', end='')

        query_client = bigquery.Client(
            credentials=self.service_account_credentials,
            project=self.service_account_credentials.project_id
        )

        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = ( bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON )
        job_config.compression = bigquery.Compression.GZIP

        dataset_ref = bigquery.DatasetReference( self.service_account_credentials.project_id, dataset_id )
        table_ref = dataset_ref.table( table_name )

        destination_uri = f"gs://{self.dest_bucket}/{self.bq_idc_version_path_string}/{self.dest_bucket_filename}"

        extract_job = query_client.extract_table(
            table_ref,
            destination_uri,
            job_config=job_config,
            # Location must match that of the source table.
            location="US",
        )  # API request

        print('done.\n\n    Job result: ' + str(extract_job.result()), end='\n') # Waits for the job to complete.

        print('    Job destination_uris: ' + str(extract_job.destination_uris), end='\n\n')

    def download_bucket_data( self ):
        
        """Downloads `self.dest_bucket_filename` from `self.dest_bucket` to `self.output_filename`."""

        local_destination_path = f"{self.output_dir}/{self.output_filename}"
        bucket_destination_path = f"{self.bq_idc_version_path_string}/{self.output_filename}"

        storage_client = storage.Client.from_service_account_json( self.gsa_key )

        bucket = storage_client.bucket(self.dest_bucket)

        filename_components = self.dest_bucket_filename.split("*")

        if len( filename_components ) > 1:
            
            target_file_prefix = filename_components[0]

            # Concatenate smaller files together in batches of `self.max_blobs_compose`:
            # one big file downloads faster than many small ones.

            blobs = []

            for blob in bucket.list_blobs( prefix=f"{self.bq_idc_version_path_string}/{target_file_prefix}" ):
                
                blobs.append(blob)

            if len( blobs ) > self.max_blobs_compose:
                
                groups = ceil( len( blobs ) / self.max_blobs_compose )

                for i in range( groups ):
                    
                    min_index = i * self.max_blobs_compose

                    max_index = min( min_index + self.max_blobs_compose, len( blobs ) )

                    compose_filename = f"{self.bq_idc_version_path_string}/{target_file_prefix}" + str(i) + ".jsonl.gz"

                    print( f"Composing {self.bq_idc_version_path_string}/{target_file_prefix}{min_index}:{max_index} into {compose_filename}...", end='' )

                    bucket.blob( compose_filename ).compose( blobs[min_index:max_index] )

                    print( 'done.' )

                print()

                print( 'Deleting smaller source files...', end='' )

                for blob in blobs:
                    
                    blob.delete()

                print( 'done.', end='\n\n' )

                blobs = []

                for blob in bucket.list_blobs( prefix=f"{self.bq_idc_version_path_string}/{target_file_prefix}"):
                    
                    blobs.append(blob)

            print('New (composed) files in bucket:')
            
            for blob in blobs:

                print(f"    {blob.name}")

            print()

            # Make one big file.
            
            print(f"Concatenating composed files into {bucket_destination_path} and downloading to {local_destination_path}...", end='')

            bucket.blob( bucket_destination_path ).compose( blobs )

            # Download the one big file.

            blob = bucket.blob( bucket_destination_path )

            blob.download_to_filename( local_destination_path )

            print( 'done.', end='\n\n' )

            # Delete the input files

            print( 'Deleting intermediate files...', end='' )

            for blob in blobs:
                
                blob.delete()

            print( 'done.', end='\n\n' )

        else:
            
            blob = bucket.blob( self.dest_bucket_filename )

            blob.download_to_filename( local_destination_path )


