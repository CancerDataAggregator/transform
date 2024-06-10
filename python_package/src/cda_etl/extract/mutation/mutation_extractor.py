from google.cloud import bigquery, storage
from google.oauth2 import service_account

from math import ceil

from os import makedirs, path

class mutation_extractor:
    
    def __init__(
        self,
        gsa_key=None,
        gsa_info=None,
        source_project=None, # top-level BigQuery project space, e.g. 'isb-cgc-bq'
        source_dataset=None, # e.g. TCGA, TARGET, HCMI
        source_version=None, # ISB-CGC-assigned version suffix on mutation table names, e.g. source_version == 'hg38_gdc_current' --> TCGA mutations table address inside BQ isb-cgc-bq project space == 'TCGA.masked_somatic_mutation_hg38_gdc_current'
        processing_bucket=None
    ):
        
        self.gsa_key = gsa_key

        self.gsa_info = gsa_info

        self.source_table = f"{source_project}.{source_dataset}.masked_somatic_mutation_{source_version}"

        self.processing_bucket_subdir = f"somatic_mutation_{source_version}/{source_dataset}"

        self.processing_bucket = processing_bucket

        # The wildcard is for the Google machinery, to indicate how to organize
        # result-fragment files that come in during parallel processing of our
        # BQ query. '*' gets replaced by a distinct integer for each fragment file
        # the query produces. We combine fragment files later on (in the bucket) into a
        # complete result set before downloading.

        self.processing_bucket_fragment_file_path = f"{source_dataset}.masked_somatic_mutation_{source_version}-*.jsonl.gz"

        self.final_filename = f"{source_dataset}.masked_somatic_mutation_{source_version}.jsonl.gz"

        self.service_account_credentials = self.__load_service_account_credentials()

        self.max_blobs_compose = 32  # Maximum number of GCS blobs to compose (merge) at one time

        self.local_output_dir = f"extracted_data/mutation/{source_version}"

        if not path.isdir( self.local_output_dir ):
            
            makedirs( self.local_output_dir )

    def __load_service_account_credentials( self ):
        
        if self.gsa_info is not None:
            
            credentials = service_account.Credentials.from_service_account_info( self.gsa_info )

        else:
            
            credentials = service_account.Credentials.from_service_account_file( self.gsa_key, scopes=["https://www.googleapis.com/auth/cloud-platform"] )

        return credentials

    def extract_table_to_bucket( self ):
        
        """Save `self.source_table` to a GCS bucket."""

        # Clear the destination bucket of old files.

        print('Clearing old bucket contents...', end='')

        storage_client = storage.Client.from_service_account_json( self.gsa_key )

        bucket = storage_client.bucket( self.processing_bucket )

        # Remove the final composed file, if present.

        previous_final_file = bucket.get_blob( f"{self.processing_bucket_subdir}/{self.final_filename}" )

        if previous_final_file is not None:
            
            previous_final_file.delete()

        filename_components = self.processing_bucket_fragment_file_path.split("*")

        if len( filename_components ) > 1:
            
            to_delete = list()

            for blob in bucket.list_blobs( prefix=f"{self.processing_bucket_subdir}/{filename_components[0]}" ):
                
                to_delete.append( blob )

            for blob in to_delete:
                
                blob.delete()

        print('done.', end='\n\n')

        # Extract the table data to bucket files.

        print('Running extract job...', end='')

        query_client = bigquery.Client(
            
            credentials = self.service_account_credentials,
            project     = self.service_account_credentials.project_id
        )

        job_config                    = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON
        job_config.compression        = bigquery.Compression.GZIP

        project_id = self.source_table.split('.')[0]
        dataset_id = self.source_table.split('.')[1]
        table_name = self.source_table.split('.')[2]

        dataset_ref = bigquery.DatasetReference( project_id, dataset_id )
        #dataset_ref = bigquery.DatasetReference( self.service_account_credentials.project_id, dataset_id )
        table_ref = dataset_ref.table( table_name )

        destination_uri = f"gs://{self.processing_bucket}/{self.processing_bucket_subdir}/{self.processing_bucket_fragment_file_path}"

        extract_job = query_client.extract_table(
            
            table_ref,
            destination_uri,
            job_config = job_config,

            # `location` must match that of the source table.

            location = 'US'
        )

        print( 'done.\n\n    Job result: ' + str( extract_job.result() ), end='\n' ) # Waits for the job to complete.

        print( '    Job destination_uris: ' + str( extract_job.destination_uris ), end='\n\n' )

    def download_bucket_data( self ):
        
        """Aggregates `self.final_filename` from `self.processing_bucket/self.processing_bucket_subdir/self.processing_bucket_fragment_file_path` and downloads to `self.local_output_dir`."""

        local_destination_path = f"{self.local_output_dir}/{self.final_filename}"

        bucket_destination_path = f"{self.processing_bucket_subdir}/{self.final_filename}"

        storage_client = storage.Client.from_service_account_json( self.gsa_key )

        bucket = storage_client.bucket( self.processing_bucket )

        filename_components = self.processing_bucket_fragment_file_path.split( '*' )

        if len( filename_components ) > 1:
            
            target_file_prefix = filename_components[0]

            # Concatenate smaller files together in batches of `self.max_blobs_compose`:
            # one big file downloads faster than many small ones.

            blobs = []

            for blob in bucket.list_blobs( prefix=f"{self.processing_bucket_subdir}/{target_file_prefix}" ):
                
                blobs.append(blob)

            if len( blobs ) > self.max_blobs_compose:
                
                groups = ceil( len( blobs ) / self.max_blobs_compose )

                for i in range( groups ):
                    
                    min_index = i * self.max_blobs_compose

                    max_index = min( min_index + self.max_blobs_compose, len( blobs ) )

                    compose_filename = f"{self.processing_bucket_subdir}/{target_file_prefix}" + str(i) + ".jsonl.gz"

                    print( f"Composing {self.processing_bucket_subdir}/{target_file_prefix}{min_index}:{max_index} into {compose_filename}...", end='' )

                    bucket.blob( compose_filename ).compose( blobs[min_index:max_index] )

                    print( 'done.' )

                print()

                print( 'Deleting smaller source files...', end='' )

                for blob in blobs:
                    
                    blob.delete()

                print( 'done.', end='\n\n' )

                blobs = []

                for blob in bucket.list_blobs( prefix=f"{self.processing_bucket_subdir}/{target_file_prefix}"):
                    
                    blobs.append(blob)

            print('New (composed) files in bucket:')
            
            for blob in blobs:

                print(f"    {blob.name}")

            print()

            # Make one big file.
            
            print( f"Concatenating composed files into {bucket_destination_path} and downloading to {local_destination_path}...", end='' )

            bucket.blob( bucket_destination_path ).compose( blobs )

            # Download the one big file.

            blob = bucket.blob( bucket_destination_path )

            blob.download_to_filename( local_destination_path )

            print( 'done.', end='\n\n' )

            # Delete the fragment files.

            print( 'Deleting intermediate files...', end='' )

            for blob in blobs:
                
                blob.delete()

            print( 'done.', end='\n\n' )

        else:
            
            blob = bucket.blob( self.processing_bucket_fragment_file_path )

            blob.download_to_filename( local_destination_path )


