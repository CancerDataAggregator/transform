#!/bin/bash

# one time only?
#   
#   gcloud sql instances describe cda-prototype | grep serviceAccountEmailAddress
#   [RESULT: p834755020376-uu1arm@gcp-sa-cloud-sql.iam.gserviceaccount.com]
#   gsutil iam ch serviceAccount:p834755020376-uu1arm@gcp-sa-cloud-sql.iam.gserviceaccount.com:objectAdmin gs://broad-cda-dev

# sometimes:
# gcloud auth application-default login

# every time:

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project broad-cda-dev

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=cda-prototype --filter='status!=DONE' --format='value(name)')

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi

echo "[ETL `date`] Clearing old SQL prepared statement files from bucket..."

gsutil rm -af gs://broad-cda-dev/cloud-sql-db-population/run_first/\*\*
gsutil rm -af gs://broad-cda-dev/cloud-sql-db-population/run_second/\*\*

echo "[ETL `date`] Uploading new SQL prepared statement files to bucket..."

gsutil cp SQL_data/clear_tables_and_drop_indices.sql gs://broad-cda-dev/cloud-sql-db-population
gsutil cp SQL_data/rebuild_indices.sql gs://broad-cda-dev/cloud-sql-db-population

gsutil cp SQL_data/run_first/*.sql.gz gs://broad-cda-dev/cloud-sql-db-population/run_first/
gsutil cp SQL_data/run_second/*.sql.gz gs://broad-cda-dev/cloud-sql-db-population/run_second/

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project broad-cda-dev

echo "[ETL `date`] Clearing old table rows and dropping tables constraints and indices..."

gcloud sql import sql cda-prototype "gs://broad-cda-dev/cloud-sql-db-population/clear_tables_and_drop_indices.sql" --database=postgres --user=postgres --quiet

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project broad-cda-dev

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=cda-prototype --filter='status!=DONE' --format='value(name)')

# If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi

echo "[ETL `date`] Running run_first SQL..."

for basename in `cd SQL_data/run_first; /bin/ls *.sql.gz; cd ../../`
do
    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project broad-cda-dev

    echo "[ETL `date`] Importing prepared statement SQL from $basename..."

    gcloud sql import sql cda-prototype "gs://broad-cda-dev/cloud-sql-db-population/run_first/$basename" --database=postgres --user=postgres --quiet

    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project broad-cda-dev

    echo "[ETL `date`] Checking for running operations..."

    PENDING_OPERATIONS=$(gcloud sql operations list --instance=cda-prototype --filter='status!=DONE' --format='value(name)')

    # If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

    if [[ $PENDING_OPERATIONS != "" ]]
    then 
        # Wait for any currently running operations to complete.

        echo "[ETL `date`] Awaiting completion of running operations..."

        gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
    fi
done

echo "[ETL `date`] Running run_second SQL..."

for basename in `cd SQL_data/run_second; /bin/ls *.sql.gz; cd ../../`
do
    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project broad-cda-dev

    echo "[ETL `date`] Importing prepared statement SQL from $basename..."

    gcloud sql import sql cda-prototype "gs://broad-cda-dev/cloud-sql-db-population/run_second/$basename" --database=postgres --user=postgres --quiet

    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project broad-cda-dev

    echo "[ETL `date`] Checking for running operations..."

    PENDING_OPERATIONS=$(gcloud sql operations list --instance=cda-prototype --filter='status!=DONE' --format='value(name)')

    # If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

    if [[ $PENDING_OPERATIONS != "" ]]
    then 
        # Wait for any currently running operations to complete.

        echo "[ETL `date`] Awaiting completion of running operations..."

        gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
    fi
done

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project broad-cda-dev

echo "[ETL `date`] Replacing dropped constraints and rebuilding dropped indices..."

gcloud sql import sql cda-prototype "gs://broad-cda-dev/cloud-sql-db-population/rebuild_indices.sql" --database=postgres --user=postgres --quiet

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project broad-cda-dev

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=cda-prototype --filter='status!=DONE' --format='value(name)')

# If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi


