#!/bin/bash

PROJECT=YOUR_PROJECT_NAME

DB_INSTANCE_NAME=YOUR_CLOUD_SQL_POSTGRESQL_INSTANCE_NAME

DATABASE_NAME=NAME_OF_POSTGRESQL_DB_WITHIN__YOUR_CLOUD_SQL_INSTANCE

DATABASE_USERNAME=YOUR_POSTGRESQL_USERNAME

LOCAL_ROOT=SQL_data

BUCKET_ROOT=SOME_BUCKET_IN_YOUR_PROJECT_SPACE

PREPROCESS_SCRIPT_NAME=clear_table_data_indices_and_constraints.sql

NEW_TABLE_DATA_SUBDIR=new_table_data

POSTPROCESS_SCRIPT_NAME=rebuild_indices_and_constraints.sql

# sometimes:
# gcloud auth application-default login

# every time:

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project $PROJECT

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=$DB_INSTANCE_NAME --filter='status!=DONE' --format='value(name)')

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi

echo "[ETL `date`] Clearing old SQL prepared statement files from bucket..."

gsutil rm -af gs://$PROJECT/$BUCKET_ROOT/$NEW_TABLE_DATA_SUBDIR/\*\*

echo "[ETL `date`] Uploading new SQL prepared statement files to bucket..."

gsutil cp $LOCAL_ROOT/$PREPROCESS_SCRIPT_NAME gs://$PROJECT/$BUCKET_ROOT

gsutil cp $LOCAL_ROOT/$NEW_TABLE_DATA_SUBDIR/*.sql.gz gs://$PROJECT/$BUCKET_ROOT/$NEW_TABLE_DATA_SUBDIR/

gsutil cp $LOCAL_ROOT/$POSTPROCESS_SCRIPT_NAME gs://$PROJECT/$BUCKET_ROOT

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project $PROJECT

echo "[ETL `date`] Clearing old table rows and dropping tables constraints and indices..."

gcloud sql import sql $DB_INSTANCE_NAME "gs://$PROJECT/$BUCKET_ROOT/$PREPROCESS_SCRIPT_NAME" --database=$DATABASE_NAME --user=$DATABASE_USERNAME --quiet

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project $PROJECT

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=$DB_INSTANCE_NAME --filter='status!=DONE' --format='value(name)')

# If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi

echo "[ETL `date`] Loading all $NEW_TABLE_DATA_SUBDIR/ INSERT SQL..."

for basename in `cd $LOCAL_ROOT/$NEW_TABLE_DATA_SUBDIR; /bin/ls *.sql.gz; cd ../../`
do
    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project $PROJECT

    echo "[ETL `date`] Importing prepared statement SQL from $basename..."

    gcloud sql import sql $DB_INSTANCE_NAME "gs://$PROJECT/$BUCKET_ROOT/$NEW_TABLE_DATA_SUBDIR/$basename" --database=$DATABASE_NAME --user=$DATABASE_USERNAME --quiet

    echo "[ETL `date`] Refreshing project credentials..."

    gcloud config set project $PROJECT

    echo "[ETL `date`] Checking for running operations..."

    PENDING_OPERATIONS=$(gcloud sql operations list --instance=$DB_INSTANCE_NAME --filter='status!=DONE' --format='value(name)')

    # If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

    if [[ $PENDING_OPERATIONS != "" ]]
    then 
        # Wait for any currently running operations to complete.

        echo "[ETL `date`] Awaiting completion of running operations..."

        gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
    fi
done

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project $PROJECT

echo "[ETL `date`] Replacing dropped constraints and rebuilding dropped indices..."

gcloud sql import sql $DB_INSTANCE_NAME "gs://$PROJECT/$BUCKET_ROOT/$POSTPROCESS_SCRIPT_NAME" --database=$DATABASE_NAME --user=$DATABASE_USERNAME --quiet

echo "[ETL `date`] Refreshing project credentials..."

gcloud config set project $PROJECT

echo "[ETL `date`] Checking for running operations..."

PENDING_OPERATIONS=$(gcloud sql operations list --instance=$DB_INSTANCE_NAME --filter='status!=DONE' --format='value(name)')

# If the previous command (stupidly) returns control without breaking but before finishing and requires more time, give it more time.

if [[ $PENDING_OPERATIONS != "" ]]
then 
    # Wait for any currently running operations to complete.

    echo "[ETL `date`] Awaiting completion of running operations..."

    gcloud sql operations wait "${PENDING_OPERATIONS}" --timeout=unlimited
fi


