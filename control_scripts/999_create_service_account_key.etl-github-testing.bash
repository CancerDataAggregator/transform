#!/usr/bin/env bash

KEY_FILE=GCS-service-account-key.etl-github-testing.json

SA_NAME=YOUR_SERVICE_ACCOUNT_NAME

PROJECT_ID=YOUR_PROJECT_ID

gcloud config set project $PROJECT_ID

gcloud iam service-accounts keys create $KEY_FILE --iam-account=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

