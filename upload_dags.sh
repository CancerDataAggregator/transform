echo "Starting Folder Copy"
cd dags
pip install cleanpy
cleanpy .
gsutil cp -r .  gs://us-central1-cda-etl-5e3c8da1-bucket/dags
echo "Done Copying"