# 001_api_cricbuzz_data_pipeline

### API > Python > Airflow > GCS > Cloud Functions > Dataflow > BigQuery > Looker

Airflow / Composer - dag_extract_and_push_gcs - First element of the pipeline. Runs a script that downloads and loads data to GCS

extract_and_push_gcs.py - Download from API to csv format and load to GCS

trigger_dataflow.py - Cloud Run functions listens for csv load to GCS and fires Dataflow / Apache Beam job


