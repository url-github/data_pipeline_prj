# 001_api_cricbuzz_data_pipeline


![Data Pipeline](https://github.com/url-github/data_pipeline_prj/blob/main/001_api_cricbuzz_data_pipeline/data_pipeline.png)

* Airflow / Composer - dag_extract_and_push_gcs - First element of the pi
peline. Runs a script that downloads and loads data to GCS

* extract_and_push_gcs.py - Download from API to csv format and load to GCS

* trigger_dataflow.py - Cloud Run functions listens for csv load to GCS and fires Dataflow / Apache Beam job


