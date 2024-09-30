# 004_airflow_dbt_retail_project

mkdir 004_airflow_dbt_retail_project
cd 004_airflow_dbt_retail_project
echo "# 004_airflow_dbt_retail_project" >> README.md

python3 -m venv venv && source venv/bin/activate

astro version
astro dev init
astro dev start
astro dev restart

# Zatrzymanie kontenerów & Usunięcie wszystkich kontenerów
docker stop $(docker ps -q) && docker rm $(docker ps -a -q)

# Check tasks
astro dev bash
airflow tasks test retail upload_csv_to_gcs 2024-01-01

# Terminal
astro dev bash

gs://data_stream_gcs # GCS

/usr/local/airflow/include/gcp/service_account.json # Keyfile Path
Google Cloud # Connection Type
gcp # Connection Id

AIRFLOW__CORE__TEST_CONNECTION=enabled # .env Działający guzik testowania
