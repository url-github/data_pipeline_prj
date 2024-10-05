mkdir 005_x_data_pipeline_using_airflow
cd 005_x_data_pipeline_using_airflow

python3 -m venv venv && source venv/bin/activate

pip install -r requirements.txt

curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.2/docker-compose.yaml'

mkdir -p ./dags ./logs ./plugins ./config

echo -e "AIRFLOW_UID=$(id -u)" > .env
echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env

docker compose up -d
docker compose down

docker compose down && docker compose up -d

# Check tasks
docker compose ps
docker exec -it xxx-scheduler-1 /bin/bash
airflow tasks test dag_name task_name 2024-01-01