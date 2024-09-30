from airflow.decorators import dag, task
from datetime import datetime

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator # operator do przesyłania plików z lokalnego systemu plików do GCS
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['a'],
)
def retail():

    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/dataset/online_retail.csv',
        dst='004_airflow_dbt_retail_project/online_retail.csv',
        bucket='data_stream_gcs',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

    create_temp_dataset = BigQueryCreateEmptyDatasetOperator(
		task_id='create_temp_dataset',
		dataset_id='temp',
		gcp_conn_id='gcp',
	)

    gcs_to_raw = aql.load_file(
		task_id='gcs_to_raw',
		input_file=File(
			'gs://data_stream_gcs/004_airflow_dbt_retail_project/online_retail.csv',
			conn_id='gcp',
			filetype=FileType.CSV,
		),
		output_table=Table(
			name='raw_invoices',
			conn_id='gcp',
			metadata=Metadata(schema='temp')
		),
		use_native_support=False,
	)

    # @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    # def check_load(scan_name='check_load', checks_subpath='sources'):
    #     from include.soda.check_function import check
    #     return check(scan_name, checks_subpath)

    # check_load()

retail()
