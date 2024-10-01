from airflow.decorators import dag, task
from datetime import datetime

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
# Cosmos: Integracja zadań dbt z przepływem zadań Airflow
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator # operator do przesyłania plików z lokalnego systemu plików do GCS
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from airflow.models.baseoperator import chain

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

    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
		task_id='create_retail_dataset',
		dataset_id='retail',
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
			metadata=Metadata(schema='retail')
		),
		use_native_support=False,
	)

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)


    transform = DbtTaskGroup(
        group_id='transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/transform']
        )
    )

	# soda/checks/transform
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    report = DbtTaskGroup(
        group_id='report',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/report']
        )
    )

	# soda/checks/report
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_report(scan_name='check_report', checks_subpath='report'):
        from include.soda.check_function import check
        return check(scan_name, checks_subpath)

    chain(
        upload_csv_to_gcs,
        create_retail_dataset,
        gcs_to_raw,
        check_load(),
        transform,
        check_transform(),
        report,
        check_report(),
	)

retail()
