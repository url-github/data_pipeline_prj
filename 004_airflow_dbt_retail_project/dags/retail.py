from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator # operator do przesyłania plików z lokalnego systemu plików do GCS
from airflow.providers.google.suite.transfers.local_to_drive import LocalFilesystemToGoogleDriveOperator

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
        dst='003_loading_data_from_www_page_to_bq/online_retail.csv',
        bucket='data_stream_gcs',
        gcp_conn_id='gcp',
        mime_type='text/csv',
    )

retail()

#  airflow tasks test retail upload_csv_to_gcs 2024-01-01