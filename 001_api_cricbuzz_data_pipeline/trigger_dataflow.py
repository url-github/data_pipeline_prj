"""
Funkcje Cloud Run nasłuchuje załadowanie csv do GCS i odpala zadanie Dataflow / Apache Beam
"""

from googleapiclient.discovery import build
import google.auth
import os

def trigger_df_job(event, context):

    bucket_name = event['bucket']
    file_name = event['name']

    credentials, project_id = google.auth.default()

    service = build('dataflow', 'v1b3', credentials=credentials)

    project = "third-essence-345723"
    templateLocation = "gs://dataflow-templates-europe-central2/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-data-flow",
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://012-ranking-data-metadata/udf.js",  # UDF
            "JSONPath": "gs://012-ranking-data-metadata/bq.json",  # Schema BQ
            "javascriptTextTransformFunctionName": "transform",  # Nazwa funkcji UDF
            "outputTable": "third-essence-345723.temp.batsmen_rankings",  # Tabela BQ
            "inputFilePattern": f"gs://{bucket_name}/{file_name}",  # Przesłany plik CSV
            "bigQueryLoadingTemporaryDirectory": "gs://012-ranking-data-metadata/temp",
        }
    }

    request = service.projects().locations().templates().launch(
        projectId=project,
        location="europe-central2",
        gcsPath=templateLocation,
        body=template_body
    )

    response = request.execute()

    print(f"Dataflow job triggered for file {file_name}: {response}")