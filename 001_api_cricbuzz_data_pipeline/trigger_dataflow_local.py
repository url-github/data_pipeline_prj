from googleapiclient.discovery import build
import google.auth
import os

def hello_pubsub():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/p/Documents/SA/605074532510-compute.json"

    service = build('dataflow', 'v1b3')
    project = "third-essence-345723"
    templateLocation = "gs://dataflow-templates-europe-central2/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-data-flow",
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://012-ranking-data-metadata/udf.js",  # UDF
            "JSONPath": "gs://012-ranking-data-metadata/bq.json",  # Schema BQ
            "javascriptTextTransformFunctionName": "transform",  # Nazwa funkcji UDF
            "outputTable": "third-essence-345723.temp.batsmen_rankings",  # Tabela BQ
            "inputFilePattern": "gs://012-ranking-data/batsmen_rankings.csv",  # Plik wejściowy GCS
            "bigQueryLoadingTemporaryDirectory": "gs://012-ranking-data-metadata/temp",
        }
    }

    request = service.projects().locations().templates().launch(
    projectId=project,
    location="europe-central2",
    gcsPath=templateLocation,
    body=template_body)

    response = request.execute()

hello_pubsub()