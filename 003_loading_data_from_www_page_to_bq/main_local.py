from flask import Flask, request, render_template, redirect, url_for
from google.cloud import storage
from google.oauth2 import service_account

app = Flask(__name__)

GCS_BUCKET_NAME = 'data_stream_gcs'
project='third-essence-345723'

credentials = service_account.Credentials.from_service_account_file(
    '/Users/p/Documents/SA/605074532510-compute.json'
)

storage_client = storage.Client(credentials=credentials, project=project)

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            return 'No file part'
        file = request.files['file']
        if file.filename == '':
            return 'No selected file'
        if file:
            bucket = storage_client.bucket(GCS_BUCKET_NAME)
            blob = bucket.blob(file.filename)
            blob.upload_from_file(file)
            return f'File {file.filename} uploaded to {GCS_BUCKET_NAME}.'
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)