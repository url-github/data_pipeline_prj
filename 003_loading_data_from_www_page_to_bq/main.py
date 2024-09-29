from flask import Flask, request, render_template
from google.cloud import storage

app = Flask(__name__)

GCS_BUCKET_NAME = 'data_stream_gcs'

storage_client = storage.Client()

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
            blob = bucket.blob(f'003_loading_data_from_www_page_to_bq/{file.filename}')
            blob.upload_from_file(file)
            return f'File {file.filename} uploaded to {GCS_BUCKET_NAME}/003_loading_data_from_www_page_to_bq.'
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)