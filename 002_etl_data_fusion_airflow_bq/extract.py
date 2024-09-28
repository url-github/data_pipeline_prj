import csv
from faker import Faker
import random
import string
from google.cloud import storage
# from google.oauth2 import service_account

# credentials = service_account.Credentials.from_service_account_file('/Users/p/Documents/SA/605074532510-compute.json')

num_employees = 100
fake = Faker()

password_characters = string.ascii_letters + string.digits

with open('employee_data.csv', mode='w', newline='') as file:
    fieldnames = ['first_name', 'last_name', 'email', 'address', 'phone_number', 'salary', 'password']
    # Zmiana na 'quotechar' i 'quoting' dla lepszego formatowania
    writer = csv.DictWriter(file, fieldnames=fieldnames, quotechar='"', quoting=csv.QUOTE_ALL)

    writer.writeheader()
    for _ in range(num_employees):
        writer.writerow({
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            # "job_title": fake.job(),
            # "department": fake.job(),
            "email": fake.email(),
            "address": fake.city(),
            "phone_number": fake.phone_number(),
            "salary": fake.random_number(digits=5),
            "password": ''.join(random.choice(password_characters) for _ in range(10))
        })

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    # storage_client = storage.Client(credentials=credentials) # local
    storage_client = storage.Client() # GCP
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to {destination_blob_name} in {bucket_name}.')

bucket_name = 'data_stream_gcs'
source_file_name = 'employee_data.csv'
destination_blob_name = '002_etl_data_fusion_airflow_bq/employee_data.csv'

upload_to_gcs(bucket_name, source_file_name, destination_blob_name)