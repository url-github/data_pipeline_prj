import os
import uuid
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from faker import Faker
from google.cloud import storage
from google.oauth2 import service_account

# Inicjalizacja Faker do generowania przykładowych danych
fake = Faker()

# Liczba wierszy do wygenerowania
num_rows = 10000

# Nazwa bucketu i folderu, do którego mają trafić pliki
bucket_name = os.getenv('BUCKET_NAME')

# Ścieżka do pliku z danymi autoryzacyjnymi GCS pobrana ze zmiennej środowiskowej
credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
credentials = service_account.Credentials.from_service_account_file(credentials_path)

# Tworzenie klienta Google Cloud Storage z wykorzystaniem danych autoryzacyjnych
storage_client = storage.Client(credentials=credentials)

# Pobranie nazwy głównego bucketu i folderu
bucket_root, folder_path = bucket_name.split('/')

# Tworzenie 50 plików z danymi
for i in range(10):
    data = []
    for _ in range(num_rows):
        data.append({
            "id": str(uuid.uuid4()),             # Generowanie UUID dla każdego wiersza
            "first_name": fake.first_name(),     # Losowe imię
            "last_name": fake.last_name(),       # Losowe nazwisko
            "email": fake.email(),               # Losowy email
            "phone_number": fake.phone_number(), # Losowy numer telefonu
            "address": fake.address(),           # Losowy adres
            "city": fake.city(),                 # Losowe miasto
            "state": fake.state(),               # Losowy stan
            "zip_code": fake.zipcode(),          # Losowy kod pocztowy
            "country": fake.country(),           # Losowy kraj
            "birthdate": fake.date_of_birth(minimum_age=18, maximum_age=80), # Losowa data urodzenia
            "gender": fake.random_element(elements=("Male", "Female", "Other")), # Losowa płeć
            # Dodaj więcej pól, jeśli potrzebujesz
        })

    # Tworzenie DataFrame z listy słowników
    df = pd.DataFrame(data)

    # Konwersja DataFrame do tabeli PyArrow
    table = pa.Table.from_pandas(df)

    # Ścieżka do pliku Parquet w folderze na bucket
    file_path = f"{folder_path}/customer_data_{i}.parquet"

    # Zapis do GCS w określonym folderze bucketu
    blob = storage_client.bucket(bucket_root).blob(file_path)
    with blob.open("wb") as f:
        pq.write_table(table, f)

    # Informacja o sukcesie
    print(f"Pomyślnie zapisano {num_rows} wierszy danych klientów do gs://{bucket_root}/{file_path}")