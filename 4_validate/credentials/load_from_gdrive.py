import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# --- CONFIGURACIÓN ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
CREDENTIALS_FILE = os.path.join(BASE_DIR, "credentials/credentials.json")  # tu JSON aquí
GDRIVE_FILE_ID = "1oERYPY0U_ARK2Hme5lg6beAiToWHgacR"

OUTPUT_LOCAL_CSV = os.path.join(BASE_DIR, "../data/sample_data.csv")

# --- AUTENTICACIÓN ---
flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
creds = flow.run_local_server(port=0)
service = build('drive', 'v3', credentials=creds)

# --- DESCARGAR CSV ---
request = service.files().get_media(fileId=GDRIVE_FILE_ID)
fh = io.FileIO(OUTPUT_LOCAL_CSV, 'wb')
downloader = MediaIoBaseDownload(fh, request)
done = False
while not done:
    status, done = downloader.next_chunk()
    print(f"Descargando {int(status.progress() * 100)}%...")

print(f" Archivo descargado en {OUTPUT_LOCAL_CSV}")

# --- OPCIONAL: leer con pandas para revisar ---
df = pd.read_csv(OUTPUT_LOCAL_CSV)
print("Primeras 5 filas:")
print(df.head())