from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import os

# Ruta de tu carpeta de salida con los archivos Parquet
processed_dir = os.path.join(os.getcwd(), "data", "processed_data")

# Autenticación con Google Drive
gauth = GoogleAuth()
gauth.LoadClientConfigFile("credentials.json")  # Usa tu archivo real
gauth.CommandLineAuth() # Esto abrirá el navegador para autenticar
drive = GoogleDrive(gauth)

# Crear una carpeta en Drive para los archivos procesados
folder_metadata = {
    'title': 'DataPipeline_Processed',
    'mimeType': 'application/vnd.google-apps.folder'
}
folder = drive.CreateFile(folder_metadata)
folder.Upload()

print(f" Carpeta creada en Google Drive: {folder['title']} ({folder['id']})")

# Subir todos los archivos del directorio processed_data
for filename in os.listdir(processed_dir):
    file_path = os.path.join(processed_dir, filename)
    if os.path.isfile(file_path):
        gfile = drive.CreateFile({'title': filename, 'parents': [{'id': folder['id']}]})
        gfile.SetContentFile(file_path)
        gfile.Upload()
        print(f" Archivo subido: {filename}")

print(" Todos los archivos Parquet fueron subidos correctamente.")