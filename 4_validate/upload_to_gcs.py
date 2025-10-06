import os
from google.cloud import storage

# Nombre del bucket en Google Cloud Storage
BUCKET_NAME = "deltaquebec-data-pipeline"

# Carpeta local donde PySpark guarda los archivos procesados
LOCAL_FOLDER = os.path.join(os.getcwd(), "data", "processed_data")

def upload_files_to_gcs(bucket_name, local_folder):
    """Sube todos los archivos de una carpeta local a un bucket de GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    for root, _, files in os.walk(local_folder):
        for file_name in files:
            local_path = os.path.join(root, file_name)
            relative_path = os.path.relpath(local_path, local_folder)
            blob_path = f"processed_data/{relative_path}"  # Carpeta remota en el bucket

            blob = bucket.blob(blob_path)
            blob.upload_from_filename(local_path)

            print(f"✅ Subido: {file_name} → gs://{bucket_name}/{blob_path}")

    print("\n🎉 Todos los archivos se subieron correctamente al bucket.")

if __name__ == "__main__":
    print(f"📂 Subiendo archivos desde: {LOCAL_FOLDER}")
    upload_files_to_gcs(BUCKET_NAME, LOCAL_FOLDER)