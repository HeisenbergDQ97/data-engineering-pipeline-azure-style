import requests
import pandas as pd
import os

API_URL = "https://jsonplaceholder.typicode.com/users"


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "sample_data.csv")

def extract_data():
    print(" Extrayendo datos desde la API pública...")

    response = requests.get(API_URL)
    response.raise_for_status()  

    data = response.json()


    df = pd.DataFrame(data)

    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")

    print(f" Datos extraídos y guardados en: {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_data()
  

