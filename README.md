# data-engineering-pipeline-gcp-style
This project implements an automated data pipeline built with Python, designed to extract, transform, validate, and load data into Google Cloud Storage (GCS).

## Project Structure
data-engineering-pipeline-azure-style/

 1_extract/          
 2_transform/       
 4_validate/         
 data/             
 upload_to_gcs.py   
 run_pipeline.py    
 requirements.txt    

 ## Pipeline Workflow

Extract: Fetches data from a public API and saves it as a CSV file.
Transform: Cleans, normalizes, and converts data to Parquet format.
Validate: Ensures data quality and schema consistency.
Load: Uploads processed files to a Google Cloud Storage bucket.


 ## Requirements

Python 3.10 or higher
Google Cloud account with a service account key
Packages listed in requirements.txt
