from google.cloud import storage

import os

client = storage.Client.from_service_account_json('../../gcp_service_account.json')
bucket = client.get_bucket('gen-ai-tu')

def get_gcs_options():
    return {
        "project": os.getenv('GCP_PROJECT'),
        "token": {
            "type": "service_account",
            "project_id": os.getenv('GCP_PROJECT'),
            "private_key_id": os.getenv('GCP_PRIVATE_KEY_ID'),
            "private_key": os.getenv('GCP_PRIVATE_KEY').replace('\\n', '\n') if os.getenv('GCP_PRIVATE_KEY') else None,
            "client_email": os.getenv('GCP_EMAIL'),
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }

def check_folder_exist(target:str):
    blobs = bucket.list_blobs(prefix=target, delimiter='/')
    return len(list(blobs)) > 0 or len(list(bucket.list_blobs(prefix=target, max_results=1))) > 0

def create_folder(target:str) -> None:
    if not target.endswith('/'):
        target += '/'
        
    if not check_folder_exist(target=target):
        blob = bucket.blob(target)
        blob.upload_from_string('')