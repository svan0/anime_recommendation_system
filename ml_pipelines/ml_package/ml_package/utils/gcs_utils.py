'''
    Google Cloud Storage helper functions
'''
import json
from google.cloud import storage

def gcs_create_empty_folder(gcs_path):
    '''
        Takes GCS path (path to a file or a folder)
        and create an empty GCS folder
    '''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_path.split('/')[2])
    if gcs_path.endswith('/'):
        # If path to a folder, keep as is
        folder_path = "/".join(gcs_path.split('/')[3:])
    else:
        # If path to a file, take the parent folder
        folder_path = "/".join(gcs_path.split('/')[3:-1])
        folder_path = folder_path + '/'

    blob = bucket.blob(folder_path)
    blob.upload_from_string('')

def download_from_gcs_to_local(gcs_path, local_path_dir):
    '''
        Takes GCS path to a file and downloads files to 
        specified local folder
    '''
    storage_client = storage.Client()
    
    bucket_name = gcs_path.split('/')[2]
    bucket = storage_client.bucket(bucket_name)
    
    file_prefix = "/".join(gcs_path.split('/')[3:])
    blobs = bucket.list_blobs(prefix = file_prefix)
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        local_path_file = f"{local_path_dir}/{filename}"
        blob.download_to_filename(local_path_file)

def write_json_to_gcs(data, gcs_path):
    '''
        Write data as a JSON file in GCS
    '''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_path.split('/')[2])
    blob = bucket.blob("/".join(gcs_path.split('/')[3:]))
    blob.upload_from_string(json.dumps(data, indent=4, sort_keys=True))
