'''
    Google Cloud Storage helper functions
'''
import os
import glob
import logging
from google.cloud import storage

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
        logging.info(f"Downloading {blob.name} to {local_path_file}")

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

def upload_local_folder_to_gcs(local_path, gcs_path):
    '''
        Upload local folder content to GCS
    '''

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(gcs_path.split('/')[2])
    gcs_path = "/".join(gcs_path.split('/')[3:])

    assert(os.path.isdir(local_path))
    for local_file in glob.glob(local_path + '/**'):
        if os.path.isfile(local_file):

           remote_path = os.path.join(gcs_path, local_file[1 + len(local_path):])
           blob = bucket.blob(remote_path)
           blob.upload_from_filename(local_file)
           
           logging.info(f"Uploading {local_file} to {remote_path}")