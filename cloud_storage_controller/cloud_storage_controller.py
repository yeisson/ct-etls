import logging
import os
import urllib3
import re

from google.cloud import storage

# Configure this environment variable via app.yaml

# CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'OAuth2Credential.json'
# [end config]

DATAFLOW_BUCKET = "geocoding_uploaded_files"

def create_file(filename, content):
    uploaded_file = content

    if not uploaded_file:
        return 'No file uploaded.', 400
    
    # Create a Cloud Storage client.
    gcs = storage.Client()

    # Get the bucket that the file will be uploaded to.
    # bucket = gcs.get_bucket(CLOUD_STORAGE_BUCKET)
    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    # Create a new blob and upload the file's content.
    blob = bucket.blob(filename)

    blob.upload_from_string(content)

    # The public URL can be used to directly access the uploaded file via HTTP.
    return blob.public_url


def get_file_as_string(filePath):
    gcs = storage.Client()

    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    logging.info(filePath)
    print(filePath)
    blob = bucket.get_blob(filePath)


    return blob.download_as_string()

def add_close_json_file(filePath):
    gcs = storage.Client()

    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    blob = bucket.get_blob(filePath)

    blob.upload_from_string(blob.download_as_string()[:-2] + "]")

    return True

def add_token_to_file(filePath, token):

    gcs = storage.Client()
    bucket = gcs.get_bucket(DATAFLOW_BUCKET)
    blob = bucket.get_blob(filePath)
    content = blob.download_as_string()

    lines = content.split('\n')
    finalContent = ""

    for line in lines:
        line = re.sub(r'[^\x00-\x7F]+',' ', line).replace('\r', '').replace('"', '').replace('\\', '') # se eliminan caracteres que no sean codificables a ascii
        # finalContent = finalContent + line + "," + token + "\n"
        # finalContent = finalContent + token + "," + line + "\n"
    
    blob.upload_from_string(finalContent)

def get_total_lines(filePath):

    gcs = storage.Client()
    bucket = gcs.get_bucket(DATAFLOW_BUCKET)
    blob = bucket.get_blob(filePath)
    content = blob.download_as_string()

    lines = content.split('\n')
    
    return len(lines)

def get_public_route(fileName):
    gcs = storage.Client()

    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    fullPath = 'output/'+fileName

    blob = bucket.get_blob(fullPath)

    blob.make_public()

    return blob.public_url