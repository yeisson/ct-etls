import logging
import os
import urllib3
import re

from google.cloud import storage

# Configure this environment variable via app.yaml

# CLOUD_STORAGE_BUCKET = os.environ['CLOUD_STORAGE_BUCKET']
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../OAuth2Credential.json'
# [end config]

def create_file(filename, content, DATAFLOW_BUCKET):
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
    return "file created"


def get_file_as_string(filePath, DATAFLOW_BUCKET):
    gcs = storage.Client()

    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    logging.info(filePath)
    print(filePath)
    blob = bucket.get_blob(filePath)


    return blob.download_as_string()


def get_total_lines(filePath, DATAFLOW_BUCKET):

    gcs = storage.Client()
    bucket = gcs.get_bucket(DATAFLOW_BUCKET)
    blob = bucket.get_blob(filePath)
    content = blob.download_as_string()

    lines = content.split('\n')
    
    return len(lines)

def get_public_route(fileName, DATAFLOW_BUCKET):
    gcs = storage.Client()

    bucket = gcs.get_bucket(DATAFLOW_BUCKET)

    fullPath = 'output/'+fileName

    blob = bucket.get_blob(fullPath)

    blob.make_public()

    return blob.public_url