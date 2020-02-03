import os
from google.cloud import storage
from google.cloud.storage import Client

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="C:/My First Project-8aad3418a6b4.json"

storage_client =storage.Client()
buckets = list(storage_client.list_buckets())

bucket = storage_client.get_bucket("adbug")


print(bucket)

