from pymongo import MongoClient
from random import random
import logging
def get_database():
 
   # Provide the mongodb atlas url to connect python to mongodb using pymongo
   CONNECTION_STRING = "mongodb://root:example@mongo:27017/"
 
   # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
   client = MongoClient(CONNECTION_STRING)
 
   # Create the database for our example (we will use the same database throughout the tutorial
   return client['query_result']

def get_id_from_url(url : str):
   if len(url) == 0: 
      return ""
   filename = url.split("/")[-1]
   id = filename.split(".")[0]
   return id

def model_prediction(url : str):
   return {
      "embedding": [random(), random(), random()],
      "label": [1, 2, 3]
   }

def upload_csv_to_bucket(filename, csv_data):        
   from google.cloud import storage     

   key_path = '/opt/airflow/keys/secret.json'     
   client = storage.Client.from_service_account_json(key_path)   
            
   bucket_name = 'krian-mai-krian-proj'       
   bucket = client.get_bucket(bucket_name)     
   logging.info(f"Upload {csv_data}")
   blob = bucket.blob(filename)        
   blob.upload_from_string(csv_data, content_type='text/csv')
