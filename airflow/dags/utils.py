from pymongo import MongoClient
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