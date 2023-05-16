import airflow
import requests
import logging
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from utils import (
  get_database, 
  get_id_from_url,
  model_prediction
)

dbname = get_database()

with DAG(
    "disaster_traffy_fondue",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A DAG for handling query_data and check_availble_in_db",
    schedule_interval = '@daily',
    start_date=datetime.now(),
    catchup=False,
    tags=["TraffyFondue"],
) as dag:

  @task(task_id="query_data")
  def query_data_task():
    path = "https://publicapi.traffy.in.th/share/fondue-ddpm/geojson"
    x = requests.get(path)
    return x.json()

  @task(task_id="check_availble_in_db")
  def check_availble_in_db_task(res : dict):
    collection_name_keys = dbname["query_keys"]
    data = collection_name_keys.find_one()
    if not isinstance(data, dict):
      return
    keys = data.get("keys", [])

    new_data = []
    features = res.get("features", list())
    recent_ids = set()
    for data in features:
      if not isinstance(data, dict):
        continue
      photo_url = data.get("properties", {}).get("photo_url", "")
      id = get_id_from_url(photo_url)
      data['_id'] = id

      if id not in keys and id not in recent_ids:
        new_data.append(data)
        recent_ids.add(id)

    return {
      "data":new_data, 
      "keys": keys
    }
  
  @task(task_id="interference_new_keys")
  def interference_new_keys_task(res : dict):
    new_data = res['data']
    for data in new_data:
      if not isinstance(data, dict):
        continue
      url = data.get("properties", {}).get("photo_url", "")
      pred_res = model_prediction(url)
      data["embedding"] = pred_res["embedding"]
      data["label"] = pred_res["label"]
    res["data"] = new_data
    return res

  @task(task_id="add_new_result_to_db")
  def add_new_result_to_db_task(res:dict):
    collection_name_items = dbname["query_items"]
    collection_name_keys = dbname["query_keys"]
    
    new_data = res['data']
    keys = res['keys']
    
    if len(new_data)>0: 
      x = collection_name_items.insert_many(new_data, ordered = False)
      keys.extend(x.inserted_ids)
      collection_name_keys.find_one_and_update({}, {"$set": {"keys": keys}}, upsert=True)
    logging.info(f"Total keys : {len(keys)}.\nNew keys : {len(x.inserted_ids)}")
    return res

  res = query_data_task()
  res = check_availble_in_db_task(res)
  res = interference_new_keys_task(res)
  res = add_new_result_to_db_task(res)
