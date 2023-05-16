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

    return new_data, keys
  
  @task(task_id="interference_new_keys")
  def interference_new_keys_task(new_data : list):
    for data in new_data:
      res = model_prediction(data)
      data["emebedding"] = res["emebedding"]
      data["label"] = res["label"]
    return new_data

  @task(task_id="add_new_result_to_db")
  def add_new_result_to_db_task(new_data : list, keys : list):
    collection_name_items = dbname["query_items"]
    collection_name_keys = dbname["query_keys"]
    if len(new_data)>0: 
      x = collection_name_items.insert_many(new_data, ordered = False)
      keys.extend(x.inserted_ids)
      collection_name_keys.find_one_and_update({}, {"$set": {"keys": keys}}, upsert=True)
    logging.info(f"Total keys : {len(keys)}.\nNew keys : {len(x.inserted_ids)}")
    return new_data
  
  @task(task_id="compute_distance")
  def compute_distance_task(res : dict):
    return 

  res = query_data_task()
  new_data, keys = check_availble_in_db_task(res)
  new_data = interference_new_keys_task(new_data)
  new_data = add_new_result_to_db_task(new_data)
  task = compute_distance_task(task)
