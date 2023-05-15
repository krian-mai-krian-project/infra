import airflow
import requests
import logging
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from utils import get_database, get_id_from_url

dbname = get_database()

with DAG(
    "disaster_traffy_fondue",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="A simple tutorial DAG",
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
  def check_availble_in_db(res : dict):
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

    collection_name_items = dbname["query_items"]
    x = collection_name_items.insert_many(new_data, ordered = False)
    keys.extend(x.inserted_ids)
    collection_name_keys.find_one_and_update({}, {"$set": {"keys": keys}}, upsert=True)

    collection_name_new_keys = dbname["query_new_keys"]
    collection_name_new_keys.insert_one({"keys": x.inserted_ids})
    return new_data

  

  task = query_data_task()
  task = check_availble_in_db(task)
