import requests
import logging
import pandas as pd
import numpy as np
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from utils import (
  get_database, 
  get_id_from_url,
  model_prediction,
  upload_csv_to_bucket
)

threshold = 0.95

dbname = get_database()

filtered_type = ["ดินถล่ม/โคลนถล่ม", "ภัยหนาว", "ภัยแล้ง", "วาตภัย", "อัคคีภัย", "อาคารถล่ม/ทรุด", "อุทกภัย", "แผ่นดินไหว", "ไฟป่า"]

max_number = 30

with DAG(
    "disaster_traffy_fondue",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    description="A DAG for handling query_data and check_availble_in_db",
    schedule_interval = '*/5 * * * *',
    start_date=datetime.now(),
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
      collection_name_keys.insert_one({"keys":[]})
      data = {}

    keys = data.get("keys", [])

    new_data = []
    features = res.get("features", list())

    logging.info(f"Features: {features}")
    logging.info(f"Keys: {keys}")
    
    recent_ids = set()
    for data in features:
      if not isinstance(data, dict):
        continue

      photo_url = data.get("properties", {}).get("photo_url", "")
      type = data.get("properties", {}).get("type", "")

      if type not in filtered_type:
        continue

      id = get_id_from_url(photo_url)
      data['_id'] = id
      if id not in keys and id not in recent_ids:
        if len(recent_ids) >= max_number:
          break
        new_data.append(data)
        recent_ids.add(id)

    logging.info(f"New data: {new_data}")
    logging.info(f"Recent ids: {recent_ids}")

    return {
      "data": new_data, 
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
    logging.info(f"Total keys : {len(keys)}.\nNew keys : {len(new_data)}")
    return None

  @task(task_id="compute_distance_and_prepare_df")
  def compute_distance_and_prepare_df_task(res:dict):
    collection_name_items = dbname["query_items"]
    collection_name_keys = dbname["query_keys"]

    keys = collection_name_keys.find_one()['keys']
    keys_mapper = { k:i for i, k in enumerate(keys)}

    res = list(collection_name_items.find())
    n = len(res)

    node_res = []
    edge_res = []

    for data in res:
        index = keys_mapper[data['_id']]
        _id = data['_id']
        disaster_type = data['properties']['type']
        photo_url = data['properties']['photo_url']
        temp = [index, _id, disaster_type, photo_url]
        node_res.append(temp)

    node_df = pd.DataFrame(node_res, columns=["Id", "Label", "Type", "Image"])
    logging.info(f"Node df : {len(node_df)} rows")

    for i in range(n):
        for j in range(i+1, n):
            id1 = keys_mapper[res[i]['_id']]
            emb1 = np.array(res[i]['embedding']).reshape(-1)

            id2 = keys_mapper[res[j]['_id']]
            emb2 = np.array(res[j]['embedding']).reshape(-1)

            cos_sim = np.dot(emb1, emb2)/(np.linalg.norm(emb1)*np.linalg.norm(emb2))

            if cos_sim > threshold:   
                edge_res.append([id1, id2, cos_sim])

    edge_df = pd.DataFrame(edge_res, columns=["Source", "Target", "Weight"])
    logging.info(f"Edge df : {len(edge_df)} rows")

    spatial_res = []

    for i in range(n):    
      obj = res[i]   
      label = obj['label']
      if label == 1:
        continue
      id = obj['properties']['ticket_id']
      lat = obj['geometry']['coordinates'][1]
      long = obj['geometry']['coordinates'][0]
      photo_url = obj['properties']['photo_url']
      state = obj['properties']['state']
      type = obj['properties']['type']
      description = obj['properties']['description']
      timestamp = obj['properties']['timestamp']

      spatial_res.append([id, lat, long, photo_url, state, type, description, timestamp])

    spatial_df = pd.DataFrame(spatial_res, columns=["id", "lat", "long", "photo_url", "state", "type", "description", "timestamp"])

    return {
       "edge_df": edge_df,
       "node_df": node_df,
       "spatial_df": spatial_df
    }

  @task(task_id="export_and_push_csv")
  def export_and_push_csv_task(res:dict):
    edge_df = res['edge_df']
    node_df = res['node_df']
    spatial_df = res['spatial_df']
    if isinstance(edge_df, pd.DataFrame) and isinstance(node_df, pd.DataFrame):
      edge_csv = edge_df.to_csv(index = False)
      node_csv = node_df.to_csv(index = False)
      spatial_csv = spatial_df.to_csv(index = False)
      now = datetime.now()
      date_time = now.strftime("%Y%m%d:%H:%M:%S")
      upload_csv_to_bucket(f"visualization/{date_time}/edge.csv", edge_csv)
      upload_csv_to_bucket(f"visualization/{date_time}/node.csv", node_csv)
      upload_csv_to_bucket(f"visualization/{date_time}/spatial.csv", spatial_csv)
    return None

  res = query_data_task()
  res = check_availble_in_db_task(res)
  res = interference_new_keys_task(res)
  res = add_new_result_to_db_task(res)
  res = compute_distance_and_prepare_df_task(res)
  res = export_and_push_csv_task(res)