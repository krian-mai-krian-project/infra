import pymongo
import numpy as np
import pandas as pd

myclient = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
mydb = myclient["query_result"]
mycol = mydb["query_keys"]
keys = mycol.find_one()['keys']
keys_mapper = { k:i for i, k in enumerate(keys)}

mycol = mydb["query_items"]
res = list(mycol.find())
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
node_df.to_csv("/home/anon.ong/infra/script/node.csv", index_label = False, index = False)

threshold = 0.95

for i in range(n):
    for j in range(i+1, n):
        id1 = keys_mapper[res[i]['_id']]
        emb1 = np.array(res[i]['embedding'])

        id2 = keys_mapper[res[j]['_id']]
        emb2 = np.array(res[j]['embedding'])

        cos_sim = np.dot(emb1, emb2)/(np.linalg.norm(emb1)*np.linalg.norm(emb2))

        if cos_sim > threshold:   
            edge_res.append([id1, id2, cos_sim])

edge_df = pd.DataFrame(edge_res, columns=["Source", "Target", "Weight"])
x = edge_df.to_csv("/home/anon.ong/infra/script/edge.csv", index_label = False, index = False)
