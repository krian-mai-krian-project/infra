import pymongo
import numpy as np
import pandas as pd

myclient = pymongo.MongoClient("mongodb://root:example@localhost:27017/")
mydb = myclient["query_result"]
mycol = mydb["query_items"]

res = list(mycol.find())
n = len(res)

table_res = []

threshold = 0.95

for i in range(n):
    for j in range(i+1, n):
        id1 = res[i]['_id']
        emb1 = np.array(res[i]['embedding'])

        id2 = res[j]['_id']
        emb2 = np.array(res[j]['embedding'])

        cos_sim = np.dot(emb1, emb2)/(np.linalg.norm(emb1)*np.linalg.norm(emb2))

        if cos_sim > threshold:   
            table_res.append([id1, id2, cos_sim])

res_df = pd.DataFrame(table_res, columns=["id1", "id2", "cos_sim"])

