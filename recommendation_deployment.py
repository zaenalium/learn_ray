import numpy as np
from sklearn.neighbors import NearestNeighbors
import ray
from ray import serve
from ray.serve.handle import DeploymentHandle
#import modin.panads as pd
from starlette.requests import Request
from typing import Dict
import pickle

# ray.init()
# serve.start()

def rec(items, pivot_table, model_knn, default_recommendation ,n_recommendations=10, ):
    if items in pivot_table.index :
        distances,indices=model_knn.kneighbors(pivot_table.loc[items].values.reshape(1,-1),n_neighbors=10+1)
        ids = []
        # Get recommendations from similar users
        for i in range(1, len(distances.flatten())):
            similar_user_id = pivot_table.index[indices.flatten()[i]]   
            ids.append(similar_user_id)
        return ids
    else:
        return default_recommendation

@serve.deployment(
    ray_actor_options={"num_cpus": 1},
    num_replicas="auto",
)
class ItemRec:
    # Take the message to return as an argument to the constructor.
    def __init__(self):
        with open('pivot.pkl', "rb") as f:
            self.pivot_table = pickle.load(f)
        with open('model_knn.pkl', "rb") as f:
            self.model = pickle.load(f)
        self.default_rec = ['85123A', '22423','85099B', '84879', '47566', '20725', '22720', '20727', 'POST', '23203']
        
    async def __call__(self, starlette_request: Request) -> Dict:
        payload = await starlette_request.json()
        stock_code = payload['stock_code']
        rec_items = recommend_items(stock_code, self.pivot_table, self.model, self.default_rec)
        return {"result": rec_items}

item_rec = ItemRec.bind()