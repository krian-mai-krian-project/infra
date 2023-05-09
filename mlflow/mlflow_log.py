import os
from random import random, randint

from mlflow import MlflowClient

# from mlflow import log_metric, log_param, log_artifacts

if __name__ == "__main__":
    client = MlflowClient("http://34.142.181.201:5000", "http://34.142.181.201:5000")
    print("Running mlflow_tracking.py")
    
    experiment_id = "0"
    run = client.create_run(experiment_id)
    print(run.info.run_id)
    client.log_param(run.info.run_id, key="param1", value=randint(0, 100))
    client.log_metric(run.info.run_id, "foo", randint(0, 100))

    if not os.path.exists("outputs"):
        os.makedirs("outputs")
    with open("outputs/test.txt", "w") as f:
        f.write("hello world!")

    local_artifacts_path = "outputs"
    remote_artifacts_path = "states"
    client.log_artifacts(run.info.run_id, local_artifacts_path, artifact_path=remote_artifacts_path)
