import os
from random import random, randint

from mlflow import MlflowClient

# from mlflow import log_metric, log_param, log_artifacts

if __name__ == "__main__":
    client = MlflowClient("http://20.2.67.135:5000", "http://20.2.67.135:5000")
    print("Running mlflow_get_artifact.py")
    run_id = "5dfe34fd8a564f449232d2dcd983ea5f"
    local_dir = "."
    remote_artifacts_path = "states"
    local_path = client.download_artifacts(run_id, remote_artifacts_path, local_dir)

    # scp datasci@20.2.67.135:/home/datasci/anon/mlflow_script/docker-compose.yml krian-mai-krian-project/infra