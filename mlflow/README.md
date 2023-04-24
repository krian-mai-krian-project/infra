# infra - mlflow
1. If you got thumbs up sticker on discord, it means you have been invited to IAM user of google cloud platform. 
2. Verify that you already enrolled by checking krina-mai-krian-proj project on the console, notify me if it should not be as expected.
3. Follow this [LINK](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account) to get secret.json.
4. `export GOOGLE_APPLICATION_CREDENTIALS =/path/to/secret.json` to attach secret
5. To log an artifact, follow mlflow_log.py
6. To download an artifact, follow mlflow_download.py