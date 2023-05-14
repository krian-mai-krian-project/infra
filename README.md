# infra
- Run `docker-compose up` in `airflow` and `mlflow`

## To add a new dags to our service
- Write `dag(.py)` file in dags folder.
- `Wait` for 5 minutes and then airflow container would refresh dir source.
- The new `dag` will appear after that.
