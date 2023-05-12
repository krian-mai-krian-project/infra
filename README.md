# infra

- Copy `.env.example` to `.env`
- Run `docker-compose up`

# To add a new dags to our service
- Write `dag(.py)` file in dags folder.
- `Wait` for 5 minutes and then airflow container would refresh dir source.
- The new `dag` will appear after that.
