gcloud composer environments storage dags import \
    --environment=anime-etl-composer-env \
    --location=us-central1 \
    --source=dags/queries
gcloud composer environments storage dags import \
    --environment=anime-etl-composer-env \
    --location=us-central1 \
    --source=dags/config.cfg
gcloud composer environments storage dags import \
    --environment=anime-etl-composer-env \
    --location=us-central1 \
    --source=dags/etl_dag.py