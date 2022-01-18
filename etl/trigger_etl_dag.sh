gcloud composer environments run anime-etl-composer-env \
    --location=us-central1 \
    dags trigger -- anime_etl_pipeline