gcloud composer environments create anime-etl-composer-env \
    --location=us-central1 \
    --environment-size=small \
    --service-account=svanowner@anime-rec-dev.iam.gserviceaccount.com \
    --image-version=composer-2.0.1-airflow-2.1.4