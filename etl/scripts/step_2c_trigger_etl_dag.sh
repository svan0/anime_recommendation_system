source .env

gcloud composer environments run $COMPOSER_ENV_NAME \
    --location=$COMPOSER_ENV_REGION \
    dags trigger -- anime_etl_pipeline