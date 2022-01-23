source .env

gcloud composer environments create $COMPOSER_ENV_NAME \
    --location=$COMPOSER_ENV_REGION \
    --environment-size=small \
    --service-account=$COMPOSER_SERVICE_ACCOUNT \
    --image-version=composer-2.0.1-airflow-2.1.4