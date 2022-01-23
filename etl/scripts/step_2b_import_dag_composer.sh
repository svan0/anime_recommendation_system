source .env

gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$COMPOSER_ENV_REGION \
    --source=dags/queries
gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$COMPOSER_ENV_REGION \
    --source=dags/config.cfg
gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$COMPOSER_ENV_REGION \
    --source=dags/etl_dag.py