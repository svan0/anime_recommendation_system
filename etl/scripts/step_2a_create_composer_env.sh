PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud composer environments create $COMPOSER_ENV_NAME \
    --location=$ETL_REGION \
    --environment-size=small \
    --service-account=$SERVICE_ACCOUNT \
    --image-version=composer-2.0.1-airflow-2.1.4