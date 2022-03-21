PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

cd $PROJECT_PATH/etl

gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$ETL_REGION \
    --source=dags/queries
gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$ETL_REGION \
    --source=dags/config.cfg
gcloud composer environments storage dags import \
    --environment=$COMPOSER_ENV_NAME \
    --location=$ETL_REGION \
    --source=dags/etl_dag.py