PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud dataflow jobs run $DATA_INGESTON_JOB_ID \
    --gcs-location $DATAFLOW_GCS_BUCKET/templates/ingestion_to_bq \
    --region $ETL_REGION \
    --num-workers 1 \
    --staging-location $DATAFLOW_GCS_BUCKET/staging