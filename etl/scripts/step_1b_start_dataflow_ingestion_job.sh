source .env

gcloud dataflow jobs run data_ingestion \
    --gcs-location $DATAFLOW_GCS_BUCKET/templates/ingestion_to_bq \
    --region $DATAFLOW_INGESTION_JOB_REGION \
    --num-workers 1 \
    --staging-location $DATAFLOW_GCS_BUCKET/staging