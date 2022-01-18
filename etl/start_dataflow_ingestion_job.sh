gcloud dataflow jobs run data_ingestion \
    --gcs-location gs://anime-rec-dev-dataflow-temp/templates/ingestion_to_bq \
    --region us-central1 \
    --num-workers 1 \
    --staging-location gs://anime-rec-dev-dataflow-temp/staging