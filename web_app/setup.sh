source .env

gcloud pubsub topics create $PUBSUB_TOPIC \
    --message-retention-duration=30d

gcloud dataflow jobs run web_app_click_stream_ingest \
    --gcs-location gs://dataflow-templates/latest/PubSub_to_BigQuery \
    --region us-central1 \
    --staging-location $DATAFLOW_TEMP_BUCKET \
    --parameters \
inputTopic=projects/$PROJECT_ID/topics/$PUBSUB_TOPIC,\
outputTableSpec=$PROJECT_ID:$BQ_DATASET_ID.$BQ_TABLE_ID,\
outputDeadletterTable=$PROJECT_ID:$BQ_DATASET_ID.$BQ_TABLE_ID