PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

bq --location us-central1 mk --dataset $PROJECT_ID:$WEB_APP_BQ_DATASET_ID
bq mk --table $PROJECT_ID:$WEB_APP_BQ_DATASET_ID.$WEB_APP_BQ_TABLE_ID click_stream_table_schema.json

gcloud pubsub topics create $WEB_APP_PUBSUB_TOPIC \
    --message-retention-duration=30d

gcloud dataflow jobs run $WEB_APP_DATAFLOW_JOB_ID \
    --gcs-location gs://dataflow-templates/latest/PubSub_to_BigQuery \
    --region $WEB_APP_REGION \
    --staging-location $DATAFLOW_GCS_BUCKET \
    --parameters \
inputTopic=projects/$PROJECT_ID/topics/$WEB_APP_PUBSUB_TOPIC,\
outputTableSpec=$PROJECT_ID:$WEB_APP_BQ_DATASET_ID.$WEB_APP_BQ_TABLE_ID,\
outputDeadletterTable=$PROJECT_ID:$WEB_APP_BQ_DATASET_ID.$WEB_APP_BQ_TABLE_ID