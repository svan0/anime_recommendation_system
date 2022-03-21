PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

cd $PROJECT_PATH/etl

python3 -m dataflow_templates.ingestion \
    --runner DataflowRunner \
    --project $PROJECT_ID \
    --staging_location $DATAFLOW_GCS_BUCKET/staging \
    --temp_location $DATAFLOW_GCS_BUCKET/temp \
    --template_location $DATAFLOW_GCS_BUCKET/templates/ingestion_to_bq \
    --region $DATAFLOW_INGESTION_JOB_REGION \
    --streaming \
    --input_topic projects/$PROJECT_ID/topics/$DATA_INGESTION_PUBSUB_TOPIC \
    --window_interval_sec 120