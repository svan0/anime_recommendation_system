python3 -m dataflow_templates.ingestion \
    --runner DataflowRunner \
    --project anime-rec-dev \
    --staging_location gs://anime-rec-dev-dataflow-temp/staging \
    --temp_location gs://anime-rec-dev-dataflow-temp/temp \
    --template_location gs://anime-rec-dev-dataflow-temp/templates/ingestion_to_bq \
    --region us-central1 \
    --streaming \
    --input_topic projects/anime-rec-dev/topics/data_ingestion_queue \
    --window_interval_sec 120