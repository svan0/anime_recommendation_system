gcloud dataflow jobs run unzip_external_data \
    --gcs-location gs://dataflow-templates-us-central1/latest/Bulk_Decompress_GCS_Files \
    --region us-central1 \
    --num-workers 1 \
    --staging-location gs://anime-rec-dev-dataflow-temp/temp \
    --parameters inputFilePattern=gs://anime-rec-dev-data-external/UserAnimeList.csv.zip,outputDirectory=gs://anime-rec-dev-data-external,outputFailureFile=gs://anime-rec-dev-data-external/decomperror.txt