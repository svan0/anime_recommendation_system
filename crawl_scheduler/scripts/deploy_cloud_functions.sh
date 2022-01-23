source .env

gcloud functions deploy $ANIME_SCHEDULE_CLOUD_FUNCTION_NAME\
    --entry-point=schedule_anime \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=cloud_function_env.yaml \
    --trigger-http

gcloud functions deploy $PROFILE_SCHEDULE_CLOUD_FUNCTION_NAME\
    --entry-point=schedule_profile \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=cloud_function_env.yaml \
    --trigger-http