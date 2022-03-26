PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

cd $PROJECT_PATH/crawl_scheduler

gcloud functions deploy $ANIME_SCHEDULE_CLOUD_FUNCTION_NAME\
    --region $CRAWLER_REGION \
    --entry-point=schedule_anime \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=env_variables.yaml \
    --trigger-http

gcloud functions deploy $PROFILE_SCHEDULE_CLOUD_FUNCTION_NAME\
    --region $CRAWLER_REGION \
    --entry-point=schedule_profile \
    --runtime=python38 \
    --source=. \
    --timeout=300 \
    --env-vars-file=env_variables.yaml \
    --trigger-http