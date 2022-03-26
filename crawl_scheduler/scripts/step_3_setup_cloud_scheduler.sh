PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud scheduler jobs create http $ANIME_SCHEDULE_CRON_JOB_NAME \
    --location $CRAWLER_REGION \
    --schedule="*/10 * * * *" \
    --uri=https://$CRAWLER_REGION-$PROJECT_ID.cloudfunctions.net/$ANIME_SCHEDULE_CLOUD_FUNCTION_NAME
    --http-method=POST \
    --headers="Content-Type: application/json" \
    --message-body="{'max_num_urls':1000}"

gcloud scheduler jobs create http $PROFILE_SCHEDULE_CRON_JOB_NAME \
    --location $CRAWLER_REGION \
    --schedule="*/10 * * * *" \
    --uri=https://$CRAWLER_REGION-$PROJECT_ID.cloudfunctions.net/$PROFILE_SCHEDULE_CLOUD_FUNCTION_NAME
    --http-method=POST \
    --headers="Content-Type: application/json" \
    --message-body="{'max_num_urls':1000}"