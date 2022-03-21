PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud pubsub subscriptions create $SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION \
    --topic=$SCHEDULE_ANIME_PUBSUB_TOPIC \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub subscriptions create $SCHEDULE_PROFILE_PUBSUB_SUBSCRIPTION \
    --topic=$SCHEDULE_PROFILE_PUBSUB_TOPIC \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub topics create $DATA_INGESTION_PUBSUB_TOPIC \
    --message-retention-duration=30d