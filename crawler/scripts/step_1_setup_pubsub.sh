source .env

gcloud pubsub subscriptions create $SCHEDULE_ANIME_PUBSUB_SUBSCRIPTION \
    --topic=anime_crawl_queue \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub subscriptions create $SCHEDULE_PROFILE_PUBSUB_SUBSCRIPTION \
    --topic=profile_crawl_queue \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub topics create $DATA_INGESTION_PUBSUB_TOPIC \
    --message-retention-duration=30d