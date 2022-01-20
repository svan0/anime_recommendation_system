gcloud pubsub subscriptions create anime_crawl_subscription \
    --topic=anime_crawl_queue \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub subscriptions create profile_crawl_subscription \
    --topic=profile_crawl_queue \
    --ack-deadline=300 \
    --message-retention-duration=160h

gcloud pubsub topics create data_ingestion_queue \
    --message-retention-duration=30d