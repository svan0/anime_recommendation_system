source .env

gcloud pubsub topics create $SCHEDULE_ANIME_PUBSUB_TOPIC --message-retention-duration=30d
gcloud pubsub topics create $SCHEDULE_PROFILE_PUBSUB_TOPIC --message-retention-duration=30d