PROJECT_PATH=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
PROJECT_PATH=$(dirname "$PROJECT_PATH")
PROJECT_PATH=$(dirname "$PROJECT_PATH")

source $PROJECT_PATH/.env

gcloud pubsub topics create $SCHEDULE_ANIME_PUBSUB_TOPIC --message-retention-duration=30d
gcloud pubsub topics create $SCHEDULE_PROFILE_PUBSUB_TOPIC --message-retention-duration=30d