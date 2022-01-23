source .env

gcloud sql instances create $SCHEDULER_DB_INSTANCE_NAME \
    --database-version=POSTGRES_13 \
    --cpu=1 \
    --memory=3840MB \
    --region=us-central1

gcloud sql users set-password $SCHEDULER_DB_USER \
    --instance=$SCHEDULER_DB_INSTANCE_NAME \
    --password=$SCHEDULER_DB_PASSWORD

gcloud sql databases create $SCHEDULER_DB \
    --instance=$SCHEDULER_DB_INSTANCE_NAME
